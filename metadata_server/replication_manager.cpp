// =============================================================================
// replication_manager.cpp
//
// This is where re-replication and adaptive replication logic lives.
//
// Re-replication flow (when a node dies):
//   1. Get all chunk_ids that were on the failed node.
//   2. For each chunk, count how many healthy replicas remain.
//   3. If count < replication_factor: find a source + target node, copy chunk.
//
// Adaptive replication flow (periodic):
//   1. Query hot chunks (access_count >= threshold).
//   2. For each: if replica count < HOT_REPLICATION_FACTOR, add more replicas.
// =============================================================================

#include "replication_manager.h"
#include "../common/config.h"

// gRPC and generated storage service stub
#include <grpcpp/grpcpp.h>
#include "storage.grpc.pb.h"

#include <iostream>
#include <algorithm>  // std::find
#include <chrono>

ReplicationManager::ReplicationManager(MetadataDB& db) : db_(db) {}

// ─────────────────────────────────────────────────────────────────────────────
// handleNodeFailure
// Called when the heartbeat monitor detects a dead node.
// ─────────────────────────────────────────────────────────────────────────────

void ReplicationManager::handleNodeFailure(const std::string& failed_node_id) {
    std::cout << "[ReplicationManager] Node failed: " << failed_node_id << "\n";

    // Step 1: Find every chunk that was stored on the failed node.
    std::vector<std::string> affected_chunks = db_.getChunksOnNode(failed_node_id);
    std::cout << "[ReplicationManager] " << affected_chunks.size()
              << " chunks affected.\n";

    // Step 2: Mark the node as dead in the DB.
    db_.updateNodeStatus(failed_node_id, false);

    // Step 3: For each affected chunk, check if we need to re-replicate.
    for (const std::string& chunk_id : affected_chunks) {
        // Remove the dead node from this chunk's location list.
        db_.removeChunkLocation(chunk_id, failed_node_id);

        // Count remaining healthy replicas for this chunk.
        std::vector<std::string> healthy_nodes = db_.getNodesHoldingChunk(chunk_id);
        int current_replicas = static_cast<int>(healthy_nodes.size());

        // We always aim for DEFAULT_REPLICATION_FACTOR replicas.
        // (Hot chunks will be promoted separately by promoteHotChunks().)
        if (current_replicas < config::DEFAULT_REPLICATION_FACTOR) {
            std::cout << "[ReplicationManager] Chunk " << chunk_id
                      << " under-replicated (" << current_replicas << "/"
                      << config::DEFAULT_REPLICATION_FACTOR << "). Re-replicating...\n";

            if (healthy_nodes.empty()) {
                std::cerr << "[ReplicationManager] WARNING: No healthy replica exists "
                          << "for chunk " << chunk_id << ". Data may be lost!\n";
                continue;
            }

            // Pick a target node that doesn't already have the chunk.
            std::string target = pickTargetNode(chunk_id);
            if (target.empty()) {
                std::cerr << "[ReplicationManager] No suitable target node for chunk "
                          << chunk_id << ".\n";
                continue;
            }

            // Get the address of a healthy source node.
            NodeRecord source_node = db_.getNode(healthy_nodes[0]);

            bool ok = replicateChunk(chunk_id, source_node.address, target);
            if (ok) {
                // Find the target node's ID from its address so we can update the DB.
                for (const NodeRecord& nr : db_.getAliveNodes()) {
                    if (nr.address == target) {
                        db_.addChunkLocation(chunk_id, nr.node_id);
                        std::cout << "[ReplicationManager] Chunk " << chunk_id
                                  << " successfully replicated to " << nr.node_id << "\n";
                        break;
                    }
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// promoteHotChunks
// Called periodically to add replicas for frequently-accessed chunks.
// ─────────────────────────────────────────────────────────────────────────────

void ReplicationManager::promoteHotChunks() {
    std::vector<ChunkRecord> hot = db_.getHotChunks(config::HOT_CHUNK_THRESHOLD);

    if (!hot.empty()) {
        std::cout << "[ReplicationManager] Adaptive replication check: "
                  << hot.size() << " hot chunk(s) found.\n";
    }

    for (const ChunkRecord& chunk : hot) {
        std::vector<std::string> current_nodes =
            db_.getNodesHoldingChunk(chunk.chunk_id);
        int current_replicas = static_cast<int>(current_nodes.size());

        if (current_replicas >= config::HOT_REPLICATION_FACTOR) {
            continue;  // Already at the target replication factor.
        }

        std::cout << "[ReplicationManager] Promoting hot chunk " << chunk.chunk_id
                  << " (access_count=" << chunk.access_count
                  << ") from " << current_replicas
                  << " to " << config::HOT_REPLICATION_FACTOR << " replicas.\n";

        // Add replicas until we reach HOT_REPLICATION_FACTOR.
        while (current_replicas < config::HOT_REPLICATION_FACTOR) {
            std::string target = pickTargetNode(chunk.chunk_id);
            if (target.empty()) break;  // No more nodes available.

            if (current_nodes.empty()) break;
            NodeRecord source_node = db_.getNode(current_nodes[0]);

            bool ok = replicateChunk(chunk.chunk_id, source_node.address, target);
            if (ok) {
                for (const NodeRecord& nr : db_.getAliveNodes()) {
                    if (nr.address == target) {
                        db_.addChunkLocation(chunk.chunk_id, nr.node_id);
                        current_nodes.push_back(nr.node_id);
                        ++current_replicas;
                        break;
                    }
                }
            } else {
                break;  // Stop if copy failed to avoid infinite loop.
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// replicateChunk — private helper
// Issues a CopyChunkTo gRPC call to the source node, asking it to push the
// chunk to the target node. Returns true on success.
// ─────────────────────────────────────────────────────────────────────────────

bool ReplicationManager::replicateChunk(const std::string& chunk_id,
                                         const std::string& source_node_address,
                                         const std::string& target_node_address) {
    // Create a gRPC channel to the source node.
    // Think of this like opening a connection in Python: grpc.insecure_channel(addr)
    auto channel = grpc::CreateChannel(source_node_address,
                                       grpc::InsecureChannelCredentials());
    auto stub = storage::StorageNodeService::NewStub(channel);

    storage::CopyChunkToRequest request;
    request.set_chunk_id(chunk_id);
    request.set_target_node_address(target_node_address);

    storage::CopyChunkToResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

    grpc::Status status = stub->CopyChunkTo(&context, request, &response);

    if (!status.ok()) {
        std::cerr << "[ReplicationManager] CopyChunkTo RPC failed for chunk "
                  << chunk_id << ": " << status.error_message() << "\n";
        return false;
    }

    if (!response.success()) {
        std::cerr << "[ReplicationManager] CopyChunkTo returned failure for chunk "
                  << chunk_id << ": " << response.message() << "\n";
        return false;
    }

    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// pickTargetNode — private helper
// Returns the address of a healthy node that doesn't already hold the chunk.
// Returns empty string if no such node is available.
// ─────────────────────────────────────────────────────────────────────────────

std::string ReplicationManager::pickTargetNode(const std::string& chunk_id) {
    // Nodes that already hold this chunk.
    std::vector<std::string> current_holders = db_.getNodesHoldingChunk(chunk_id);

    // All alive nodes.
    std::vector<NodeRecord> alive_nodes = db_.getAliveNodes();

    for (const NodeRecord& node : alive_nodes) {
        // Check if this node is NOT already holding the chunk.
        bool already_has = std::find(current_holders.begin(),
                                     current_holders.end(),
                                     node.node_id) != current_holders.end();
        if (!already_has) {
            return node.address;  // Found a suitable target.
        }
    }

    return "";  // No suitable target node found.
}
