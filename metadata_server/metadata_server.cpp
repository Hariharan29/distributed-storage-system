// =============================================================================
// metadata_server.cpp — gRPC RPC handler implementations
//
// Each method receives a `request` proto message, does its work (talking to
// the DB), and fills in the `response` proto message before returning.
//
// Return values:
//   grpc::Status::OK         → success
//   grpc::Status(code, msg)  → error (client sees this as an exception)
// =============================================================================

#include "metadata_server.h"
#include "../common/config.h"

// gRPC storage stub for issuing DeleteChunk calls to storage nodes
#include "storage.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <random>

// Simple UUID-like generator using random hex strings.
// In production you would use a proper UUID library.
static std::string generateId() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);

    std::ostringstream oss;
    for (int i = 0; i < 32; ++i) {
        oss << std::hex << dis(gen);
        if (i == 7 || i == 11 || i == 15 || i == 19) oss << '-';
    }
    return oss.str();
}

// ─────────────────────────────────────────────────────────────────────────────

MetadataServiceImpl::MetadataServiceImpl(MetadataDB& db,
                                          HeartbeatMonitor& monitor,
                                          ReplicationManager& repl_mgr)
    : db_(db), monitor_(monitor), repl_mgr_(repl_mgr) {}

// ─────────────────────────────────────────────────────────────────────────────
// InitiateUpload — Step 1 of PUT
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status MetadataServiceImpl::InitiateUpload(
    grpc::ServerContext*,
    const metadata::InitiateUploadRequest* request,
    metadata::InitiateUploadResponse*      response) {

    std::string filename = request->filename();

    // Reject if this filename already exists.
    FileRecord existing = db_.getFile(filename);
    if (existing.found) {
        return grpc::Status(grpc::StatusCode::ALREADY_EXISTS,
                            "File already exists: " + filename);
    }

    if (static_cast<int>(db_.getAliveNodes().size()) < config::DEFAULT_REPLICATION_FACTOR) {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                            "Required replication factor cannot be satisfied");
    }

    std::string file_id = generateId();

    // Insert the file record (no chunk data yet — it's added below).
    db_.insertFile(file_id,
                   filename,
                   request->total_size_bytes(),
                   request->chunks_size(),
                   config::DEFAULT_REPLICATION_FACTOR);

    response->set_file_id(file_id);

    // For each chunk the client described, decide where to store it.
    for (const metadata::ChunkInfo& chunk_info : request->chunks()) {
        auto* assignment = response->add_assignments();
        assignment->set_chunk_index(chunk_info.chunk_index());

        // ── Deduplication check ───────────────────────────────────────────────
        // chunk_id = SHA-256 of the chunk data (sent by the client)
        std::string chunk_id = chunk_info.sha256();  // SHA-256 IS the chunk_id
        assignment->set_chunk_id(chunk_id);

        std::string existing_chunk = db_.findChunkBySha256(chunk_info.sha256());
        if (!existing_chunk.empty()) {
            // A chunk with identical content already exists on some nodes.
            // Tell the client to skip uploading this chunk (dedup hit).
            assignment->set_already_exists(true);

            db_.addFileChunk(file_id, existing_chunk, chunk_info.chunk_index());

            std::cout << "[MetadataServer] Dedup hit for chunk " << chunk_id << "\n";
            continue;
        }

        assignment->set_already_exists(false);

        // ── Node selection ────────────────────────────────────────────────────
        // Pick DEFAULT_REPLICATION_FACTOR distinct nodes for this chunk.
        std::vector<NodeRecord> chosen = pickNodes(config::DEFAULT_REPLICATION_FACTOR);
        if (static_cast<int>(chosen.size()) != config::DEFAULT_REPLICATION_FACTOR) {
            return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                                "Required replication factor cannot be satisfied");
        }

        db_.insertChunk(chunk_id, chunk_info.sha256(), chunk_info.size_bytes());
        db_.addFileChunk(file_id, chunk_id, chunk_info.chunk_index());

        for (const NodeRecord& node : chosen) {
            assignment->add_node_addresses(node.address);
        }
    }

    std::cout << "[MetadataServer] Initiated upload for file '" << filename
              << "' (id=" << file_id << ")\n";

    return grpc::Status::OK;
}

// ─────────────────────────────────────────────────────────────────────────────
// FinalizeUpload — Step 2 of PUT (client confirms all chunks written)
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status MetadataServiceImpl::FinalizeUpload(
    grpc::ServerContext*,
    const metadata::FinalizeUploadRequest* request,
    metadata::FinalizeUploadResponse*      response) {

    const std::vector<ChunkRecord> chunks = db_.getChunksForFile(request->file_id());
    if (chunks.empty() && request->file_id().empty()) {
        response->set_success(false);
        response->set_message("Unknown upload.");
        return grpc::Status::OK;
    }

    std::unordered_map<std::string, std::vector<std::string>> stored_addresses;
    for (const auto& result : request->chunk_results()) {
        stored_addresses[result.chunk_id()] = {result.stored_node_addresses().begin(),
                                               result.stored_node_addresses().end()};
    }

    const std::vector<NodeRecord> alive_nodes = db_.getAliveNodes();
    std::unordered_map<std::string, std::string> node_by_address;
    for (const NodeRecord& node : alive_nodes) node_by_address[node.address] = node.node_id;

    for (const ChunkRecord& chunk : chunks) {
        for (const std::string& address : stored_addresses[chunk.chunk_id]) {
            const auto it = node_by_address.find(address);
            if (it != node_by_address.end()) db_.addChunkLocation(chunk.chunk_id, it->second);
        }
    }

    bool complete = true;
    for (const ChunkRecord& chunk : chunks) {
        if (static_cast<int>(db_.getNodesHoldingChunk(chunk.chunk_id).size()) <
            config::DEFAULT_REPLICATION_FACTOR) {
            complete = false;
            break;
        }
    }

    if (!complete) {
        for (const ChunkRecord& chunk : chunks) {
            if (db_.getFileReferenceCount(chunk.chunk_id) == 1) {
                deleteChunkFromNodes(chunk.chunk_id);
            }
        }
        db_.deleteFile(request->file_id());
        for (const ChunkRecord& chunk : chunks) db_.deleteChunkMetadata(chunk.chunk_id);
        response->set_success(false);
        response->set_message("Required replication factor was not achieved; upload was discarded.");
        return grpc::Status::OK;
    }

    std::cout << "[MetadataServer] Upload finalized for file_id: "
              << request->file_id() << "\n";

    response->set_success(true);
    response->set_message("Upload complete.");
    return grpc::Status::OK;
}

// ─────────────────────────────────────────────────────────────────────────────
// GetFileInfo — Step 1 of GET
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status MetadataServiceImpl::GetFileInfo(
    grpc::ServerContext*,
    const metadata::GetFileInfoRequest* request,
    metadata::GetFileInfoResponse*      response) {

    FileRecord file = db_.getFile(request->filename());
    if (!file.found) {
        response->set_found(false);
        return grpc::Status::OK;
    }

    response->set_found(true);
    response->set_file_id(file.file_id);
    response->set_filename(file.filename);
    response->set_total_size_bytes(file.total_size_bytes);

    // Get all chunks in order (chunk_index ASC).
    std::vector<ChunkRecord> chunks = db_.getChunksForFile(file.file_id);

    for (const ChunkRecord& chunk : chunks) {
        // Increment access count for adaptive replication tracking.
        db_.incrementAccessCount(chunk.chunk_id);

        // Get the addresses of all healthy nodes holding this chunk.
        std::vector<std::string> node_ids = db_.getNodesHoldingChunk(chunk.chunk_id);

        auto* loc = response->add_chunks();
        loc->set_chunk_id(chunk.chunk_id);
        loc->set_chunk_index(chunk.chunk_index);
        loc->set_sha256(chunk.sha256);

        for (const std::string& node_id : node_ids) {
            NodeRecord node = db_.getNode(node_id);
            if (node.is_alive) {
                loc->add_node_addresses(node.address);
            }
        }
    }

    return grpc::Status::OK;
}

// ─────────────────────────────────────────────────────────────────────────────
// DeleteFile
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status MetadataServiceImpl::DeleteFile(
    grpc::ServerContext*,
    const metadata::DeleteFileRequest* request,
    metadata::DeleteFileResponse*      response) {

    FileRecord file = db_.getFile(request->filename());
    if (!file.found) {
        response->set_success(false);
        response->set_message("File not found: " + request->filename());
        return grpc::Status::OK;
    }

    std::vector<ChunkRecord> chunks = db_.getChunksForFile(file.file_id);
    for (const ChunkRecord& chunk : chunks) {
        if (db_.getFileReferenceCount(chunk.chunk_id) == 1) {
            deleteChunkFromNodes(chunk.chunk_id);
        }
    }

    // Delete the file and all its chunks from the DB.
    // ON DELETE CASCADE handles chunk_locations automatically.
    db_.deleteFile(file.file_id);
    for (const ChunkRecord& chunk : chunks) db_.deleteChunkMetadata(chunk.chunk_id);

    std::cout << "[MetadataServer] Deleted file: " << request->filename() << "\n";

    response->set_success(true);
    response->set_message("File deleted.");
    return grpc::Status::OK;
}

// ─────────────────────────────────────────────────────────────────────────────
// ListFiles
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status MetadataServiceImpl::ListFiles(
    grpc::ServerContext*,
    const metadata::ListFilesRequest*,
    metadata::ListFilesResponse* response) {

    std::vector<FileRecord> files = db_.listFiles();
    for (const FileRecord& f : files) {
        auto* entry = response->add_files();
        entry->set_filename(f.filename);
        entry->set_total_size_bytes(f.total_size_bytes);
        entry->set_chunk_count(f.chunk_count);
        entry->set_created_at(f.created_at);
    }

    return grpc::Status::OK;
}

// ─────────────────────────────────────────────────────────────────────────────
// RegisterNode — Storage nodes call this on startup
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status MetadataServiceImpl::RegisterNode(
    grpc::ServerContext*,
    const metadata::RegisterNodeRequest* request,
    metadata::RegisterNodeResponse*      response) {

    db_.upsertNode(request->node_id(), request->address());
    std::cout << "[MetadataServer] Registered node: "
              << request->node_id() << " at " << request->address() << "\n";

    response->set_success(true);
    response->set_message("Node registered.");
    return grpc::Status::OK;
}

// ─────────────────────────────────────────────────────────────────────────────
// Heartbeat — Storage nodes call this every HEARTBEAT_INTERVAL_SECONDS
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status MetadataServiceImpl::Heartbeat(
    grpc::ServerContext*,
    const metadata::HeartbeatRequest* request,
    metadata::HeartbeatResponse*      response) {

    monitor_.recordHeartbeat(request->node_id());
    response->set_acknowledged(true);
    return grpc::Status::OK;
}

// ─────────────────────────────────────────────────────────────────────────────
// Private helpers
// ─────────────────────────────────────────────────────────────────────────────

std::vector<NodeRecord> MetadataServiceImpl::pickNodes(int count) {
    std::vector<NodeRecord> alive = db_.getAliveNodes();

    if (alive.empty()) return {};

    std::vector<NodeRecord> chosen;
    std::lock_guard<std::mutex> lock(node_mutex_);

    // Round-robin: pick `count` nodes starting from round_robin_counter_.
    // Using modulo to wrap around when we reach the end of the list.
    size_t n = alive.size();
    for (int i = 0; i < count && i < static_cast<int>(n); ++i) {
        chosen.push_back(alive[round_robin_counter_ % n]);
        round_robin_counter_ = (round_robin_counter_ + 1) % n;
    }

    return chosen;
}

void MetadataServiceImpl::deleteChunkFromNodes(const std::string& chunk_id) {
    std::vector<std::string> node_ids = db_.getNodesHoldingChunk(chunk_id);

    for (const std::string& node_id : node_ids) {
        NodeRecord node = db_.getNode(node_id);
        if (!node.is_alive) continue;

        auto channel = grpc::CreateChannel(node.address,
                                           grpc::InsecureChannelCredentials());
        auto stub = storage::StorageNodeService::NewStub(channel);

        storage::DeleteChunkRequest req;
        req.set_chunk_id(chunk_id);

        storage::DeleteChunkResponse resp;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

        grpc::Status status = stub->DeleteChunk(&ctx, req, &resp);
        if (!status.ok()) {
            std::cerr << "[MetadataServer] Failed to delete chunk " << chunk_id
                      << " from node " << node_id << ": "
                      << status.error_message() << "\n";
        }
    }
}
