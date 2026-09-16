// =============================================================================
// storage_node_server.cpp
// =============================================================================

#include "storage_node_server.h"
#include "../common/config.h"

#include <grpcpp/grpcpp.h>
#include <chrono>
#include <iostream>

StorageNodeServiceImpl::StorageNodeServiceImpl(const std::string& data_dir)
    : store_(data_dir) {}

// ─────────────────────────────────────────────────────────────────────────────
// StoreChunk — write a chunk to local disk
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status StorageNodeServiceImpl::StoreChunk(
    grpc::ServerContext*,
    const storage::StoreChunkRequest* request,
    storage::StoreChunkResponse*      response) {

    const std::string& chunk_id = request->chunk_id();
    const std::string& raw      = request->data();  // protobuf `bytes` → std::string

    // Convert the protobuf bytes field to a vector<uint8_t> for ChunkStore.
    std::vector<uint8_t> data(raw.begin(), raw.end());

    // ── Integrity check ───────────────────────────────────────────────────────
    // Verify that the data we received matches the expected SHA-256.
    // This catches corruption in transit.
    std::string computed_hash = SHA256Util::hash(data);
    if (computed_hash != request->sha256()) {
        std::cerr << "[StorageNode] Hash mismatch for chunk " << chunk_id
                  << " (expected=" << request->sha256()
                  << " got=" << computed_hash << ")\n";
        response->set_success(false);
        response->set_message("Hash mismatch — data may be corrupted in transit.");
        return grpc::Status::OK;
    }

    // ── Deduplication check ───────────────────────────────────────────────────
    // Since chunk_id = SHA-256, if the file already exists the content is identical.
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (store_.hasChunk(chunk_id)) {
            std::cout << "[StorageNode] Chunk " << chunk_id << " already exists (dedup).\n";
            response->set_success(true);
            response->set_message("Chunk already exists.");
            return grpc::Status::OK;
        }

        bool ok = store_.writeChunk(chunk_id, data);
        if (!ok) {
            response->set_success(false);
            response->set_message("Failed to write chunk to disk.");
            return grpc::Status::OK;
        }
    }

    std::cout << "[StorageNode] Stored chunk " << chunk_id
              << " (" << data.size() << " bytes)\n";

    response->set_success(true);
    response->set_message("Chunk stored.");
    return grpc::Status::OK;
}

// ─────────────────────────────────────────────────────────────────────────────
// FetchChunk — read a chunk from local disk
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status StorageNodeServiceImpl::FetchChunk(
    grpc::ServerContext*,
    const storage::FetchChunkRequest* request,
    storage::FetchChunkResponse*      response) {

    const std::string& chunk_id = request->chunk_id();

    std::vector<uint8_t> data;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        data = store_.readChunk(chunk_id);
    }

    if (data.empty() && !store_.hasChunk(chunk_id)) {
        response->set_found(false);
        return grpc::Status::OK;
    }

    // Compute and return the hash so the client can verify integrity.
    std::string hash = SHA256Util::hash(data);

    response->set_found(true);
    response->set_data(std::string(data.begin(), data.end()));
    response->set_sha256(hash);

    return grpc::Status::OK;
}

// ─────────────────────────────────────────────────────────────────────────────
// DeleteChunk — remove a chunk from local disk
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status StorageNodeServiceImpl::DeleteChunk(
    grpc::ServerContext*,
    const storage::DeleteChunkRequest* request,
    storage::DeleteChunkResponse*      response) {

    std::lock_guard<std::mutex> lock(mutex_);
    bool ok = store_.deleteChunk(request->chunk_id());

    response->set_success(ok);
    response->set_message(ok ? "Chunk deleted." : "Chunk not found.");
    return grpc::Status::OK;
}

// ─────────────────────────────────────────────────────────────────────────────
// CopyChunkTo — re-replication: read local chunk and push to another node
//
// Flow:
//   1. Read chunk data from local disk.
//   2. Open a gRPC connection to the target storage node.
//   3. Call StoreChunk on the target node.
// ─────────────────────────────────────────────────────────────────────────────

grpc::Status StorageNodeServiceImpl::CopyChunkTo(
    grpc::ServerContext*,
    const storage::CopyChunkToRequest* request,
    storage::CopyChunkToResponse*      response) {

    const std::string& chunk_id = request->chunk_id();
    const std::string& target   = request->target_node_address();

    // Step 1: Read the chunk from local disk.
    std::vector<uint8_t> data;
    std::string          hash;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        data = store_.readChunk(chunk_id);
    }

    if (data.empty()) {
        response->set_success(false);
        response->set_message("Chunk not found locally: " + chunk_id);
        return grpc::Status::OK;
    }

    hash = SHA256Util::hash(data);

    // Step 2: Connect to the target storage node.
    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub    = storage::StorageNodeService::NewStub(channel);

    // Step 3: Call StoreChunk on the target node.
    storage::StoreChunkRequest store_req;
    store_req.set_chunk_id(chunk_id);
    store_req.set_data(std::string(data.begin(), data.end()));
    store_req.set_sha256(hash);

    storage::StoreChunkResponse store_resp;
    grpc::ClientContext         ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

    grpc::Status status = stub->StoreChunk(&ctx, store_req, &store_resp);

    if (!status.ok()) {
        response->set_success(false);
        response->set_message("gRPC call to target failed: " + status.error_message());
        return grpc::Status::OK;
    }

    if (!store_resp.success()) {
        response->set_success(false);
        response->set_message("Target node rejected chunk: " + store_resp.message());
        return grpc::Status::OK;
    }

    std::cout << "[StorageNode] Copied chunk " << chunk_id << " to " << target << "\n";

    response->set_success(true);
    response->set_message("Chunk copied successfully.");
    return grpc::Status::OK;
}
