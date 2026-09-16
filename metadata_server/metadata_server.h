#pragma once

// =============================================================================
// metadata_server.h — gRPC service implementation for the metadata server
//
// Python equivalent:
//   class MetadataServicer(metadata_pb2_grpc.MetadataServiceServicer):
//       def InitiateUpload(self, request, context): ...
//
// In C++, we inherit from the generated base class MetadataService::Service
// and override every RPC method we care about.
//
// This class is the "front door" of the metadata server. It receives all
// incoming gRPC calls and delegates real work to:
//   - MetadataDB       → for reading/writing the database
//   - HeartbeatMonitor → for recording heartbeats
//   - ReplicationManager → (indirectly via HeartbeatMonitor)
// =============================================================================

#include "metadata_db.h"
#include "heartbeat_monitor.h"
#include "replication_manager.h"

// Generated gRPC service base class (from metadata.proto)
#include "metadata.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <string>
#include <vector>
#include <mutex>

class MetadataServiceImpl final : public metadata::MetadataService::Service {
public:
    MetadataServiceImpl(MetadataDB& db,
                        HeartbeatMonitor& monitor,
                        ReplicationManager& repl_mgr);

    // ── Client RPCs ───────────────────────────────────────────────────────────

    // Step 1 of PUT: Assign storage nodes for each chunk.
    // Returns a map of chunk_id → [node addresses to write to].
    grpc::Status InitiateUpload(
        grpc::ServerContext*                       context,
        const metadata::InitiateUploadRequest*     request,
        metadata::InitiateUploadResponse*          response) override;

    // Step 2 of PUT: Mark the upload as complete.
    grpc::Status FinalizeUpload(
        grpc::ServerContext*                       context,
        const metadata::FinalizeUploadRequest*     request,
        metadata::FinalizeUploadResponse*          response) override;

    // GET: Return chunk locations for a file (client fetches directly from nodes).
    grpc::Status GetFileInfo(
        grpc::ServerContext*                       context,
        const metadata::GetFileInfoRequest*        request,
        metadata::GetFileInfoResponse*             response) override;

    // DELETE: Remove file metadata; tell storage nodes to delete their chunks.
    grpc::Status DeleteFile(
        grpc::ServerContext*                       context,
        const metadata::DeleteFileRequest*         request,
        metadata::DeleteFileResponse*              response) override;

    // LIST: Return all stored files.
    grpc::Status ListFiles(
        grpc::ServerContext*                       context,
        const metadata::ListFilesRequest*          request,
        metadata::ListFilesResponse*               response) override;

    // ── Storage Node RPCs ─────────────────────────────────────────────────────

    // Called by storage nodes on startup to announce themselves.
    grpc::Status RegisterNode(
        grpc::ServerContext*                       context,
        const metadata::RegisterNodeRequest*       request,
        metadata::RegisterNodeResponse*            response) override;

    // Called by storage nodes every HEARTBEAT_INTERVAL_SECONDS.
    grpc::Status Heartbeat(
        grpc::ServerContext*                       context,
        const metadata::HeartbeatRequest*          request,
        metadata::HeartbeatResponse*               response) override;

private:
    MetadataDB&          db_;
    HeartbeatMonitor&    monitor_;
    ReplicationManager&  repl_mgr_;

    // Round-robin counter for distributing chunks across nodes.
    // Protected by node_mutex_ because gRPC calls come from multiple threads.
    int        round_robin_counter_ = 0;
    std::mutex node_mutex_;

    // Pick `count` distinct alive storage nodes using round-robin selection.
    // Returns a vector of node addresses (e.g. "storage_node1:50051").
    std::vector<NodeRecord> pickNodes(int count);

    // Issue DeleteChunk gRPC calls to all nodes holding a chunk.
    void deleteChunkFromNodes(const std::string& chunk_id);
};
