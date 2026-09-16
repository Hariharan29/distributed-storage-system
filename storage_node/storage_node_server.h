#pragma once

// =============================================================================
// storage_node_server.h — gRPC service for a storage node
//
// Python equivalent:
//   class StorageNodeServicer(storage_pb2_grpc.StorageNodeServiceServicer):
//       def StoreChunk(self, request, context): ...
//       def FetchChunk(self, request, context): ...
//
// Handles four RPCs:
//   StoreChunk  → called by clients writing a chunk during PUT
//   FetchChunk  → called by clients reading a chunk during GET
//   DeleteChunk → called by metadata server during DELETE file
//   CopyChunkTo → called by metadata server during re-replication
//
// Thread safety:
//   gRPC delivers concurrent calls from a thread pool. ChunkStore's
//   read/write operations are protected by a std::mutex so we don't
//   corrupt data with simultaneous writes to the same chunk.
//   (In practice, two different chunk_ids can be written concurrently —
//   a finer-grained per-chunk lock would improve throughput, but a single
//   mutex is simpler and correct for an educational project.)
// =============================================================================

#include "chunk_store.h"
#include "storage.grpc.pb.h"
#include "../common/sha256.h"

#include <grpcpp/grpcpp.h>
#include <mutex>
#include <string>

class StorageNodeServiceImpl final : public storage::StorageNodeService::Service {
public:
    explicit StorageNodeServiceImpl(const std::string& data_dir);

    grpc::Status StoreChunk(
        grpc::ServerContext*                context,
        const storage::StoreChunkRequest*   request,
        storage::StoreChunkResponse*        response) override;

    grpc::Status FetchChunk(
        grpc::ServerContext*                context,
        const storage::FetchChunkRequest*   request,
        storage::FetchChunkResponse*        response) override;

    grpc::Status DeleteChunk(
        grpc::ServerContext*                context,
        const storage::DeleteChunkRequest*  request,
        storage::DeleteChunkResponse*       response) override;

    grpc::Status CopyChunkTo(
        grpc::ServerContext*                context,
        const storage::CopyChunkToRequest*  request,
        storage::CopyChunkToResponse*       response) override;

private:
    ChunkStore  store_;   // Handles all disk I/O
    std::mutex  mutex_;   // Protects store_ from concurrent access
};
