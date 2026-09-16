#pragma once

// =============================================================================
// client.h — High-level PUT / GET / DELETE / LIST operations
//
// Python equivalent:
//   class StorageClient:
//       def __init__(self, metadata_server_address: str): ...
//       def put(self, local_filepath: str): ...
//       def get(self, filename: str, output_path: str): ...
//       def delete_file(self, filename: str): ...
//       def list_files(self): ...
//
// The client:
//   1. Talks to the METADATA SERVER via gRPC for coordination.
//   2. Talks DIRECTLY to STORAGE NODES for chunk data (no proxy).
//
// Concurrency: during PUT, chunks are uploaded to storage nodes in parallel
// using std::thread. Each thread handles one chunk independently.
// =============================================================================

#include "metadata.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <string>
#include <memory>
#include <vector>
#include <cstdint>

class StorageClient {
public:
    // metadata_server_address: e.g. "localhost:50050" or "metadata_server:50050"
    explicit StorageClient(const std::string& metadata_server_address);

    // Upload a local file to the distributed storage system.
    // Prints progress to stdout.
    void put(const std::string& local_filepath);

    // Download a file from the distributed storage system.
    // Saves the reassembled file to output_path.
    void get(const std::string& filename, const std::string& output_path);

    // Delete a file from the distributed storage system.
    void deleteFile(const std::string& filename);

    // Print a list of all stored files.
    void listFiles();

private:
    // The gRPC stub is like a Python client object:
    //   stub = metadata_pb2_grpc.MetadataServiceStub(channel)
    std::unique_ptr<metadata::MetadataService::Stub> stub_;

    // Upload one chunk to all its assigned storage nodes.
    // Called from a thread during PUT.
    std::vector<std::string> uploadChunk(const std::string& chunk_id,
                                         const std::vector<uint8_t>& data,
                                         const std::string& sha256,
                                         const std::vector<std::string>& node_addresses);
};
