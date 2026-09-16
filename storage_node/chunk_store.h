#pragma once

// =============================================================================
// chunk_store.h — Filesystem read/write for raw chunk data
//
// Python equivalent:
//   class ChunkStore:
//       def __init__(self, data_dir: str): ...
//       def write(self, chunk_id: str, data: bytes) -> bool: ...
//       def read(self, chunk_id: str) -> bytes: ...
//       def delete(self, chunk_id: str) -> bool: ...
//       def has(self, chunk_id: str) -> bool: ...
//
// Each chunk is stored as a plain file:
//   /data/<chunk_id>
//
// Since chunk_id = SHA-256 of the content, the filename itself is the hash.
// This makes it trivially easy to check if a chunk already exists (dedup).
// =============================================================================

#include <string>
#include <vector>
#include <cstdint>

class ChunkStore {
public:
    // data_dir: directory where chunk files will be stored, e.g. "/data"
    explicit ChunkStore(const std::string& data_dir);

    // Write raw bytes to disk as a file named chunk_id.
    // Returns false if the write fails.
    bool writeChunk(const std::string& chunk_id,
                    const std::vector<uint8_t>& data);

    // Read a chunk's raw bytes from disk.
    // Returns an empty vector if the chunk is not found or can't be read.
    std::vector<uint8_t> readChunk(const std::string& chunk_id);

    // Delete a chunk file from disk.
    // Returns true if deleted, false if not found or delete failed.
    bool deleteChunk(const std::string& chunk_id);

    // Check if a chunk file exists on this node (used for deduplication).
    bool hasChunk(const std::string& chunk_id);

    // Return the full filesystem path for a chunk (useful for debugging).
    std::string getChunkPath(const std::string& chunk_id);

private:
    std::string data_dir_;  // e.g. "/data"
};
