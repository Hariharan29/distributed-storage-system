#pragma once

// =============================================================================
// chunker.h — Splits files into fixed-size chunks with SHA-256 hashes
//
// Python equivalent:
//   import hashlib
//
//   def split(filepath, chunk_size=1048576):
//       chunks = []
//       with open(filepath, 'rb') as f:
//           index = 0
//           while True:
//               data = f.read(chunk_size)
//               if not data: break
//               sha = hashlib.sha256(data).hexdigest()
//               chunks.append({'index': index, 'chunk_id': sha, 'sha256': sha, 'data': data})
//               index += 1
//       return chunks
//
// Key design choice: chunk_id = sha256(data)
//   This means two chunks with identical content always have the same ID.
//   The metadata server can detect duplicates without downloading anything.
// =============================================================================

#include <string>
#include <vector>
#include <cstdint>

// One chunk of a file. Plain data container (like a Python dataclass).
struct Chunk {
    int                  index;    // Position in the file: 0, 1, 2, ...
    std::string          chunk_id; // SHA-256 of `data` (used as chunk_id)
    std::string          sha256;   // Same as chunk_id (redundant but explicit)
    std::vector<uint8_t> data;     // Raw bytes of this chunk
};

class Chunker {
public:
    // chunk_size: how many bytes per chunk. Defaults to config::CHUNK_SIZE_BYTES.
    explicit Chunker(size_t chunk_size);

    // Read the file at `filepath` and return a list of chunks in order.
    // Throws std::runtime_error if the file cannot be opened.
    std::vector<Chunk> split(const std::string& filepath);

    // Convenience: get the total number of chunks for a file without splitting it.
    static int chunkCount(int64_t file_size_bytes, size_t chunk_size);

private:
    size_t chunk_size_;
};
