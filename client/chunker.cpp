// =============================================================================
// chunker.cpp
// =============================================================================

#include "chunker.h"
#include "../common/sha256.h"

#include <fstream>
#include <stdexcept>
#include <iostream>
#include <cmath>  // std::ceil

Chunker::Chunker(size_t chunk_size) : chunk_size_(chunk_size) {}

int Chunker::chunkCount(int64_t file_size_bytes, size_t chunk_size) {
    if (file_size_bytes == 0) return 0;
    // Integer ceiling division: (size + chunk_size - 1) / chunk_size
    return static_cast<int>((file_size_bytes + chunk_size - 1) / chunk_size);
}

std::vector<Chunk> Chunker::split(const std::string& filepath) {
    // Open in binary mode to get raw bytes.
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Chunker: Cannot open file: " + filepath);
    }

    std::vector<Chunk> chunks;
    std::vector<uint8_t> buffer(chunk_size_);
    int index = 0;

    // Read chunk_size_ bytes at a time until the file is exhausted.
    // The last chunk may be smaller than chunk_size_.
    while (true) {
        // file.read() fills buffer and sets file.gcount() to bytes actually read.
        file.read(reinterpret_cast<char*>(buffer.data()),
                  static_cast<std::streamsize>(chunk_size_));

        std::streamsize bytes_read = file.gcount();
        if (bytes_read == 0) break;  // End of file.

        // Copy only the bytes that were actually read (last chunk may be short).
        std::vector<uint8_t> chunk_data(buffer.begin(),
                                         buffer.begin() + bytes_read);

        // SHA-256 of the chunk content = the chunk's unique ID.
        std::string hash = SHA256Util::hash(chunk_data);

        Chunk c;
        c.index    = index++;
        c.chunk_id = hash;   // chunk_id IS the hash
        c.sha256   = hash;
        c.data     = std::move(chunk_data);

        chunks.push_back(std::move(c));
    }

    std::cout << "[Chunker] Split '" << filepath << "' into "
              << chunks.size() << " chunk(s).\n";

    return chunks;
}
