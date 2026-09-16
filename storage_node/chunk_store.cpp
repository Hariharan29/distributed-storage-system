// =============================================================================
// chunk_store.cpp — Filesystem implementation for chunk storage
//
// Files are stored as:
//   /data/<chunk_id>
//
// Since chunk_id is the SHA-256 hash of the content, two identical chunks
// will always produce the same filename, giving us free deduplication.
// =============================================================================

#include "chunk_store.h"

#include <fstream>     // std::ofstream, std::ifstream
#include <filesystem>  // std::filesystem (C++17)
#include <iostream>
#include <stdexcept>

namespace fs = std::filesystem;

ChunkStore::ChunkStore(const std::string& data_dir) : data_dir_(data_dir) {
    // Create the data directory if it doesn't already exist.
    // Equivalent to Python's os.makedirs(data_dir, exist_ok=True)
    fs::create_directories(data_dir_);
    std::cout << "[ChunkStore] Data directory: " << data_dir_ << "\n";
}

std::string ChunkStore::getChunkPath(const std::string& chunk_id) {
    // Build the full path: /data/<chunk_id>
    // std::filesystem::path handles OS path separators for us.
    return (fs::path(data_dir_) / chunk_id).string();
}

bool ChunkStore::hasChunk(const std::string& chunk_id) {
    return fs::exists(getChunkPath(chunk_id));
}

bool ChunkStore::writeChunk(const std::string& chunk_id,
                              const std::vector<uint8_t>& data) {
    std::string path = getChunkPath(chunk_id);

    // Open in binary mode — critical to avoid newline translation on Windows.
    // std::ios::trunc means "overwrite if the file already exists".
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        std::cerr << "[ChunkStore] Cannot open for writing: " << path << "\n";
        return false;
    }

    // Write all bytes at once.
    // reinterpret_cast<const char*> is needed because ofstream works with
    // char*, but our data is uint8_t* (both are single-byte types).
    file.write(reinterpret_cast<const char*>(data.data()),
               static_cast<std::streamsize>(data.size()));

    if (!file) {
        std::cerr << "[ChunkStore] Write failed for chunk: " << chunk_id << "\n";
        return false;
    }

    return true;
}

std::vector<uint8_t> ChunkStore::readChunk(const std::string& chunk_id) {
    std::string path = getChunkPath(chunk_id);

    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "[ChunkStore] Chunk not found: " << chunk_id << "\n";
        return {};  // Return an empty vector to signal failure.
    }

    // Read the entire file into a vector.
    // istreambuf_iterator reads one byte at a time, filling the vector.
    // This is the idiomatic C++ way to read a whole binary file.
    std::vector<uint8_t> data(
        (std::istreambuf_iterator<char>(file)),
        std::istreambuf_iterator<char>()
    );

    return data;
}

bool ChunkStore::deleteChunk(const std::string& chunk_id) {
    std::string path = getChunkPath(chunk_id);

    if (!fs::exists(path)) {
        return false;  // Nothing to delete.
    }

    std::error_code ec;
    fs::remove(path, ec);  // ec captures errors without throwing exceptions.

    if (ec) {
        std::cerr << "[ChunkStore] Delete failed for chunk " << chunk_id
                  << ": " << ec.message() << "\n";
        return false;
    }

    return true;
}
