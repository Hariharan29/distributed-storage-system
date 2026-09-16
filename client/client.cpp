// =============================================================================
// client.cpp — PUT / GET / DELETE / LIST implementation
// =============================================================================

#include "client.h"
#include "chunker.h"
#include "../common/config.h"
#include "../common/sha256.h"
#include "storage.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <fstream>
#include <thread>
#include <mutex>
#include <filesystem>
#include <algorithm>  // std::sort
#include <stdexcept>
#include <iomanip>
#include <chrono>
#include <unordered_map>

namespace fs = std::filesystem;

StorageClient::StorageClient(const std::string& metadata_server_address) {
    // Create a channel (connection) to the metadata server.
    // This is equivalent to Python's: grpc.insecure_channel(address)
    auto channel = grpc::CreateChannel(metadata_server_address,
                                       grpc::InsecureChannelCredentials());

    // Create the stub (proxy object that has one method per RPC).
    stub_ = metadata::MetadataService::NewStub(channel);
}

// ─────────────────────────────────────────────────────────────────────────────
// PUT
// ─────────────────────────────────────────────────────────────────────────────

void StorageClient::put(const std::string& local_filepath) {
    if (!fs::exists(local_filepath)) {
        std::cerr << "[Client] File not found: " << local_filepath << "\n";
        return;
    }

    std::string filename     = fs::path(local_filepath).filename().string();
    int64_t     file_size    = static_cast<int64_t>(fs::file_size(local_filepath));

    std::cout << "[Client] Uploading '" << filename
              << "' (" << file_size << " bytes)...\n";

    // ── Step 1: Split file into chunks ────────────────────────────────────────
    Chunker chunker(config::CHUNK_SIZE_BYTES);
    std::vector<Chunk> chunks = chunker.split(local_filepath);

    // ── Step 2: InitiateUpload RPC ────────────────────────────────────────────
    metadata::InitiateUploadRequest init_req;
    init_req.set_filename(filename);
    init_req.set_total_size_bytes(file_size);

    for (const Chunk& c : chunks) {
        auto* info = init_req.add_chunks();
        info->set_chunk_index(c.index);
        info->set_sha256(c.sha256);
        info->set_size_bytes(static_cast<int64_t>(c.data.size()));
    }

    metadata::InitiateUploadResponse init_resp;
    grpc::ClientContext ctx1;
    ctx1.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    grpc::Status status = stub_->InitiateUpload(&ctx1, init_req, &init_resp);

    if (!status.ok()) {
        std::cerr << "[Client] InitiateUpload failed: " << status.error_message() << "\n";
        return;
    }

    std::cout << "[Client] Got upload plan. File ID: " << init_resp.file_id() << "\n";

    // ── Step 3: Upload chunks in parallel ─────────────────────────────────────
    // We create one thread per chunk. Each thread uploads its chunk to all
    // assigned storage nodes.
    //
    // Python equivalent:
    //   threads = [threading.Thread(target=upload_chunk, args=(c,)) for c in chunks]
    //   for t in threads: t.start()
    //   for t in threads: t.join()

    std::vector<std::thread> upload_threads;
    std::mutex               print_mutex;  // Prevents garbled console output.
    std::mutex               results_mutex;
    std::unordered_map<std::string, std::vector<std::string>> stored_nodes;

    for (const auto& assignment : init_resp.assignments()) {
        if (assignment.already_exists()) {
            std::lock_guard<std::mutex> lock(print_mutex);
            std::cout << "[Client] Chunk " << assignment.chunk_index()
                      << " deduplicated (skipping upload).\n";
            continue;
        }

        // Find the chunk data for this assignment.
        const Chunk* chunk_ptr = nullptr;
        for (const Chunk& c : chunks) {
            if (c.index == assignment.chunk_index()) {
                chunk_ptr = &c;
                break;
            }
        }
        if (!chunk_ptr) continue;

        // Capture by value so the thread owns its data.
        std::vector<std::string> node_addrs(
            assignment.node_addresses().begin(),
            assignment.node_addresses().end());

        const Chunk& chunk = *chunk_ptr;

        upload_threads.emplace_back([this, &chunk, node_addrs, &print_mutex,
                                     &results_mutex, &stored_nodes]() {
            std::vector<std::string> successful =
                uploadChunk(chunk.chunk_id, chunk.data, chunk.sha256, node_addrs);
            {
                std::lock_guard<std::mutex> lock(results_mutex);
                stored_nodes[chunk.chunk_id] = std::move(successful);
            }
            std::lock_guard<std::mutex> lock(print_mutex);
            std::cout << "[Client] Chunk " << chunk.index << " upload complete.\n";
        });
    }

    // Wait for all upload threads to finish before calling FinalizeUpload.
    for (std::thread& t : upload_threads) {
        if (t.joinable()) t.join();
    }

    // ── Step 4: FinalizeUpload RPC ────────────────────────────────────────────
    metadata::FinalizeUploadRequest fin_req;
    fin_req.set_file_id(init_resp.file_id());
    for (const auto& [chunk_id, addresses] : stored_nodes) {
        auto* result = fin_req.add_chunk_results();
        result->set_chunk_id(chunk_id);
        for (const std::string& address : addresses) {
            result->add_stored_node_addresses(address);
        }
    }

    metadata::FinalizeUploadResponse fin_resp;
    grpc::ClientContext ctx2;
    ctx2.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    status = stub_->FinalizeUpload(&ctx2, fin_req, &fin_resp);

    if (!status.ok() || !fin_resp.success()) {
        std::cerr << "[Client] FinalizeUpload failed.\n";
        return;
    }

    std::cout << "[Client] Upload complete: '" << filename << "'\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// GET
// ─────────────────────────────────────────────────────────────────────────────

void StorageClient::get(const std::string& filename,
                         const std::string& output_path) {
    std::cout << "[Client] Fetching '" << filename << "'...\n";

    // ── Step 1: GetFileInfo RPC ───────────────────────────────────────────────
    metadata::GetFileInfoRequest req;
    req.set_filename(filename);

    metadata::GetFileInfoResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    grpc::Status status = stub_->GetFileInfo(&ctx, req, &resp);

    if (!status.ok()) {
        std::cerr << "[Client] GetFileInfo failed: " << status.error_message() << "\n";
        return;
    }

    if (!resp.found()) {
        std::cerr << "[Client] File not found: " << filename << "\n";
        return;
    }

    std::cout << "[Client] File has " << resp.chunks_size() << " chunk(s).\n";

    // ── Step 2: Fetch each chunk in order ─────────────────────────────────────
    // Sort chunks by index to ensure correct order when writing the output file.
    // (The metadata server should return them in order, but let's be safe.)
    std::vector<metadata::ChunkLocation> chunk_list(
        resp.chunks().begin(), resp.chunks().end());
    std::sort(chunk_list.begin(), chunk_list.end(),
              [](const auto& a, const auto& b) {
                  return a.chunk_index() < b.chunk_index();
              });

    // Pre-allocate a vector to store chunk data in order.
    std::vector<std::vector<uint8_t>> chunk_data(chunk_list.size());

    for (size_t i = 0; i < chunk_list.size(); ++i) {
        const auto& loc = chunk_list[i];

        bool fetched = false;
        // Try each node in turn; fall back to the next if one fails.
        for (const std::string& addr : loc.node_addresses()) {
            auto channel = grpc::CreateChannel(addr,
                                               grpc::InsecureChannelCredentials());
            auto stub = storage::StorageNodeService::NewStub(channel);

            storage::FetchChunkRequest fetch_req;
            fetch_req.set_chunk_id(loc.chunk_id());

            storage::FetchChunkResponse fetch_resp;
            grpc::ClientContext fetch_ctx;
            fetch_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
            grpc::Status fetch_status = stub->FetchChunk(&fetch_ctx, fetch_req, &fetch_resp);

            if (!fetch_status.ok() || !fetch_resp.found()) {
                std::cerr << "[Client] Node " << addr << " failed for chunk "
                          << loc.chunk_index() << ". Trying next node...\n";
                continue;
            }

            // ── Integrity check ───────────────────────────────────────────────
            const std::string& raw = fetch_resp.data();
            std::vector<uint8_t> data(raw.begin(), raw.end());
            std::string actual_hash = SHA256Util::hash(data);

            if (actual_hash != loc.sha256()) {
                std::cerr << "[Client] Hash mismatch for chunk " << loc.chunk_index()
                          << " from node " << addr << ". Trying next node...\n";
                continue;
            }

            chunk_data[i] = std::move(data);
            fetched = true;
            std::cout << "[Client] Chunk " << loc.chunk_index()
                      << " fetched from " << addr << "\n";
            break;
        }

        if (!fetched) {
            std::cerr << "[Client] FATAL: Could not fetch chunk "
                      << loc.chunk_index() << " from any node!\n";
            return;
        }
    }

    // ── Step 3: Reassemble chunks into output file ────────────────────────────
    std::ofstream out_file(output_path, std::ios::binary | std::ios::trunc);
    if (!out_file.is_open()) {
        std::cerr << "[Client] Cannot write output file: " << output_path << "\n";
        return;
    }

    for (const auto& data : chunk_data) {
        out_file.write(reinterpret_cast<const char*>(data.data()),
                       static_cast<std::streamsize>(data.size()));
    }

    std::cout << "[Client] File saved to: " << output_path << "\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// DELETE
// ─────────────────────────────────────────────────────────────────────────────

void StorageClient::deleteFile(const std::string& filename) {
    metadata::DeleteFileRequest req;
    req.set_filename(filename);

    metadata::DeleteFileResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    grpc::Status status = stub_->DeleteFile(&ctx, req, &resp);

    if (!status.ok()) {
        std::cerr << "[Client] DeleteFile RPC failed: " << status.error_message() << "\n";
        return;
    }

    if (resp.success()) {
        std::cout << "[Client] Deleted: " << filename << "\n";
    } else {
        std::cerr << "[Client] " << resp.message() << "\n";
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LIST
// ─────────────────────────────────────────────────────────────────────────────

void StorageClient::listFiles() {
    metadata::ListFilesRequest  req;
    metadata::ListFilesResponse resp;
    grpc::ClientContext         ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));

    grpc::Status status = stub_->ListFiles(&ctx, req, &resp);

    if (!status.ok()) {
        std::cerr << "[Client] ListFiles RPC failed: " << status.error_message() << "\n";
        return;
    }

    if (resp.files_size() == 0) {
        std::cout << "[Client] No files stored.\n";
        return;
    }

    std::cout << "\n";
    std::cout << std::left
              << std::setw(40) << "Filename"
              << std::setw(15) << "Size (bytes)"
              << std::setw(10) << "Chunks"
              << "Created\n";
    std::cout << std::string(80, '-') << "\n";

    for (const auto& f : resp.files()) {
        std::cout << std::left
                  << std::setw(40) << f.filename()
                  << std::setw(15) << f.total_size_bytes()
                  << std::setw(10) << f.chunk_count()
                  << f.created_at() << "\n";
    }
    std::cout << "\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// uploadChunk — private helper, called from PUT threads
// ─────────────────────────────────────────────────────────────────────────────

std::vector<std::string> StorageClient::uploadChunk(
    const std::string& chunk_id, const std::vector<uint8_t>& data,
    const std::string& sha256, const std::vector<std::string>& node_addresses) {
    std::vector<std::string> successful_nodes;
    for (const std::string& addr : node_addresses) {
        auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        auto stub    = storage::StorageNodeService::NewStub(channel);

        storage::StoreChunkRequest req;
        req.set_chunk_id(chunk_id);
        req.set_data(std::string(data.begin(), data.end()));
        req.set_sha256(sha256);

        storage::StoreChunkResponse resp;
        grpc::ClientContext         ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

        grpc::Status status = stub->StoreChunk(&ctx, req, &resp);

        if (!status.ok() || !resp.success()) {
            std::cerr << "[Client] Failed to store chunk " << chunk_id
                      << " on node " << addr << ": "
                      << (status.ok() ? resp.message() : status.error_message())
                      << "\n";
        } else {
            successful_nodes.push_back(addr);
        }
    }
    return successful_nodes;
}
