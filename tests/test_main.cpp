#include "../common/sha256.h"
#include "../client/chunker.h"
#include "../metadata_server/metadata_db.h"
#include "../storage_node/chunk_store.h"
#include "../storage_node/storage_node_server.h"
#include "../common/config.h"

#include <sqlite3.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

class TestFailure : public std::runtime_error {
public:
    explicit TestFailure(const std::string& message) : std::runtime_error(message) {}
};

#define REQUIRE(condition) \
    do { \
        if (!(condition)) { \
            throw TestFailure(std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
                              ": requirement failed: " #condition); \
        } \
    } while (false)

#define REQUIRE_EQ(actual, expected) \
    do { \
        const auto& actual_value = (actual); \
        const auto& expected_value = (expected); \
        if (!(actual_value == expected_value)) { \
            std::ostringstream message; \
            message << __FILE__ << ":" << __LINE__ << ": expected " #actual " == " #expected; \
            throw TestFailure(message.str()); \
        } \
    } while (false)

class TempDirectory {
public:
    TempDirectory() {
        static std::atomic<unsigned> next_id{0};
        const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        path_ = fs::temp_directory_path() /
                 ("distributed_storage_test_" + std::to_string(stamp) + "_" +
                  std::to_string(next_id.fetch_add(1)));
        fs::create_directories(path_);
    }

    ~TempDirectory() {
        std::error_code error;
        fs::remove_all(path_, error);
    }

    const fs::path& path() const { return path_; }

private:
    fs::path path_;
};

void writeBytes(const fs::path& path, const std::vector<uint8_t>& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    REQUIRE(output.is_open());
    output.write(reinterpret_cast<const char*>(bytes.data()),
                 static_cast<std::streamsize>(bytes.size()));
    REQUIRE(static_cast<bool>(output));
}

std::vector<uint8_t> readBytes(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    REQUIRE(input.is_open());
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(input)),
                                std::istreambuf_iterator<char>());
}

bool tableExists(sqlite3* db, const std::string& table) {
    sqlite3_stmt* statement = nullptr;
    REQUIRE(sqlite3_prepare_v2(db,
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?;",
        -1, &statement, nullptr) == SQLITE_OK);
    sqlite3_bind_text(statement, 1, table.c_str(), -1, SQLITE_TRANSIENT);
    const bool exists = sqlite3_step(statement) == SQLITE_ROW;
    sqlite3_finalize(statement);
    return exists;
}

bool columnExists(sqlite3* db, const std::string& table, const std::string& column) {
    sqlite3_stmt* statement = nullptr;
    const std::string query = "PRAGMA table_info(" + table + ");";
    REQUIRE(sqlite3_prepare_v2(db, query.c_str(), -1, &statement, nullptr) == SQLITE_OK);
    bool exists = false;
    while (sqlite3_step(statement) == SQLITE_ROW) {
        const auto* value = reinterpret_cast<const char*>(sqlite3_column_text(statement, 1));
        if (value && column == value) {
            exists = true;
            break;
        }
    }
    sqlite3_finalize(statement);
    return exists;
}

void execSql(sqlite3* db, const char* sql) {
    char* error = nullptr;
    const int result = sqlite3_exec(db, sql, nullptr, nullptr, &error);
    if (result != SQLITE_OK) {
        const std::string message = error ? error : "SQLite statement failed";
        sqlite3_free(error);
        throw TestFailure(message);
    }
}

void testSha256KnownInput() {
    REQUIRE_EQ(SHA256Util::hash("hello world"),
               "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9");
}

void testSha256DeterministicAndDistinct() {
    REQUIRE_EQ(SHA256Util::hash("same input"), SHA256Util::hash("same input"));
    REQUIRE(SHA256Util::hash("first") != SHA256Util::hash("second"));
}

void testSha256BinaryAndFile() {
    TempDirectory temp;
    const std::vector<uint8_t> bytes{0, 1, 2, 127, 128, 254, 255};
    const fs::path file = temp.path() / "binary.dat";
    writeBytes(file, bytes);
    REQUIRE_EQ(SHA256Util::hash(bytes), SHA256Util::hashFile(file.string()));
}

void testChunkerSmallerThanChunkSize() {
    TempDirectory temp;
    const std::vector<uint8_t> bytes{1, 2, 3, 4};
    const fs::path file = temp.path() / "small.bin";
    writeBytes(file, bytes);

    const auto chunks = Chunker(config::CHUNK_SIZE_BYTES).split(file.string());
    REQUIRE_EQ(chunks.size(), size_t{1});
    REQUIRE_EQ(chunks[0].index, 0);
    REQUIRE_EQ(chunks[0].data, bytes);
    REQUIRE_EQ(chunks[0].chunk_id, SHA256Util::hash(bytes));
    REQUIRE_EQ(chunks[0].sha256, chunks[0].chunk_id);
}

void testChunkerExactChunkSize() {
    TempDirectory temp;
    const std::vector<uint8_t> bytes(config::CHUNK_SIZE_BYTES, 0x5a);
    const fs::path file = temp.path() / "exact.bin";
    writeBytes(file, bytes);

    const auto chunks = Chunker(config::CHUNK_SIZE_BYTES).split(file.string());
    REQUIRE_EQ(chunks.size(), size_t{1});
    REQUIRE_EQ(chunks[0].data.size(), size_t{config::CHUNK_SIZE_BYTES});
}

void testChunkerSlightlyLargerThanChunkSize() {
    TempDirectory temp;
    std::vector<uint8_t> bytes(config::CHUNK_SIZE_BYTES + 1, 0x2a);
    const fs::path file = temp.path() / "two.bin";
    writeBytes(file, bytes);

    const auto chunks = Chunker(config::CHUNK_SIZE_BYTES).split(file.string());
    REQUIRE_EQ(chunks.size(), size_t{2});
    REQUIRE_EQ(chunks[0].index, 0);
    REQUIRE_EQ(chunks[1].index, 1);
    REQUIRE_EQ(chunks[0].data.size(), size_t{config::CHUNK_SIZE_BYTES});
    REQUIRE_EQ(chunks[1].data.size(), size_t{1});
}

void testChunkerMultiChunkReassembly() {
    TempDirectory temp;
    std::vector<uint8_t> bytes(config::CHUNK_SIZE_BYTES * 2 + 17);
    for (size_t index = 0; index < bytes.size(); ++index) {
        bytes[index] = static_cast<uint8_t>(index % 251);
    }
    const fs::path file = temp.path() / "multi.bin";
    writeBytes(file, bytes);

    const auto chunks = Chunker(config::CHUNK_SIZE_BYTES).split(file.string());
    REQUIRE_EQ(chunks.size(), size_t{3});
    std::vector<uint8_t> rebuilt;
    for (size_t index = 0; index < chunks.size(); ++index) {
        REQUIRE_EQ(chunks[index].index, static_cast<int>(index));
        rebuilt.insert(rebuilt.end(), chunks[index].data.begin(), chunks[index].data.end());
        REQUIRE_EQ(chunks[index].chunk_id, SHA256Util::hash(chunks[index].data));
    }
    REQUIRE_EQ(rebuilt, bytes);
    REQUIRE_EQ(Chunker::chunkCount(static_cast<int64_t>(bytes.size()), config::CHUNK_SIZE_BYTES), 3);
}

void testChunkStoreLifecycle() {
    TempDirectory temp;
    ChunkStore store((temp.path() / "chunks").string());
    const std::vector<uint8_t> data{10, 20, 30};
    const std::string id = SHA256Util::hash(data);

    REQUIRE(store.writeChunk(id, data));
    REQUIRE(store.hasChunk(id));
    REQUIRE_EQ(store.readChunk(id), data);
    REQUIRE_EQ(store.getChunkPath(id), (temp.path() / "chunks" / id).string());
    REQUIRE(store.deleteChunk(id));
    REQUIRE(!store.hasChunk(id));
    REQUIRE(store.readChunk(id).empty());
    REQUIRE(!store.deleteChunk(id));
}

void testMetadataFreshSchema() {
    TempDirectory temp;
    MetadataDB db((temp.path() / "metadata.db").string());
    sqlite3* handle = db.dbHandle();

    REQUIRE(tableExists(handle, "files"));
    REQUIRE(tableExists(handle, "chunks"));
    REQUIRE(tableExists(handle, "file_chunks"));
    REQUIRE(tableExists(handle, "chunk_locations"));
    REQUIRE(tableExists(handle, "storage_nodes"));
    REQUIRE(tableExists(handle, "schema_metadata"));
    REQUIRE(columnExists(handle, "chunks", "access_count"));
    REQUIRE(columnExists(handle, "chunks", "last_accessed"));
}

void testMetadataFileAndChunkRoundTrip() {
    TempDirectory temp;
    MetadataDB db((temp.path() / "metadata.db").string());
    db.insertFile("file-1", "example.bin", 123, 1, config::DEFAULT_REPLICATION_FACTOR);
    REQUIRE(db.insertChunk("chunk-1", "sha-1", 123));
    db.addFileChunk("file-1", "chunk-1", 0);

    const FileRecord file = db.getFile("example.bin");
    REQUIRE(file.found);
    REQUIRE_EQ(file.file_id, "file-1");
    REQUIRE_EQ(file.total_size_bytes, int64_t{123});
    REQUIRE_EQ(file.chunk_count, 1);

    const ChunkRecord chunk = db.getChunk("chunk-1");
    REQUIRE(chunk.found);
    REQUIRE_EQ(chunk.sha256, "sha-1");
    REQUIRE_EQ(chunk.size_bytes, int64_t{123});
    const auto file_chunks = db.getChunksForFile("file-1");
    REQUIRE_EQ(file_chunks.size(), size_t{1});
    REQUIRE_EQ(file_chunks[0].chunk_index, 0);
}

void testMetadataDeduplicatesChunkRecordsAndReferences() {
    TempDirectory temp;
    MetadataDB db((temp.path() / "metadata.db").string());
    db.insertFile("file-1", "one.bin", 3, 1, 2);
    db.insertFile("file-2", "two.bin", 3, 1, 2);
    REQUIRE(db.insertChunk("chunk-1", "same-sha", 3));
    REQUIRE(!db.insertChunk("chunk-1", "same-sha", 3));
    REQUIRE_EQ(db.findChunkBySha256("same-sha"), "chunk-1");
    db.addFileChunk("file-1", "chunk-1", 0);
    db.addFileChunk("file-2", "chunk-1", 0);
    REQUIRE_EQ(db.getFileReferenceCount("chunk-1"), 2);
}

void testMetadataLocationsAreUniqueAndAliveFiltered() {
    TempDirectory temp;
    MetadataDB db((temp.path() / "metadata.db").string());
    db.insertChunk("chunk-1", "sha-1", 3);
    db.upsertNode("node-1", "node-1:50051");
    db.upsertNode("node-2", "node-2:50051");
    db.addChunkLocation("chunk-1", "node-1");
    db.addChunkLocation("chunk-1", "node-1");
    db.addChunkLocation("chunk-1", "node-2");
    REQUIRE_EQ(db.getNodesHoldingChunk("chunk-1").size(), size_t{2});

    db.updateNodeStatus("node-1", false);
    const auto alive_holders = db.getNodesHoldingChunk("chunk-1");
    REQUIRE_EQ(alive_holders.size(), size_t{1});
    REQUIRE_EQ(alive_holders[0], "node-2");
}

void testMetadataAccessCountAndHotWindow() {
    TempDirectory temp;
    MetadataDB db((temp.path() / "metadata.db").string());
    db.insertChunk("chunk-1", "sha-1", 3);
    for (int index = 0; index < config::HOT_CHUNK_THRESHOLD; ++index) {
        db.incrementAccessCount("chunk-1");
    }

    const ChunkRecord chunk = db.getChunk("chunk-1");
    REQUIRE_EQ(chunk.access_count, config::HOT_CHUNK_THRESHOLD);
    REQUIRE_EQ(db.getHotChunks(config::HOT_CHUNK_THRESHOLD).size(), size_t{1});

    sqlite3_stmt* statement = nullptr;
    REQUIRE(sqlite3_prepare_v2(db.dbHandle(),
        "SELECT last_accessed IS NOT NULL FROM chunks WHERE chunk_id = 'chunk-1';",
        -1, &statement, nullptr) == SQLITE_OK);
    REQUIRE(sqlite3_step(statement) == SQLITE_ROW);
    REQUIRE(sqlite3_column_int(statement, 0) == 1);
    sqlite3_finalize(statement);
}

void testMetadataNodeStatusAndHeartbeat() {
    TempDirectory temp;
    MetadataDB db((temp.path() / "metadata.db").string());
    db.upsertNode("node-1", "node-1:50051");
    REQUIRE(db.getNode("node-1").is_alive);
    db.updateNodeStatus("node-1", false);
    REQUIRE(!db.getNode("node-1").is_alive);
    db.updateNodeLastSeen("node-1");
    const NodeRecord node = db.getNode("node-1");
    REQUIRE(node.is_alive);
    REQUIRE(!node.last_seen.empty());
}

void testMetadataLegacySchemaMigration() {
    TempDirectory temp;
    const fs::path database_path = temp.path() / "legacy.db";
    sqlite3* legacy = nullptr;
    REQUIRE(sqlite3_open(database_path.string().c_str(), &legacy) == SQLITE_OK);
    execSql(legacy, R"SQL(
        CREATE TABLE files (
            file_id TEXT PRIMARY KEY, filename TEXT UNIQUE NOT NULL,
            total_size_bytes INTEGER NOT NULL, chunk_count INTEGER NOT NULL,
            replication_factor INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE chunks (
            chunk_id TEXT PRIMARY KEY, sha256 TEXT UNIQUE NOT NULL,
            size_bytes INTEGER NOT NULL
        );
        CREATE TABLE file_chunks (
            file_id TEXT NOT NULL, chunk_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
            PRIMARY KEY (file_id, chunk_index)
        );
        CREATE TABLE chunk_locations (
            chunk_id TEXT NOT NULL, node_id TEXT NOT NULL, PRIMARY KEY (chunk_id, node_id)
        );
        CREATE TABLE storage_nodes (
            node_id TEXT PRIMARY KEY, address TEXT NOT NULL, is_alive INTEGER DEFAULT 1,
            last_seen TEXT
        );
        INSERT INTO chunks(chunk_id, sha256, size_bytes) VALUES ('legacy-chunk', 'legacy-sha', 7);
    )SQL");
    sqlite3_close(legacy);

    MetadataDB db(database_path.string());
    REQUIRE(columnExists(db.dbHandle(), "chunks", "access_count"));
    REQUIRE(columnExists(db.dbHandle(), "chunks", "last_accessed"));
    REQUIRE(db.getChunk("legacy-chunk").found);
    db.incrementAccessCount("legacy-chunk");
    REQUIRE_EQ(db.getChunk("legacy-chunk").access_count, 1);
}

void testMetadataConcurrentChunkInserts() {
    TempDirectory temp;
    MetadataDB db((temp.path() / "metadata.db").string());
    constexpr int thread_count = 4;
    constexpr int inserts_per_thread = 25;
    std::vector<std::thread> threads;
    std::atomic<int> successful_inserts{0};
    for (int thread_index = 0; thread_index < thread_count; ++thread_index) {
        threads.emplace_back([&db, thread_index, &successful_inserts]() {
            for (int index = 0; index < inserts_per_thread; ++index) {
                const std::string id = "chunk-" + std::to_string(thread_index) + "-" + std::to_string(index);
                if (db.insertChunk(id, id, index + 1)) {
                    ++successful_inserts;
                }
            }
        });
    }
    for (auto& thread : threads) thread.join();

    sqlite3_stmt* statement = nullptr;
    REQUIRE_EQ(successful_inserts.load(), thread_count * inserts_per_thread);
    REQUIRE(sqlite3_prepare_v2(db.dbHandle(), "SELECT COUNT(*) FROM chunks;", -1, &statement, nullptr) == SQLITE_OK);
    REQUIRE(sqlite3_step(statement) == SQLITE_ROW);
    REQUIRE_EQ(sqlite3_column_int(statement, 0), thread_count * inserts_per_thread);
    sqlite3_finalize(statement);
}

void testStorageNodeRejectsInvalidHash() {
    TempDirectory temp;
    StorageNodeServiceImpl service((temp.path() / "node").string());
    storage::StoreChunkRequest request;
    request.set_chunk_id("chunk-1");
    request.set_data("payload");
    request.set_sha256("not-the-right-hash");
    storage::StoreChunkResponse response;

    REQUIRE(service.StoreChunk(nullptr, &request, &response).ok());
    REQUIRE(!response.success());
}

void testStorageNodeStoreFetchDelete() {
    TempDirectory temp;
    StorageNodeServiceImpl service((temp.path() / "node").string());
    const std::string data = "storage payload";
    const std::string hash = SHA256Util::hash(data);

    storage::StoreChunkRequest store_request;
    store_request.set_chunk_id(hash);
    store_request.set_data(data);
    store_request.set_sha256(hash);
    storage::StoreChunkResponse store_response;
    REQUIRE(service.StoreChunk(nullptr, &store_request, &store_response).ok());
    REQUIRE(store_response.success());

    storage::StoreChunkResponse duplicate_response;
    REQUIRE(service.StoreChunk(nullptr, &store_request, &duplicate_response).ok());
    REQUIRE(duplicate_response.success());

    storage::FetchChunkRequest fetch_request;
    fetch_request.set_chunk_id(hash);
    storage::FetchChunkResponse fetch_response;
    REQUIRE(service.FetchChunk(nullptr, &fetch_request, &fetch_response).ok());
    REQUIRE(fetch_response.found());
    REQUIRE_EQ(fetch_response.data(), data);
    REQUIRE_EQ(fetch_response.sha256(), hash);

    storage::DeleteChunkRequest delete_request;
    delete_request.set_chunk_id(hash);
    storage::DeleteChunkResponse delete_response;
    REQUIRE(service.DeleteChunk(nullptr, &delete_request, &delete_response).ok());
    REQUIRE(delete_response.success());
}

void testStorageNodeReportsMissingChunk() {
    TempDirectory temp;
    StorageNodeServiceImpl service((temp.path() / "node").string());
    storage::FetchChunkRequest request;
    request.set_chunk_id("missing");
    storage::FetchChunkResponse response;

    REQUIRE(service.FetchChunk(nullptr, &request, &response).ok());
    REQUIRE(!response.found());
}

} // namespace

int main() {
    const std::vector<std::pair<std::string, std::function<void()>>> tests = {
        {"SHA256_KnownInput", testSha256KnownInput},
        {"SHA256_DeterministicAndDistinct", testSha256DeterministicAndDistinct},
        {"SHA256_BinaryAndFile", testSha256BinaryAndFile},
        {"Chunker_SmallerThanChunkSize", testChunkerSmallerThanChunkSize},
        {"Chunker_ExactChunkSize", testChunkerExactChunkSize},
        {"Chunker_SlightlyLargerThanChunkSize", testChunkerSlightlyLargerThanChunkSize},
        {"Chunker_MultiChunkReassembly", testChunkerMultiChunkReassembly},
        {"ChunkStore_Lifecycle", testChunkStoreLifecycle},
        {"MetadataDB_FreshSchema", testMetadataFreshSchema},
        {"MetadataDB_FileAndChunkRoundTrip", testMetadataFileAndChunkRoundTrip},
        {"Deduplication_ReusesExistingChunk", testMetadataDeduplicatesChunkRecordsAndReferences},
        {"MetadataDB_UniqueAliveLocations", testMetadataLocationsAreUniqueAndAliveFiltered},
        {"MetadataDB_AccessCountAndHotWindow", testMetadataAccessCountAndHotWindow},
        {"MetadataDB_NodeStatusAndHeartbeat", testMetadataNodeStatusAndHeartbeat},
        {"MetadataDB_LegacySchemaMigration", testMetadataLegacySchemaMigration},
        {"MetadataDB_ConcurrentChunkInserts", testMetadataConcurrentChunkInserts},
        {"StorageNode_RejectsInvalidHash", testStorageNodeRejectsInvalidHash},
        {"StorageNode_StoreFetchDelete", testStorageNodeStoreFetchDelete},
        {"StorageNode_ReportsMissingChunk", testStorageNodeReportsMissingChunk},
    };

    int failures = 0;
    for (const auto& [name, test] : tests) {
        try {
            test();
            std::cout << "[PASS] " << name << "\n";
        } catch (const std::exception& error) {
            ++failures;
            std::cerr << "[FAIL] " << name << ": " << error.what() << "\n";
        }
    }

    std::cout << "Executed " << tests.size() << " test cases, "
              << failures << " failure(s).\n";
    return failures == 0 ? 0 : 1;
}
