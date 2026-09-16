#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <sqlite3.h>
#include <string>
#include <vector>

struct FileRecord {
    std::string file_id;
    std::string filename;
    int64_t total_size_bytes = 0;
    int32_t chunk_count = 0;
    int32_t replication_factor = 2;
    std::string created_at;
    bool found = false;
};

struct ChunkRecord {
    std::string chunk_id;
    int32_t chunk_index = 0;
    std::string sha256;
    int64_t size_bytes = 0;
    int32_t access_count = 0;
    bool found = false;
};

struct NodeRecord {
    std::string node_id;
    std::string address;
    bool is_alive = true;
    std::string last_seen;
};

class MetadataDB {
public:
    explicit MetadataDB(const std::string& db_path);
    ~MetadataDB();

    sqlite3* dbHandle() const { return db_; }

    void beginTransaction();
    void commitTransaction();
    void rollbackTransaction();
    void reconcileMetadata();

    void insertFile(const std::string& file_id, const std::string& filename,
                    int64_t total_size_bytes, int32_t chunk_count,
                    int32_t replication_factor);
    FileRecord getFile(const std::string& filename);
    void deleteFile(const std::string& file_id);
    std::vector<FileRecord> listFiles();

    bool insertChunk(const std::string& chunk_id, const std::string& sha256,
                     int64_t size_bytes);
    void addFileChunk(const std::string& file_id, const std::string& chunk_id,
                      int32_t chunk_index);
    ChunkRecord getChunk(const std::string& chunk_id);
    std::vector<ChunkRecord> getChunksForFile(const std::string& file_id);
    std::string findChunkBySha256(const std::string& sha256);
    int getFileReferenceCount(const std::string& chunk_id);
    void deleteChunkMetadata(const std::string& chunk_id);
    void incrementAccessCount(const std::string& chunk_id);

    void addChunkLocation(const std::string& chunk_id, const std::string& node_id);
    void removeChunkLocation(const std::string& chunk_id, const std::string& node_id);
    std::vector<std::string> getNodesHoldingChunk(const std::string& chunk_id);
    std::vector<std::string> getChunksOnNode(const std::string& node_id);

    void upsertNode(const std::string& node_id, const std::string& address);
    void updateNodeStatus(const std::string& node_id, bool is_alive);
    void updateNodeLastSeen(const std::string& node_id);
    NodeRecord getNode(const std::string& node_id);
    std::vector<NodeRecord> getAliveNodes();
    std::vector<NodeRecord> getTimedOutNodes(int timeout_seconds);
    std::vector<ChunkRecord> getHotChunks(int threshold);

private:
    sqlite3* db_ = nullptr;
    std::recursive_mutex mutex_;
    std::unique_ptr<std::lock_guard<std::recursive_mutex>> tx_guard_;

    void createTables();
    void exec(const char* sql);
};
