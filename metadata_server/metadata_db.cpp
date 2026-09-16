#include "metadata_db.h"
#include "../common/config.h"

#include <stdexcept>

namespace {
void check(int result, sqlite3* db, const char* operation) {
    if (result != SQLITE_OK && result != SQLITE_DONE) {
        throw std::runtime_error(std::string(operation) + ": " + sqlite3_errmsg(db));
    }
}

void bindText(sqlite3_stmt* stmt, int index, const std::string& value) {
    sqlite3_bind_text(stmt, index, value.c_str(), -1, SQLITE_TRANSIENT);
}

bool tableHasColumn(sqlite3* db, const std::string& table_name, const std::string& column_name) {
    sqlite3_stmt* stmt = nullptr;
    const std::string sql = "PRAGMA table_info(" + table_name + ");";
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }

    bool found = false;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const auto* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        if (name && column_name == name) {
            found = true;
            break;
        }
    }

    sqlite3_finalize(stmt);
    return found;
}

void ensureColumn(sqlite3* db, const std::string& table_name,
                  const std::string& column_name,
                  const std::string& column_definition) {
    if (tableHasColumn(db, table_name, column_name)) {
        return;
    }

    const std::string sql = "ALTER TABLE " + table_name + " ADD COLUMN " +
                           column_name + " " + column_definition + ";";
    char* err = nullptr;
    const int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err);
    if (rc != SQLITE_OK) {
        std::string message = err ? err : "migration failed";
        sqlite3_free(err);
        throw std::runtime_error("migration for " + table_name + ": " + message);
    }
}

void ensureVersionTable(sqlite3* db) {
    sqlite3_exec(db,
                 "CREATE TABLE IF NOT EXISTS schema_metadata (name TEXT PRIMARY KEY, value TEXT NOT NULL);",
                 nullptr, nullptr, nullptr);
}

int getSchemaVersion(sqlite3* db) {
    sqlite3_stmt* stmt = nullptr;
    int version = 0;

    if (sqlite3_prepare_v2(db,
                           "SELECT value FROM schema_metadata WHERE name = 'version';",
                           -1, &stmt, nullptr) != SQLITE_OK) {
        return 0;
    }

    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const auto* value = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        if (value) {
            version = std::stoi(value);
        }
    }
    sqlite3_finalize(stmt);
    return version;
}

void setSchemaVersion(sqlite3* db, int version) {
    sqlite3_stmt* stmt = nullptr;
    const std::string sql = "INSERT INTO schema_metadata(name, value) VALUES('version', ?) "
                           "ON CONFLICT(name) DO UPDATE SET value = excluded.value;";
    check(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr), db, "prepare schema version set");
    sqlite3_bind_text(stmt, 1, std::to_string(version).c_str(), -1, SQLITE_TRANSIENT);
    check(sqlite3_step(stmt), db, "set schema version");
    sqlite3_finalize(stmt);
}

void applySchemaMigrations(sqlite3* db) {
    ensureVersionTable(db);
    const int version = getSchemaVersion(db);

    if (version < 1) {
        ensureColumn(db, "chunks", "access_count", "INTEGER DEFAULT 0");
        ensureColumn(db, "chunks", "last_accessed", "TEXT DEFAULT CURRENT_TIMESTAMP");
        setSchemaVersion(db, 1);
    }
}

ChunkRecord readChunk(sqlite3_stmt* stmt) {
    ChunkRecord record;
    record.found = true;
    record.chunk_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
    record.chunk_index = sqlite3_column_int(stmt, 1);
    record.sha256 = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
    record.size_bytes = sqlite3_column_int64(stmt, 3);
    record.access_count = sqlite3_column_int(stmt, 4);
    return record;
}
}

MetadataDB::MetadataDB(const std::string& db_path) {
    check(sqlite3_open(db_path.c_str(), &db_), db_, "open database");
    sqlite3_busy_timeout(db_, 5000);
    exec("PRAGMA journal_mode=WAL;");
    exec("PRAGMA foreign_keys=ON;");
    exec("PRAGMA busy_timeout = 5000;");
    createTables();
    applySchemaMigrations(db_);
    reconcileMetadata();
}

MetadataDB::~MetadataDB() {
    if (db_) sqlite3_close(db_);
}

void MetadataDB::exec(const char* sql) {
    char* error = nullptr;
    const int result = sqlite3_exec(db_, sql, nullptr, nullptr, &error);
    if (result != SQLITE_OK) {
        std::string message = error ? error : "SQLite operation failed";
        sqlite3_free(error);
        throw std::runtime_error(message);
    }
}

void MetadataDB::createTables() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    exec(R"(CREATE TABLE IF NOT EXISTS files (
        file_id TEXT PRIMARY KEY, filename TEXT UNIQUE NOT NULL,
        total_size_bytes INTEGER NOT NULL, chunk_count INTEGER NOT NULL,
        replication_factor INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );)");
    exec(R"(CREATE TABLE IF NOT EXISTS chunks (
        chunk_id TEXT PRIMARY KEY, sha256 TEXT UNIQUE NOT NULL,
        size_bytes INTEGER NOT NULL, access_count INTEGER DEFAULT 0,
        last_accessed TEXT DEFAULT CURRENT_TIMESTAMP
    );)");
    exec(R"(CREATE TABLE IF NOT EXISTS file_chunks (
        file_id TEXT NOT NULL, chunk_id TEXT NOT NULL, chunk_index INTEGER NOT NULL,
        PRIMARY KEY (file_id, chunk_index),
        FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE,
        FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id) ON DELETE RESTRICT
    );)");
    exec("CREATE INDEX IF NOT EXISTS idx_file_chunks_chunk_id ON file_chunks(chunk_id);");
    exec(R"(CREATE TABLE IF NOT EXISTS chunk_locations (
        chunk_id TEXT NOT NULL, node_id TEXT NOT NULL, PRIMARY KEY (chunk_id, node_id),
        FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id) ON DELETE CASCADE
    );)");
    exec(R"(CREATE TABLE IF NOT EXISTS storage_nodes (
        node_id TEXT PRIMARY KEY, address TEXT NOT NULL, is_alive INTEGER DEFAULT 1,
        last_seen TEXT
    );)");
}

void MetadataDB::insertFile(const std::string& file_id, const std::string& filename,
                            int64_t size, int32_t count, int32_t replication_factor) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "INSERT INTO files VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP);", -1, &stmt, nullptr), db_, "prepare file insert");
    bindText(stmt, 1, file_id); bindText(stmt, 2, filename);
    sqlite3_bind_int64(stmt, 3, size); sqlite3_bind_int(stmt, 4, count);
    sqlite3_bind_int(stmt, 5, replication_factor);
    check(sqlite3_step(stmt), db_, "insert file");
    sqlite3_finalize(stmt);
}

FileRecord MetadataDB::getFile(const std::string& filename) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "SELECT file_id, filename, total_size_bytes, chunk_count, replication_factor, created_at FROM files WHERE filename = ?;", -1, &stmt, nullptr), db_, "prepare file lookup");
    bindText(stmt, 1, filename);
    FileRecord record;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        record.found = true;
        record.file_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        record.filename = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        record.total_size_bytes = sqlite3_column_int64(stmt, 2);
        record.chunk_count = sqlite3_column_int(stmt, 3);
        record.replication_factor = sqlite3_column_int(stmt, 4);
        record.created_at = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 5));
    }
    sqlite3_finalize(stmt);
    return record;
}

void MetadataDB::deleteFile(const std::string& file_id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "DELETE FROM files WHERE file_id = ?;", -1, &stmt, nullptr), db_, "prepare file deletion");
    bindText(stmt, 1, file_id);
    check(sqlite3_step(stmt), db_, "delete file");
    sqlite3_finalize(stmt);
}

std::vector<FileRecord> MetadataDB::listFiles() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "SELECT file_id, filename, total_size_bytes, chunk_count, replication_factor, created_at FROM files ORDER BY created_at DESC;", -1, &stmt, nullptr), db_, "prepare file list");
    std::vector<FileRecord> files;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        FileRecord record;
        record.found = true;
        record.file_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        record.filename = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        record.total_size_bytes = sqlite3_column_int64(stmt, 2);
        record.chunk_count = sqlite3_column_int(stmt, 3);
        record.replication_factor = sqlite3_column_int(stmt, 4);
        record.created_at = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 5));
        files.push_back(std::move(record));
    }
    sqlite3_finalize(stmt);
    return files;
}

bool MetadataDB::insertChunk(const std::string& chunk_id, const std::string& sha256, int64_t size) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "INSERT OR IGNORE INTO chunks (chunk_id, sha256, size_bytes) VALUES (?, ?, ?);", -1, &stmt, nullptr), db_, "prepare chunk insert");
    bindText(stmt, 1, chunk_id); bindText(stmt, 2, sha256); sqlite3_bind_int64(stmt, 3, size);
    check(sqlite3_step(stmt), db_, "insert chunk");
    const bool inserted = sqlite3_changes(db_) > 0;
    sqlite3_finalize(stmt);
    return inserted;
}

void MetadataDB::addFileChunk(const std::string& file_id, const std::string& chunk_id, int32_t index) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "INSERT INTO file_chunks (file_id, chunk_id, chunk_index) VALUES (?, ?, ?);", -1, &stmt, nullptr), db_, "prepare file chunk insert");
    bindText(stmt, 1, file_id); bindText(stmt, 2, chunk_id); sqlite3_bind_int(stmt, 3, index);
    check(sqlite3_step(stmt), db_, "insert file chunk");
    sqlite3_finalize(stmt);
}

ChunkRecord MetadataDB::getChunk(const std::string& chunk_id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "SELECT chunk_id, 0, sha256, size_bytes, access_count FROM chunks WHERE chunk_id = ?;", -1, &stmt, nullptr), db_, "prepare chunk lookup");
    bindText(stmt, 1, chunk_id);
    ChunkRecord record;
    if (sqlite3_step(stmt) == SQLITE_ROW) record = readChunk(stmt);
    sqlite3_finalize(stmt);
    return record;
}

std::vector<ChunkRecord> MetadataDB::getChunksForFile(const std::string& file_id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "SELECT c.chunk_id, fc.chunk_index, c.sha256, c.size_bytes, c.access_count FROM file_chunks fc JOIN chunks c ON c.chunk_id = fc.chunk_id WHERE fc.file_id = ? ORDER BY fc.chunk_index;", -1, &stmt, nullptr), db_, "prepare file chunks lookup");
    bindText(stmt, 1, file_id);
    std::vector<ChunkRecord> chunks;
    while (sqlite3_step(stmt) == SQLITE_ROW) chunks.push_back(readChunk(stmt));
    sqlite3_finalize(stmt);
    return chunks;
}

std::string MetadataDB::findChunkBySha256(const std::string& sha256) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "SELECT chunk_id FROM chunks WHERE sha256 = ?;", -1, &stmt, nullptr), db_, "prepare hash lookup");
    bindText(stmt, 1, sha256);
    std::string id;
    if (sqlite3_step(stmt) == SQLITE_ROW) id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
    sqlite3_finalize(stmt);
    return id;
}

int MetadataDB::getFileReferenceCount(const std::string& chunk_id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "SELECT COUNT(*) FROM file_chunks WHERE chunk_id = ?;", -1, &stmt, nullptr), db_, "prepare reference count");
    bindText(stmt, 1, chunk_id);
    const int count = sqlite3_step(stmt) == SQLITE_ROW ? sqlite3_column_int(stmt, 0) : 0;
    sqlite3_finalize(stmt);
    return count;
}

void MetadataDB::deleteChunkMetadata(const std::string& chunk_id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "DELETE FROM chunks WHERE chunk_id = ? AND NOT EXISTS (SELECT 1 FROM file_chunks WHERE chunk_id = ?);", -1, &stmt, nullptr), db_, "prepare chunk deletion");
    bindText(stmt, 1, chunk_id); bindText(stmt, 2, chunk_id);
    check(sqlite3_step(stmt), db_, "delete chunk metadata");
    sqlite3_finalize(stmt);
}

void MetadataDB::incrementAccessCount(const std::string& chunk_id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "UPDATE chunks SET access_count = access_count + 1, last_accessed = CURRENT_TIMESTAMP WHERE chunk_id = ?;", -1, &stmt, nullptr), db_, "prepare access increment");
    bindText(stmt, 1, chunk_id); check(sqlite3_step(stmt), db_, "increment access count"); sqlite3_finalize(stmt);
}

void MetadataDB::addChunkLocation(const std::string& chunk_id, const std::string& node_id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "INSERT OR IGNORE INTO chunk_locations VALUES (?, ?);", -1, &stmt, nullptr), db_, "prepare location insert");
    bindText(stmt, 1, chunk_id); bindText(stmt, 2, node_id); check(sqlite3_step(stmt), db_, "insert location"); sqlite3_finalize(stmt);
}

void MetadataDB::removeChunkLocation(const std::string& chunk_id, const std::string& node_id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "DELETE FROM chunk_locations WHERE chunk_id = ? AND node_id = ?;", -1, &stmt, nullptr), db_, "prepare location deletion");
    bindText(stmt, 1, chunk_id); bindText(stmt, 2, node_id); check(sqlite3_step(stmt), db_, "delete location"); sqlite3_finalize(stmt);
}

std::vector<std::string> MetadataDB::getNodesHoldingChunk(const std::string& chunk_id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "SELECT cl.node_id FROM chunk_locations cl JOIN storage_nodes sn ON sn.node_id = cl.node_id WHERE cl.chunk_id = ? AND sn.is_alive = 1;", -1, &stmt, nullptr), db_, "prepare chunk locations lookup");
    bindText(stmt, 1, chunk_id); std::vector<std::string> nodes;
    while (sqlite3_step(stmt) == SQLITE_ROW) nodes.emplace_back(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)));
    sqlite3_finalize(stmt); return nodes;
}

std::vector<std::string> MetadataDB::getChunksOnNode(const std::string& node_id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "SELECT chunk_id FROM chunk_locations WHERE node_id = ?;", -1, &stmt, nullptr), db_, "prepare node chunks lookup");
    bindText(stmt, 1, node_id); std::vector<std::string> chunks;
    while (sqlite3_step(stmt) == SQLITE_ROW) chunks.emplace_back(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)));
    sqlite3_finalize(stmt); return chunks;
}

void MetadataDB::upsertNode(const std::string& id, const std::string& address) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "INSERT INTO storage_nodes (node_id, address, is_alive, last_seen) VALUES (?, ?, 1, CURRENT_TIMESTAMP) ON CONFLICT(node_id) DO UPDATE SET address = excluded.address, is_alive = 1, last_seen = CURRENT_TIMESTAMP;", -1, &stmt, nullptr), db_, "prepare node upsert");
    bindText(stmt, 1, id); bindText(stmt, 2, address); check(sqlite3_step(stmt), db_, "upsert node"); sqlite3_finalize(stmt);
}

void MetadataDB::updateNodeStatus(const std::string& id, bool alive) {
    std::lock_guard<std::recursive_mutex> lock(mutex_); sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "UPDATE storage_nodes SET is_alive = ? WHERE node_id = ?;", -1, &stmt, nullptr), db_, "prepare node status update");
    sqlite3_bind_int(stmt, 1, alive); bindText(stmt, 2, id); check(sqlite3_step(stmt), db_, "update node status"); sqlite3_finalize(stmt);
}

void MetadataDB::beginTransaction() {
    if (tx_guard_) {
        return;
    }
    tx_guard_ = std::make_unique<std::lock_guard<std::recursive_mutex>>(mutex_);
    char* error = nullptr;
    const int result = sqlite3_exec(db_, "BEGIN IMMEDIATE;", nullptr, nullptr, &error);
    if (result != SQLITE_OK) {
        std::string message = error ? error : "BEGIN IMMEDIATE failed";
        sqlite3_free(error);
        tx_guard_.reset();
        throw std::runtime_error(message);
    }
}

void MetadataDB::commitTransaction() {
    if (!tx_guard_) {
        return;
    }
    char* error = nullptr;
    const int result = sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, &error);
    if (result != SQLITE_OK) {
        std::string message = error ? error : "COMMIT failed";
        sqlite3_free(error);
        throw std::runtime_error(message);
    }
    tx_guard_.reset();
}

void MetadataDB::rollbackTransaction() {
    if (!tx_guard_) {
        return;
    }
    char* error = nullptr;
    sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, &error);
    if (error) sqlite3_free(error);
    tx_guard_.reset();
}

void MetadataDB::reconcileMetadata() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    exec("DELETE FROM chunk_locations WHERE node_id NOT IN (SELECT node_id FROM storage_nodes);");
    exec("DELETE FROM file_chunks WHERE file_id NOT IN (SELECT file_id FROM files);");
    exec("DELETE FROM chunk_locations WHERE chunk_id NOT IN (SELECT DISTINCT chunk_id FROM file_chunks);");
}

void MetadataDB::updateNodeLastSeen(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_); sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "UPDATE storage_nodes SET is_alive = 1, last_seen = CURRENT_TIMESTAMP WHERE node_id = ?;", -1, &stmt, nullptr), db_, "prepare heartbeat update");
    bindText(stmt, 1, id); check(sqlite3_step(stmt), db_, "update heartbeat"); sqlite3_finalize(stmt);
}

NodeRecord MetadataDB::getNode(const std::string& id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_); sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "SELECT node_id, address, is_alive, last_seen FROM storage_nodes WHERE node_id = ?;", -1, &stmt, nullptr), db_, "prepare node lookup");
    bindText(stmt, 1, id); NodeRecord node;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        node.node_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        node.address = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        node.is_alive = sqlite3_column_int(stmt, 2) != 0;
        const auto* seen = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3)); node.last_seen = seen ? seen : "";
    }
    sqlite3_finalize(stmt); return node;
}

std::vector<NodeRecord> MetadataDB::getAliveNodes() {
    std::lock_guard<std::recursive_mutex> lock(mutex_); sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "SELECT node_id, address, is_alive, last_seen FROM storage_nodes WHERE is_alive = 1;", -1, &stmt, nullptr), db_, "prepare alive nodes lookup");
    std::vector<NodeRecord> nodes;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        NodeRecord node; node.node_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        node.address = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1)); node.is_alive = true;
        const auto* seen = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3)); node.last_seen = seen ? seen : "";
        nodes.push_back(std::move(node));
    }
    sqlite3_finalize(stmt); return nodes;
}

std::vector<NodeRecord> MetadataDB::getTimedOutNodes(int timeout) {
    std::lock_guard<std::recursive_mutex> lock(mutex_); sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_, "SELECT node_id, address, is_alive, last_seen FROM storage_nodes WHERE is_alive = 1 AND last_seen < datetime('now', ? || ' seconds');", -1, &stmt, nullptr), db_, "prepare timed out nodes lookup");
    const std::string offset = "-" + std::to_string(timeout); bindText(stmt, 1, offset);
    std::vector<NodeRecord> nodes;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        NodeRecord node; node.node_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        node.address = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        const auto* seen = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3)); node.last_seen = seen ? seen : "";
        nodes.push_back(std::move(node));
    }
    sqlite3_finalize(stmt); return nodes;
}

std::vector<ChunkRecord> MetadataDB::getHotChunks(int threshold) {
    std::lock_guard<std::recursive_mutex> lock(mutex_); sqlite3_stmt* stmt = nullptr;
    check(sqlite3_prepare_v2(db_,
        "SELECT chunk_id, 0, sha256, size_bytes, access_count FROM chunks WHERE access_count >= ? AND last_accessed >= datetime('now', '-' || ? || ' seconds');",
        -1, &stmt, nullptr), db_, "prepare hot chunks lookup");
    sqlite3_bind_int(stmt, 1, threshold);
    sqlite3_bind_int(stmt, 2, config::HOT_CHUNK_ACCESS_WINDOW_SECONDS);
    std::vector<ChunkRecord> chunks;
    while (sqlite3_step(stmt) == SQLITE_ROW) chunks.push_back(readChunk(stmt));
    sqlite3_finalize(stmt); return chunks;
}
