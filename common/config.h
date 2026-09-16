#pragma once

// =============================================================================
// config.h — Project-wide constants
//
// Python equivalent: a constants.py file imported everywhere.
// In C++, we use `#pragma once` to ensure this header is only included once
// per translation unit (same idea as Python's module import cache).
//
// To change a setting, edit the value here. No other file needs to change.
// =============================================================================

namespace config {

    // ── Chunking ──────────────────────────────────────────────────────────────

    // Size of each file chunk in bytes. 1 MB = 1,048,576 bytes.
    // Files are split into pieces of this size before being stored.
    constexpr int CHUNK_SIZE_BYTES = 1 * 1024 * 1024;  // 1 MB

    // ── Replication ───────────────────────────────────────────────────────────

    // How many storage nodes should hold a copy of each chunk by default.
    // With 3 nodes and factor=2, every chunk lives on exactly 2 of the 3 nodes.
    constexpr int DEFAULT_REPLICATION_FACTOR = 2;

    // Replication factor for "hot" (frequently accessed) chunks.
    // When a chunk's access_count exceeds HOT_CHUNK_THRESHOLD, it gets promoted
    // to this replication factor so reads are spread across more nodes.
    constexpr int HOT_REPLICATION_FACTOR = 3;

    // Number of times a chunk must be accessed before it is considered "hot"
    // and promoted to HOT_REPLICATION_FACTOR replicas.
    constexpr int HOT_CHUNK_THRESHOLD = 10;

    // ── Heartbeat ─────────────────────────────────────────────────────────────

    // How often (in seconds) a storage node sends a heartbeat to the metadata server.
    constexpr int HEARTBEAT_INTERVAL_SECONDS = 5;

    // If a storage node has not sent a heartbeat within this many seconds,
    // the metadata server marks it as dead and triggers re-replication.
    constexpr int HEARTBEAT_TIMEOUT_SECONDS = 15;

    // How often (in seconds) the HeartbeatMonitor scans for dead nodes.
    constexpr int MONITOR_SCAN_INTERVAL_SECONDS = 10;

    // How often (in seconds) the HeartbeatMonitor runs the adaptive replication check.
    constexpr int ADAPTIVE_REPLICATION_INTERVAL_SECONDS = 60;

    // How long gRPC calls should wait before failing.
    constexpr int DEFAULT_GRPC_TIMEOUT_SECONDS = 10;
    constexpr int REPLICATION_GRPC_TIMEOUT_SECONDS = 5;
    constexpr int HEARTBEAT_GRPC_TIMEOUT_SECONDS = 3;

    // Width of the hot-chunk access window used for adaptive replication.
    constexpr int HOT_CHUNK_ACCESS_WINDOW_SECONDS = 60;

    // ── Network ───────────────────────────────────────────────────────────────

    // Default port the metadata server listens on.
    constexpr int METADATA_SERVER_PORT = 50050;

    // Default port each storage node listens on.
    // (Each node runs in its own container so they can all use the same port.)
    constexpr int STORAGE_NODE_PORT = 50051;

    // ── gRPC message size limits ──────────────────────────────────────────────

    // gRPC's default max message size is 4MB. Our chunks are 1MB, but with
    // protobuf overhead we set a safe limit of 8MB.
    constexpr int GRPC_MAX_MESSAGE_SIZE = 8 * 1024 * 1024;  // 8 MB

    // ── Paths ─────────────────────────────────────────────────────────────────

    // Default directory where storage nodes save chunk files.
    // Each node gets its own /data directory via Docker volume mount.
    inline const char* DEFAULT_DATA_DIR = "/data";

    // Default path for the metadata server's SQLite database file.
    inline const char* DEFAULT_DB_PATH = "/data/metadata.db";

} // namespace config
