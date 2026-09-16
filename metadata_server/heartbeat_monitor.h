#pragma once

// =============================================================================
// heartbeat_monitor.h — Background thread that watches for dead storage nodes
//
// Python analogy:
//   import threading
//   class HeartbeatMonitor(threading.Thread):
//       def run(self): ...
//
// The monitor runs two tasks in a loop:
//   1. Every MONITOR_SCAN_INTERVAL_SECONDS:
//      → Scan for nodes that haven't sent a heartbeat recently.
//      → Mark them dead and trigger re-replication.
//
//   2. Every ADAPTIVE_REPLICATION_INTERVAL_SECONDS:
//      → Run the hot-chunk promotion check.
//
// The thread is stopped cleanly with stop(), which sets an atomic flag
// and joins the thread (waits for it to finish its current iteration).
// =============================================================================

#include "metadata_db.h"
#include "replication_manager.h"

#include <thread>
#include <atomic>
#include <string>

class HeartbeatMonitor {
public:
    // Takes references to the shared DB and replication manager.
    // `&` means "reference" — no copy is made, and we don't own these objects.
    HeartbeatMonitor(MetadataDB& db, ReplicationManager& repl_mgr);

    // Destructor: stop the thread if it's still running.
    ~HeartbeatMonitor();

    // Start the background monitoring thread.
    void start();

    // Stop the background thread gracefully (waits for it to finish).
    void stop();

    // Called by the gRPC Heartbeat handler when a storage node pings in.
    // Updates the node's last_seen timestamp in the database.
    // This method is called from the gRPC thread pool, so it must be thread-safe.
    void recordHeartbeat(const std::string& node_id);

private:
    MetadataDB&          db_;
    ReplicationManager&  repl_mgr_;

    // std::atomic<bool> is a thread-safe boolean flag.
    // Python equivalent: threading.Event() used as a stop signal.
    std::atomic<bool> running_{false};

    std::thread monitor_thread_;

    // The actual loop body — runs in the background thread.
    void monitorLoop();
};
