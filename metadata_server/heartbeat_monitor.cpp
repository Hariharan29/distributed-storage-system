// =============================================================================
// heartbeat_monitor.cpp
//
// The monitor loop runs two independent timers using a simple counter approach:
//
//   Every 1 second: wake up, check the stop flag.
//   Every MONITOR_SCAN_INTERVAL_SECONDS ticks: scan for dead nodes.
//   Every ADAPTIVE_REPLICATION_INTERVAL_SECONDS ticks: promote hot chunks.
//
// We sleep in 1-second increments rather than one long sleep so that stop()
// can wake the thread quickly. This is a common C++ pattern.
//
// Python equivalent:
//   while not stop_event.wait(timeout=1.0):
//       tick_count += 1
//       if tick_count % SCAN_INTERVAL == 0: scan_for_dead_nodes()
//       if tick_count % ADAPTIVE_INTERVAL == 0: promote_hot_chunks()
// =============================================================================

#include "heartbeat_monitor.h"
#include "../common/config.h"

#include <iostream>
#include <chrono>    // std::chrono::seconds
#include <thread>    // std::this_thread::sleep_for

HeartbeatMonitor::HeartbeatMonitor(MetadataDB& db, ReplicationManager& repl_mgr)
    : db_(db), repl_mgr_(repl_mgr) {}

HeartbeatMonitor::~HeartbeatMonitor() {
    // Ensure the thread is stopped when this object is destroyed.
    // This prevents "thread still running" crashes on shutdown.
    stop();
}

void HeartbeatMonitor::start() {
    if (running_.load()) return;  // Don't start twice.

    running_.store(true);

    // Launch the monitor loop in a new background thread.
    // `this` captures the pointer to the current object so monitorLoop()
    // can access db_, repl_mgr_, and running_.
    monitor_thread_ = std::thread([this]() { monitorLoop(); });

    std::cout << "[HeartbeatMonitor] Started.\n";
}

void HeartbeatMonitor::stop() {
    if (!running_.load()) return;

    running_.store(false);  // Signal the thread to exit its loop.

    // join() waits for the thread to finish, like Python's thread.join().
    if (monitor_thread_.joinable()) {
        monitor_thread_.join();
    }

    std::cout << "[HeartbeatMonitor] Stopped.\n";
}

// Called by the gRPC thread when a storage node sends a heartbeat.
// This is safe to call from any thread because updateNodeLastSeen()
// acquires its own mutex internally.
void HeartbeatMonitor::recordHeartbeat(const std::string& node_id) {
    db_.updateNodeLastSeen(node_id);
}

// ─────────────────────────────────────────────────────────────────────────────
// monitorLoop — runs inside the background thread
// ─────────────────────────────────────────────────────────────────────────────

void HeartbeatMonitor::monitorLoop() {
    int tick_count = 0;  // Counts 1-second ticks since start.

    while (running_.load()) {
        // Sleep for 1 second, then check the stop flag and counters.
        std::this_thread::sleep_for(std::chrono::seconds(1));
        ++tick_count;

        if (!running_.load()) break;  // Stop flag was set during sleep.

        // ── Dead-node scan ────────────────────────────────────────────────────
        if (tick_count % config::MONITOR_SCAN_INTERVAL_SECONDS == 0) {
            std::vector<NodeRecord> timed_out =
                db_.getTimedOutNodes(config::HEARTBEAT_TIMEOUT_SECONDS);

            for (const NodeRecord& node : timed_out) {
                std::cout << "[HeartbeatMonitor] Node " << node.node_id
                          << " timed out (last seen: " << node.last_seen
                          << "). Triggering re-replication.\n";

                // Delegate all re-replication logic to ReplicationManager.
                repl_mgr_.handleNodeFailure(node.node_id);
            }
        }

        // ── Adaptive replication check ────────────────────────────────────────
        if (tick_count % config::ADAPTIVE_REPLICATION_INTERVAL_SECONDS == 0) {
            repl_mgr_.promoteHotChunks();

            // Reset tick_count to avoid integer overflow on very long-running servers.
            tick_count = 0;
        }
    }
}
