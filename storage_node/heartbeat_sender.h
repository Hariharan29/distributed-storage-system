#pragma once

// =============================================================================
// heartbeat_sender.h — Background thread that pings the metadata server
//
// Python equivalent:
//   class HeartbeatSender(threading.Thread):
//       def run(self):
//           while not stop_event.is_set():
//               stub.Heartbeat(HeartbeatRequest(node_id=self.node_id))
//               time.sleep(HEARTBEAT_INTERVAL_SECONDS)
//
// This runs inside every storage node, keeping its entry in the
// metadata server's storage_nodes table marked as alive.
// =============================================================================

#include <string>
#include <thread>
#include <atomic>

class HeartbeatSender {
public:
    HeartbeatSender(const std::string& node_id,
                    const std::string& metadata_server_address);

    ~HeartbeatSender();

    void start();
    void stop();

private:
    std::string       node_id_;
    std::string       metadata_address_;
    std::atomic<bool> running_{false};
    std::thread       sender_thread_;

    void senderLoop();
};
