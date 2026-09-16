// =============================================================================
// metadata_server/main.cpp — Entry point for the metadata server
//
// This is like Python's if __name__ == "__main__": block.
// It wires everything together, starts the gRPC server, and blocks until
// CTRL+C or SIGTERM is received.
// =============================================================================

#include "metadata_db.h"
#include "heartbeat_monitor.h"
#include "replication_manager.h"
#include "metadata_server.h"
#include "../common/config.h"

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>
#include <csignal>    // signal(), SIGTERM
#include <cstdlib>    // std::getenv
#include <chrono>
#include <thread>

// Global flag set by signal handler to trigger graceful shutdown.
// volatile tells the compiler not to optimize away reads of this variable
// (it can change at any time due to the signal handler).
static volatile bool g_shutdown = false;

void signalHandler(int /*signum*/) {
    std::cout << "\n[MetadataServer] Shutdown signal received.\n";
    g_shutdown = true;
}

int main() {
    // ── Register signal handlers for graceful shutdown ────────────────────────
    std::signal(SIGINT,  signalHandler);
    std::signal(SIGTERM, signalHandler);

    // ── Read configuration from environment variables ─────────────────────────
    // Docker Compose sets these; fallback to defaults for local development.
    const char* db_path_env  = std::getenv("DB_PATH");
    const char* port_env     = std::getenv("METADATA_PORT");

    std::string db_path  = db_path_env  ? db_path_env  : config::DEFAULT_DB_PATH;
    std::string port_str = port_env     ? port_env     : std::to_string(config::METADATA_SERVER_PORT);

    std::string server_address = "0.0.0.0:" + port_str;

    std::cout << "[MetadataServer] Starting...\n";
    std::cout << "[MetadataServer] Database: " << db_path << "\n";
    std::cout << "[MetadataServer] Listening on: " << server_address << "\n";

    // ── Instantiate components ────────────────────────────────────────────────
    // Order matters: DB must be created first because the others depend on it.
    MetadataDB          db(db_path);
    ReplicationManager  repl_mgr(db);
    HeartbeatMonitor    monitor(db, repl_mgr);
    MetadataServiceImpl service(db, monitor, repl_mgr);

    // ── Start background monitoring thread ────────────────────────────────────
    monitor.start();

    // ── Build and start the gRPC server ──────────────────────────────────────
    grpc::ServerBuilder builder;

    // Listen on the given address without TLS (insecure = no encryption).
    // For a production system you would use grpc::SslServerCredentials().
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    // Register our service implementation.
    builder.RegisterService(&service);

    // Increase max message size to handle large metadata responses.
    builder.SetMaxReceiveMessageSize(config::GRPC_MAX_MESSAGE_SIZE);
    builder.SetMaxSendMessageSize(config::GRPC_MAX_MESSAGE_SIZE);

    // Build() creates the server; Start() begins accepting connections.
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();

    if (!server) {
        std::cerr << "[MetadataServer] Failed to start server on " << server_address << "\n";
        monitor.stop();
        return 1;
    }

    std::cout << "[MetadataServer] Ready.\n";

    // ── Wait for shutdown signal ──────────────────────────────────────────────
    // Spin in a loop checking the global flag.
    // A cleaner alternative would be server->Wait() but that blocks CTRL+C handling.
    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // ── Graceful shutdown ─────────────────────────────────────────────────────
    std::cout << "[MetadataServer] Shutting down...\n";
    monitor.stop();
    server->Shutdown();

    std::cout << "[MetadataServer] Goodbye.\n";
    return 0;
}
