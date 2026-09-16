// =============================================================================
// storage_node/main.cpp — Entry point for a storage node
//
// Configuration comes from environment variables set by Docker Compose:
//   NODE_ID       = "node1" / "node2" / "node3"
//   NODE_PORT     = "50051"
//   METADATA_ADDR = "metadata_server:50050"
//   DATA_DIR      = "/data"  (optional, defaults to /data)
// =============================================================================

#include "storage_node_server.h"
#include "heartbeat_sender.h"
#include "metadata.grpc.pb.h"
#include "../common/config.h"

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>
#include <csignal>
#include <cstdlib>
#include <chrono>
#include <thread>

static volatile bool g_shutdown = false;

void signalHandler(int) {
    std::cout << "\n[StorageNode] Shutdown signal received.\n";
    g_shutdown = true;
}

// Register this node with the metadata server.
// Retries a few times in case the metadata server hasn't started yet.
static bool registerWithMetadataServer(const std::string& metadata_addr,
                                        const std::string& node_id,
                                        const std::string& node_address) {
    auto channel = grpc::CreateChannel(metadata_addr,
                                       grpc::InsecureChannelCredentials());
    auto stub = metadata::MetadataService::NewStub(channel);

    constexpr int MAX_RETRIES = 10;
    for (int attempt = 0; attempt < MAX_RETRIES; ++attempt) {
        metadata::RegisterNodeRequest request;
        request.set_node_id(node_id);
        request.set_address(node_address);

        metadata::RegisterNodeResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));

        grpc::Status status = stub->RegisterNode(&context, request, &response);

        if (status.ok() && response.success()) {
            std::cout << "[StorageNode] Registered with metadata server.\n";
            return true;
        }

        std::cerr << "[StorageNode] Registration attempt " << (attempt + 1)
                  << " failed. Retrying in 3s...\n";
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }

    return false;
}

int main() {
    std::signal(SIGINT,  signalHandler);
    std::signal(SIGTERM, signalHandler);

    // ── Read environment variables ────────────────────────────────────────────
    const char* node_id_env      = std::getenv("NODE_ID");
    const char* node_port_env    = std::getenv("NODE_PORT");
    const char* metadata_env     = std::getenv("METADATA_ADDR");
    const char* data_dir_env     = std::getenv("DATA_DIR");

    std::string node_id      = node_id_env   ? node_id_env   : "node1";
    std::string node_port    = node_port_env ? node_port_env : std::to_string(config::STORAGE_NODE_PORT);
    std::string metadata_addr = metadata_env  ? metadata_env  : "localhost:50050";
    std::string data_dir     = data_dir_env  ? data_dir_env  : config::DEFAULT_DATA_DIR;

    // The address this node will advertise to clients and peers.
    // In Docker, the container hostname is the service name (e.g. "storage_node1").
    const char* hostname_env = std::getenv("HOSTNAME");
    std::string hostname     = hostname_env ? hostname_env : node_id;
    std::string node_address = hostname + ":" + node_port;

    std::cout << "[StorageNode] Node ID: "    << node_id       << "\n";
    std::cout << "[StorageNode] Address: "    << node_address  << "\n";
    std::cout << "[StorageNode] Metadata: "   << metadata_addr << "\n";
    std::cout << "[StorageNode] Data dir: "   << data_dir      << "\n";

    // ── Start gRPC server ─────────────────────────────────────────────────────
    StorageNodeServiceImpl service(data_dir);

    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:" + node_port,
                             grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    builder.SetMaxReceiveMessageSize(config::GRPC_MAX_MESSAGE_SIZE);
    builder.SetMaxSendMessageSize(config::GRPC_MAX_MESSAGE_SIZE);

    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    if (!server) {
        std::cerr << "[StorageNode] Failed to start gRPC server.\n";
        return 1;
    }
    std::cout << "[StorageNode] gRPC server started on port " << node_port << "\n";

    // ── Register with metadata server (with retries) ──────────────────────────
    bool registered = registerWithMetadataServer(metadata_addr, node_id, node_address);
    if (!registered) {
        std::cerr << "[StorageNode] Could not register with metadata server. Exiting.\n";
        server->Shutdown();
        return 1;
    }

    // ── Start heartbeat sender ────────────────────────────────────────────────
    HeartbeatSender heartbeat(node_id, metadata_addr);
    heartbeat.start();

    std::cout << "[StorageNode] Ready.\n";

    // ── Wait for shutdown ─────────────────────────────────────────────────────
    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "[StorageNode] Shutting down...\n";
    heartbeat.stop();
    server->Shutdown();
    std::cout << "[StorageNode] Goodbye.\n";
    return 0;
}
