// =============================================================================
// heartbeat_sender.cpp
// =============================================================================

#include "heartbeat_sender.h"
#include "../common/config.h"
#include "metadata.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <iostream>
#include <chrono>
#include <thread>

HeartbeatSender::HeartbeatSender(const std::string& node_id,
                                   const std::string& metadata_server_address)
    : node_id_(node_id), metadata_address_(metadata_server_address) {}

HeartbeatSender::~HeartbeatSender() {
    stop();
}

void HeartbeatSender::start() {
    if (running_.load()) return;
    running_.store(true);
    sender_thread_ = std::thread([this]() { senderLoop(); });
    std::cout << "[HeartbeatSender] Started for node " << node_id_ << "\n";
}

void HeartbeatSender::stop() {
    if (!running_.load()) return;
    running_.store(false);
    if (sender_thread_.joinable()) {
        sender_thread_.join();
    }
    std::cout << "[HeartbeatSender] Stopped.\n";
}

void HeartbeatSender::senderLoop() {
    // Create a channel to the metadata server once and reuse it.
    // Creating a channel is expensive; keeping it alive is the right approach.
    auto channel = grpc::CreateChannel(metadata_address_,
                                       grpc::InsecureChannelCredentials());
    auto stub = metadata::MetadataService::NewStub(channel);

    while (running_.load()) {
        // Sleep first so we don't flood the server immediately on startup.
        // We sleep in 1-second increments for fast shutdown response.
        for (int i = 0; i < config::HEARTBEAT_INTERVAL_SECONDS && running_.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        if (!running_.load()) break;

        metadata::HeartbeatRequest request;
        request.set_node_id(node_id_);

        metadata::HeartbeatResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));

        grpc::Status status = stub->Heartbeat(&context, request, &response);

        if (!status.ok()) {
            // Log but don't crash — the metadata server might be temporarily
            // unavailable. The storage node keeps trying.
            std::cerr << "[HeartbeatSender] Heartbeat failed: "
                      << status.error_message() << "\n";
        }
    }
}
