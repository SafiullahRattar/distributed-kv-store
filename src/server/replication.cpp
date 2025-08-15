#include "replication.h"

#include <iostream>

namespace kvstore {

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

ReplicationManager::ReplicationManager(std::string self_id, NodeRole role)
    : self_id_(std::move(self_id))
    , role_(role)
{}

ReplicationManager::~ReplicationManager() {
    stop_heartbeat();
}

// ---------------------------------------------------------------------------
// Peer management
// ---------------------------------------------------------------------------

void ReplicationManager::add_peer(const std::string& peer_address) {
    std::lock_guard lock(peers_mutex_);
    if (peers_.count(peer_address)) {
        return;  // already known
    }

    PeerInfo info;
    info.address        = peer_address;
    info.channel        = grpc::CreateChannel(peer_address,
                                              grpc::InsecureChannelCredentials());
    info.stub           = InternalReplication::NewStub(info.channel);
    info.last_heartbeat = std::chrono::steady_clock::now();
    info.alive          = true;

    peers_.emplace(peer_address, std::move(info));
}

void ReplicationManager::remove_peer(const std::string& peer_address) {
    std::lock_guard lock(peers_mutex_);
    peers_.erase(peer_address);
}

std::vector<std::pair<std::string, bool>> ReplicationManager::peer_status() const {
    std::lock_guard lock(peers_mutex_);
    std::vector<std::pair<std::string, bool>> result;
    result.reserve(peers_.size());
    for (const auto& [addr, info] : peers_) {
        result.emplace_back(addr, info.alive);
    }
    return result;
}

// ---------------------------------------------------------------------------
// Replication RPCs
// ---------------------------------------------------------------------------

uint32_t ReplicationManager::replicate_put(const std::string& key,
                                           const std::string& value,
                                           uint64_t timestamp) {
    std::lock_guard lock(peers_mutex_);
    uint32_t acks = 0;

    for (auto& [addr, peer] : peers_) {
        if (!peer.alive) continue;

        ReplicateWriteRequest request;
        request.set_key(key);
        request.set_value(value);
        request.set_timestamp(timestamp);

        ReplicateWriteResponse response;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(500));

        auto status = peer.stub->ReplicateWrite(&ctx, request, &response);
        if (status.ok() && response.success()) {
            ++acks;
        } else {
            std::cerr << "[replication] PUT to " << addr
                      << " failed: " << status.error_message() << "\n";
        }
    }
    return acks;
}

uint32_t ReplicationManager::replicate_delete(const std::string& key,
                                              uint64_t timestamp) {
    std::lock_guard lock(peers_mutex_);
    uint32_t acks = 0;

    for (auto& [addr, peer] : peers_) {
        if (!peer.alive) continue;

        ReplicateDeleteRequest request;
        request.set_key(key);
        request.set_timestamp(timestamp);

        ReplicateDeleteResponse response;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(500));

        auto status = peer.stub->ReplicateDelete(&ctx, request, &response);
        if (status.ok() && response.success()) {
            ++acks;
        } else {
            std::cerr << "[replication] DELETE to " << addr
                      << " failed: " << status.error_message() << "\n";
        }
    }
    return acks;
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

void ReplicationManager::start_heartbeat(std::chrono::milliseconds interval) {
    if (heartbeat_running_.exchange(true)) {
        return;  // already running
    }
    heartbeat_thread_ = std::thread([this, interval] {
        heartbeat_loop(interval);
    });
}

void ReplicationManager::stop_heartbeat() {
    heartbeat_running_.store(false);
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }
}

void ReplicationManager::heartbeat_loop(std::chrono::milliseconds interval) {
    while (heartbeat_running_.load()) {
        {
            std::lock_guard lock(peers_mutex_);
            for (auto& [addr, peer] : peers_) {
                HeartbeatRequest request;
                request.set_node_id(self_id_);

                HeartbeatResponse response;
                grpc::ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(1000));

                auto status = peer.stub->Heartbeat(&ctx, request, &response);
                if (status.ok() && response.alive()) {
                    peer.alive = true;
                    peer.last_heartbeat = std::chrono::steady_clock::now();
                } else {
                    // Mark dead after missing heartbeat
                    auto elapsed = std::chrono::steady_clock::now() -
                                   peer.last_heartbeat;
                    if (elapsed > std::chrono::seconds(10)) {
                        if (peer.alive) {
                            std::cerr << "[heartbeat] peer " << addr
                                      << " marked DEAD\n";
                        }
                        peer.alive = false;
                    }
                }
            }
        }
        std::this_thread::sleep_for(interval);
    }
}

}  // namespace kvstore
