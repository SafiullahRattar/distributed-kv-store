#ifndef KVSTORE_REPLICATION_H
#define KVSTORE_REPLICATION_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"

namespace kvstore {

// ============================================================================
// ReplicationManager
//
// Manages leader-follower replication.  The leader calls replicate_put /
// replicate_delete which fan out RPCs to every known follower.  Heartbeats
// are sent periodically so the leader can detect dead followers.
//
// This is a *semi-synchronous* replication model: the leader waits for at
// least one follower ACK before confirming the write (best-effort quorum of
// 1).  If no followers are reachable the write still succeeds locally --
// availability over strict consistency.
// ============================================================================

enum class NodeRole {
    LEADER,
    FOLLOWER,
};

struct PeerInfo {
    std::string                                                  address;
    std::shared_ptr<grpc::Channel>                               channel;
    std::unique_ptr<::kvstore::InternalReplication::Stub>        stub;
    std::chrono::steady_clock::time_point                        last_heartbeat;
    bool                                                         alive = true;
};

class ReplicationManager {
public:
    /// `self_id` is this node's identifier (e.g. "node-1:50051").
    explicit ReplicationManager(std::string self_id, NodeRole role);
    ~ReplicationManager();

    // Non-copyable
    ReplicationManager(const ReplicationManager&)            = delete;
    ReplicationManager& operator=(const ReplicationManager&) = delete;

    // -----------------------------------------------------------------------
    // Peer management
    // -----------------------------------------------------------------------

    /// Register a follower / peer that we will replicate to (leader) or
    /// accept heartbeats from (follower).
    void add_peer(const std::string& peer_address);

    /// Remove a peer.
    void remove_peer(const std::string& peer_address);

    /// All known peers and their liveness status.
    [[nodiscard]] std::vector<std::pair<std::string, bool>> peer_status() const;

    // -----------------------------------------------------------------------
    // Replication (called by the leader after a local write)
    // -----------------------------------------------------------------------

    /// Fan-out PUT to all followers.  Returns the number of ACKs received.
    uint32_t replicate_put(const std::string& key, const std::string& value,
                           uint64_t timestamp);

    /// Fan-out DELETE to all followers.  Returns the number of ACKs received.
    uint32_t replicate_delete(const std::string& key, uint64_t timestamp);

    // -----------------------------------------------------------------------
    // Heartbeat
    // -----------------------------------------------------------------------

    /// Start the background heartbeat thread (leader-side).
    void start_heartbeat(std::chrono::milliseconds interval =
                             std::chrono::milliseconds(2000));

    /// Stop the heartbeat thread.
    void stop_heartbeat();

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    [[nodiscard]] NodeRole    role()    const { return role_; }
    [[nodiscard]] std::string self_id() const { return self_id_; }

private:
    void heartbeat_loop(std::chrono::milliseconds interval);

    std::string                                 self_id_;
    NodeRole                                    role_;

    mutable std::mutex                          peers_mutex_;
    std::unordered_map<std::string, PeerInfo>   peers_;

    std::atomic<bool>                           heartbeat_running_{false};
    std::thread                                 heartbeat_thread_;
};

}  // namespace kvstore

#endif  // KVSTORE_REPLICATION_H
