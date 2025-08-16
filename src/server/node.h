#ifndef KVSTORE_NODE_H
#define KVSTORE_NODE_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"

#include "consistent_hash.h"
#include "replication.h"
#include "storage.h"
#include "wal.h"

namespace kvstore {

// ============================================================================
// KVStoreServiceImpl
//
// gRPC service handler for client-facing operations (Get / Put / Delete).
// Wraps all the internal components: storage, WAL, consistent hash ring, and
// replication manager.
// ============================================================================
class KVStoreServiceImpl final : public ::kvstore::KVStore::Service {
public:
    KVStoreServiceImpl(std::string node_id,
                       std::shared_ptr<ThreadSafeStorage>   storage,
                       std::shared_ptr<WriteAheadLog>       wal,
                       std::shared_ptr<ConsistentHashRing>  ring,
                       std::shared_ptr<ReplicationManager>  replication);

    grpc::Status Get(grpc::ServerContext* context,
                     const GetRequest* request,
                     GetResponse* response) override;

    grpc::Status Put(grpc::ServerContext* context,
                     const PutRequest* request,
                     PutResponse* response) override;

    grpc::Status Delete(grpc::ServerContext* context,
                        const DeleteRequest* request,
                        DeleteResponse* response) override;

private:
    [[nodiscard]] uint64_t next_timestamp();

    std::string                          node_id_;
    std::shared_ptr<ThreadSafeStorage>   storage_;
    std::shared_ptr<WriteAheadLog>       wal_;
    std::shared_ptr<ConsistentHashRing>  ring_;
    std::shared_ptr<ReplicationManager>  replication_;
    std::atomic<uint64_t>                logical_clock_{0};
};

// ============================================================================
// InternalReplicationServiceImpl
//
// gRPC service handler for inter-node traffic: replication, heartbeats, and
// key transfers.
// ============================================================================
class InternalReplicationServiceImpl final
    : public ::kvstore::InternalReplication::Service {
public:
    InternalReplicationServiceImpl(
        std::string node_id,
        std::shared_ptr<ThreadSafeStorage> storage,
        std::shared_ptr<WriteAheadLog>     wal);

    grpc::Status ReplicateWrite(grpc::ServerContext* context,
                                const ReplicateWriteRequest* request,
                                ReplicateWriteResponse* response) override;

    grpc::Status ReplicateDelete(grpc::ServerContext* context,
                                 const ReplicateDeleteRequest* request,
                                 ReplicateDeleteResponse* response) override;

    grpc::Status Heartbeat(grpc::ServerContext* context,
                           const HeartbeatRequest* request,
                           HeartbeatResponse* response) override;

    grpc::Status TransferKeys(grpc::ServerContext* context,
                              const TransferKeysRequest* request,
                              TransferKeysResponse* response) override;

private:
    std::string                          node_id_;
    std::shared_ptr<ThreadSafeStorage>   storage_;
    std::shared_ptr<WriteAheadLog>       wal_;
};

// ============================================================================
// Node
//
// Top-level object that ties everything together and runs the gRPC server.
// ============================================================================
struct NodeConfig {
    std::string node_id;             // e.g. "node-1"
    std::string listen_address;      // e.g. "0.0.0.0:50051"
    std::string wal_path;            // e.g. "/tmp/kv_node1.wal"
    NodeRole    role = NodeRole::LEADER;
    std::vector<std::string> peers;  // addresses of other nodes
};

class Node {
public:
    explicit Node(NodeConfig config);

    /// Start serving (blocks the calling thread).
    void run();

    /// Initiate graceful shutdown.
    void shutdown();

    // Access internal components (useful for testing)
    [[nodiscard]] std::shared_ptr<ThreadSafeStorage>   storage()     const { return storage_; }
    [[nodiscard]] std::shared_ptr<WriteAheadLog>       wal()         const { return wal_; }
    [[nodiscard]] std::shared_ptr<ConsistentHashRing>  ring()        const { return ring_; }
    [[nodiscard]] std::shared_ptr<ReplicationManager>  replication() const { return replication_; }

private:
    void recover_from_wal();

    NodeConfig                               config_;
    std::shared_ptr<ThreadSafeStorage>       storage_;
    std::shared_ptr<WriteAheadLog>           wal_;
    std::shared_ptr<ConsistentHashRing>      ring_;
    std::shared_ptr<ReplicationManager>      replication_;
    std::unique_ptr<grpc::Server>            server_;
};

}  // namespace kvstore

#endif  // KVSTORE_NODE_H
