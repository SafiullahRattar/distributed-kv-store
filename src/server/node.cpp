#include "node.h"

#include <iostream>
#include <utility>

namespace kvstore {

// ============================================================================
// KVStoreServiceImpl
// ============================================================================

KVStoreServiceImpl::KVStoreServiceImpl(
    std::string node_id,
    std::shared_ptr<ThreadSafeStorage>   storage,
    std::shared_ptr<WriteAheadLog>       wal,
    std::shared_ptr<ConsistentHashRing>  ring,
    std::shared_ptr<ReplicationManager>  replication)
    : node_id_(std::move(node_id))
    , storage_(std::move(storage))
    , wal_(std::move(wal))
    , ring_(std::move(ring))
    , replication_(std::move(replication))
{}

uint64_t KVStoreServiceImpl::next_timestamp() {
    return logical_clock_.fetch_add(1, std::memory_order_relaxed);
}

grpc::Status KVStoreServiceImpl::Get(
    grpc::ServerContext* /*context*/,
    const GetRequest* request,
    GetResponse* response)
{
    auto result = storage_->get(request->key());
    if (result.has_value()) {
        response->set_found(true);
        response->set_value(*result);
    } else {
        response->set_found(false);
    }
    return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::Put(
    grpc::ServerContext* /*context*/,
    const PutRequest* request,
    PutResponse* response)
{
    const auto& key   = request->key();
    const auto& value = request->value();

    // 1. Write to WAL first (durability guarantee)
    wal_->log_put(key, value);

    // 2. Apply to in-memory storage
    storage_->put(key, value);

    // 3. Replicate to followers (leader only, best-effort)
    if (replication_ && replication_->role() == NodeRole::LEADER) {
        auto ts = next_timestamp();
        auto acks = replication_->replicate_put(key, value, ts);
        // We could enforce a quorum here, but for availability we accept
        // the write even if no followers ACK.
        (void)acks;
    }

    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::Delete(
    grpc::ServerContext* /*context*/,
    const DeleteRequest* request,
    DeleteResponse* response)
{
    const auto& key = request->key();

    // 1. WAL
    wal_->log_delete(key);

    // 2. Storage
    storage_->erase(key);

    // 3. Replicate
    if (replication_ && replication_->role() == NodeRole::LEADER) {
        auto ts = next_timestamp();
        replication_->replicate_delete(key, ts);
    }

    response->set_success(true);
    return grpc::Status::OK;
}

// ============================================================================
// InternalReplicationServiceImpl
// ============================================================================

InternalReplicationServiceImpl::InternalReplicationServiceImpl(
    std::string node_id,
    std::shared_ptr<ThreadSafeStorage> storage,
    std::shared_ptr<WriteAheadLog>     wal)
    : node_id_(std::move(node_id))
    , storage_(std::move(storage))
    , wal_(std::move(wal))
{}

grpc::Status InternalReplicationServiceImpl::ReplicateWrite(
    grpc::ServerContext* /*context*/,
    const ReplicateWriteRequest* request,
    ReplicateWriteResponse* response)
{
    wal_->log_put(request->key(), request->value());
    storage_->put(request->key(), request->value());
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status InternalReplicationServiceImpl::ReplicateDelete(
    grpc::ServerContext* /*context*/,
    const ReplicateDeleteRequest* request,
    ReplicateDeleteResponse* response)
{
    wal_->log_delete(request->key());
    storage_->erase(request->key());
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status InternalReplicationServiceImpl::Heartbeat(
    grpc::ServerContext* /*context*/,
    const HeartbeatRequest* /*request*/,
    HeartbeatResponse* response)
{
    response->set_alive(true);
    response->set_node_id(node_id_);
    return grpc::Status::OK;
}

grpc::Status InternalReplicationServiceImpl::TransferKeys(
    grpc::ServerContext* /*context*/,
    const TransferKeysRequest* request,
    TransferKeysResponse* response)
{
    uint32_t count = 0;
    for (const auto& pair : request->pairs()) {
        wal_->log_put(pair.key(), pair.value());
        storage_->put(pair.key(), pair.value());
        ++count;
    }
    response->set_success(true);
    response->set_keys_received(count);
    return grpc::Status::OK;
}

// ============================================================================
// Node
// ============================================================================

Node::Node(NodeConfig config)
    : config_(std::move(config))
    , storage_(std::make_shared<ThreadSafeStorage>())
    , wal_(std::make_shared<WriteAheadLog>(config_.wal_path))
    , ring_(std::make_shared<ConsistentHashRing>())
    , replication_(std::make_shared<ReplicationManager>(config_.node_id,
                                                       config_.role))
{
    // Recover state from the WAL (if any previous data exists)
    recover_from_wal();

    // Register ourselves on the consistent hash ring
    ring_->add_node(config_.node_id);

    // Register peers
    for (const auto& peer : config_.peers) {
        ring_->add_node(peer);
        replication_->add_peer(peer);
    }
}

void Node::recover_from_wal() {
    auto records = WriteAheadLog::replay(config_.wal_path);
    std::size_t applied = 0;
    for (const auto& record : records) {
        switch (record.type) {
            case WALRecordType::PUT:
                storage_->put(record.key, record.value);
                break;
            case WALRecordType::DELETE:
                storage_->erase(record.key);
                break;
        }
        ++applied;
    }
    if (applied > 0) {
        std::cout << "[node] Recovered " << applied
                  << " operations from WAL\n";
    }
}

void Node::run() {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(config_.listen_address,
                             grpc::InsecureServerCredentials());

    auto kv_service = std::make_unique<KVStoreServiceImpl>(
        config_.node_id, storage_, wal_, ring_, replication_);
    auto repl_service = std::make_unique<InternalReplicationServiceImpl>(
        config_.node_id, storage_, wal_);

    builder.RegisterService(kv_service.get());
    builder.RegisterService(repl_service.get());

    server_ = builder.BuildAndStart();

    std::cout << "[node] " << config_.node_id
              << " listening on " << config_.listen_address << "\n";
    std::cout << "[node] role: "
              << (config_.role == NodeRole::LEADER ? "LEADER" : "FOLLOWER")
              << "\n";

    // Start heartbeat for leaders
    if (config_.role == NodeRole::LEADER) {
        replication_->start_heartbeat();
    }

    server_->Wait();
}

void Node::shutdown() {
    replication_->stop_heartbeat();
    if (server_) {
        server_->Shutdown();
    }
}

}  // namespace kvstore
