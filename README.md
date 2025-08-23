# Distributed Key-Value Store

A distributed, replicated key-value store written in C++17. Built to demonstrate practical distributed systems design: consistent hashing for data partitioning, write-ahead logging for durability, and leader-follower replication for fault tolerance.

## Why This Exists

Most tutorials stop at a single-node key-value store. This project goes further by solving the hard problems that show up when you distribute state across multiple machines: how to partition data fairly, how to survive crashes, and how to keep replicas consistent -- all while keeping the code clean enough to reason about.

## Architecture

```
                          ┌─────────────┐
                          │  CLI Client  │
                          └──────┬───────┘
                                 │ gRPC (Get/Put/Delete)
                                 ▼
                 ┌───────────────────────────────┐
                 │           Leader Node          │
                 │  ┌───────┐  ┌─────┐  ┌─────┐  │
                 │  │Storage│  │ WAL │  │Ring │  │
                 │  └───────┘  └─────┘  └─────┘  │
                 └──────┬────────────┬────────────┘
          ReplicateWrite│            │ReplicateWrite
            ┌───────────┘            └──────────┐
            ▼                                   ▼
   ┌─────────────────┐                ┌─────────────────┐
   │  Follower Node 1 │                │  Follower Node 2 │
   │ ┌───────┐ ┌─────┐│                │ ┌───────┐ ┌─────┐│
   │ │Storage│ │ WAL ││                │ │Storage│ │ WAL ││
   │ └───────┘ └─────┘│                │ └───────┘ └─────┘│
   └───────────────────┘                └───────────────────┘


            Consistent Hash Ring (virtual nodes)
    ┌──────────────────────────────────────────┐
    │         0                                │
    │     ╱       ╲                            │
    │   N3         N1    ← 150 virtual nodes   │
    │   │           │      per physical node   │
    │   N1         N2                          │
    │     ╲       ╱                            │
    │        N2                                │
    │       2^32                               │
    └──────────────────────────────────────────┘
    Keys are hashed and assigned to the next
    clockwise node on the ring.
```

### Data Flow (Write Path)

1. Client sends `PUT key value` via gRPC to the leader.
2. Leader writes the operation to its **WAL** (fsync to disk).
3. Leader applies the write to its **in-memory store**.
4. Leader fans out `ReplicateWrite` RPCs to all followers.
5. Each follower writes to its own WAL, then applies to memory.
6. Leader returns success to the client.

### Data Flow (Read Path)

1. Client sends `GET key` via gRPC to any node (leader or follower).
2. Node reads directly from its in-memory store and returns the result.

This is an **AP-leaning** design: reads from followers may be slightly stale, but the system stays available even when followers are unreachable.

## Components

| Component | File | Purpose |
|---|---|---|
| **ThreadSafeStorage** | `storage.h/cpp` | Concurrent hash map with shared_mutex (readers-writer lock) |
| **WriteAheadLog** | `wal.h/cpp` | Append-only binary log with CRC-32 checksums for crash recovery |
| **ConsistentHashRing** | `consistent_hash.h/cpp` | Consistent hashing with virtual nodes for balanced key distribution |
| **ReplicationManager** | `replication.h/cpp` | Leader-to-follower write fanout with heartbeat-based failure detection |
| **Node** | `node.h/cpp` | Top-level orchestrator: ties storage, WAL, ring, and replication together |
| **CLI Client** | `client/main.cpp` | Interactive REPL for GET/PUT/DELETE operations |

## Build Instructions

### Prerequisites

- C++17 compiler (GCC 8+, Clang 7+, MSVC 2019+)
- CMake 3.16+
- gRPC and Protobuf development libraries
- pthread

#### Installing Dependencies (Ubuntu/Debian)

```bash
sudo apt update
sudo apt install -y build-essential cmake libgrpc++-dev libprotobuf-dev \
    protobuf-compiler protobuf-compiler-grpc
```

#### Installing Dependencies (Arch Linux)

```bash
sudo pacman -S base-devel cmake grpc protobuf
```

#### Installing Dependencies (macOS)

```bash
brew install cmake grpc protobuf
```

### Building

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Running Tests

```bash
cd build
ctest --output-on-failure
```

### Running Benchmarks

```bash
./build/bench_throughput
```

## Usage

### Starting a Single Node

```bash
./build/kv_server --id node-1 --listen 0.0.0.0:50051 --wal /tmp/node1.wal
```

### Starting a 3-Node Cluster

```bash
# Terminal 1: Leader
./build/kv_server --id node-1 --listen 0.0.0.0:50051 --role leader \
    --wal /tmp/node1.wal --peer localhost:50052 --peer localhost:50053

# Terminal 2: Follower 1
./build/kv_server --id node-2 --listen 0.0.0.0:50052 --role follower \
    --wal /tmp/node2.wal --peer localhost:50051

# Terminal 3: Follower 2
./build/kv_server --id node-3 --listen 0.0.0.0:50053 --role follower \
    --wal /tmp/node3.wal --peer localhost:50051
```

### Using the CLI Client

```bash
./build/kv_client --server localhost:50051

Connecting to localhost:50051...
kv> put user:1 Alice
OK
kv> put user:2 Bob
OK
kv> get user:1
Alice
kv> del user:2
OK
kv> get user:2
(nil)
kv> quit
Bye.
```

You can point the client at any node in the cluster. Writes to the leader are replicated; reads from any node return local state.

## Design Decisions

### Why Consistent Hashing?

Naive modular hashing (`hash(key) % N`) remaps nearly all keys when a node is added or removed. Consistent hashing limits key movement to ~`K/N` keys (where K is total keys and N is nodes), making cluster resizing practical. Virtual nodes (150 per physical node by default) smooth out the distribution -- without them, a small number of nodes can create severe hotspots.

### Why a Write-Ahead Log?

The WAL provides durability without the overhead of writing to a full database on every operation. By appending operations sequentially, we get O(1) write amplification. On recovery, we replay the log to reconstruct state. Each record includes a CRC-32 checksum so we can detect and skip partially-written records (e.g., from a power failure mid-write).

### Why Leader-Follower (Not Raft/Paxos)?

Full consensus protocols like Raft add significant complexity. For this design, a single leader handles all writes and replicates asynchronously to followers. This is simpler to implement correctly and is the pattern used by systems like Redis Sentinel and early Kafka. The trade-off is that followers may serve slightly stale reads, and the leader is a single point of failure for writes (though followers retain all replicated data).

### Why Semi-Synchronous Replication?

The leader sends replication RPCs to all followers but does not block indefinitely waiting for ACKs. If no followers respond within 500ms, the write still succeeds locally. This favors **availability** over strict **consistency** (AP in CAP terms). For use cases requiring strong consistency, you would switch to synchronous replication with a quorum requirement.

### Thread Safety Strategy

Storage uses `std::shared_mutex` for reader-writer locking: multiple concurrent reads proceed in parallel, while writes take an exclusive lock. This is ideal for read-heavy workloads. The WAL uses a regular `std::mutex` since all operations are writes (appends). The consistent hash ring also uses `std::mutex` since lookups are fast and contention is low.

## Performance Characteristics

| Operation | Complexity | Notes |
|---|---|---|
| GET (local) | O(1) average | Hash map lookup under shared lock |
| PUT (local) | O(1) average | Hash map insert + WAL append |
| PUT (replicated) | O(F) | F = number of followers; RPCs sent in sequence |
| Ring lookup | O(log V) | V = total virtual nodes; `std::map::lower_bound` |
| WAL replay | O(R) | R = number of records; sequential read |
| Node add/remove | O(V log V) | V virtual nodes inserted/removed from the ring |

Typical throughput on a single node (in-memory, no replication):
- **Reads:** 2-5M ops/sec (single thread)
- **Writes:** 500K-1M ops/sec (single thread, with WAL fsync)
- **Ring lookups:** 1-3M lookups/sec

## License

MIT
