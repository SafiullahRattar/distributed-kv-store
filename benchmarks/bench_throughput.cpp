#include "storage.h"
#include "consistent_hash.h"
#include "wal.h"

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::high_resolution_clock;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

std::string random_string(std::mt19937& rng, std::size_t length) {
    static constexpr char kChars[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::uniform_int_distribution<int> dist(0, sizeof(kChars) - 2);
    std::string s(length, '\0');
    for (auto& c : s) {
        c = kChars[dist(rng)];
    }
    return s;
}

double elapsed_ms(Clock::time_point start, Clock::time_point end) {
    return std::chrono::duration<double, std::milli>(end - start).count();
}

// ---------------------------------------------------------------------------
// Benchmark: in-memory storage throughput
// ---------------------------------------------------------------------------

void bench_storage() {
    std::cout << "=== In-Memory Storage Benchmark ===\n";

    kvstore::ThreadSafeStorage store;
    constexpr int kOps = 100'000;
    std::mt19937 rng(42);

    // Pre-generate keys/values
    std::vector<std::string> keys, values;
    keys.reserve(kOps);
    values.reserve(kOps);
    for (int i = 0; i < kOps; ++i) {
        keys.push_back(random_string(rng, 16));
        values.push_back(random_string(rng, 64));
    }

    // --- Single-threaded PUT ---
    auto t0 = Clock::now();
    for (int i = 0; i < kOps; ++i) {
        store.put(keys[i], values[i]);
    }
    auto t1 = Clock::now();
    double put_ms = elapsed_ms(t0, t1);
    std::printf("  Single-thread PUT:  %d ops in %.1f ms  (%.0f ops/s)\n",
                kOps, put_ms, kOps / (put_ms / 1000.0));

    // --- Single-threaded GET ---
    t0 = Clock::now();
    for (int i = 0; i < kOps; ++i) {
        auto val = store.get(keys[i]);
        (void)val;
    }
    t1 = Clock::now();
    double get_ms = elapsed_ms(t0, t1);
    std::printf("  Single-thread GET:  %d ops in %.1f ms  (%.0f ops/s)\n",
                kOps, get_ms, kOps / (get_ms / 1000.0));

    // --- Multi-threaded mixed (4 threads) ---
    constexpr int kThreads = 4;
    constexpr int kOpsPerThread = kOps / kThreads;

    t0 = Clock::now();
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&store, &keys, &values, t, kOpsPerThread] {
            int base = t * kOpsPerThread;
            for (int i = 0; i < kOpsPerThread; ++i) {
                int idx = base + i;
                if (i % 2 == 0) {
                    store.put(keys[idx], values[idx]);
                } else {
                    store.get(keys[idx]);
                }
            }
        });
    }
    for (auto& th : threads) th.join();
    t1 = Clock::now();
    double mixed_ms = elapsed_ms(t0, t1);
    std::printf("  Multi-thread mixed: %d ops in %.1f ms  (%.0f ops/s, %d threads)\n",
                kOps, mixed_ms, kOps / (mixed_ms / 1000.0), kThreads);

    std::cout << "\n";
}

// ---------------------------------------------------------------------------
// Benchmark: consistent hash ring lookup
// ---------------------------------------------------------------------------

void bench_consistent_hash() {
    std::cout << "=== Consistent Hash Ring Benchmark ===\n";

    kvstore::ConsistentHashRing ring(150);
    for (int i = 1; i <= 10; ++i) {
        ring.add_node("node-" + std::to_string(i));
    }

    constexpr int kOps = 100'000;
    std::mt19937 rng(123);
    std::vector<std::string> keys;
    keys.reserve(kOps);
    for (int i = 0; i < kOps; ++i) {
        keys.push_back(random_string(rng, 16));
    }

    auto t0 = Clock::now();
    for (int i = 0; i < kOps; ++i) {
        auto node = ring.get_node(keys[i]);
        (void)node;
    }
    auto t1 = Clock::now();
    double ms = elapsed_ms(t0, t1);
    std::printf("  %d lookups (10 nodes, 150 vnodes): %.1f ms  (%.0f ops/s)\n",
                kOps, ms, kOps / (ms / 1000.0));

    std::cout << "\n";
}

// ---------------------------------------------------------------------------
// Benchmark: WAL write throughput
// ---------------------------------------------------------------------------

void bench_wal() {
    std::cout << "=== WAL Write Benchmark ===\n";

    auto wal_path = std::filesystem::temp_directory_path() / "bench_wal.log";
    std::filesystem::remove(wal_path);

    constexpr int kOps = 50'000;
    std::mt19937 rng(99);

    std::vector<std::string> keys, values;
    keys.reserve(kOps);
    values.reserve(kOps);
    for (int i = 0; i < kOps; ++i) {
        keys.push_back(random_string(rng, 16));
        values.push_back(random_string(rng, 64));
    }

    {
        kvstore::WriteAheadLog wal(wal_path.string());

        auto t0 = Clock::now();
        for (int i = 0; i < kOps; ++i) {
            wal.log_put(keys[i], values[i]);
        }
        auto t1 = Clock::now();
        double ms = elapsed_ms(t0, t1);
        std::printf("  %d sequential writes: %.1f ms  (%.0f ops/s)\n",
                    kOps, ms, kOps / (ms / 1000.0));
    }

    // Replay benchmark
    {
        auto t0 = Clock::now();
        auto records = kvstore::WriteAheadLog::replay(wal_path.string());
        auto t1 = Clock::now();
        double ms = elapsed_ms(t0, t1);
        std::printf("  Replay %zu records: %.1f ms  (%.0f records/s)\n",
                    records.size(), ms, records.size() / (ms / 1000.0));
    }

    std::filesystem::remove(wal_path);
    std::cout << "\n";
}

}  // anonymous namespace

int main() {
    std::cout << "Distributed KV Store -- Micro Benchmarks\n";
    std::cout << "=========================================\n\n";

    bench_storage();
    bench_consistent_hash();
    bench_wal();

    std::cout << "Done.\n";
    return 0;
}
