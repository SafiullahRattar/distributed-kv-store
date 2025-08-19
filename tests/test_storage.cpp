#include "storage.h"

#include <algorithm>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using kvstore::ThreadSafeStorage;

// ---------------------------------------------------------------------------
// Basic CRUD
// ---------------------------------------------------------------------------

TEST(StorageTest, PutAndGet) {
    ThreadSafeStorage store;
    store.put("name", "Alice");

    auto result = store.get("name");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "Alice");
}

TEST(StorageTest, GetMissing) {
    ThreadSafeStorage store;
    auto result = store.get("nonexistent");
    EXPECT_FALSE(result.has_value());
}

TEST(StorageTest, Overwrite) {
    ThreadSafeStorage store;
    store.put("key", "v1");
    store.put("key", "v2");

    auto result = store.get("key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "v2");
}

TEST(StorageTest, Erase) {
    ThreadSafeStorage store;
    store.put("key", "value");
    EXPECT_TRUE(store.erase("key"));
    EXPECT_FALSE(store.get("key").has_value());
}

TEST(StorageTest, EraseNonexistent) {
    ThreadSafeStorage store;
    EXPECT_FALSE(store.erase("ghost"));
}

TEST(StorageTest, SizeAndEmpty) {
    ThreadSafeStorage store;
    EXPECT_TRUE(store.empty());
    EXPECT_EQ(store.size(), 0u);

    store.put("a", "1");
    store.put("b", "2");
    EXPECT_FALSE(store.empty());
    EXPECT_EQ(store.size(), 2u);

    store.erase("a");
    EXPECT_EQ(store.size(), 1u);
}

// ---------------------------------------------------------------------------
// Bulk operations
// ---------------------------------------------------------------------------

TEST(StorageTest, Snapshot) {
    ThreadSafeStorage store;
    store.put("x", "10");
    store.put("y", "20");
    store.put("z", "30");

    auto snap = store.snapshot();
    EXPECT_EQ(snap.size(), 3u);

    // Verify all keys present
    std::vector<std::string> keys;
    for (const auto& [k, v] : snap) {
        keys.push_back(k);
    }
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(keys, (std::vector<std::string>{"x", "y", "z"}));
}

TEST(StorageTest, CollectIf) {
    ThreadSafeStorage store;
    store.put("user:1", "Alice");
    store.put("user:2", "Bob");
    store.put("config:timeout", "30");

    auto users = store.collect_if([](const std::string& key) {
        return key.find("user:") == 0;
    });
    EXPECT_EQ(users.size(), 2u);
}

TEST(StorageTest, RemoveIf) {
    ThreadSafeStorage store;
    store.put("temp:1", "a");
    store.put("temp:2", "b");
    store.put("perm:1", "c");

    auto removed = store.remove_if([](const std::string& key) {
        return key.find("temp:") == 0;
    });
    EXPECT_EQ(removed.size(), 2u);
    EXPECT_EQ(store.size(), 1u);
    EXPECT_TRUE(store.get("perm:1").has_value());
}

TEST(StorageTest, Clear) {
    ThreadSafeStorage store;
    store.put("a", "1");
    store.put("b", "2");
    store.clear();
    EXPECT_TRUE(store.empty());
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

TEST(StorageTest, ConcurrentPutGet) {
    ThreadSafeStorage store;
    constexpr int kNumThreads = 8;
    constexpr int kOpsPerThread = 1000;

    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);

    // Writers
    for (int t = 0; t < kNumThreads / 2; ++t) {
        threads.emplace_back([&store, t] {
            for (int i = 0; i < kOpsPerThread; ++i) {
                std::string key = "t" + std::to_string(t) + "_" + std::to_string(i);
                store.put(key, std::to_string(i));
            }
        });
    }

    // Readers
    for (int t = 0; t < kNumThreads / 2; ++t) {
        threads.emplace_back([&store, t] {
            for (int i = 0; i < kOpsPerThread; ++i) {
                std::string key = "t" + std::to_string(t) + "_" + std::to_string(i);
                store.get(key);  // may or may not find -- that's fine
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Each writer inserted kOpsPerThread unique keys
    EXPECT_EQ(store.size(),
              static_cast<std::size_t>((kNumThreads / 2) * kOpsPerThread));
}

TEST(StorageTest, ConcurrentErases) {
    ThreadSafeStorage store;
    constexpr int kNumKeys = 500;

    for (int i = 0; i < kNumKeys; ++i) {
        store.put("key" + std::to_string(i), "val");
    }

    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&store, t, kNumKeys] {
            for (int i = t; i < kNumKeys; i += 4) {
                store.erase("key" + std::to_string(i));
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    EXPECT_TRUE(store.empty());
}
