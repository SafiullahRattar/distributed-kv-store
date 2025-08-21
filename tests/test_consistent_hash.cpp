#include "consistent_hash.h"

#include <cmath>
#include <map>
#include <set>
#include <string>

#include <gtest/gtest.h>

using kvstore::ConsistentHashRing;

// ---------------------------------------------------------------------------
// Basic ring operations
// ---------------------------------------------------------------------------

TEST(ConsistentHashTest, EmptyRingReturnsNullopt) {
    ConsistentHashRing ring;
    auto node = ring.get_node("any-key");
    EXPECT_FALSE(node.has_value());
}

TEST(ConsistentHashTest, SingleNodeOwnsAllKeys) {
    ConsistentHashRing ring;
    ring.add_node("node-1");

    for (int i = 0; i < 100; ++i) {
        auto node = ring.get_node("key-" + std::to_string(i));
        ASSERT_TRUE(node.has_value());
        EXPECT_EQ(*node, "node-1");
    }
}

TEST(ConsistentHashTest, AddAndRemoveNode) {
    ConsistentHashRing ring;
    ring.add_node("node-1");
    EXPECT_TRUE(ring.has_node("node-1"));
    EXPECT_EQ(ring.node_count(), 1u);

    ring.remove_node("node-1");
    EXPECT_FALSE(ring.has_node("node-1"));
    EXPECT_EQ(ring.node_count(), 0u);
}

TEST(ConsistentHashTest, DuplicateAddIsNoop) {
    ConsistentHashRing ring;
    ring.add_node("node-1");
    ring.add_node("node-1");
    EXPECT_EQ(ring.node_count(), 1u);
}

TEST(ConsistentHashTest, MultipleNodesListed) {
    ConsistentHashRing ring;
    ring.add_node("node-1");
    ring.add_node("node-2");
    ring.add_node("node-3");

    auto nodes = ring.nodes();
    EXPECT_EQ(nodes.size(), 3u);

    std::set<std::string> node_set(nodes.begin(), nodes.end());
    EXPECT_TRUE(node_set.count("node-1"));
    EXPECT_TRUE(node_set.count("node-2"));
    EXPECT_TRUE(node_set.count("node-3"));
}

// ---------------------------------------------------------------------------
// Key distribution
// ---------------------------------------------------------------------------

TEST(ConsistentHashTest, KeysDistributedAcrossNodes) {
    ConsistentHashRing ring(150);
    ring.add_node("node-1");
    ring.add_node("node-2");
    ring.add_node("node-3");

    std::map<std::string, int> distribution;
    constexpr int kNumKeys = 10000;

    for (int i = 0; i < kNumKeys; ++i) {
        auto node = ring.get_node("key-" + std::to_string(i));
        ASSERT_TRUE(node.has_value());
        distribution[*node]++;
    }

    // Each node should get a reasonable share (at least 15% with virtual nodes)
    for (const auto& [node, count] : distribution) {
        double share = static_cast<double>(count) / kNumKeys;
        EXPECT_GT(share, 0.15)
            << node << " got only " << (share * 100) << "% of keys";
        EXPECT_LT(share, 0.55)
            << node << " got " << (share * 100) << "% of keys (too many)";
    }
}

TEST(ConsistentHashTest, MinimalDisruptionOnNodeAdd) {
    ConsistentHashRing ring(150);
    ring.add_node("node-1");
    ring.add_node("node-2");

    constexpr int kNumKeys = 5000;
    std::map<std::string, std::string> before;
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key = "key-" + std::to_string(i);
        before[key] = *ring.get_node(key);
    }

    // Add a third node
    ring.add_node("node-3");

    int moved = 0;
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key = "key-" + std::to_string(i);
        auto new_owner = *ring.get_node(key);
        if (new_owner != before[key]) {
            ++moved;
        }
    }

    // Ideally ~1/3 of keys move.  Allow generous margin.
    double move_ratio = static_cast<double>(moved) / kNumKeys;
    EXPECT_LT(move_ratio, 0.60)
        << "Too many keys moved (" << (move_ratio * 100)
        << "%), consistent hashing should minimize disruption";
}

// ---------------------------------------------------------------------------
// Replication helpers
// ---------------------------------------------------------------------------

TEST(ConsistentHashTest, GetNodesReturnsDistinctNodes) {
    ConsistentHashRing ring(150);
    ring.add_node("node-1");
    ring.add_node("node-2");
    ring.add_node("node-3");

    auto replicas = ring.get_nodes("some-key", 3);
    ASSERT_EQ(replicas.size(), 3u);

    std::set<std::string> unique(replicas.begin(), replicas.end());
    EXPECT_EQ(unique.size(), 3u);
}

TEST(ConsistentHashTest, GetNodesCappedAtNodeCount) {
    ConsistentHashRing ring(150);
    ring.add_node("node-1");
    ring.add_node("node-2");

    auto replicas = ring.get_nodes("key", 5);
    EXPECT_EQ(replicas.size(), 2u);  // only 2 physical nodes exist
}

TEST(ConsistentHashTest, GetNodesEmptyRing) {
    ConsistentHashRing ring;
    auto replicas = ring.get_nodes("key", 3);
    EXPECT_TRUE(replicas.empty());
}

// ---------------------------------------------------------------------------
// Hash function properties
// ---------------------------------------------------------------------------

TEST(ConsistentHashTest, HashDeterministic) {
    auto h1 = ConsistentHashRing::hash("hello");
    auto h2 = ConsistentHashRing::hash("hello");
    EXPECT_EQ(h1, h2);
}

TEST(ConsistentHashTest, HashDifferentInputs) {
    auto h1 = ConsistentHashRing::hash("key-1");
    auto h2 = ConsistentHashRing::hash("key-2");
    // Not strictly guaranteed, but extremely unlikely to collide
    EXPECT_NE(h1, h2);
}

TEST(ConsistentHashTest, HashAvalanche) {
    // Changing one character should flip many bits
    auto h1 = ConsistentHashRing::hash("test-key-0");
    auto h2 = ConsistentHashRing::hash("test-key-1");

    uint32_t diff = h1 ^ h2;
    int bits_changed = __builtin_popcount(diff);
    // A good hash should flip roughly half the bits; allow at least 8
    EXPECT_GE(bits_changed, 8)
        << "Only " << bits_changed << " bits changed -- poor avalanche";
}
