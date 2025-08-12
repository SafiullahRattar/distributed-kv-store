#ifndef KVSTORE_CONSISTENT_HASH_H
#define KVSTORE_CONSISTENT_HASH_H

#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

// ============================================================================
// ConsistentHashRing
//
// Maps keys to nodes using consistent hashing with virtual nodes.  Each
// physical node is placed at `num_virtual_nodes` positions on a 32-bit hash
// ring, improving key distribution uniformity.
//
// Thread safety: all public methods acquire the internal mutex.
// ============================================================================
class ConsistentHashRing {
public:
    /// `num_virtual_nodes` controls how many positions each physical node
    /// occupies on the ring.  Higher values give better balance at the cost of
    /// slightly more memory.  150 is a reasonable default.
    explicit ConsistentHashRing(std::size_t num_virtual_nodes = 150);

    // -----------------------------------------------------------------------
    // Ring membership
    // -----------------------------------------------------------------------

    /// Add a physical node (e.g. "node-1:50051") to the ring.
    void add_node(const std::string& node_id);

    /// Remove a physical node and all its virtual positions.
    void remove_node(const std::string& node_id);

    /// True if `node_id` is currently on the ring.
    [[nodiscard]] bool has_node(const std::string& node_id) const;

    /// List of all physical node IDs currently on the ring.
    [[nodiscard]] std::vector<std::string> nodes() const;

    /// Number of physical nodes on the ring.
    [[nodiscard]] std::size_t node_count() const;

    // -----------------------------------------------------------------------
    // Key routing
    // -----------------------------------------------------------------------

    /// Determine which physical node owns `key`.
    /// Returns std::nullopt when the ring is empty.
    [[nodiscard]] std::optional<std::string> get_node(std::string_view key) const;

    /// Return the N successor nodes for `key` (for replication).
    /// The returned list will have at most `count` *distinct* physical nodes.
    [[nodiscard]] std::vector<std::string> get_nodes(std::string_view key,
                                                     std::size_t count) const;

    // -----------------------------------------------------------------------
    // Hashing
    // -----------------------------------------------------------------------

    /// Exposed for testing.  Uses MurmurHash3-like finalizer on a seed built
    /// from the input bytes.
    [[nodiscard]] static uint32_t hash(std::string_view data);

private:
    std::size_t                        num_virtual_nodes_;
    mutable std::mutex                 mutex_;

    // ring_  maps  hash-value -> physical node ID.
    // Using std::map gives us O(log N) lookup with upper_bound / wrap-around.
    std::map<uint32_t, std::string>    ring_;

    // Track which physical nodes are present.
    std::vector<std::string>           physical_nodes_;

    /// Compute the hash for a virtual node.
    [[nodiscard]] static uint32_t virtual_node_hash(const std::string& node_id,
                                                    std::size_t index);
};

}  // namespace kvstore

#endif  // KVSTORE_CONSISTENT_HASH_H
