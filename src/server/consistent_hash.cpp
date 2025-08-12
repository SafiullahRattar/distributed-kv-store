#include "consistent_hash.h"

#include <algorithm>
#include <set>
#include <sstream>

namespace kvstore {

// ---------------------------------------------------------------------------
// Hashing
//
// We use a simple but effective hash based on FNV-1a combined with a
// MurmurHash3-style bit finalizer.  This is deterministic, fast, and gives
// good distribution -- perfect for consistent hashing.
// ---------------------------------------------------------------------------

uint32_t ConsistentHashRing::hash(std::string_view data) {
    // FNV-1a 32-bit
    uint32_t h = 2166136261u;
    for (auto c : data) {
        h ^= static_cast<uint32_t>(static_cast<uint8_t>(c));
        h *= 16777619u;
    }
    // MurmurHash3 finalizer (improves avalanche)
    h ^= h >> 16;
    h *= 0x85EBCA6Bu;
    h ^= h >> 13;
    h *= 0xC2B2AE35u;
    h ^= h >> 16;
    return h;
}

uint32_t ConsistentHashRing::virtual_node_hash(const std::string& node_id,
                                               std::size_t index) {
    // Build a string like "node-1:50051#42" to hash
    std::ostringstream oss;
    oss << node_id << '#' << index;
    return hash(oss.str());
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

ConsistentHashRing::ConsistentHashRing(std::size_t num_virtual_nodes)
    : num_virtual_nodes_(num_virtual_nodes)
{}

// ---------------------------------------------------------------------------
// Ring membership
// ---------------------------------------------------------------------------

void ConsistentHashRing::add_node(const std::string& node_id) {
    std::lock_guard lock(mutex_);
    // Prevent duplicate adds
    if (std::find(physical_nodes_.begin(), physical_nodes_.end(), node_id)
            != physical_nodes_.end()) {
        return;
    }
    physical_nodes_.push_back(node_id);
    for (std::size_t i = 0; i < num_virtual_nodes_; ++i) {
        uint32_t h = virtual_node_hash(node_id, i);
        ring_[h] = node_id;
    }
}

void ConsistentHashRing::remove_node(const std::string& node_id) {
    std::lock_guard lock(mutex_);
    physical_nodes_.erase(
        std::remove(physical_nodes_.begin(), physical_nodes_.end(), node_id),
        physical_nodes_.end());

    for (std::size_t i = 0; i < num_virtual_nodes_; ++i) {
        uint32_t h = virtual_node_hash(node_id, i);
        ring_.erase(h);
    }
}

bool ConsistentHashRing::has_node(const std::string& node_id) const {
    std::lock_guard lock(mutex_);
    return std::find(physical_nodes_.begin(), physical_nodes_.end(), node_id)
           != physical_nodes_.end();
}

std::vector<std::string> ConsistentHashRing::nodes() const {
    std::lock_guard lock(mutex_);
    return physical_nodes_;
}

std::size_t ConsistentHashRing::node_count() const {
    std::lock_guard lock(mutex_);
    return physical_nodes_.size();
}

// ---------------------------------------------------------------------------
// Key routing
// ---------------------------------------------------------------------------

std::optional<std::string> ConsistentHashRing::get_node(std::string_view key) const {
    std::lock_guard lock(mutex_);
    if (ring_.empty()) {
        return std::nullopt;
    }

    uint32_t h = hash(key);

    // Find the first virtual node whose hash is >= h
    auto it = ring_.lower_bound(h);
    if (it == ring_.end()) {
        // Wrap around to the first entry on the ring
        it = ring_.begin();
    }
    return it->second;
}

std::vector<std::string> ConsistentHashRing::get_nodes(std::string_view key,
                                                       std::size_t count) const {
    std::lock_guard lock(mutex_);
    std::vector<std::string> result;
    if (ring_.empty()) {
        return result;
    }

    // Cap at the number of distinct physical nodes
    count = std::min(count, physical_nodes_.size());

    uint32_t h = hash(key);
    auto it = ring_.lower_bound(h);
    if (it == ring_.end()) {
        it = ring_.begin();
    }

    std::set<std::string> seen;
    auto start = it;
    bool wrapped = false;

    while (result.size() < count) {
        if (seen.find(it->second) == seen.end()) {
            seen.insert(it->second);
            result.push_back(it->second);
        }
        ++it;
        if (it == ring_.end()) {
            it = ring_.begin();
            wrapped = true;
        }
        // Safety: if we've gone all the way around, stop
        if (wrapped && it == start) {
            break;
        }
    }

    return result;
}

}  // namespace kvstore
