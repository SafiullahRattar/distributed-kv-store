#ifndef KVSTORE_STORAGE_H
#define KVSTORE_STORAGE_H

#include <cstdint>
#include <functional>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace kvstore {

// ============================================================================
// ThreadSafeStorage
//
// In-memory key-value store backed by an unordered_map and protected by a
// shared (reader-writer) mutex.  Reads acquire a shared lock so they can
// proceed concurrently; writes acquire an exclusive lock.
// ============================================================================
class ThreadSafeStorage {
public:
    ThreadSafeStorage() = default;

    // Non-copyable, non-movable (contains a mutex)
    ThreadSafeStorage(const ThreadSafeStorage&)            = delete;
    ThreadSafeStorage& operator=(const ThreadSafeStorage&) = delete;

    // -----------------------------------------------------------------------
    // Core operations
    // -----------------------------------------------------------------------

    /// Retrieve the value for `key`.  Returns std::nullopt when missing.
    [[nodiscard]] std::optional<std::string> get(std::string_view key) const;

    /// Insert or overwrite `key` with `value`.
    void put(std::string_view key, std::string_view value);

    /// Erase `key`.  Returns true if the key existed.
    bool erase(std::string_view key);

    /// Number of entries currently stored.
    [[nodiscard]] std::size_t size() const;

    /// True when the map is empty.
    [[nodiscard]] bool empty() const;

    // -----------------------------------------------------------------------
    // Bulk helpers (used during rebalancing / snapshotting)
    // -----------------------------------------------------------------------

    /// Return a snapshot of all key-value pairs.
    [[nodiscard]] std::vector<std::pair<std::string, std::string>> snapshot() const;

    /// Apply a predicate to every entry; collect those that match.
    [[nodiscard]] std::vector<std::pair<std::string, std::string>>
    collect_if(std::function<bool(const std::string& key)> predicate) const;

    /// Remove all entries that satisfy `predicate`, returning the removed pairs.
    std::vector<std::pair<std::string, std::string>>
    remove_if(std::function<bool(const std::string& key)> predicate);

    /// Remove all entries.
    void clear();

private:
    mutable std::shared_mutex                             mutex_;
    std::unordered_map<std::string, std::string>          data_;
};

}  // namespace kvstore

#endif  // KVSTORE_STORAGE_H
