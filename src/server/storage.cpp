#include "storage.h"

#include <algorithm>

namespace kvstore {

std::optional<std::string> ThreadSafeStorage::get(std::string_view key) const {
    std::shared_lock lock(mutex_);
    if (auto it = data_.find(std::string(key)); it != data_.end()) {
        return it->second;
    }
    return std::nullopt;
}

void ThreadSafeStorage::put(std::string_view key, std::string_view value) {
    std::unique_lock lock(mutex_);
    data_.insert_or_assign(std::string(key), std::string(value));
}

bool ThreadSafeStorage::erase(std::string_view key) {
    std::unique_lock lock(mutex_);
    return data_.erase(std::string(key)) > 0;
}

std::size_t ThreadSafeStorage::size() const {
    std::shared_lock lock(mutex_);
    return data_.size();
}

bool ThreadSafeStorage::empty() const {
    std::shared_lock lock(mutex_);
    return data_.empty();
}

std::vector<std::pair<std::string, std::string>> ThreadSafeStorage::snapshot() const {
    std::shared_lock lock(mutex_);
    std::vector<std::pair<std::string, std::string>> pairs;
    pairs.reserve(data_.size());
    for (const auto& [k, v] : data_) {
        pairs.emplace_back(k, v);
    }
    return pairs;
}

std::vector<std::pair<std::string, std::string>>
ThreadSafeStorage::collect_if(std::function<bool(const std::string& key)> predicate) const {
    std::shared_lock lock(mutex_);
    std::vector<std::pair<std::string, std::string>> result;
    for (const auto& [k, v] : data_) {
        if (predicate(k)) {
            result.emplace_back(k, v);
        }
    }
    return result;
}

std::vector<std::pair<std::string, std::string>>
ThreadSafeStorage::remove_if(std::function<bool(const std::string& key)> predicate) {
    std::unique_lock lock(mutex_);
    std::vector<std::pair<std::string, std::string>> removed;
    for (auto it = data_.begin(); it != data_.end(); ) {
        if (predicate(it->first)) {
            removed.emplace_back(it->first, it->second);
            it = data_.erase(it);
        } else {
            ++it;
        }
    }
    return removed;
}

void ThreadSafeStorage::clear() {
    std::unique_lock lock(mutex_);
    data_.clear();
}

}  // namespace kvstore
