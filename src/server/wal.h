#ifndef KVSTORE_WAL_H
#define KVSTORE_WAL_H

#include <cstdint>
#include <fstream>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace kvstore {

// ============================================================================
// Write-Ahead Log (WAL)
//
// Binary, append-only log that records every mutation before it is applied to
// the in-memory store.  On recovery the log is replayed to restore state.
//
// Record layout (little-endian):
//   [4 bytes] CRC-32 of (type + key_len + val_len + key + value)
//   [1 byte ] record type  (PUT = 1, DELETE = 2)
//   [4 bytes] key length
//   [4 bytes] value length  (0 for DELETE)
//   [N bytes] key
//   [M bytes] value
// ============================================================================

enum class WALRecordType : uint8_t {
    PUT    = 1,
    DELETE = 2,
};

struct WALRecord {
    WALRecordType type;
    std::string   key;
    std::string   value;  // empty for DELETE
};

class WriteAheadLog {
public:
    /// Open (or create) the WAL file at `path`.
    explicit WriteAheadLog(const std::string& path);
    ~WriteAheadLog();

    // Non-copyable
    WriteAheadLog(const WriteAheadLog&)            = delete;
    WriteAheadLog& operator=(const WriteAheadLog&) = delete;

    // -----------------------------------------------------------------------
    // Append operations
    // -----------------------------------------------------------------------

    /// Log a PUT before applying it.
    void log_put(std::string_view key, std::string_view value);

    /// Log a DELETE before applying it.
    void log_delete(std::string_view key);

    /// Flush all buffered data to the OS.
    void sync();

    // -----------------------------------------------------------------------
    // Recovery
    // -----------------------------------------------------------------------

    /// Read every valid record from the WAL file.
    /// Records with a bad CRC are silently skipped (tail corruption).
    [[nodiscard]] static std::vector<WALRecord> replay(const std::string& path);

    // -----------------------------------------------------------------------
    // Maintenance
    // -----------------------------------------------------------------------

    /// Truncate the WAL (e.g. after a successful snapshot/compaction).
    void truncate();

    /// Number of records written since last truncation.
    [[nodiscard]] uint64_t record_count() const { return record_count_; }

    /// Path of the WAL file on disk.
    [[nodiscard]] const std::string& path() const { return path_; }

private:
    void append_record(WALRecordType type,
                       std::string_view key,
                       std::string_view value);

    static uint32_t compute_crc(WALRecordType type,
                                std::string_view key,
                                std::string_view value);

    std::string   path_;
    std::ofstream file_;
    std::mutex    mutex_;
    uint64_t      record_count_ = 0;
};

}  // namespace kvstore

#endif  // KVSTORE_WAL_H
