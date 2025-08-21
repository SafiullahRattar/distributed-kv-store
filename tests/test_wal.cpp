#include "wal.h"

#include <cstdio>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>

using kvstore::WALRecordType;
using kvstore::WriteAheadLog;

namespace {

// RAII helper that creates a temp file path and removes it on destruction.
class TempFile {
public:
    TempFile() : path_(std::filesystem::temp_directory_path() /
                       ("test_wal_" + std::to_string(counter_++) + ".wal")) {}
    ~TempFile() { std::filesystem::remove(path_); }

    [[nodiscard]] std::string path() const { return path_.string(); }

private:
    static inline int counter_ = 0;
    std::filesystem::path path_;
};

}  // anonymous namespace

// ---------------------------------------------------------------------------
// Basic write + replay
// ---------------------------------------------------------------------------

TEST(WALTest, WriteAndReplay) {
    TempFile tmp;

    {
        WriteAheadLog wal(tmp.path());
        wal.log_put("name", "Alice");
        wal.log_put("age", "30");
        wal.log_delete("name");
        EXPECT_EQ(wal.record_count(), 3u);
    }

    auto records = WriteAheadLog::replay(tmp.path());
    ASSERT_EQ(records.size(), 3u);

    EXPECT_EQ(records[0].type, WALRecordType::PUT);
    EXPECT_EQ(records[0].key,  "name");
    EXPECT_EQ(records[0].value, "Alice");

    EXPECT_EQ(records[1].type, WALRecordType::PUT);
    EXPECT_EQ(records[1].key,  "age");
    EXPECT_EQ(records[1].value, "30");

    EXPECT_EQ(records[2].type, WALRecordType::DELETE);
    EXPECT_EQ(records[2].key,  "name");
    EXPECT_TRUE(records[2].value.empty());
}

TEST(WALTest, EmptyReplay) {
    auto records = WriteAheadLog::replay("/nonexistent/path/wal.log");
    EXPECT_TRUE(records.empty());
}

// ---------------------------------------------------------------------------
// Durability across open/close cycles
// ---------------------------------------------------------------------------

TEST(WALTest, AppendAcrossRestarts) {
    TempFile tmp;

    {
        WriteAheadLog wal(tmp.path());
        wal.log_put("k1", "v1");
    }

    // "Restart" -- open the same file and append more
    {
        WriteAheadLog wal(tmp.path());
        wal.log_put("k2", "v2");
    }

    auto records = WriteAheadLog::replay(tmp.path());
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[0].key, "k1");
    EXPECT_EQ(records[1].key, "k2");
}

// ---------------------------------------------------------------------------
// Truncation
// ---------------------------------------------------------------------------

TEST(WALTest, Truncate) {
    TempFile tmp;

    WriteAheadLog wal(tmp.path());
    wal.log_put("a", "1");
    wal.log_put("b", "2");
    EXPECT_EQ(wal.record_count(), 2u);

    wal.truncate();
    EXPECT_EQ(wal.record_count(), 0u);

    wal.log_put("c", "3");
    EXPECT_EQ(wal.record_count(), 1u);

    auto records = WriteAheadLog::replay(tmp.path());
    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0].key, "c");
}

// ---------------------------------------------------------------------------
// CRC integrity
// ---------------------------------------------------------------------------

TEST(WALTest, CorruptedRecordIsSkipped) {
    TempFile tmp;

    {
        WriteAheadLog wal(tmp.path());
        wal.log_put("good", "data");
    }

    // Corrupt the file by flipping a byte in the middle
    {
        std::fstream f(tmp.path(), std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(f.is_open());
        f.seekp(6, std::ios::beg);  // skip into the CRC/type area
        char byte = 0xFF;
        f.write(&byte, 1);
    }

    // Replay should detect the CRC mismatch and return 0 records
    auto records = WriteAheadLog::replay(tmp.path());
    EXPECT_TRUE(records.empty());
}

// ---------------------------------------------------------------------------
// Large values
// ---------------------------------------------------------------------------

TEST(WALTest, LargeKeyAndValue) {
    TempFile tmp;

    std::string big_key(1024, 'K');
    std::string big_value(1024 * 64, 'V');  // 64 KB

    {
        WriteAheadLog wal(tmp.path());
        wal.log_put(big_key, big_value);
    }

    auto records = WriteAheadLog::replay(tmp.path());
    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0].key, big_key);
    EXPECT_EQ(records[0].value, big_value);
}

// ---------------------------------------------------------------------------
// Multiple record types interleaved
// ---------------------------------------------------------------------------

TEST(WALTest, InterleavedPutsAndDeletes) {
    TempFile tmp;

    {
        WriteAheadLog wal(tmp.path());
        for (int i = 0; i < 100; ++i) {
            wal.log_put("key" + std::to_string(i), "val" + std::to_string(i));
            if (i % 3 == 0) {
                wal.log_delete("key" + std::to_string(i));
            }
        }
    }

    auto records = WriteAheadLog::replay(tmp.path());
    // 100 puts + 34 deletes (i=0,3,6,...,99)
    EXPECT_EQ(records.size(), 134u);
}
