#include "wal.h"

#include <cstring>
#include <stdexcept>

namespace kvstore {

// ---------------------------------------------------------------------------
// CRC-32 (ISO 3309 polynomial, same as zlib/gzip)
// ---------------------------------------------------------------------------
namespace {

constexpr uint32_t kCrcTable[] = {
    0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA, 0x076DC419, 0x706AF48F,
    0xE963A535, 0x9E6495A3, 0x0EDB8832, 0x79DCB8A4, 0xE0D5E91B, 0x97D2D988,
    0x09B64C2B, 0x7EB17CBB, 0xE7B82D09, 0x90BF1D91, 0x1DB71064, 0x6AB020F2,
    0xF3B97148, 0x84BE41DE, 0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7,
    0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC, 0x14015C4F, 0x63066CD9,
    0xFA0F3D63, 0x8D080DF5, 0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172,
    0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B, 0x35B5A8FA, 0x42B2986C,
    0xDBBBB9D6, 0xACBCB9C2, 0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59,
    0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116, 0x21B4F4B5, 0x56B3C423,
    0xCFBA9599, 0xB8BDA50F, 0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924,
    0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D, 0x76DC4190, 0x01DB7106,
    0x98D220BC, 0xEFD5102A, 0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433,
    0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818, 0x7F6A0D69, 0x086D3D2B,
    0x91646C97, 0xE6635C01, 0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E,
    0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457, 0x65B0D9C6, 0x12B7E950,
    0x8BBEB8EA, 0xFCB9887C, 0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65,
    0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2, 0x4ADFA541, 0x3DD895D7,
    0xA4D1C46D, 0xD3D6F4FB, 0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0,
    0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7822, 0x3B6E20C8, 0x4C69105E,
    0xD56041E4, 0xA2677172, 0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B,
    0x35B5A8FA, 0x42B2986C, 0xDBBBB9D6, 0xACBCB9C2, 0x32D86CE3, 0x45DF5C75,
    0xDCD60DCF, 0xABD13D59, 0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116,
    0x21B4F4B5, 0x56B3C423, 0xCFBA9599, 0xB8BDA50F, 0x2802B89E, 0x5F058808,
    0xC60CD9B2, 0xB10BE924, 0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D,
    0x76DC4190, 0x01DB7106, 0x98D220BC, 0xEFD5102A, 0x71B18589, 0x06B6B51F,
    0x9FBFE4A5, 0xE8B8D433, 0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818,
    0x7F6A0D69, 0x086D3D2B, 0x91646C97, 0xE6635C01, 0x6B6B51F4, 0x1C6C6162,
    0x856530D8, 0xF262004E, 0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457,
    0x65B0D9C6, 0x12B7E950, 0x8BBEB8EA, 0xFCB9887C, 0x62DD1DDF, 0x15DA2D49,
    0x8CD37CF3, 0xFBD44C65, 0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2,
    0x4ADFA541, 0x3DD895D7, 0xA4D1C46D, 0xD3D6F4FB, 0x4369E96A, 0x346ED9FC,
    0xAD678846, 0xDA60B8D0, 0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7822,
    0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A, 0x9C0906A9, 0xEB0E363F,
    0x72076785, 0x05005713, 0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38,
    0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21, 0x86D3D2D4, 0xF1D4E242,
    0x68DDB3F6, 0x1FDA836E, 0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777,
    0x88085AE6, 0xFF0F6B70, 0x66063BCA, 0x11010B5C, 0x8F659EFF, 0xF862AE69,
    0x616BFFD3, 0x166CCF45, 0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2,
    0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB, 0xAE0B1073, 0xD90C2060,
    0x40054A19, 0x37024B8F, 0x09B64C2B, 0x7EB17CBB, 0xE7B82D09, 0x90BF1D91,
    0x1DB71064, 0x6AB020F2, 0xF3B97148, 0x84BE41DE, 0x1ADAD47D, 0x6DDDE4EB,
    0xF4D4B551, 0x83D385C7, 0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC,
    0x14015C4F, 0x63066CD9, 0xFA0F3D63, 0x8D080DF5,
};

uint32_t crc32_update(uint32_t crc, const void* data, std::size_t length) {
    auto* bytes = static_cast<const uint8_t*>(data);
    crc = ~crc;
    for (std::size_t i = 0; i < length; ++i) {
        crc = kCrcTable[(crc ^ bytes[i]) & 0xFF] ^ (crc >> 8);
    }
    return ~crc;
}

// Helpers for little-endian serialization
void write_u32(std::ostream& os, uint32_t v) {
    uint8_t buf[4];
    buf[0] = static_cast<uint8_t>(v);
    buf[1] = static_cast<uint8_t>(v >> 8);
    buf[2] = static_cast<uint8_t>(v >> 16);
    buf[3] = static_cast<uint8_t>(v >> 24);
    os.write(reinterpret_cast<const char*>(buf), 4);
}

uint32_t read_u32(std::istream& is) {
    uint8_t buf[4];
    is.read(reinterpret_cast<char*>(buf), 4);
    return static_cast<uint32_t>(buf[0])
         | (static_cast<uint32_t>(buf[1]) << 8)
         | (static_cast<uint32_t>(buf[2]) << 16)
         | (static_cast<uint32_t>(buf[3]) << 24);
}

void write_u8(std::ostream& os, uint8_t v) {
    os.write(reinterpret_cast<const char*>(&v), 1);
}

uint8_t read_u8(std::istream& is) {
    uint8_t v = 0;
    is.read(reinterpret_cast<char*>(&v), 1);
    return v;
}

}  // anonymous namespace

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

WriteAheadLog::WriteAheadLog(const std::string& path)
    : path_(path)
    , file_(path, std::ios::binary | std::ios::app)
{
    if (!file_.is_open()) {
        throw std::runtime_error("WAL: failed to open " + path);
    }
}

WriteAheadLog::~WriteAheadLog() {
    if (file_.is_open()) {
        file_.flush();
        file_.close();
    }
}

// ---------------------------------------------------------------------------
// Append
// ---------------------------------------------------------------------------

void WriteAheadLog::log_put(std::string_view key, std::string_view value) {
    append_record(WALRecordType::PUT, key, value);
}

void WriteAheadLog::log_delete(std::string_view key) {
    append_record(WALRecordType::DELETE, key, "");
}

void WriteAheadLog::sync() {
    std::lock_guard lock(mutex_);
    file_.flush();
}

void WriteAheadLog::append_record(WALRecordType type,
                                  std::string_view key,
                                  std::string_view value) {
    std::lock_guard lock(mutex_);

    uint32_t crc = compute_crc(type, key, value);

    write_u32(file_, crc);
    write_u8(file_, static_cast<uint8_t>(type));
    write_u32(file_, static_cast<uint32_t>(key.size()));
    write_u32(file_, static_cast<uint32_t>(value.size()));
    file_.write(key.data(), static_cast<std::streamsize>(key.size()));
    file_.write(value.data(), static_cast<std::streamsize>(value.size()));
    file_.flush();

    ++record_count_;
}

uint32_t WriteAheadLog::compute_crc(WALRecordType type,
                                    std::string_view key,
                                    std::string_view value) {
    uint32_t crc = 0;
    auto t = static_cast<uint8_t>(type);
    crc = crc32_update(crc, &t, 1);

    uint32_t klen = static_cast<uint32_t>(key.size());
    uint32_t vlen = static_cast<uint32_t>(value.size());
    crc = crc32_update(crc, &klen, 4);
    crc = crc32_update(crc, &vlen, 4);
    crc = crc32_update(crc, key.data(), key.size());
    crc = crc32_update(crc, value.data(), value.size());
    return crc;
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

std::vector<WALRecord> WriteAheadLog::replay(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        return {};  // no WAL on disk yet -- fresh start
    }

    std::vector<WALRecord> records;

    while (file.peek() != EOF) {
        auto saved_pos = file.tellg();

        // Read header
        uint32_t stored_crc = read_u32(file);
        if (!file.good()) break;

        uint8_t raw_type = read_u8(file);
        if (!file.good()) break;

        uint32_t key_len = read_u32(file);
        if (!file.good()) break;

        uint32_t val_len = read_u32(file);
        if (!file.good()) break;

        // Sanity check lengths (guard against garbage at end of file)
        if (key_len > 64 * 1024 * 1024 || val_len > 64 * 1024 * 1024) {
            break;  // treat as corruption
        }

        std::string key(key_len, '\0');
        std::string value(val_len, '\0');
        file.read(key.data(), key_len);
        if (!file.good() && static_cast<std::size_t>(file.gcount()) != key_len) break;
        file.read(value.data(), val_len);
        if (!file.good() && static_cast<std::size_t>(file.gcount()) != val_len) break;

        // Verify CRC
        auto type = static_cast<WALRecordType>(raw_type);
        uint32_t computed_crc = compute_crc(type, key, value);
        if (computed_crc != stored_crc) {
            // Corruption detected -- stop replaying.  This typically means the
            // process crashed mid-write and the tail of the log is partial.
            break;
        }

        records.push_back({type, std::move(key), std::move(value)});
    }

    return records;
}

// ---------------------------------------------------------------------------
// Truncate
// ---------------------------------------------------------------------------

void WriteAheadLog::truncate() {
    std::lock_guard lock(mutex_);
    file_.close();
    // Re-open in truncation mode
    file_.open(path_, std::ios::binary | std::ios::trunc);
    if (!file_.is_open()) {
        throw std::runtime_error("WAL: failed to truncate " + path_);
    }
    record_count_ = 0;
}

}  // namespace kvstore
