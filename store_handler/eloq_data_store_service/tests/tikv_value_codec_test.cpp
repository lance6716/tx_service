#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "eloq_value_codec.h"

namespace EloqDS::tests
{
namespace
{

uint64_t ReadNativeUint64(std::string_view value, size_t offset)
{
    uint64_t result = 0;
    std::memcpy(&result, value.data() + offset, sizeof(result));
    return result;
}

std::string BuildRocksDBCompatibleValue(
    const std::vector<std::string_view> &record_parts,
    uint64_t ts,
    uint64_t ttl)
{
    const bool has_ttl = ttl > 0;
    uint64_t encoded_ts = has_ttl ? (ts | EloqValueCodec::kTTLFlagMask) : ts;

    std::string value;
    value.append(reinterpret_cast<const char *>(&encoded_ts),
                 sizeof(encoded_ts));
    if (has_ttl)
    {
        value.append(reinterpret_cast<const char *>(&ttl), sizeof(ttl));
    }
    for (std::string_view part : record_parts)
    {
        value.append(part.data(), part.size());
    }
    return value;
}

TEST(TikvValueCodecTest, EncodeDecodeWithoutTTL)
{
    const uint64_t ts = 123456789;
    const std::vector<std::string_view> parts{
        "abc", std::string_view("\0x", 2), "def"};

    std::string encoded = EloqValueCodec::EncodeValue(parts, ts, 0);
    EXPECT_EQ(encoded, BuildRocksDBCompatibleValue(parts, ts, 0));
    EXPECT_EQ(ReadNativeUint64(encoded, 0), ts);

    auto decoded = EloqValueCodec::DecodeValue(encoded);
    EXPECT_EQ(decoded.ts, ts);
    EXPECT_EQ(decoded.ttl, 0);
    EXPECT_EQ(decoded.record, std::string("abc\0xdef", 8));
}

TEST(TikvValueCodecTest, EncodeDecodeWithTTL)
{
    const uint64_t ts = 987654321;
    const uint64_t ttl = 1716969600123ULL;
    const std::vector<std::string_view> parts{"record", "-body"};

    std::string encoded = EloqValueCodec::EncodeValue(parts, ts, ttl);
    EXPECT_EQ(encoded, BuildRocksDBCompatibleValue(parts, ts, ttl));

    const uint64_t encoded_ts = ReadNativeUint64(encoded, 0);
    EXPECT_NE(encoded_ts & EloqValueCodec::kTTLFlagMask, 0);
    EXPECT_EQ(ReadNativeUint64(encoded, sizeof(uint64_t)), ttl);

    auto decoded = EloqValueCodec::DecodeValue(encoded);
    EXPECT_EQ(decoded.ts, ts);
    EXPECT_EQ(decoded.ttl, ttl);
    EXPECT_EQ(decoded.record, "record-body");
}

TEST(TikvValueCodecTest, TTLFlagDoesNotCorruptTimestamp)
{
    const uint64_t max_elog_ts = EloqValueCodec::kTimestampMask;
    const uint64_t ttl = 42;

    uint64_t encoded_ts = EloqValueCodec::EncodeHasTTLIntoTs(max_elog_ts, true);
    EXPECT_EQ(encoded_ts, UINT64_MAX);

    auto decoded_ts = EloqValueCodec::DecodeHasTTLFromTs(encoded_ts);
    EXPECT_EQ(decoded_ts.first, max_elog_ts);
    EXPECT_TRUE(decoded_ts.second);

    auto decoded = EloqValueCodec::DecodeValue(
        EloqValueCodec::EncodeValue("", max_elog_ts, ttl));
    EXPECT_EQ(decoded.ts, max_elog_ts);
    EXPECT_EQ(decoded.ttl, ttl);
    EXPECT_TRUE(decoded.record.empty());
}

TEST(TikvValueCodecTest, EmptyRecordBodyRoundTripsWithoutTTL)
{
    auto decoded =
        EloqValueCodec::DecodeValue(EloqValueCodec::EncodeValue("", 7, 0));
    EXPECT_EQ(decoded.ts, 7);
    EXPECT_EQ(decoded.ttl, 0);
    EXPECT_TRUE(decoded.record.empty());
}

TEST(TikvValueCodecTest, RejectsTruncatedValues)
{
    EXPECT_THROW(EloqValueCodec::DecodeValue(std::string(7, 'x')),
                 std::invalid_argument);

    std::string missing_ttl;
    uint64_t encoded_ts = EloqValueCodec::EncodeHasTTLIntoTs(1, true);
    missing_ttl.append(reinterpret_cast<const char *>(&encoded_ts),
                       sizeof(encoded_ts));
    EXPECT_THROW(EloqValueCodec::DecodeValue(missing_ttl),
                 std::invalid_argument);
}

}  // namespace
}  // namespace EloqDS::tests
