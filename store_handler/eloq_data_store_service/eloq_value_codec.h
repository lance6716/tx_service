/**
 *    Copyright (C) 2026 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */

#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace EloqDS
{
namespace EloqValueCodec
{

// Use the most significant bit of the Eloq logical timestamp to mark that an
// encoded value carries an explicit TTL field. The remaining 63 bits store the
// original Eloq timestamp; TiKV commit timestamps must never be substituted
// here.
constexpr uint64_t kTTLFlagMask = 1ULL << 63;
constexpr uint64_t kTimestampMask = ~kTTLFlagMask;

struct DecodedValue
{
    std::string record;
    uint64_t ts{0};
    uint64_t ttl{0};
};

inline uint64_t EncodeHasTTLIntoTs(uint64_t ts, bool has_ttl)
{
    assert((ts & kTTLFlagMask) == 0 &&
           "Eloq logical timestamp MSB is reserved for TTL");
    return has_ttl ? (ts | kTTLFlagMask) : ts;
}

inline std::pair<uint64_t, bool> DecodeHasTTLFromTs(uint64_t encoded_ts)
{
    const bool has_ttl = (encoded_ts & kTTLFlagMask) != 0;
    return {encoded_ts & kTimestampMask, has_ttl};
}

inline void AppendUint64(std::string &out, uint64_t value)
{
    const char *data = reinterpret_cast<const char *>(&value);
    out.append(data, sizeof(value));
}

inline uint64_t ReadUint64(std::string_view value, size_t offset)
{
    uint64_t result = 0;
    std::memcpy(&result, value.data() + offset, sizeof(result));
    return result;
}

inline std::string EncodeValue(
    const std::vector<std::string_view> &record_parts,
    uint64_t ts,
    uint64_t ttl)
{
    const bool has_ttl = ttl > 0;
    size_t value_size = sizeof(uint64_t) + (has_ttl ? sizeof(uint64_t) : 0);
    for (std::string_view part : record_parts)
    {
        value_size += part.size();
    }

    std::string value;
    value.reserve(value_size);
    AppendUint64(value, EncodeHasTTLIntoTs(ts, has_ttl));
    if (has_ttl)
    {
        AppendUint64(value, ttl);
    }
    for (std::string_view part : record_parts)
    {
        value.append(part.data(), part.size());
    }
    return value;
}

inline std::string EncodeValue(std::string_view record,
                               uint64_t ts,
                               uint64_t ttl)
{
    return EncodeValue(std::vector<std::string_view>{record}, ts, ttl);
}

inline DecodedValue DecodeValue(std::string_view value)
{
    if (value.size() < sizeof(uint64_t))
    {
        throw std::invalid_argument("Eloq encoded value is missing timestamp");
    }

    size_t offset = 0;
    auto [ts, has_ttl] = DecodeHasTTLFromTs(ReadUint64(value, offset));
    offset += sizeof(uint64_t);

    uint64_t ttl = 0;
    if (has_ttl)
    {
        if (value.size() < sizeof(uint64_t) * 2)
        {
            throw std::invalid_argument("Eloq encoded value is missing TTL");
        }
        ttl = ReadUint64(value, offset);
        offset += sizeof(uint64_t);
    }

    return DecodedValue{
        std::string(value.data() + offset, value.size() - offset), ts, ttl};
}

}  // namespace EloqValueCodec
}  // namespace EloqDS
