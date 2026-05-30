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

#include <cstdint>
#include <cstring>
#include <exception>
#include <string>
#include <string_view>
#include <vector>

#include "eloq_value_codec.h"

namespace EloqDS
{

struct ExpiredTtlScanItem
{
    std::string key;
    std::string value;
};

struct ExpiredTtlCleanupCandidate
{
    std::string physical_key;
    std::string logical_key;
    uint64_t record_ts{0};
    uint64_t ttl{0};
};

struct ExpiredTtlCandidateScanBatch
{
    std::vector<ExpiredTtlCleanupCandidate> candidates;
    uint32_t scanned_items{0};
    uint32_t expired_items{0};
    uint32_t skipped_items{0};
    uint32_t malformed_items{0};
    bool range_finished{true};
    bool ok{true};
    std::string next_cursor;
    std::string error_message;
};

enum class ExpiredTtlDeleteDecision
{
    Delete,
    Skip,
    RetiredTombstone,
    Malformed
};

struct ExpiredTtlDeleteCheck
{
    ExpiredTtlDeleteDecision decision{ExpiredTtlDeleteDecision::Skip};
    uint64_t record_ts{0};
    uint64_t ttl{0};
};

struct ExpiredTtlCleanupRunResult
{
    ExpiredTtlCandidateScanBatch scan_batch;
    uint32_t reread_items{0};
    uint32_t not_found_items{0};
    uint32_t delete_skipped_items{0};
    uint32_t delete_malformed_items{0};
    uint32_t delete_attempt_items{0};
    uint32_t deleted_items{0};
    bool range_finished{true};
    bool ok{true};
    std::string next_cursor;
    std::string error_message;
};

inline bool StartsWithForExpiredTtlCleanup(std::string_view value,
                                           std::string_view prefix)
{
    return value.size() >= prefix.size() &&
           value.compare(0, prefix.size(), prefix) == 0;
}

inline bool IsBaseTableForExpiredTtlCleanup(std::string_view table_name)
{
    // Archive cleanup has a different safety watermark. The base TTL helper is
    // deliberately scoped to normal base/index table prefixes only.
    return !table_name.empty() && table_name != "mvcc_archives";
}

inline std::string BuildExpiredTtlPartitionPrefix(std::string_view table_name,
                                                  int32_t partition_id)
{
    std::string prefix;
    prefix.reserve(table_name.size() + 1 + 11 + 1);
    prefix.append(table_name.data(), table_name.size());
    prefix.push_back('/');
    prefix.append(std::to_string(partition_id));
    prefix.push_back('/');
    return prefix;
}

inline std::string KeyAfterForExpiredTtlCleanup(std::string_view key)
{
    std::string next(key.data(), key.size());
    next.push_back('\0');
    return next;
}

inline std::string PrefixUpperBoundForExpiredTtlCleanup(std::string_view prefix)
{
    std::string bound(prefix.data(), prefix.size());
    for (auto it = bound.rbegin(); it != bound.rend(); ++it)
    {
        auto byte = static_cast<unsigned char>(*it);
        if (byte != 0xff)
        {
            *it = static_cast<char>(byte + 1);
            bound.erase(it.base(), bound.end());
            return bound;
        }
    }
    return "";
}

inline std::string NormalizeExpiredTtlScanCursor(std::string_view prefix,
                                                 std::string_view cursor)
{
    if (!cursor.empty() && StartsWithForExpiredTtlCleanup(cursor, prefix))
    {
        return std::string(cursor.data(), cursor.size());
    }
    return std::string(prefix.data(), prefix.size());
}

inline bool IsExpiredForCleanup(uint64_t ttl, uint64_t now_ms)
{
    return ttl > 0 && ttl < now_ms;
}

inline bool IsRetiredTombstoneRecord(std::string_view record)
{
    // DataStoreServiceClient::SerializeTxRecord(true, nullptr) encodes a
    // retired tombstone as exactly one bool byte. Check the canonical true byte
    // explicitly instead of interpreting arbitrary one-byte payloads as bools:
    // object-table values and unit tests may contain single data bytes.
    const bool is_deleted = true;
    return record.size() == sizeof(is_deleted) &&
           std::memcmp(record.data(), &is_deleted, sizeof(is_deleted)) == 0;
}

inline ExpiredTtlDeleteCheck CheckExpiredTtlDeleteCandidate(
    std::string_view current_value,
    uint64_t now_ms)
{
    try
    {
        auto decoded = EloqValueCodec::DecodeValue(current_value);
        if (IsRetiredTombstoneRecord(decoded.record))
        {
            return {ExpiredTtlDeleteDecision::RetiredTombstone,
                    decoded.ts,
                    decoded.ttl};
        }
        if (IsExpiredForCleanup(decoded.ttl, now_ms))
        {
            return {ExpiredTtlDeleteDecision::Delete,
                    decoded.ts,
                    decoded.ttl};
        }
        return {ExpiredTtlDeleteDecision::Skip, decoded.ts, decoded.ttl};
    }
    catch (const std::exception &)
    {
        return {ExpiredTtlDeleteDecision::Malformed, 0, 0};
    }
}

inline ExpiredTtlCandidateScanBatch CollectExpiredTtlCandidatesFromScan(
    std::string_view physical_prefix,
    const std::vector<ExpiredTtlScanItem> &scan_items,
    bool scan_has_more,
    std::string_view scan_next_cursor,
    uint64_t now_ms,
    uint32_t max_candidates)
{
    ExpiredTtlCandidateScanBatch batch;
    batch.range_finished = true;

    if (max_candidates == 0)
    {
        batch.range_finished = false;
        batch.next_cursor = NormalizeExpiredTtlScanCursor(physical_prefix, "");
        return batch;
    }

    std::string next_cursor;
    for (const ExpiredTtlScanItem &item : scan_items)
    {
        if (!StartsWithForExpiredTtlCleanup(item.key, physical_prefix))
        {
            batch.range_finished = true;
            batch.next_cursor.clear();
            return batch;
        }

        ++batch.scanned_items;
        next_cursor = KeyAfterForExpiredTtlCleanup(item.key);

        try
        {
            auto decoded = EloqValueCodec::DecodeValue(item.value);
            if (IsRetiredTombstoneRecord(decoded.record))
            {
                ++batch.skipped_items;
            }
            else if (IsExpiredForCleanup(decoded.ttl, now_ms))
            {
                ++batch.expired_items;
                batch.candidates.push_back(ExpiredTtlCleanupCandidate{
                    item.key,
                    item.key.substr(physical_prefix.size()),
                    decoded.ts,
                    decoded.ttl});
                if (batch.candidates.size() >= max_candidates)
                {
                    batch.range_finished = false;
                    batch.next_cursor = next_cursor;
                    return batch;
                }
            }
            else
            {
                ++batch.skipped_items;
            }
        }
        catch (const std::exception &)
        {
            // Malformed/truncated values are not safe cleanup candidates. Keep
            // scanning conservatively, but expose the count for observability.
            ++batch.skipped_items;
            ++batch.malformed_items;
        }
    }

    if (scan_items.empty())
    {
        batch.range_finished = true;
        return batch;
    }

    if (scan_has_more)
    {
        batch.range_finished = false;
        if (!scan_next_cursor.empty() &&
            StartsWithForExpiredTtlCleanup(scan_next_cursor, physical_prefix))
        {
            batch.next_cursor.assign(scan_next_cursor.data(),
                                     scan_next_cursor.size());
        }
        else
        {
            batch.next_cursor = std::move(next_cursor);
        }
    }
    return batch;
}

}  // namespace EloqDS
