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
#include <exception>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "tikv_archive_cleanup_watermark.h"
#include "tikv_expired_ttl_cleanup.h"

namespace EloqDS
{

struct RetiredTombstoneScanItem
{
    std::string key;
    std::string value;
};

struct RetiredTombstoneCleanupCandidate
{
    std::string physical_key;
    std::string logical_key;
    uint64_t record_ts{0};
    uint64_t ttl{0};
};

struct RetiredTombstoneCandidateScanBatch
{
    std::vector<RetiredTombstoneCleanupCandidate> candidates;
    uint32_t scanned_items{0};
    uint32_t candidate_items{0};
    uint32_t skipped_items{0};
    uint32_t malformed_items{0};
    bool range_finished{true};
    bool ok{true};
    std::string next_cursor;
    std::string error_message;
};

inline bool IsBaseTableForRetiredTombstoneCleanup(
    std::string_view table_name)
{
    // mvcc_archives rows are handled by archive-retention cleanup. This helper
    // is deliberately scoped to current base/index table tombstone records.
    return IsBaseTableForExpiredTtlCleanup(table_name);
}

inline bool RetiredTombstoneCleanupEnabled(
    const ArchiveCleanupWatermark &watermark)
{
    return ArchiveOrTombstoneCleanupEnabled(watermark) &&
           watermark.timestamp_ > 0;
}

inline bool IsSafeRetiredTombstoneCandidate(
    const EloqValueCodec::DecodedValue &decoded,
    const ArchiveCleanupWatermark &watermark)
{
    // Keep equality conservative: a snapshot exactly at the safe watermark may
    // still need a tombstone committed at that timestamp for delete visibility.
    return IsRetiredTombstoneRecord(decoded.record) &&
           decoded.ts < watermark.timestamp_;
}

inline RetiredTombstoneCandidateScanBatch
CollectRetiredTombstoneCandidatesFromScan(
    std::string_view physical_prefix,
    const std::vector<RetiredTombstoneScanItem> &scan_items,
    bool scan_has_more,
    std::string_view scan_next_cursor,
    const ArchiveCleanupWatermark &watermark,
    uint32_t max_candidates)
{
    RetiredTombstoneCandidateScanBatch batch;
    batch.range_finished = true;

    if (!RetiredTombstoneCleanupEnabled(watermark))
    {
        return batch;
    }

    if (max_candidates == 0)
    {
        batch.range_finished = false;
        batch.next_cursor = NormalizeExpiredTtlScanCursor(physical_prefix, "");
        return batch;
    }

    std::string next_cursor;
    for (const RetiredTombstoneScanItem &item : scan_items)
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
            if (IsSafeRetiredTombstoneCandidate(decoded, watermark))
            {
                ++batch.candidate_items;
                batch.candidates.push_back(RetiredTombstoneCleanupCandidate{
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
