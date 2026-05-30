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

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "tikv_archive_cleanup_watermark.h"
#include "tikv_expired_ttl_cleanup.h"

namespace EloqDS
{

constexpr std::string_view kMvccArchivesTableName = "mvcc_archives";
constexpr size_t kArchiveCommitTsBytes = sizeof(uint64_t);
constexpr size_t kArchiveCommitTsSuffixBytes = 1 + kArchiveCommitTsBytes;

struct ArchiveRetentionScanItem
{
    std::string key;
    std::string value;
};

struct ArchiveRetentionKey
{
    std::string archive_key_prefix;
    uint64_t commit_ts{0};
};

struct ArchiveRetentionAnchor
{
    std::string physical_key;
    std::string archive_key_prefix;
    uint64_t commit_ts{0};
};

struct ArchiveRetentionCleanupCandidate
{
    std::string physical_key;
    std::string archive_key_prefix;
    uint64_t commit_ts{0};
};

struct ArchiveRetentionCandidateScanBatch
{
    std::vector<ArchiveRetentionCleanupCandidate> candidates;
    uint32_t scanned_items{0};
    uint32_t candidate_items{0};
    uint32_t anchor_items{0};
    uint32_t skipped_items{0};
    uint32_t malformed_items{0};
    bool has_carry_anchor{false};
    ArchiveRetentionAnchor carry_anchor;
    bool range_finished{true};
    bool ok{true};
    std::string next_cursor;
    std::string error_message;
};

inline bool IsArchiveTableForRetentionCleanup(std::string_view table_name)
{
    return table_name == kMvccArchivesTableName;
}

inline uint64_t DecodeBigEndianArchiveCommitTs(std::string_view bytes)
{
    uint64_t value = 0;
    for (unsigned char byte : bytes)
    {
        value = (value << 8) | byte;
    }
    return value;
}

inline bool ParseArchiveRetentionKey(std::string_view physical_prefix,
                                     std::string_view physical_key,
                                     ArchiveRetentionKey &parsed)
{
    if (!StartsWithForExpiredTtlCleanup(physical_key, physical_prefix) ||
        physical_key.size() <
            physical_prefix.size() + kArchiveCommitTsSuffixBytes)
    {
        return false;
    }

    const size_t commit_ts_offset = physical_key.size() - kArchiveCommitTsBytes;
    const size_t separator_offset = commit_ts_offset - 1;
    if (physical_key[separator_offset] != '/')
    {
        return false;
    }

    parsed.archive_key_prefix.assign(
        physical_key.data() + physical_prefix.size(),
        separator_offset - physical_prefix.size());
    if (parsed.archive_key_prefix.empty())
    {
        return false;
    }

    parsed.commit_ts = DecodeBigEndianArchiveCommitTs(
        physical_key.substr(commit_ts_offset, kArchiveCommitTsBytes));
    return true;
}

inline ArchiveRetentionAnchor MakeArchiveRetentionAnchor(
    const ArchiveRetentionScanItem &item,
    const ArchiveRetentionKey &parsed)
{
    return ArchiveRetentionAnchor{
        item.key, parsed.archive_key_prefix, parsed.commit_ts};
}

inline ArchiveRetentionCleanupCandidate MakeArchiveRetentionCandidate(
    const ArchiveRetentionAnchor &anchor)
{
    return ArchiveRetentionCleanupCandidate{
        anchor.physical_key, anchor.archive_key_prefix, anchor.commit_ts};
}

inline bool ArchiveRetentionCleanupEnabled(
    const ArchiveCleanupWatermark &watermark)
{
    return ArchiveOrTombstoneCleanupEnabled(watermark) &&
           watermark.timestamp_ > 0;
}

inline ArchiveRetentionCandidateScanBatch CollectArchiveRetentionCandidatesFromScan(
    std::string_view physical_prefix,
    const std::vector<ArchiveRetentionScanItem> &scan_items,
    bool scan_has_more,
    std::string_view scan_next_cursor,
    const ArchiveCleanupWatermark &watermark,
    uint32_t max_candidates,
    const ArchiveRetentionAnchor *initial_anchor = nullptr)
{
    ArchiveRetentionCandidateScanBatch batch;
    batch.range_finished = true;

    if (!ArchiveRetentionCleanupEnabled(watermark))
    {
        return batch;
    }

    if (max_candidates == 0)
    {
        batch.range_finished = false;
        batch.next_cursor = NormalizeExpiredTtlScanCursor(physical_prefix, "");
        return batch;
    }

    bool has_pending_anchor = initial_anchor != nullptr;
    ArchiveRetentionAnchor pending_anchor;
    if (has_pending_anchor)
    {
        pending_anchor = *initial_anchor;
    }

    std::string next_cursor;
    for (const ArchiveRetentionScanItem &item : scan_items)
    {
        if (!StartsWithForExpiredTtlCleanup(item.key, physical_prefix))
        {
            batch.range_finished = true;
            batch.next_cursor.clear();
            if (has_pending_anchor)
            {
                ++batch.anchor_items;
            }
            return batch;
        }

        ++batch.scanned_items;
        next_cursor = KeyAfterForExpiredTtlCleanup(item.key);

        ArchiveRetentionKey parsed;
        if (!ParseArchiveRetentionKey(physical_prefix, item.key, parsed))
        {
            ++batch.skipped_items;
            ++batch.malformed_items;
            continue;
        }

        if (has_pending_anchor &&
            pending_anchor.archive_key_prefix != parsed.archive_key_prefix)
        {
            ++batch.anchor_items;
            has_pending_anchor = false;
        }

        if (parsed.commit_ts > watermark.timestamp_)
        {
            ++batch.skipped_items;
            continue;
        }

        ArchiveRetentionAnchor current_anchor =
            MakeArchiveRetentionAnchor(item, parsed);
        if (has_pending_anchor && pending_anchor.physical_key == item.key)
        {
            pending_anchor = std::move(current_anchor);
            continue;
        }

        if (has_pending_anchor)
        {
            batch.candidates.push_back(
                MakeArchiveRetentionCandidate(pending_anchor));
            ++batch.candidate_items;
            pending_anchor = std::move(current_anchor);
            if (batch.candidates.size() >= max_candidates)
            {
                batch.range_finished = false;
                // Carry the current anchor to the next call, but advance the
                // TiKV cursor past it. The next page can then prove whether
                // this anchor has a newer <= watermark version without getting
                // stuck on an inclusive forward scan start key.
                batch.next_cursor =
                    KeyAfterForExpiredTtlCleanup(pending_anchor.physical_key);
                batch.has_carry_anchor = true;
                batch.carry_anchor = pending_anchor;
                return batch;
            }
        }
        else
        {
            pending_anchor = std::move(current_anchor);
            has_pending_anchor = true;
        }
    }

    if (has_pending_anchor)
    {
        ++batch.anchor_items;
        batch.has_carry_anchor = scan_has_more;
        if (batch.has_carry_anchor)
        {
            batch.carry_anchor = pending_anchor;
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
        if (has_pending_anchor)
        {
            // Carry the retained anchor to the next page, but advance the
            // TiKV cursor past the last key scanned in this page. The last key
            // can be newer than the retained anchor and above the cleanup
            // watermark; going back to the anchor would make an inclusive
            // forward scan repeatedly revisit that newer key.
            batch.next_cursor = std::move(next_cursor);
        }
        else if (!scan_next_cursor.empty() &&
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
