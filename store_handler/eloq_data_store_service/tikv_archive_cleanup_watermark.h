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

namespace EloqDS
{

enum class ArchiveCleanupWatermarkSource
{
    Unknown,
    TxService,
    RetentionWindow,
};

struct ArchiveCleanupWatermark
{
    ArchiveCleanupWatermarkSource source_{
        ArchiveCleanupWatermarkSource::Unknown};
    uint64_t timestamp_{0};

    bool Available() const
    {
        return source_ != ArchiveCleanupWatermarkSource::Unknown;
    }
};

inline ArchiveCleanupWatermark UnknownArchiveCleanupWatermark()
{
    return {};
}

inline ArchiveCleanupWatermark TxServiceArchiveCleanupWatermark(
    uint64_t oldest_snapshot_ts)
{
    if (oldest_snapshot_ts == 0)
    {
        return UnknownArchiveCleanupWatermark();
    }
    return {ArchiveCleanupWatermarkSource::TxService, oldest_snapshot_ts};
}

inline ArchiveCleanupWatermark RetentionWindowArchiveCleanupWatermark(
    uint64_t current_eloq_ts, uint64_t retention_window_us)
{
    if (retention_window_us == 0 || current_eloq_ts <= retention_window_us)
    {
        return UnknownArchiveCleanupWatermark();
    }
    return {ArchiveCleanupWatermarkSource::RetentionWindow,
            current_eloq_ts - retention_window_us};
}

/**
 * Compute the conservative cutoff for future TiKV archive/tombstone cleanup.
 *
 * tx_service_oldest_snapshot_ts is the TxService-facing source: it should be
 * the oldest active/recoverable Eloq snapshot or SI transaction timestamp that
 * may still need logical mvcc_archives rows. Pass 0 when that source is not
 * available. The fallback retention window is an Eloq timestamp duration in
 * microseconds and is disabled when it is 0.
 *
 * This helper intentionally has no TiKV GC safepoint input. TiKV safepoints are
 * physical storage GC metadata and must never drive Eloq archive retention.
 */
inline ArchiveCleanupWatermark ComputeSafeArchiveCleanupWatermark(
    uint64_t tx_service_oldest_snapshot_ts,
    uint64_t current_eloq_ts,
    uint64_t fallback_retention_window_us)
{
    ArchiveCleanupWatermark watermark =
        TxServiceArchiveCleanupWatermark(tx_service_oldest_snapshot_ts);
    if (watermark.Available())
    {
        return watermark;
    }
    return RetentionWindowArchiveCleanupWatermark(
        current_eloq_ts, fallback_retention_window_us);
}

inline bool ArchiveOrTombstoneCleanupEnabled(
    const ArchiveCleanupWatermark &watermark)
{
    return watermark.Available();
}

}  // namespace EloqDS
