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

#include "tikv_archive_cleanup_watermark.h"

#include <gtest/gtest.h>

namespace EloqDS
{
namespace
{

TEST(TikvArchiveCleanupWatermarkTest, UnknownWatermarkDisablesCleanup)
{
    const ArchiveCleanupWatermark watermark =
        ComputeSafeArchiveCleanupWatermark(
            0, /*current_eloq_ts=*/1000000, /*fallback_retention_window_us=*/0);

    EXPECT_FALSE(watermark.Available());
    EXPECT_FALSE(ArchiveOrTombstoneCleanupEnabled(watermark));
    EXPECT_EQ(watermark.source_, ArchiveCleanupWatermarkSource::Unknown);
    EXPECT_EQ(watermark.timestamp_, 0U);
}

TEST(TikvArchiveCleanupWatermarkTest, TxServiceWatermarkEnablesCleanup)
{
    const ArchiveCleanupWatermark watermark =
        ComputeSafeArchiveCleanupWatermark(
            /*tx_service_oldest_snapshot_ts=*/12345,
            /*current_eloq_ts=*/1000000,
            /*fallback_retention_window_us=*/0);

    EXPECT_TRUE(watermark.Available());
    EXPECT_TRUE(ArchiveOrTombstoneCleanupEnabled(watermark));
    EXPECT_EQ(watermark.source_, ArchiveCleanupWatermarkSource::TxService);
    EXPECT_EQ(watermark.timestamp_, 12345U);
}

TEST(TikvArchiveCleanupWatermarkTest, RetentionWindowFallbackEnablesCleanup)
{
    const ArchiveCleanupWatermark watermark =
        ComputeSafeArchiveCleanupWatermark(
            0,
            /*current_eloq_ts=*/100000000,
            /*fallback_retention_window_us=*/10000000);

    EXPECT_TRUE(watermark.Available());
    EXPECT_TRUE(ArchiveOrTombstoneCleanupEnabled(watermark));
    EXPECT_EQ(watermark.source_, ArchiveCleanupWatermarkSource::RetentionWindow);
    EXPECT_EQ(watermark.timestamp_, 90000000U);
}

TEST(TikvArchiveCleanupWatermarkTest,
     RetentionWindowFallbackIsUnknownUntilCurrentTsPassesWindow)
{
    const ArchiveCleanupWatermark equal_window =
        ComputeSafeArchiveCleanupWatermark(
            0,
            /*current_eloq_ts=*/10000000,
            /*fallback_retention_window_us=*/10000000);
    EXPECT_FALSE(equal_window.Available());
    EXPECT_FALSE(ArchiveOrTombstoneCleanupEnabled(equal_window));

    const ArchiveCleanupWatermark before_window =
        ComputeSafeArchiveCleanupWatermark(
            0,
            /*current_eloq_ts=*/9999999,
            /*fallback_retention_window_us=*/10000000);
    EXPECT_FALSE(before_window.Available());
    EXPECT_FALSE(ArchiveOrTombstoneCleanupEnabled(before_window));
}

TEST(TikvArchiveCleanupWatermarkTest,
     TxServiceWatermarkIsPreferredOverRetentionFallback)
{
    const ArchiveCleanupWatermark watermark =
        ComputeSafeArchiveCleanupWatermark(
            /*tx_service_oldest_snapshot_ts=*/5000,
            /*current_eloq_ts=*/100000000,
            /*fallback_retention_window_us=*/10000000);

    EXPECT_TRUE(watermark.Available());
    EXPECT_EQ(watermark.source_, ArchiveCleanupWatermarkSource::TxService);
    EXPECT_EQ(watermark.timestamp_, 5000U);
}

}  // namespace
}  // namespace EloqDS
