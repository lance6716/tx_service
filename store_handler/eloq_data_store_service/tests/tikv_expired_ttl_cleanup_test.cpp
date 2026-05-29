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

#include "tikv_expired_ttl_cleanup.h"

#include <gtest/gtest.h>

namespace EloqDS
{
namespace
{

ExpiredTtlScanItem Item(std::string key,
                        std::string record,
                        uint64_t ts,
                        uint64_t ttl)
{
    return ExpiredTtlScanItem{
        std::move(key), EloqValueCodec::EncodeValue(record, ts, ttl)};
}

std::string TruncatedTtlValue()
{
    std::string value = EloqValueCodec::EncodeValue("bad", 13, 666);
    value.resize(sizeof(uint64_t));
    return value;
}

TEST(TikvExpiredTtlCleanupTest, ExcludesMvccArchivesTable)
{
    EXPECT_TRUE(IsBaseTableForExpiredTtlCleanup("db.table"));
    EXPECT_TRUE(IsBaseTableForExpiredTtlCleanup("db.$index"));
    EXPECT_FALSE(IsBaseTableForExpiredTtlCleanup("mvcc_archives"));
    EXPECT_FALSE(IsBaseTableForExpiredTtlCleanup(""));
}

TEST(TikvExpiredTtlCleanupTest, CollectsOnlyExpiredTtlCandidates)
{
    const std::string prefix = BuildExpiredTtlPartitionPrefix("db.table", 7);
    const uint64_t now_ms = 1000;
    std::vector<ExpiredTtlScanItem> items;
    items.push_back(Item(prefix + "expired", "old", 10, 999));
    items.push_back(Item(prefix + "live", "live", 11, 1000));
    items.push_back(Item(prefix + "no-ttl", "forever", 12, 0));
    items.push_back(ExpiredTtlScanItem{prefix + "bad", "short"});
    items.push_back(
        ExpiredTtlScanItem{prefix + "bad-ttl", TruncatedTtlValue()});

    ExpiredTtlCandidateScanBatch batch = CollectExpiredTtlCandidatesFromScan(
        prefix, items, false, "", now_ms, 10);

    ASSERT_TRUE(batch.ok);
    EXPECT_TRUE(batch.range_finished);
    EXPECT_EQ(batch.scanned_items, 5U);
    EXPECT_EQ(batch.expired_items, 1U);
    EXPECT_EQ(batch.skipped_items, 4U);
    EXPECT_EQ(batch.malformed_items, 2U);
    ASSERT_EQ(batch.candidates.size(), 1U);
    EXPECT_EQ(batch.candidates[0].physical_key, prefix + "expired");
    EXPECT_EQ(batch.candidates[0].logical_key, "expired");
    EXPECT_EQ(batch.candidates[0].record_ts, 10U);
    EXPECT_EQ(batch.candidates[0].ttl, 999U);
}

TEST(TikvExpiredTtlCleanupTest, CandidateLimitStopsWithoutSkippingRest)
{
    const std::string prefix = BuildExpiredTtlPartitionPrefix("db.table", 3);
    std::vector<ExpiredTtlScanItem> items;
    items.push_back(Item(prefix + "a", "a", 1, 10));
    items.push_back(Item(prefix + "b", "b", 2, 11));

    ExpiredTtlCandidateScanBatch batch = CollectExpiredTtlCandidatesFromScan(
        prefix, items, true, prefix + "c", 100, 1);

    EXPECT_FALSE(batch.range_finished);
    EXPECT_EQ(batch.scanned_items, 1U);
    EXPECT_EQ(batch.expired_items, 1U);
    EXPECT_EQ(batch.skipped_items, 0U);
    EXPECT_EQ(batch.malformed_items, 0U);
    ASSERT_EQ(batch.candidates.size(), 1U);
    EXPECT_EQ(batch.candidates[0].logical_key, "a");
    EXPECT_EQ(batch.next_cursor, KeyAfterForExpiredTtlCleanup(prefix + "a"));
}

TEST(TikvExpiredTtlCleanupTest, ScanMoreUsesReturnedCursor)
{
    const std::string prefix = BuildExpiredTtlPartitionPrefix("db.table", 9);
    std::vector<ExpiredTtlScanItem> items;
    items.push_back(Item(prefix + "a", "a", 1, 0));

    ExpiredTtlCandidateScanBatch batch = CollectExpiredTtlCandidatesFromScan(
        prefix, items, true, prefix + "next", 100, 10);

    EXPECT_FALSE(batch.range_finished);
    EXPECT_TRUE(batch.candidates.empty());
    EXPECT_EQ(batch.scanned_items, 1U);
    EXPECT_EQ(batch.expired_items, 0U);
    EXPECT_EQ(batch.skipped_items, 1U);
    EXPECT_EQ(batch.malformed_items, 0U);
    EXPECT_EQ(batch.next_cursor, prefix + "next");
}

TEST(TikvExpiredTtlCleanupTest, OutsidePrefixEndsRangeConservatively)
{
    const std::string prefix = BuildExpiredTtlPartitionPrefix("db.table", 1);
    std::vector<ExpiredTtlScanItem> items;
    items.push_back(Item(prefix + "a", "a", 1, 10));
    items.push_back(Item("db.table/2/a", "other", 2, 10));

    ExpiredTtlCandidateScanBatch batch = CollectExpiredTtlCandidatesFromScan(
        prefix, items, true, "db.table/2/b", 100, 10);

    EXPECT_TRUE(batch.range_finished);
    EXPECT_EQ(batch.scanned_items, 1U);
    EXPECT_EQ(batch.expired_items, 1U);
    EXPECT_EQ(batch.skipped_items, 0U);
    EXPECT_EQ(batch.malformed_items, 0U);
    ASSERT_EQ(batch.candidates.size(), 1U);
    EXPECT_TRUE(batch.next_cursor.empty());
}

TEST(TikvExpiredTtlCleanupTest, NormalizesInvalidCursorToPrefix)
{
    const std::string prefix = BuildExpiredTtlPartitionPrefix("db.table", 4);
    EXPECT_EQ(NormalizeExpiredTtlScanCursor(prefix, ""), prefix);
    EXPECT_EQ(NormalizeExpiredTtlScanCursor(prefix, "other/1/a"), prefix);
    EXPECT_EQ(NormalizeExpiredTtlScanCursor(prefix, prefix + "k"),
              prefix + "k");
}

}  // namespace
}  // namespace EloqDS
