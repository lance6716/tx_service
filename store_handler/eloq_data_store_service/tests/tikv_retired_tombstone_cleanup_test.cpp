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

#include "tikv_retired_tombstone_cleanup.h"

#include <gtest/gtest.h>

namespace EloqDS
{
namespace
{

std::string TombstoneRecord()
{
    const bool is_deleted = true;
    return std::string(reinterpret_cast<const char *>(&is_deleted),
                       sizeof(is_deleted));
}

std::string OneByteRecord(unsigned char byte)
{
    return std::string(1, static_cast<char>(byte));
}

RetiredTombstoneScanItem Item(std::string key,
                              std::string record,
                              uint64_t ts,
                              uint64_t ttl)
{
    return RetiredTombstoneScanItem{
        std::move(key), EloqValueCodec::EncodeValue(record, ts, ttl)};
}

std::string TruncatedTtlValue()
{
    std::string value = EloqValueCodec::EncodeValue("bad", 13, 666);
    value.resize(sizeof(uint64_t));
    return value;
}

TEST(TikvRetiredTombstoneCleanupTest, ExcludesArchivesAndEmptyTables)
{
    EXPECT_TRUE(IsBaseTableForRetiredTombstoneCleanup("db.table"));
    EXPECT_TRUE(IsBaseTableForRetiredTombstoneCleanup("db.$index"));
    EXPECT_FALSE(IsBaseTableForRetiredTombstoneCleanup("mvcc_archives"));
    EXPECT_FALSE(IsBaseTableForRetiredTombstoneCleanup(""));
}

TEST(TikvRetiredTombstoneCleanupTest, UnknownWatermarkDisablesCandidates)
{
    const std::string prefix = BuildExpiredTtlPartitionPrefix("db.table", 1);
    std::vector<RetiredTombstoneScanItem> items;
    items.push_back(Item(prefix + "deleted", TombstoneRecord(), 10, 999));

    RetiredTombstoneCandidateScanBatch batch =
        CollectRetiredTombstoneCandidatesFromScan(
            prefix, items, true, prefix + "next", UnknownArchiveCleanupWatermark(), 10);

    EXPECT_TRUE(batch.range_finished);
    EXPECT_EQ(batch.scanned_items, 0U);
    EXPECT_TRUE(batch.candidates.empty());
}

TEST(TikvRetiredTombstoneCleanupTest,
     CollectsOnlyTombstonesOlderThanWatermark)
{
    const std::string prefix = BuildExpiredTtlPartitionPrefix("db.table", 7);
    std::vector<RetiredTombstoneScanItem> items;
    items.push_back(Item(prefix + "old-delete", TombstoneRecord(), 99, 1000));
    items.push_back(Item(prefix + "exact-delete", TombstoneRecord(), 100, 1000));
    items.push_back(Item(prefix + "new-delete", TombstoneRecord(), 101, 1000));
    items.push_back(Item(prefix + "live", "payload", 10, 1000));
    items.push_back(Item(prefix + "one-byte", OneByteRecord(2), 10, 1000));
    items.push_back(RetiredTombstoneScanItem{prefix + "bad", "short"});
    items.push_back(
        RetiredTombstoneScanItem{prefix + "bad-ttl", TruncatedTtlValue()});

    RetiredTombstoneCandidateScanBatch batch =
        CollectRetiredTombstoneCandidatesFromScan(
            prefix, items, false, "", TxServiceArchiveCleanupWatermark(100), 10);

    ASSERT_TRUE(batch.ok);
    EXPECT_TRUE(batch.range_finished);
    EXPECT_EQ(batch.scanned_items, 7U);
    EXPECT_EQ(batch.candidate_items, 1U);
    EXPECT_EQ(batch.skipped_items, 6U);
    EXPECT_EQ(batch.malformed_items, 2U);
    ASSERT_EQ(batch.candidates.size(), 1U);
    EXPECT_EQ(batch.candidates[0].physical_key, prefix + "old-delete");
    EXPECT_EQ(batch.candidates[0].logical_key, "old-delete");
    EXPECT_EQ(batch.candidates[0].record_ts, 99U);
    EXPECT_EQ(batch.candidates[0].ttl, 1000U);
}

TEST(TikvRetiredTombstoneCleanupTest, RecheckDeleteDecisionIsConservative)
{
    RetiredTombstoneDeleteCheck old_tombstone =
        CheckRetiredTombstoneDeleteCandidate(
            EloqValueCodec::EncodeValue(TombstoneRecord(), 99, 1000),
            TxServiceArchiveCleanupWatermark(100));
    EXPECT_EQ(old_tombstone.decision,
              RetiredTombstoneDeleteDecision::Delete);
    EXPECT_EQ(old_tombstone.record_ts, 99U);
    EXPECT_EQ(old_tombstone.ttl, 1000U);

    RetiredTombstoneDeleteCheck exact_tombstone =
        CheckRetiredTombstoneDeleteCandidate(
            EloqValueCodec::EncodeValue(TombstoneRecord(), 100, 1000),
            TxServiceArchiveCleanupWatermark(100));
    EXPECT_EQ(exact_tombstone.decision,
              RetiredTombstoneDeleteDecision::Skip);
    EXPECT_EQ(exact_tombstone.record_ts, 100U);

    RetiredTombstoneDeleteCheck live =
        CheckRetiredTombstoneDeleteCandidate(
            EloqValueCodec::EncodeValue("payload", 50, 1000),
            TxServiceArchiveCleanupWatermark(100));
    EXPECT_EQ(live.decision, RetiredTombstoneDeleteDecision::Skip);

    RetiredTombstoneDeleteCheck disabled =
        CheckRetiredTombstoneDeleteCandidate(
            EloqValueCodec::EncodeValue(TombstoneRecord(), 99, 1000),
            UnknownArchiveCleanupWatermark());
    EXPECT_EQ(disabled.decision, RetiredTombstoneDeleteDecision::Skip);

    RetiredTombstoneDeleteCheck malformed =
        CheckRetiredTombstoneDeleteCandidate("short",
                                             TxServiceArchiveCleanupWatermark(100));
    EXPECT_EQ(malformed.decision, RetiredTombstoneDeleteDecision::Malformed);

    RetiredTombstoneDeleteCheck truncated_ttl =
        CheckRetiredTombstoneDeleteCandidate(
            TruncatedTtlValue(), TxServiceArchiveCleanupWatermark(100));
    EXPECT_EQ(truncated_ttl.decision,
              RetiredTombstoneDeleteDecision::Malformed);
}

TEST(TikvRetiredTombstoneCleanupTest, CandidateLimitStopsAtCandidate)
{
    const std::string prefix = BuildExpiredTtlPartitionPrefix("db.table", 3);
    std::vector<RetiredTombstoneScanItem> items;
    items.push_back(Item(prefix + "a", TombstoneRecord(), 1, 10));
    items.push_back(Item(prefix + "b", TombstoneRecord(), 2, 11));

    RetiredTombstoneCandidateScanBatch batch =
        CollectRetiredTombstoneCandidatesFromScan(
            prefix, items, true, prefix + "c", TxServiceArchiveCleanupWatermark(100), 1);

    EXPECT_FALSE(batch.range_finished);
    EXPECT_EQ(batch.scanned_items, 1U);
    EXPECT_EQ(batch.candidate_items, 1U);
    EXPECT_EQ(batch.skipped_items, 0U);
    ASSERT_EQ(batch.candidates.size(), 1U);
    EXPECT_EQ(batch.candidates[0].logical_key, "a");
    EXPECT_EQ(batch.next_cursor, KeyAfterForExpiredTtlCleanup(prefix + "a"));
}

TEST(TikvRetiredTombstoneCleanupTest, BuildsDeleteKeysFromCandidatesOnly)
{
    std::vector<RetiredTombstoneCleanupCandidate> candidates;
    candidates.push_back(
        RetiredTombstoneCleanupCandidate{"db.table/1/a", "a", 1, 10});
    candidates.push_back(
        RetiredTombstoneCleanupCandidate{"db.table/1/b", "b", 2, 11});

    std::vector<std::string> keys =
        BuildRetiredTombstoneDeleteKeys(candidates);

    ASSERT_EQ(keys.size(), 2U);
    EXPECT_EQ(keys[0], "db.table/1/a");
    EXPECT_EQ(keys[1], "db.table/1/b");
}

TEST(TikvRetiredTombstoneCleanupTest, NormalizesRetryCursor)
{
    const std::string prefix = BuildExpiredTtlPartitionPrefix("db.table", 12);

    EXPECT_EQ(BuildRetiredTombstoneRetryCursor("db.table", 12, ""), prefix);
    EXPECT_EQ(BuildRetiredTombstoneRetryCursor("db.table", 12, "other/12/k"),
              prefix);
    EXPECT_EQ(BuildRetiredTombstoneRetryCursor("db.table", 12, prefix + "k"),
              prefix + "k");
}

TEST(TikvRetiredTombstoneCleanupTest, ScanMoreUsesReturnedCursor)
{
    const std::string prefix = BuildExpiredTtlPartitionPrefix("db.table", 9);
    std::vector<RetiredTombstoneScanItem> items;
    items.push_back(Item(prefix + "live", "payload", 1, 0));

    RetiredTombstoneCandidateScanBatch batch =
        CollectRetiredTombstoneCandidatesFromScan(
            prefix, items, true, prefix + "next", TxServiceArchiveCleanupWatermark(100), 10);

    EXPECT_FALSE(batch.range_finished);
    EXPECT_TRUE(batch.candidates.empty());
    EXPECT_EQ(batch.scanned_items, 1U);
    EXPECT_EQ(batch.candidate_items, 0U);
    EXPECT_EQ(batch.skipped_items, 1U);
    EXPECT_EQ(batch.malformed_items, 0U);
    EXPECT_EQ(batch.next_cursor, prefix + "next");
}

TEST(TikvRetiredTombstoneCleanupTest, OutsidePrefixEndsRangeConservatively)
{
    const std::string prefix = BuildExpiredTtlPartitionPrefix("db.table", 1);
    std::vector<RetiredTombstoneScanItem> items;
    items.push_back(Item(prefix + "a", TombstoneRecord(), 1, 10));
    items.push_back(Item("db.table/2/a", TombstoneRecord(), 2, 10));

    RetiredTombstoneCandidateScanBatch batch =
        CollectRetiredTombstoneCandidatesFromScan(
            prefix, items, true, "db.table/2/b", TxServiceArchiveCleanupWatermark(100), 10);

    EXPECT_TRUE(batch.range_finished);
    EXPECT_EQ(batch.scanned_items, 1U);
    EXPECT_EQ(batch.candidate_items, 1U);
    EXPECT_EQ(batch.skipped_items, 0U);
    ASSERT_EQ(batch.candidates.size(), 1U);
    EXPECT_TRUE(batch.next_cursor.empty());
}

TEST(TikvRetiredTombstoneCleanupTest, ZeroCandidateLimitReturnsPrefixCursor)
{
    const std::string prefix = BuildExpiredTtlPartitionPrefix("db.table", 4);
    std::vector<RetiredTombstoneScanItem> items;
    items.push_back(Item(prefix + "deleted", TombstoneRecord(), 1, 10));

    RetiredTombstoneCandidateScanBatch batch =
        CollectRetiredTombstoneCandidatesFromScan(
            prefix, items, true, prefix + "next", TxServiceArchiveCleanupWatermark(100), 0);

    EXPECT_FALSE(batch.range_finished);
    EXPECT_EQ(batch.scanned_items, 0U);
    EXPECT_TRUE(batch.candidates.empty());
    EXPECT_EQ(batch.next_cursor, prefix);
}

}  // namespace
}  // namespace EloqDS
