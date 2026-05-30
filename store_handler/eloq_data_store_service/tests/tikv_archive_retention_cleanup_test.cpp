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

#include "tikv_archive_retention_cleanup.h"

#include <gtest/gtest.h>

namespace EloqDS
{
namespace
{

uint64_t HostToBigEndian(uint64_t value)
{
    uint64_t result = 0;
    for (size_t i = 0; i < sizeof(value); ++i)
    {
        result = (result << 8) | ((value >> (i * 8)) & 0xff);
    }
    return result;
}

std::string ArchiveKey(std::string_view table,
                       std::string_view key,
                       uint64_t commit_ts)
{
    const uint64_t be_commit_ts = HostToBigEndian(commit_ts);
    std::string archive_key;
    archive_key.reserve(table.size() + 1 + key.size() + 1 + sizeof(uint64_t));
    archive_key.append(table.data(), table.size());
    archive_key.push_back('/');
    archive_key.append(key.data(), key.size());
    archive_key.push_back('/');
    archive_key.append(reinterpret_cast<const char *>(&be_commit_ts),
                       sizeof(be_commit_ts));
    return archive_key;
}

ArchiveRetentionScanItem Item(std::string_view physical_prefix,
                              std::string_view table,
                              std::string_view key,
                              uint64_t commit_ts)
{
    std::string physical_key(physical_prefix.data(), physical_prefix.size());
    physical_key.append(ArchiveKey(table, key, commit_ts));
    return ArchiveRetentionScanItem{std::move(physical_key), "value"};
}

TEST(TikvArchiveRetentionCleanupTest, ParsesArchiveKeySuffix)
{
    const std::string prefix =
        BuildExpiredTtlPartitionPrefix(kMvccArchivesTableName, 3);
    const ArchiveRetentionScanItem item = Item(prefix, "db.table", "doc", 123);

    ArchiveRetentionKey parsed;
    ASSERT_TRUE(ParseArchiveRetentionKey(prefix, item.key, parsed));
    EXPECT_EQ(parsed.archive_key_prefix, "db.table/doc");
    EXPECT_EQ(parsed.commit_ts, 123U);

    ArchiveRetentionKey malformed;
    EXPECT_FALSE(ParseArchiveRetentionKey(prefix, prefix + "too-short", malformed));
    EXPECT_FALSE(ParseArchiveRetentionKey(prefix,
                                          prefix + "db.table/doc-12345678",
                                          malformed));
}

TEST(TikvArchiveRetentionCleanupTest, KeepsVisibleAnchorAtWatermark)
{
    const std::string prefix =
        BuildExpiredTtlPartitionPrefix(kMvccArchivesTableName, 4);
    std::vector<ArchiveRetentionScanItem> items;
    items.push_back(Item(prefix, "db.table", "doc", 100));
    items.push_back(Item(prefix, "db.table", "doc", 200));
    items.push_back(Item(prefix, "db.table", "doc", 300));
    items.push_back(Item(prefix, "db.table", "doc2", 50));
    items.push_back(Item(prefix, "db.table", "doc2", 150));

    ArchiveRetentionCandidateScanBatch batch =
        CollectArchiveRetentionCandidatesFromScan(
            prefix, items, false, "", TxServiceArchiveCleanupWatermark(250), 10);

    ASSERT_TRUE(batch.ok);
    EXPECT_TRUE(batch.range_finished);
    EXPECT_EQ(batch.scanned_items, 5U);
    EXPECT_EQ(batch.candidate_items, 2U);
    EXPECT_EQ(batch.skipped_items, 1U);
    ASSERT_EQ(batch.candidates.size(), 2U);
    EXPECT_EQ(batch.candidates[0].archive_key_prefix, "db.table/doc");
    EXPECT_EQ(batch.candidates[0].commit_ts, 100U);
    EXPECT_EQ(batch.candidates[1].archive_key_prefix, "db.table/doc2");
    EXPECT_EQ(batch.candidates[1].commit_ts, 50U);
}

TEST(TikvArchiveRetentionCleanupTest, ExactWatermarkVersionCanBeAnchor)
{
    const std::string prefix =
        BuildExpiredTtlPartitionPrefix(kMvccArchivesTableName, 5);
    std::vector<ArchiveRetentionScanItem> items;
    items.push_back(Item(prefix, "db.table", "doc", 100));
    items.push_back(Item(prefix, "db.table", "doc", 200));

    ArchiveRetentionCandidateScanBatch batch =
        CollectArchiveRetentionCandidatesFromScan(
            prefix, items, false, "", TxServiceArchiveCleanupWatermark(200), 10);

    ASSERT_EQ(batch.candidates.size(), 1U);
    EXPECT_EQ(batch.candidates[0].commit_ts, 100U);
    EXPECT_EQ(batch.anchor_items, 1U);
}

TEST(TikvArchiveRetentionCleanupTest, SingleOlderVersionRemainsAnchor)
{
    const std::string prefix =
        BuildExpiredTtlPartitionPrefix(kMvccArchivesTableName, 6);
    std::vector<ArchiveRetentionScanItem> items;
    items.push_back(Item(prefix, "db.table", "doc", 100));
    items.push_back(Item(prefix, "db.table", "doc", 300));

    ArchiveRetentionCandidateScanBatch batch =
        CollectArchiveRetentionCandidatesFromScan(
            prefix, items, false, "", TxServiceArchiveCleanupWatermark(250), 10);

    EXPECT_TRUE(batch.candidates.empty());
    EXPECT_EQ(batch.anchor_items, 1U);
    EXPECT_EQ(batch.skipped_items, 1U);
}

TEST(TikvArchiveRetentionCleanupTest, MalformedRowsAreSkipped)
{
    const std::string prefix =
        BuildExpiredTtlPartitionPrefix(kMvccArchivesTableName, 7);
    std::vector<ArchiveRetentionScanItem> items;
    items.push_back(ArchiveRetentionScanItem{prefix + "bad", "value"});
    items.push_back(Item(prefix, "db.table", "doc", 100));

    ArchiveRetentionCandidateScanBatch batch =
        CollectArchiveRetentionCandidatesFromScan(
            prefix, items, false, "", TxServiceArchiveCleanupWatermark(250), 10);

    EXPECT_TRUE(batch.candidates.empty());
    EXPECT_EQ(batch.scanned_items, 2U);
    EXPECT_EQ(batch.skipped_items, 1U);
    EXPECT_EQ(batch.malformed_items, 1U);
    EXPECT_EQ(batch.anchor_items, 1U);
}

TEST(TikvArchiveRetentionCleanupTest, CandidateLimitCarriesVisibleAnchor)
{
    const std::string prefix =
        BuildExpiredTtlPartitionPrefix(kMvccArchivesTableName, 8);
    std::vector<ArchiveRetentionScanItem> first_items;
    first_items.push_back(Item(prefix, "db.table", "doc", 100));
    first_items.push_back(Item(prefix, "db.table", "doc", 200));
    first_items.push_back(Item(prefix, "db.table", "doc", 300));

    ArchiveRetentionCandidateScanBatch first =
        CollectArchiveRetentionCandidatesFromScan(
            prefix, first_items, true, prefix + "next", TxServiceArchiveCleanupWatermark(300), 1);

    ASSERT_FALSE(first.range_finished);
    ASSERT_EQ(first.candidates.size(), 1U);
    EXPECT_EQ(first.candidates[0].commit_ts, 100U);
    ASSERT_TRUE(first.has_carry_anchor);
    EXPECT_EQ(first.carry_anchor.commit_ts, 200U);
    EXPECT_EQ(first.next_cursor, KeyAfterForExpiredTtlCleanup(first_items[1].key));

    std::vector<ArchiveRetentionScanItem> second_items;
    second_items.push_back(first_items[2]);
    ArchiveRetentionCandidateScanBatch second =
        CollectArchiveRetentionCandidatesFromScan(prefix,
                                                  second_items,
                                                  false,
                                                  "",
                                                  TxServiceArchiveCleanupWatermark(300),
                                                  10,
                                                  &first.carry_anchor);

    ASSERT_EQ(second.candidates.size(), 1U);
    EXPECT_EQ(second.candidates[0].commit_ts, 200U);
    EXPECT_EQ(second.anchor_items, 1U);
}

TEST(TikvArchiveRetentionCleanupTest, BatchBoundaryAdvancesPastCarryAnchor)
{
    const std::string prefix =
        BuildExpiredTtlPartitionPrefix(kMvccArchivesTableName, 10);
    std::vector<ArchiveRetentionScanItem> first_items;
    first_items.push_back(Item(prefix, "db.table", "doc", 100));

    ArchiveRetentionCandidateScanBatch first =
        CollectArchiveRetentionCandidatesFromScan(
            prefix, first_items, true, prefix + "next", TxServiceArchiveCleanupWatermark(250), 10);

    EXPECT_FALSE(first.range_finished);
    EXPECT_TRUE(first.candidates.empty());
    ASSERT_TRUE(first.has_carry_anchor);
    EXPECT_EQ(first.carry_anchor.commit_ts, 100U);
    EXPECT_EQ(first.next_cursor, KeyAfterForExpiredTtlCleanup(first_items[0].key));

    std::vector<ArchiveRetentionScanItem> second_items;
    second_items.push_back(Item(prefix, "db.table", "doc", 200));
    ArchiveRetentionCandidateScanBatch second =
        CollectArchiveRetentionCandidatesFromScan(prefix,
                                                  second_items,
                                                  false,
                                                  "",
                                                  TxServiceArchiveCleanupWatermark(250),
                                                  10,
                                                  &first.carry_anchor);

    ASSERT_EQ(second.candidates.size(), 1U);
    EXPECT_EQ(second.candidates[0].commit_ts, 100U);
    EXPECT_EQ(second.anchor_items, 1U);
}

TEST(TikvArchiveRetentionCleanupTest, CarryAnchorAdvancesPastNewerSkippedRow)
{
    const std::string prefix =
        BuildExpiredTtlPartitionPrefix(kMvccArchivesTableName, 11);
    std::vector<ArchiveRetentionScanItem> first_items;
    first_items.push_back(Item(prefix, "db.table", "doc", 100));

    ArchiveRetentionCandidateScanBatch first =
        CollectArchiveRetentionCandidatesFromScan(
            prefix, first_items, true, "", TxServiceArchiveCleanupWatermark(250), 10);

    ASSERT_FALSE(first.range_finished);
    ASSERT_TRUE(first.has_carry_anchor);
    EXPECT_EQ(first.carry_anchor.commit_ts, 100U);
    EXPECT_EQ(first.next_cursor, KeyAfterForExpiredTtlCleanup(first_items[0].key));

    std::vector<ArchiveRetentionScanItem> second_items;
    second_items.push_back(Item(prefix, "db.table", "doc", 300));
    ArchiveRetentionCandidateScanBatch second =
        CollectArchiveRetentionCandidatesFromScan(prefix,
                                                  second_items,
                                                  true,
                                                  "",
                                                  TxServiceArchiveCleanupWatermark(250),
                                                  10,
                                                  &first.carry_anchor);

    EXPECT_FALSE(second.range_finished);
    EXPECT_TRUE(second.candidates.empty());
    ASSERT_TRUE(second.has_carry_anchor);
    EXPECT_EQ(second.carry_anchor.commit_ts, 100U);
    EXPECT_EQ(second.skipped_items, 1U);
    EXPECT_EQ(second.anchor_items, 1U);
    EXPECT_EQ(second.next_cursor,
              KeyAfterForExpiredTtlCleanup(second_items[0].key));
}

TEST(TikvArchiveRetentionCleanupTest, UnknownWatermarkDisablesCandidates)
{
    const std::string prefix =
        BuildExpiredTtlPartitionPrefix(kMvccArchivesTableName, 9);
    std::vector<ArchiveRetentionScanItem> items;
    items.push_back(Item(prefix, "db.table", "doc", 100));
    items.push_back(Item(prefix, "db.table", "doc", 200));

    ArchiveRetentionCandidateScanBatch batch =
        CollectArchiveRetentionCandidatesFromScan(
            prefix, items, true, prefix + "next", UnknownArchiveCleanupWatermark(), 10);

    EXPECT_TRUE(batch.range_finished);
    EXPECT_EQ(batch.scanned_items, 0U);
    EXPECT_TRUE(batch.candidates.empty());
}

}  // namespace
}  // namespace EloqDS
