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

#include <atomic>
#include <cstddef>
#include <string>
#include <string_view>

#include "data_store.h"
#include "tikv_config.h"
#include "tikv_expired_ttl_cleanup.h"
#include "tikv_kv_client.h"

namespace EloqDS
{

class TikvDataStore : public DataStore
{
public:
    TikvDataStore(const TikvConfig &config,
                  uint32_t shard_id,
                  DataStoreService *data_store_service);

    ~TikvDataStore() override;

    bool Initialize() override;

    bool StartDB(int64_t term) override;

    void Shutdown() override;

    void Read(ReadRequest *read_req) override;

    void BatchWriteRecords(WriteRecordsRequest *batch_write_req) override;

    void FlushData(FlushDataRequest *flush_data_req) override;

    void DeleteRange(DeleteRangeRequest *delete_range_req) override;

    void CreateTable(CreateTableRequest *create_table_req) override;

    void DropTable(DropTableRequest *drop_table_req) override;

    void ScanNext(ScanRequest *scan_req) override;

    void ScanClose(ScanRequest *scan_req) override;

    void CreateSnapshotForBackup(CreateSnapshotForBackupRequest *req) override;

    void SwitchToReadOnly() override;

    void SwitchToReadWrite() override;

    ExpiredTtlCandidateScanBatch ScanExpiredBaseTtlCandidates(
        std::string_view table_name,
        int32_t partition_id,
        std::string_view cursor,
        uint32_t max_scan_items,
        uint32_t max_candidates,
        uint64_t now_ms);

    ExpiredTtlCleanupRunResult RunExpiredBaseTtlCleanupOnce(
        std::string_view table_name,
        int32_t partition_id,
        std::string_view cursor,
        uint32_t max_scan_items,
        uint32_t max_delete_items,
        uint64_t now_ms);

private:
    static std::string BuildKeyPrefix(std::string_view table_name,
                                      int32_t partition_id);

    static std::string BuildKey(std::string_view table_name,
                                int32_t partition_id,
                                std::string_view key);

    static std::string BuildKey(const WriteRecordsRequest *batch_write_req,
                                size_t record_index);

    TikvConfig config_;
    TikvKvClient kv_client_;
    std::atomic<bool> started_{false};
};

}  // namespace EloqDS
