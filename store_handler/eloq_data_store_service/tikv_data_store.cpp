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

#include "tikv_data_store.h"

#include <glog/logging.h>

#include <cassert>
#include <chrono>
#include <exception>
#include <string_view>
#include <utility>
#include <vector>

#include "data_store_service.h"
#include "ds_request.pb.h"
#include "eloq_value_codec.h"
#include "internal_request.h"
#include "object_pool.h"

namespace EloqDS
{
namespace
{
constexpr std::string_view kKeySeparator = "/";

remote::CommonResult MakeCommonResult(remote::DataStoreError error,
                                      std::string error_message = "")
{
    remote::CommonResult result;
    result.set_error_code(error);
    result.set_error_msg(std::move(error_message));
    return result;
}

remote::CommonResult NotStartedResult()
{
    return MakeCommonResult(remote::DataStoreError::DB_NOT_OPEN,
                            "TiKV data store is not started.");
}

remote::CommonResult ShardWriteStatusResult(
    DataStoreService *data_store_service, uint32_t shard_id)
{
    if (data_store_service == nullptr)
    {
        return MakeCommonResult(remote::DataStoreError::NO_ERROR);
    }

    const DSShardStatus shard_status =
        data_store_service->FetchDSShardStatus(shard_id);
    if (shard_status == DSShardStatus::ReadWrite)
    {
        return MakeCommonResult(remote::DataStoreError::NO_ERROR);
    }

    if (shard_status == DSShardStatus::Closed)
    {
        return MakeCommonResult(
            remote::DataStoreError::REQUESTED_NODE_NOT_OWNER,
            "Requested data not on local node.");
    }

    if (shard_status == DSShardStatus::ReadOnly)
    {
        return MakeCommonResult(remote::DataStoreError::WRITE_TO_READ_ONLY_DB,
                                "Write to read-only DB.");
    }

    return MakeCommonResult(remote::DataStoreError::DB_NOT_OPEN,
                            "KV store not opened yet.");
}

bool IsExpired(uint64_t ttl)
{
    if (ttl == 0)
    {
        return false;
    }

    const uint64_t now_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    return ttl < now_ms;
}

}  // namespace

TikvDataStore::TikvDataStore(const TikvConfig &config,
                             uint32_t shard_id,
                             DataStoreService *data_store_service)
    : DataStore(shard_id, data_store_service), config_(config)
{
}

TikvDataStore::~TikvDataStore()
{
    if (started_.load(std::memory_order_acquire) ||
        kv_client_.IsInitialized())
    {
        Shutdown();
    }
}

bool TikvDataStore::Initialize()
{
    // The TiKV backend does not own local on-disk state. StartDB() establishes
    // the client connection to PD/TiKV when the DSS shard is opened.
    return true;
}

bool TikvDataStore::StartDB(int64_t term)
{
    (void) term;

    if (kv_client_.IsInitialized())
    {
        started_.store(true, std::memory_order_release);
        return true;
    }

    bool ok = kv_client_.Initialize(config_);
    started_.store(ok, std::memory_order_release);
    if (!ok)
    {
        LOG(ERROR) << "Failed to start TiKV data store for shard "
                   << shard_id_ << ": " << kv_client_.LastError();
    }
    return ok;
}

void TikvDataStore::Shutdown()
{
    started_.store(false, std::memory_order_release);
    kv_client_.Shutdown();

    if (data_store_service_ != nullptr)
    {
        data_store_service_->ForceEraseScanIters(shard_id_);
    }
}

void TikvDataStore::Read(ReadRequest *read_req)
{
    PoolableGuard req_guard(read_req);

    read_req->SetRecord("");
    read_req->SetRecordTs(0);
    read_req->SetRecordTtl(0);

    if (!kv_client_.IsInitialized())
    {
        read_req->SetFinish(remote::DataStoreError::DB_NOT_OPEN);
        return;
    }

    const std::string physical_key = BuildKey(read_req->GetTableName(),
                                              read_req->GetPartitionId(),
                                              read_req->GetKey());
    try
    {
        KvGetResult result = kv_client_.Get(physical_key);
        if (!result.found)
        {
            read_req->SetFinish(remote::DataStoreError::KEY_NOT_FOUND);
            return;
        }

        auto decoded = EloqValueCodec::DecodeValue(result.value);
        if (IsExpired(decoded.ttl))
        {
            read_req->SetFinish(remote::DataStoreError::KEY_NOT_FOUND);
            return;
        }

        read_req->SetRecord(std::move(decoded.record));
        read_req->SetRecordTs(decoded.ts);
        read_req->SetRecordTtl(decoded.ttl);
        read_req->SetFinish(remote::DataStoreError::NO_ERROR);
    }
    catch (const std::exception &e)
    {
        LOG(ERROR) << "TiKV Read failed, key: " << physical_key
                   << ", error: " << e.what();
        read_req->SetFinish(remote::DataStoreError::READ_FAILED);
    }
}

void TikvDataStore::BatchWriteRecords(WriteRecordsRequest *batch_write_req)
{
    PoolableGuard req_guard(batch_write_req);

    if (batch_write_req->RecordsCount() == 0)
    {
        batch_write_req->SetFinish(
            MakeCommonResult(remote::DataStoreError::NO_ERROR));
        return;
    }

    if (!kv_client_.IsInitialized())
    {
        batch_write_req->SetFinish(NotStartedResult());
        return;
    }

    remote::CommonResult shard_status_result =
        ShardWriteStatusResult(data_store_service_, shard_id_);
    if (shard_status_result.error_code() != remote::DataStoreError::NO_ERROR)
    {
        batch_write_req->SetFinish(shard_status_result);
        return;
    }

    std::vector<KvMutation> mutations;
    mutations.reserve(batch_write_req->RecordsCount());

    try
    {
        const uint16_t parts_count_per_record =
            batch_write_req->PartsCountPerRecord();
        std::vector<std::string_view> record_parts;
        record_parts.reserve(parts_count_per_record);

        for (size_t i = 0; i < batch_write_req->RecordsCount(); ++i)
        {
            std::string physical_key = BuildKey(batch_write_req, i);
            const WriteOpType op_type = batch_write_req->KeyOpType(i);
            if (op_type == WriteOpType::DELETE)
            {
                mutations.emplace_back(
                    KvMutation::Delete(std::move(physical_key)));
                continue;
            }

            assert(op_type == WriteOpType::PUT);
            record_parts.clear();
            for (uint16_t part = 0; part < parts_count_per_record; ++part)
            {
                record_parts.emplace_back(batch_write_req->GetRecordPart(
                    i * parts_count_per_record + part));
            }

            std::string value = EloqValueCodec::EncodeValue(
                record_parts,
                batch_write_req->GetRecordTs(i),
                batch_write_req->GetRecordTtl(i));
            mutations.emplace_back(
                KvMutation::Put(std::move(physical_key), std::move(value)));
        }
    }
    catch (const std::exception &e)
    {
        LOG(ERROR) << "TiKV BatchWriteRecords failed while building "
                      "mutations, table: "
                   << batch_write_req->GetTableName()
                   << ", error: " << e.what();
        batch_write_req->SetFinish(MakeCommonResult(
            remote::DataStoreError::WRITE_FAILED, e.what()));
        return;
    }

    if (!kv_client_.CommitBatch(mutations))
    {
        batch_write_req->SetFinish(MakeCommonResult(
            remote::DataStoreError::WRITE_FAILED, kv_client_.LastError()));
        return;
    }

    batch_write_req->SetFinish(
        MakeCommonResult(remote::DataStoreError::NO_ERROR));
}

void TikvDataStore::FlushData(FlushDataRequest *flush_data_req)
{
    PoolableGuard req_guard(flush_data_req);

    if (!kv_client_.IsInitialized())
    {
        flush_data_req->SetFinish(NotStartedResult());
        return;
    }

    // TiKV commits are durable when CommitBatch returns. There is no
    // RocksDB-style local flush step for this backend.
    flush_data_req->SetFinish(
        MakeCommonResult(remote::DataStoreError::NO_ERROR));
}

void TikvDataStore::DeleteRange(DeleteRangeRequest *delete_range_req)
{
    PoolableGuard req_guard(delete_range_req);

    if (!kv_client_.IsInitialized())
    {
        delete_range_req->SetFinish(NotStartedResult());
        return;
    }

    delete_range_req->SetFinish(MakeCommonResult(
        remote::DataStoreError::WRITE_FAILED,
        "TiKV DeleteRange is not implemented yet."));
}

void TikvDataStore::CreateTable(CreateTableRequest *create_table_req)
{
    PoolableGuard req_guard(create_table_req);

    if (!kv_client_.IsInitialized())
    {
        create_table_req->SetFinish(NotStartedResult());
        return;
    }

    // TiKV has no per-table physical creation step for the key-prefix based
    // DSS mapping.
    create_table_req->SetFinish(
        MakeCommonResult(remote::DataStoreError::NO_ERROR));
}

void TikvDataStore::DropTable(DropTableRequest *drop_table_req)
{
    PoolableGuard req_guard(drop_table_req);

    if (!kv_client_.IsInitialized())
    {
        drop_table_req->SetFinish(NotStartedResult());
        return;
    }

    drop_table_req->SetFinish(MakeCommonResult(
        remote::DataStoreError::WRITE_FAILED,
        "TiKV DropTable is not implemented yet."));
}

void TikvDataStore::ScanNext(ScanRequest *scan_req)
{
    PoolableGuard req_guard(scan_req);
    scan_req->ClearSessionId();

    if (!kv_client_.IsInitialized())
    {
        scan_req->SetFinish(remote::DataStoreError::DB_NOT_OPEN,
                            "TiKV data store is not started.");
        return;
    }

    scan_req->SetFinish(remote::DataStoreError::READ_FAILED,
                        "TiKV ScanNext is not implemented yet.");
}

void TikvDataStore::ScanClose(ScanRequest *scan_req)
{
    PoolableGuard req_guard(scan_req);

    const std::string &session_id = scan_req->GetSessionId();
    if (!session_id.empty() && data_store_service_ != nullptr)
    {
        data_store_service_->EraseScanIter(shard_id_, session_id);
    }

    scan_req->ClearSessionId();
    scan_req->SetFinish(remote::DataStoreError::NO_ERROR);
}

void TikvDataStore::CreateSnapshotForBackup(
    CreateSnapshotForBackupRequest *req)
{
    PoolableGuard req_guard(req);
    req->SetFinish(
        remote::DataStoreError::CREATE_SNAPSHOT_ERROR,
        "TiKV backend does not support RocksDB snapshot backup files.");
}

void TikvDataStore::SwitchToReadOnly()
{
    // DSS shard mode is tracked by DataStoreService. TiKV has no local
    // background writer to pause for read-only mode.
}

void TikvDataStore::SwitchToReadWrite()
{
    // No local TiKV backend state needs to be resumed.
}

std::string TikvDataStore::BuildKeyPrefix(std::string_view table_name,
                                          int32_t partition_id)
{
    std::string prefix;
    const std::string partition_id_str = std::to_string(partition_id);
    prefix.reserve(table_name.size() + kKeySeparator.size() +
                   partition_id_str.size() + kKeySeparator.size());
    prefix.append(table_name.data(), table_name.size());
    prefix.append(kKeySeparator.data(), kKeySeparator.size());
    prefix.append(partition_id_str);
    prefix.append(kKeySeparator.data(), kKeySeparator.size());
    return prefix;
}

std::string TikvDataStore::BuildKey(std::string_view table_name,
                                    int32_t partition_id,
                                    std::string_view key)
{
    // Do not add TikvConfig::key_prefix_ here. TikvKvClient owns the optional
    // cluster-wide key prefix so all direct get/commit/scan paths encode it in
    // exactly one place.
    std::string physical_key = BuildKeyPrefix(table_name, partition_id);
    physical_key.append(key.data(), key.size());
    return physical_key;
}

std::string TikvDataStore::BuildKey(
    const WriteRecordsRequest *batch_write_req, size_t record_index)
{
    assert(batch_write_req != nullptr);

    std::string physical_key =
        BuildKeyPrefix(batch_write_req->GetTableName(),
                       batch_write_req->GetPartitionId());
    const uint16_t parts_count_per_key = batch_write_req->PartsCountPerKey();
    for (uint16_t part = 0; part < parts_count_per_key; ++part)
    {
        const std::string_view key_part = batch_write_req->GetKeyPart(
            record_index * parts_count_per_key + part);
        physical_key.append(key_part.data(), key_part.size());
    }
    return physical_key;
}

}  // namespace EloqDS
