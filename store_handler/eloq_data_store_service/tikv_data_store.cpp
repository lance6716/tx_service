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

#include <algorithm>
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

remote::CommonResult ShardReadStatusResult(DataStoreService *data_store_service,
                                           uint32_t shard_id)
{
    if (data_store_service == nullptr)
    {
        return MakeCommonResult(remote::DataStoreError::NO_ERROR);
    }

    const DSShardStatus shard_status =
        data_store_service->FetchDSShardStatus(shard_id);
    if (shard_status == DSShardStatus::ReadWrite ||
        shard_status == DSShardStatus::ReadOnly)
    {
        return MakeCommonResult(remote::DataStoreError::NO_ERROR);
    }

    if (shard_status == DSShardStatus::Closed)
    {
        return MakeCommonResult(
            remote::DataStoreError::REQUESTED_NODE_NOT_OWNER,
            "Requested data not on local node.");
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

std::string KeyAfter(std::string_view key)
{
    std::string next(key.data(), key.size());
    next.push_back('\0');
    return next;
}

std::string PrefixUpperBound(std::string_view prefix)
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

bool StartsWith(std::string_view value, std::string_view prefix)
{
    return value.size() >= prefix.size() &&
           value.compare(0, prefix.size(), prefix) == 0;
}

bool StripKeyPrefix(std::string_view physical_key,
                    std::string_view physical_prefix,
                    std::string_view &logical_key)
{
    if (!StartsWith(physical_key, physical_prefix) ||
        physical_key.size() <= physical_prefix.size())
    {
        return false;
    }

    logical_key = physical_key.substr(physical_prefix.size());
    return true;
}

bool MatchesSearchConditions(std::string_view record, const ScanRequest *req)
{
    assert(req != nullptr);
    if (record.empty())
    {
        return true;
    }

    const int conditions_size = req->GetSearchConditionsSize();
    for (int cond_idx = 0; cond_idx < conditions_size; ++cond_idx)
    {
        const remote::SearchCondition *cond =
            req->GetSearchConditions(cond_idx);
        assert(cond != nullptr);
        if (cond->field_name() == "type")
        {
            if (cond->value().empty())
            {
                return false;
            }

            const int8_t obj_type =
                static_cast<int8_t>(cond->value()[0]);
            const int8_t store_obj_type = static_cast<int8_t>(record[0]);
            if (obj_type != store_obj_type)
            {
                return false;
            }
        }
    }

    return true;
}

std::string BuildTablePrefix(std::string_view table_name)
{
    std::string prefix;
    prefix.reserve(table_name.size() + kKeySeparator.size());
    prefix.append(table_name.data(), table_name.size());
    prefix.append(kKeySeparator.data(), kKeySeparator.size());
    return prefix;
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

    remote::CommonResult shard_status_result =
        ShardWriteStatusResult(data_store_service_, shard_id_);
    if (shard_status_result.error_code() != remote::DataStoreError::NO_ERROR)
    {
        delete_range_req->SetFinish(shard_status_result);
        return;
    }

    const std::string physical_start_key =
        BuildKey(delete_range_req->GetTableName(),
                 delete_range_req->GetPartitionId(),
                 delete_range_req->GetStartKey());

    std::string physical_end_key;
    if (delete_range_req->GetEndKey().empty())
    {
        // An empty logical end means "to the end of this table partition", not
        // "to the end of the configured TiKV key prefix". Always pass an
        // explicit partition upper bound to TikvKvClient::DeleteRange().
        physical_end_key = PrefixUpperBound(
            BuildKeyPrefix(delete_range_req->GetTableName(),
                           delete_range_req->GetPartitionId()));
    }
    else
    {
        physical_end_key = BuildKey(delete_range_req->GetTableName(),
                                    delete_range_req->GetPartitionId(),
                                    delete_range_req->GetEndKey());
    }

    if (physical_end_key.empty())
    {
        delete_range_req->SetFinish(MakeCommonResult(
            remote::DataStoreError::WRITE_FAILED,
            "Unable to build TiKV DeleteRange upper bound."));
        return;
    }

    if (physical_start_key >= physical_end_key)
    {
        delete_range_req->SetFinish(
            MakeCommonResult(remote::DataStoreError::NO_ERROR));
        return;
    }

    // SkipWal is a RocksDB-only optimization. TiKV deletes are durable once the
    // underlying transaction commits.
    if (!kv_client_.DeleteRange(physical_start_key, physical_end_key))
    {
        delete_range_req->SetFinish(MakeCommonResult(
            remote::DataStoreError::WRITE_FAILED, kv_client_.LastError()));
        return;
    }

    delete_range_req->SetFinish(
        MakeCommonResult(remote::DataStoreError::NO_ERROR));
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

    remote::CommonResult shard_status_result =
        ShardWriteStatusResult(data_store_service_, shard_id_);
    if (shard_status_result.error_code() != remote::DataStoreError::NO_ERROR)
    {
        drop_table_req->SetFinish(shard_status_result);
        return;
    }

    const std::string table_prefix =
        BuildTablePrefix(drop_table_req->GetTableName());
    const std::string table_prefix_upper = PrefixUpperBound(table_prefix);
    if (table_prefix_upper.empty())
    {
        drop_table_req->SetFinish(MakeCommonResult(
            remote::DataStoreError::WRITE_FAILED,
            "Unable to build TiKV DropTable upper bound."));
        return;
    }

    if (!kv_client_.DeleteRange(table_prefix, table_prefix_upper))
    {
        drop_table_req->SetFinish(MakeCommonResult(
            remote::DataStoreError::WRITE_FAILED, kv_client_.LastError()));
        return;
    }

    // TiKV commits are durable; no RocksDB-style Flush() is required after the
    // range cleanup.
    drop_table_req->SetFinish(
        MakeCommonResult(remote::DataStoreError::NO_ERROR));
}

void TikvDataStore::ScanNext(ScanRequest *scan_req)
{
    PoolableGuard req_guard(scan_req);
    const std::string session_id = scan_req->GetSessionId();
    scan_req->ClearSessionId();

    if (!kv_client_.IsInitialized())
    {
        scan_req->SetFinish(remote::DataStoreError::DB_NOT_OPEN,
                            "TiKV data store is not started.");
        return;
    }

    remote::CommonResult shard_status_result =
        ShardReadStatusResult(data_store_service_, shard_id_);
    if (shard_status_result.error_code() != remote::DataStoreError::NO_ERROR)
    {
        scan_req->SetFinish(static_cast<remote::DataStoreError>(
                                shard_status_result.error_code()),
                            shard_status_result.error_msg());
        return;
    }

    const uint32_t batch_size = scan_req->BatchSize();
    if (batch_size == 0)
    {
        scan_req->SetFinish(remote::DataStoreError::NO_ERROR);
        return;
    }

    const std::string key_prefix =
        BuildKeyPrefix(scan_req->GetTableName(), scan_req->GetPartitionId());
    const std::string key_prefix_upper = PrefixUpperBound(key_prefix);
    const bool scan_forward = scan_req->ScanForward();

    std::string cursor;
    if (!session_id.empty() && StartsWith(session_id, key_prefix))
    {
        // TiKV scans are stateless: the response session id carries the next
        // physical cursor. Existing callers still pass the last returned
        // logical key, but the cursor is more precise when filters skipped
        // keys after the last returned item.
        cursor = session_id;
    }
    else if (!scan_req->GetStartKey().empty())
    {
        const std::string start_key =
            BuildKey(scan_req->GetTableName(),
                     scan_req->GetPartitionId(),
                     scan_req->GetStartKey());
        if (scan_forward)
        {
            cursor =
                scan_req->InclusiveStart() ? start_key : KeyAfter(start_key);
        }
        else
        {
            cursor =
                scan_req->InclusiveStart() ? KeyAfter(start_key) : start_key;
        }
    }
    else
    {
        cursor = scan_forward ? key_prefix : key_prefix_upper;
    }

    std::string end_key;
    if (!scan_req->GetEndKey().empty())
    {
        const std::string physical_end_key =
            BuildKey(scan_req->GetTableName(),
                     scan_req->GetPartitionId(),
                     scan_req->GetEndKey());
        if (scan_forward)
        {
            end_key = scan_req->InclusiveEnd()
                          ? KeyAfter(physical_end_key)
                          : physical_end_key;
        }
        else
        {
            end_key = scan_req->InclusiveEnd()
                          ? physical_end_key
                          : KeyAfter(physical_end_key);
        }
    }
    else
    {
        end_key = scan_forward ? key_prefix_upper : key_prefix;
    }

    uint32_t record_count = 0;
    bool scan_completed = false;
    const bool has_search_conditions =
        scan_req->GetSearchConditionsSize() > 0;

    try
    {
        while (record_count < batch_size && !scan_completed)
        {
            KvScanOptions options;
            options.start_key = cursor;
            options.end_key = end_key;
            options.limit = batch_size - record_count;
            if (has_search_conditions)
            {
                options.limit = std::max(options.limit,
                                         config_.scan_batch_size_);
            }
            options.reverse = !scan_forward;

            KvScanResult result = kv_client_.Scan(options);
            if (result.items.empty())
            {
                scan_completed = true;
                break;
            }

            bool consumed_all_items = true;
            for (const KvScanItem &item : result.items)
            {
                std::string_view logical_key;
                if (!StripKeyPrefix(item.key, key_prefix, logical_key))
                {
                    scan_completed = true;
                    consumed_all_items = false;
                    break;
                }

                auto decoded = EloqValueCodec::DecodeValue(item.value);
                if (!MatchesSearchConditions(decoded.record, scan_req))
                {
                    cursor =
                        scan_forward ? KeyAfter(item.key) : item.key;
                    continue;
                }

                cursor = scan_forward ? KeyAfter(item.key) : item.key;
                scan_req->AddItem(std::string(logical_key),
                                  std::move(decoded.record),
                                  decoded.ts,
                                  decoded.ttl);
                ++record_count;
                if (record_count == batch_size)
                {
                    consumed_all_items = false;
                    break;
                }
            }

            if (record_count == batch_size)
            {
                break;
            }

            if (scan_completed)
            {
                break;
            }

            if (result.has_more)
            {
                cursor = consumed_all_items ? result.next_cursor : cursor;
                if (cursor.empty())
                {
                    scan_completed = true;
                }
            }
            else
            {
                scan_completed = true;
            }
        }

        if (!scan_completed && scan_req->GenerateSessionId() &&
            !cursor.empty())
        {
            scan_req->SetSessionId(cursor);
        }

        scan_req->SetFinish(remote::DataStoreError::NO_ERROR);
    }
    catch (const std::exception &e)
    {
        LOG(ERROR) << "TiKV ScanNext failed, table: "
                   << scan_req->GetTableName()
                   << ", partition: " << scan_req->GetPartitionId()
                   << ", error: " << e.what();
        scan_req->SetFinish(remote::DataStoreError::READ_FAILED, e.what());
    }
}

void TikvDataStore::ScanClose(ScanRequest *scan_req)
{
    PoolableGuard req_guard(scan_req);

    // TiKV scans are stateless. ScanNext returns the next physical cursor in
    // the response session id; there is no server-side iterator to release.
    scan_req->ClearSessionId();
    scan_req->SetFinish(remote::DataStoreError::NO_ERROR);
}

ExpiredTtlCandidateScanBatch TikvDataStore::ScanExpiredBaseTtlCandidates(
    std::string_view table_name,
    int32_t partition_id,
    std::string_view cursor,
    uint32_t max_scan_items,
    uint32_t max_candidates,
    uint64_t now_ms)
{
    ExpiredTtlCandidateScanBatch batch;
    if (!IsBaseTableForExpiredTtlCleanup(table_name) ||
        max_scan_items == 0 || max_candidates == 0)
    {
        return batch;
    }

    if (!kv_client_.IsInitialized())
    {
        batch.ok = false;
        batch.range_finished = false;
        batch.error_message = "TiKV data store is not started.";
        return batch;
    }

    const std::string physical_prefix =
        BuildExpiredTtlPartitionPrefix(table_name, partition_id);
    const std::string prefix_upper =
        PrefixUpperBoundForExpiredTtlCleanup(physical_prefix);
    if (prefix_upper.empty())
    {
        batch.ok = false;
        batch.range_finished = false;
        batch.error_message =
            "Unable to build TiKV expired TTL scan upper bound.";
        return batch;
    }

    try
    {
        KvScanOptions options;
        options.start_key =
            NormalizeExpiredTtlScanCursor(physical_prefix, cursor);
        options.end_key = prefix_upper;
        options.limit = max_scan_items;

        KvScanResult scan_result = kv_client_.Scan(options);
        std::vector<ExpiredTtlScanItem> items;
        items.reserve(scan_result.items.size());
        for (KvScanItem &item : scan_result.items)
        {
            items.push_back(
                ExpiredTtlScanItem{std::move(item.key), std::move(item.value)});
        }

        return CollectExpiredTtlCandidatesFromScan(physical_prefix,
                                                   items,
                                                   scan_result.has_more,
                                                   scan_result.next_cursor,
                                                   now_ms,
                                                   max_candidates);
    }
    catch (const std::exception &e)
    {
        batch.ok = false;
        batch.range_finished = false;
        batch.error_message = e.what();
        return batch;
    }
}

ExpiredTtlCleanupRunResult TikvDataStore::RunExpiredBaseTtlCleanupOnce(
    std::string_view table_name,
    int32_t partition_id,
    std::string_view cursor,
    uint32_t max_scan_items,
    uint32_t max_delete_items,
    uint64_t now_ms)
{
    ExpiredTtlCleanupRunResult result;
    if (!IsBaseTableForExpiredTtlCleanup(table_name) ||
        max_scan_items == 0 || max_delete_items == 0)
    {
        return result;
    }

    ExpiredTtlCandidateScanBatch scan_batch =
        ScanExpiredBaseTtlCandidates(table_name,
                                     partition_id,
                                     cursor,
                                     max_scan_items,
                                     max_delete_items,
                                     now_ms);
    result.scan_batch = scan_batch;
    result.range_finished = scan_batch.range_finished;
    result.next_cursor = scan_batch.next_cursor;
    if (!scan_batch.ok)
    {
        result.ok = false;
        result.error_message = scan_batch.error_message;
        return result;
    }
    if (scan_batch.candidates.empty())
    {
        return result;
    }

    std::vector<std::string> keys;
    keys.reserve(scan_batch.candidates.size());
    for (const ExpiredTtlCleanupCandidate &candidate :
         scan_batch.candidates)
    {
        keys.push_back(candidate.physical_key);
    }

    KvConditionalDeleteResult delete_result;
    const bool deleted = kv_client_.DeleteKeysIf(
        keys,
        [now_ms](std::string_view current_value) {
            const ExpiredTtlDeleteCheck check =
                CheckExpiredTtlDeleteCandidate(current_value, now_ms);
            switch (check.decision)
            {
            case ExpiredTtlDeleteDecision::Delete:
                return KvConditionalDeleteDecision::Delete;
            case ExpiredTtlDeleteDecision::Malformed:
                return KvConditionalDeleteDecision::Malformed;
            case ExpiredTtlDeleteDecision::Skip:
                return KvConditionalDeleteDecision::Skip;
            }
            return KvConditionalDeleteDecision::Skip;
        },
        &delete_result);

    result.reread_items = delete_result.checked_items;
    result.not_found_items = delete_result.not_found_items;
    result.delete_skipped_items = delete_result.skipped_items;
    result.delete_malformed_items = delete_result.malformed_items;
    result.delete_attempt_items = delete_result.delete_attempt_items;
    result.deleted_items = delete_result.deleted_items;
    if (!deleted)
    {
        result.ok = false;
        result.error_message = kv_client_.LastError();
        if (result.error_message.empty())
        {
            result.error_message =
                "TiKV expired TTL cleanup delete failed.";
        }
        result.range_finished = false;
        result.next_cursor = NormalizeExpiredTtlScanCursor(
            BuildExpiredTtlPartitionPrefix(table_name, partition_id),
            cursor);
    }
    return result;
}

ArchiveRetentionCandidateScanBatch TikvDataStore::ScanArchiveRetentionCandidates(
    int32_t partition_id,
    std::string_view cursor,
    uint32_t max_scan_items,
    uint32_t max_candidates,
    const ArchiveCleanupWatermark &watermark,
    const ArchiveRetentionAnchor *initial_anchor)
{
    ArchiveRetentionCandidateScanBatch batch;
    if (!ArchiveRetentionCleanupEnabled(watermark) ||
        max_scan_items == 0 || max_candidates == 0)
    {
        return batch;
    }

    if (!kv_client_.IsInitialized())
    {
        batch.ok = false;
        batch.range_finished = false;
        batch.error_message = "TiKV data store is not started.";
        return batch;
    }

    const std::string physical_prefix =
        BuildKeyPrefix(kMvccArchivesTableName, partition_id);
    const std::string prefix_upper = PrefixUpperBound(physical_prefix);
    if (prefix_upper.empty())
    {
        batch.ok = false;
        batch.range_finished = false;
        batch.error_message =
            "Unable to build TiKV archive retention scan upper bound.";
        return batch;
    }

    try
    {
        KvScanOptions options;
        options.start_key =
            NormalizeExpiredTtlScanCursor(physical_prefix, cursor);
        options.end_key = prefix_upper;
        options.limit = max_scan_items;

        KvScanResult scan_result = kv_client_.Scan(options);
        std::vector<ArchiveRetentionScanItem> items;
        items.reserve(scan_result.items.size());
        for (KvScanItem &item : scan_result.items)
        {
            items.push_back(ArchiveRetentionScanItem{std::move(item.key),
                                                     std::move(item.value)});
        }

        return CollectArchiveRetentionCandidatesFromScan(physical_prefix,
                                                         items,
                                                         scan_result.has_more,
                                                         scan_result.next_cursor,
                                                         watermark,
                                                         max_candidates,
                                                         initial_anchor);
    }
    catch (const std::exception &e)
    {
        batch.ok = false;
        batch.range_finished = false;
        batch.error_message = e.what();
        return batch;
    }
}

void TikvDataStore::CreateSnapshotForBackup(
    CreateSnapshotForBackupRequest *req)
{
    PoolableGuard req_guard(req);
    req->SetFinish(
        remote::DataStoreError::CREATE_SNAPSHOT_ERROR,
        "TiKV backend does not support DataStore snapshot backup files.");
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
