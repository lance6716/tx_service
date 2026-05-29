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

#include <utility>

#include "data_store_service.h"
#include "ds_request.pb.h"
#include "internal_request.h"
#include "object_pool.h"

namespace EloqDS
{
namespace
{

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

    LOG(WARNING) << "TiKV Read is not implemented yet.";
    read_req->SetFinish(remote::DataStoreError::READ_FAILED);
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

    batch_write_req->SetFinish(MakeCommonResult(
        remote::DataStoreError::WRITE_FAILED,
        "TiKV BatchWriteRecords is not implemented yet."));
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

}  // namespace EloqDS
