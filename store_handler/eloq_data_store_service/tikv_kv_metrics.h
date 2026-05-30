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

#include <pingcap/kv/Backoff.h>

#include "tx_service_metrics.h"

namespace EloqDS::tikv_metrics
{

enum class Operation
{
    Unknown,
    Read,
    Write,
    DeleteKeys,
    Scan,
    RangeDelete
};

inline thread_local Operation current_operation = Operation::Unknown;

inline const char *OperationLabel(Operation operation)
{
    switch (operation)
    {
    case Operation::Read:
        return "read";
    case Operation::Write:
        return "write";
    case Operation::DeleteKeys:
        return "delete_keys";
    case Operation::Scan:
        return "scan";
    case Operation::RangeDelete:
        return "range_delete";
    case Operation::Unknown:
        break;
    }
    return "unknown";
}

inline const char *CurrentOperationLabel()
{
    return OperationLabel(current_operation);
}

class OperationScope
{
public:
    explicit OperationScope(Operation operation)
        : previous_operation_(current_operation)
    {
        current_operation = operation;
    }

    OperationScope(const OperationScope &) = delete;
    OperationScope &operator=(const OperationScope &) = delete;

    ~OperationScope()
    {
        current_operation = previous_operation_;
    }

private:
    Operation previous_operation_;
};

inline bool KvMetricsEnabled()
{
    return metrics::enable_kv_metrics && metrics::kv_meter != nullptr;
}

inline const char *BackoffTypeLabel(pingcap::kv::BackoffType type)
{
    switch (type)
    {
    case pingcap::kv::boTiKVRPC:
        return "tikv_rpc";
    case pingcap::kv::boTxnLock:
        return "txn_lock";
    case pingcap::kv::boTxnLockFast:
        return "txn_lock_fast";
    case pingcap::kv::boPDRPC:
        return "pd_rpc";
    case pingcap::kv::boRegionMiss:
        return "region_miss";
    case pingcap::kv::boRegionScheduling:
        return "region_scheduling";
    case pingcap::kv::boServerBusy:
        return "server_busy";
    case pingcap::kv::boTiKVDiskFull:
        return "tikv_disk_full";
    case pingcap::kv::boTxnNotFound:
        return "txn_not_found";
    case pingcap::kv::boMaxTsNotSynced:
        return "max_ts_not_synced";
    case pingcap::kv::boMaxDataNotReady:
        return "max_data_not_ready";
    case pingcap::kv::boMaxRegionNotInitialized:
        return "max_region_not_initialized";
    case pingcap::kv::boTiFlashRPC:
        return "tiflash_rpc";
    }
    return "unknown";
}

inline const char *BoolLabel(bool value)
{
    return value ? "true" : "false";
}

inline void CollectBackoffMetric(const pingcap::kv::BackoffEvent &event)
{
    if (!KvMetricsEnabled())
    {
        return;
    }

    metrics::kv_meter->Collect(metrics::NAME_KV_TIKV_BACKOFF_TOTAL,
                               1,
                               CurrentOperationLabel(),
                               BackoffTypeLabel(event.type),
                               BoolLabel(event.max_sleep_exceeded));
}

}  // namespace EloqDS::tikv_metrics
