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

#include <pingcap/Config.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "INIReader.h"
#include "tikv_archive_cleanup_watermark.h"

namespace EloqDS
{

struct TikvConfig
{
    TikvConfig() = default;
    explicit TikvConfig(const INIReader &config_reader);

    static std::vector<std::string> SplitEndpoints(std::string_view endpoints);

    ArchiveCleanupWatermark ComputeArchiveCleanupWatermark(
        uint64_t tx_service_oldest_snapshot_ts,
        uint64_t current_eloq_ts) const
    {
        return ComputeSafeArchiveCleanupWatermark(
            tx_service_oldest_snapshot_ts,
            current_eloq_ts,
            archive_cleanup_retention_window_us_);
    }

    std::vector<std::string> pd_endpoints_{"127.0.0.1:2379"};
    std::string key_prefix_;
    uint32_t request_timeout_seconds_{5};
    uint32_t scan_batch_size_{256};
    // Fallback for future archive/tombstone cleanup. 0 means cleanup stays
    // disabled unless TxService provides a safe archive cleanup watermark.
    uint64_t archive_cleanup_retention_window_us_{0};
    pingcap::ClusterConfig cluster_config_;
};

}  // namespace EloqDS
