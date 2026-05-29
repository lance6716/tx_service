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

namespace EloqDS
{

struct TikvConfig
{
    TikvConfig() = default;
    explicit TikvConfig(const INIReader &config_reader);

    static std::vector<std::string> SplitEndpoints(std::string_view endpoints);

    std::vector<std::string> pd_endpoints_{"127.0.0.1:2379"};
    std::string key_prefix_;
    uint32_t request_timeout_seconds_{5};
    uint32_t scan_batch_size_{256};
    pingcap::ClusterConfig cluster_config_;
};

}  // namespace EloqDS
