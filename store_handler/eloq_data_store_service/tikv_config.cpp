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

#include "tikv_config.h"

#include <gflags/gflags.h>

#include <algorithm>
#include <cctype>
#include <utility>

DEFINE_string(tikv_pd_endpoints,
              "",
              "Comma separated TiKV PD endpoints, e.g. 127.0.0.1:2379");
DEFINE_string(tikv_key_prefix,
              "",
              "Optional binary-safe TiKV key prefix for this EloqDoc cluster");
DEFINE_uint32(tikv_request_timeout_seconds,
              5,
              "TiKV request timeout in seconds");
DEFINE_uint32(tikv_scan_batch_size, 256, "TiKV scan/delete-range batch size");

namespace EloqDS
{
namespace
{

std::string Trim(std::string_view input)
{
    auto begin = input.begin();
    auto end = input.end();
    while (begin != end && std::isspace(static_cast<unsigned char>(*begin)))
    {
        ++begin;
    }
    while (begin != end && std::isspace(static_cast<unsigned char>(*(end - 1))))
    {
        --end;
    }
    return std::string(begin, end);
}

bool IsFlagDefault(const char *flag_name)
{
    return gflags::GetCommandLineFlagInfoOrDie(flag_name).is_default;
}

uint32_t PositiveUInt32OrDefault(long value, uint32_t default_value)
{
    return value > 0 ? static_cast<uint32_t>(value) : default_value;
}

}  // namespace

std::vector<std::string> TikvConfig::SplitEndpoints(std::string_view endpoints)
{
    std::vector<std::string> result;
    size_t start = 0;
    while (start <= endpoints.size())
    {
        const size_t comma = endpoints.find(',', start);
        const size_t end =
            comma == std::string_view::npos ? endpoints.size() : comma;
        std::string endpoint = Trim(endpoints.substr(start, end - start));
        if (!endpoint.empty())
        {
            result.emplace_back(std::move(endpoint));
        }
        if (comma == std::string_view::npos)
        {
            break;
        }
        start = comma + 1;
    }
    return result;
}

TikvConfig::TikvConfig(const INIReader &config_reader)
{
    std::string endpoints =
        !IsFlagDefault("tikv_pd_endpoints")
            ? FLAGS_tikv_pd_endpoints
            : config_reader.Get("store", "tikv_pd_endpoints", "127.0.0.1:2379");
    pd_endpoints_ = SplitEndpoints(endpoints);
    if (pd_endpoints_.empty())
    {
        pd_endpoints_.emplace_back("127.0.0.1:2379");
    }

    key_prefix_ = !IsFlagDefault("tikv_key_prefix")
                      ? FLAGS_tikv_key_prefix
                      : config_reader.Get("store", "tikv_key_prefix", "");
    request_timeout_seconds_ = PositiveUInt32OrDefault(
        !IsFlagDefault("tikv_request_timeout_seconds")
            ? static_cast<long>(FLAGS_tikv_request_timeout_seconds)
            : config_reader.GetInteger(
                  "store", "tikv_request_timeout_seconds", 5),
        5);
    scan_batch_size_ = PositiveUInt32OrDefault(
        !IsFlagDefault("tikv_scan_batch_size")
            ? static_cast<long>(FLAGS_tikv_scan_batch_size)
            : config_reader.GetInteger("store", "tikv_scan_batch_size", 256),
        256);
}

}  // namespace EloqDS
