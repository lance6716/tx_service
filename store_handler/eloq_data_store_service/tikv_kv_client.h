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

#include <pingcap/kv/Cluster.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "tikv_config.h"

namespace EloqDS
{

struct KvGetResult
{
    bool found{false};
    std::string value;
};

struct KvScanItem
{
    std::string key;
    std::string value;
};

struct KvScanOptions
{
    std::string start_key;
    std::string end_key;
    uint32_t limit{0};
    bool reverse{false};
    bool key_only{false};
    uint64_t version{0};
};

struct KvScanResult
{
    std::vector<KvScanItem> items;
    bool has_more{false};
    std::string next_cursor;
};

struct KvMutation
{
    enum class Op
    {
        Put,
        Delete
    };

    static KvMutation Put(std::string key, std::string value)
    {
        return KvMutation{Op::Put, std::move(key), std::move(value)};
    }

    static KvMutation Delete(std::string key)
    {
        return KvMutation{Op::Delete, std::move(key), {}};
    }

    Op op{Op::Put};
    std::string key;
    std::string value;
};

class TikvKvClient
{
public:
    TikvKvClient() = default;
    ~TikvKvClient();

    TikvKvClient(const TikvKvClient &) = delete;
    TikvKvClient &operator=(const TikvKvClient &) = delete;

    bool Initialize(const TikvConfig &config);
    void Shutdown();

    bool IsInitialized() const;
    std::string LastError() const;

    KvGetResult Get(const std::string &key);
    bool CommitBatch(const std::vector<KvMutation> &mutations);
    KvScanResult Scan(const KvScanOptions &options);
    bool DeleteRange(const std::string &start_key, const std::string &end_key);

private:
    std::string EncodeKey(const std::string &key) const;
    std::string StripKeyPrefix(const std::string &key) const;
    std::string PrefixUpperBound() const;
    std::string EncodeScanStartKey(const KvScanOptions &options) const;
    std::string EncodeScanEndKey(const KvScanOptions &options) const;
    uint32_t EffectiveScanLimit(uint32_t requested_limit) const;
    int RequestTimeoutSeconds() const;

    void EnsureInitialized() const;
    void ClearLastError() const;
    void SetLastError(const std::string &error) const;

    TikvConfig config_;
    pingcap::kv::ClusterPtr cluster_;
    mutable std::mutex error_mu_;
    mutable std::string last_error_;
};

}  // namespace EloqDS
