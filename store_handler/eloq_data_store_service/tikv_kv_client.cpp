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

#include "tikv_kv_client.h"

#include <glog/logging.h>
#include <pingcap/Exception.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/LockResolver.h>
#include <pingcap/kv/RegionClient.h>
#include <pingcap/kv/Snapshot.h>
#include <pingcap/kv/Txn.h>

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "tx_service_metrics.h"

namespace EloqDS
{
namespace
{

bool StartsWith(const std::string &value, const std::string &prefix)
{
    return value.size() >= prefix.size() &&
           value.compare(0, prefix.size(), prefix) == 0;
}

void SetGetRequestContext(kvrpcpb::GetRequest &request,
                          const pingcap::kv::MinCommitTSPushed &pushed)
{
    auto *context = request.mutable_context();
    context->set_priority(::kvrpcpb::Normal);
    context->set_not_fill_cache(false);
    for (auto ts : pushed.getTimestamps())
    {
        context->add_resolved_locks(ts);
    }
}

bool KvMetricsEnabled()
{
    return metrics::enable_kv_metrics && metrics::kv_meter != nullptr;
}

class KvMetricScope
{
public:
    KvMetricScope(const metrics::Name &total_metric,
                  const metrics::Name &duration_metric)
        : total_metric_(total_metric),
          duration_metric_(duration_metric),
          enabled_(KvMetricsEnabled()),
          start_(enabled_ ? metrics::Clock::now() : metrics::TimePoint{})
    {
    }

    ~KvMetricScope()
    {
        if (!enabled_)
        {
            return;
        }

        metrics::kv_meter->CollectDuration(duration_metric_, start_);
        metrics::kv_meter->Collect(total_metric_, 1);
    }

private:
    const metrics::Name &total_metric_;
    const metrics::Name &duration_metric_;
    bool enabled_{false};
    metrics::TimePoint start_;
};

}  // namespace

TikvKvClient::~TikvKvClient()
{
    Shutdown();
}

bool TikvKvClient::Initialize(const TikvConfig &config)
{
    Shutdown();
    ClearLastError();

    if (config.pd_endpoints_.empty())
    {
        SetLastError("TiKV PD endpoints are empty");
        return false;
    }

    try
    {
        config_ = config;
        cluster_ = std::make_unique<pingcap::kv::Cluster>(
            config_.pd_endpoints_, config_.cluster_config_);
        return true;
    }
    catch (const std::exception &e)
    {
        SetLastError(e.what());
        LOG(ERROR) << "Failed to initialize TiKV client: " << e.what();
        cluster_.reset();
        return false;
    }
}

void TikvKvClient::Shutdown()
{
    cluster_.reset();
}

bool TikvKvClient::IsInitialized() const
{
    return cluster_ != nullptr;
}

std::string TikvKvClient::LastError() const
{
    std::lock_guard<std::mutex> lock(error_mu_);
    return last_error_;
}

KvGetResult TikvKvClient::Get(const std::string &key)
{
    EnsureInitialized();
    ClearLastError();
    KvMetricScope metrics_scope(metrics::NAME_KV_READ_TOTAL,
                                metrics::NAME_KV_READ_DURATION);

    try
    {
        const std::string encoded_key = EncodeKey(key);
        const uint64_t read_ts = cluster_->pd_client->getTS();
        pingcap::kv::Backoffer bo(pingcap::kv::GetMaxBackoff);
        pingcap::kv::MinCommitTSPushed min_commit_ts_pushed;

        for (;;)
        {
            kvrpcpb::GetRequest request;
            request.set_key(encoded_key);
            request.set_version(read_ts);
            SetGetRequestContext(request, min_commit_ts_pushed);

            auto location = cluster_->region_cache->locateKey(bo, encoded_key);
            pingcap::kv::RegionClient region_client(cluster_.get(),
                                                    location.region);

            kvrpcpb::GetResponse response;
            try
            {
                using KvGetRpc = pingcap::kv::RPC_NAME(KvGet);
                region_client.sendReqToRegion<KvGetRpc>(
                    bo,
                    request,
                    &response,
                    pingcap::kv::labelFilterInvalid,
                    RequestTimeoutSeconds());
            }
            catch (pingcap::Exception &e)
            {
                bo.backoff(pingcap::kv::boRegionMiss, e);
                continue;
            }

            if (response.has_error())
            {
                auto lock =
                    pingcap::kv::extractLockFromKeyErr(response.error());
                std::vector<pingcap::kv::LockPtr> locks{lock};
                std::vector<uint64_t> pushed;
                auto before_expired = cluster_->lock_resolver->resolveLocks(
                    bo, read_ts, locks, pushed);

                if (!pushed.empty())
                {
                    min_commit_ts_pushed.addTimestamps(pushed);
                }
                if (before_expired > 0)
                {
                    bo.backoffWithMaxSleep(
                        pingcap::kv::boTxnLockFast,
                        before_expired,
                        pingcap::Exception(
                            "key error : " +
                                response.error().ShortDebugString(),
                            pingcap::LockError));
                }
                continue;
            }

            if (response.not_found())
            {
                return KvGetResult{false, {}};
            }
            return KvGetResult{true, response.value()};
        }
    }
    catch (const std::exception &e)
    {
        SetLastError(e.what());
        LOG(ERROR) << "TiKV Get failed: " << e.what();
        throw;
    }
}

bool TikvKvClient::CommitBatch(const std::vector<KvMutation> &mutations)
{
    EnsureInitialized();
    ClearLastError();
    if (mutations.empty())
    {
        return true;
    }
    KvMetricScope metrics_scope(metrics::NAME_KV_WRITE_TOTAL,
                                metrics::NAME_KV_WRITE_DURATION);

    try
    {
        pingcap::kv::Txn txn(cluster_.get());
        for (const auto &mutation : mutations)
        {
            const std::string encoded_key = EncodeKey(mutation.key);
            switch (mutation.op)
            {
            case KvMutation::Op::Put:
                txn.set(encoded_key, mutation.value);
                break;
            case KvMutation::Op::Delete:
                txn.del(encoded_key);
                break;
            }
        }
        txn.commit();
        return true;
    }
    catch (const std::exception &e)
    {
        SetLastError(e.what());
        LOG(ERROR) << "TiKV CommitBatch failed: " << e.what();
        return false;
    }
}

bool TikvKvClient::DeleteKeysIf(
    const std::vector<std::string> &keys,
    const std::function<KvConditionalDeleteDecision(std::string_view)>
        &predicate,
    KvConditionalDeleteResult *result)
{
    EnsureInitialized();
    ClearLastError();
    if (result != nullptr)
    {
        *result = KvConditionalDeleteResult{};
    }
    if (keys.empty())
    {
        return true;
    }
    KvMetricScope metrics_scope(metrics::NAME_KV_WRITE_TOTAL,
                                metrics::NAME_KV_WRITE_DURATION);

    try
    {
        pingcap::kv::Txn txn(cluster_.get());
        uint32_t delete_attempt_items = 0;
        for (const std::string &key : keys)
        {
            const std::string encoded_key = EncodeKey(key);
            auto [value, found] = txn.get(encoded_key);
            if (result != nullptr)
            {
                ++result->checked_items;
            }

            if (!found)
            {
                if (result != nullptr)
                {
                    ++result->not_found_items;
                    ++result->skipped_items;
                }
                continue;
            }

            const KvConditionalDeleteDecision decision = predicate(value);
            switch (decision)
            {
            case KvConditionalDeleteDecision::Delete:
                txn.del(encoded_key);
                ++delete_attempt_items;
                if (result != nullptr)
                {
                    ++result->delete_attempt_items;
                }
                break;
            case KvConditionalDeleteDecision::Malformed:
                if (result != nullptr)
                {
                    ++result->malformed_items;
                    ++result->skipped_items;
                }
                break;
            case KvConditionalDeleteDecision::Skip:
                if (result != nullptr)
                {
                    ++result->skipped_items;
                }
                break;
            }
        }

        if (delete_attempt_items == 0)
        {
            return true;
        }
        txn.commit();
        if (result != nullptr)
        {
            result->deleted_items = result->delete_attempt_items;
        }
        return true;
    }
    catch (const std::exception &e)
    {
        SetLastError(e.what());
        LOG(ERROR) << "TiKV DeleteKeysIf failed: " << e.what();
        if (result != nullptr)
        {
            result->deleted_items = 0;
        }
        return false;
    }
}

KvScanResult TikvKvClient::Scan(const KvScanOptions &options)
{
    EnsureInitialized();
    ClearLastError();
    KvMetricScope metrics_scope(metrics::NAME_KV_SCAN_TOTAL,
                                metrics::NAME_KV_SCAN_DURATION);

    try
    {
        pingcap::kv::ScanOptions tikv_options;
        tikv_options.start_key = EncodeScanStartKey(options);
        tikv_options.end_key = EncodeScanEndKey(options);
        tikv_options.limit = EffectiveScanLimit(options.limit);
        tikv_options.reverse = options.reverse;
        tikv_options.key_only = options.key_only;
        tikv_options.version = options.version;

        pingcap::kv::Snapshot snapshot(cluster_.get());
        auto tikv_result = snapshot.ScanOnce(tikv_options);

        KvScanResult result;
        result.items.reserve(tikv_result.pairs.size());
        for (const auto &pair : tikv_result.pairs)
        {
            result.items.push_back(
                KvScanItem{StripKeyPrefix(pair.key()), pair.value()});
        }
        result.has_more = tikv_result.has_more;
        result.next_cursor = StripKeyPrefix(tikv_result.next_start_key);
        return result;
    }
    catch (const std::exception &e)
    {
        SetLastError(e.what());
        LOG(ERROR) << "TiKV Scan failed: " << e.what();
        throw;
    }
}

bool TikvKvClient::DeleteRange(const std::string &start_key,
                               const std::string &end_key)
{
    EnsureInitialized();
    ClearLastError();

    std::string cursor = EncodeKey(start_key);
    const std::string end =
        end_key.empty() ? PrefixUpperBound() : EncodeKey(end_key);
    const uint32_t batch_size = EffectiveScanLimit(0);
    KvMetricScope metrics_scope(metrics::NAME_KV_RANGE_DELETE_TOTAL,
                                metrics::NAME_KV_RANGE_DELETE_DURATION);

    try
    {
        for (;;)
        {
            pingcap::kv::Snapshot snapshot(cluster_.get());
            pingcap::kv::ScanOptions scan_options;
            scan_options.start_key = cursor;
            scan_options.end_key = end;
            scan_options.limit = batch_size;
            scan_options.key_only = true;

            auto scan_result = snapshot.ScanOnce(scan_options);
            if (scan_result.pairs.empty())
            {
                return true;
            }

            pingcap::kv::Txn txn(cluster_.get());
            for (const auto &pair : scan_result.pairs)
            {
                txn.del(pair.key());
            }
            txn.commit();

            if (!scan_result.has_more)
            {
                return true;
            }
            cursor = scan_result.next_start_key;
        }
    }
    catch (const std::exception &e)
    {
        SetLastError(e.what());
        LOG(ERROR) << "TiKV DeleteRange failed: " << e.what();
        return false;
    }
}

std::string TikvKvClient::EncodeKey(const std::string &key) const
{
    if (config_.key_prefix_.empty())
    {
        return key;
    }
    std::string encoded_key;
    encoded_key.reserve(config_.key_prefix_.size() + key.size());
    encoded_key.append(config_.key_prefix_);
    encoded_key.append(key);
    return encoded_key;
}

std::string TikvKvClient::StripKeyPrefix(const std::string &key) const
{
    if (config_.key_prefix_.empty())
    {
        return key;
    }
    if (!StartsWith(key, config_.key_prefix_))
    {
        return key;
    }
    return key.substr(config_.key_prefix_.size());
}

std::string TikvKvClient::PrefixUpperBound() const
{
    if (config_.key_prefix_.empty())
    {
        return "";
    }

    std::string bound = config_.key_prefix_;
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

std::string TikvKvClient::EncodeScanStartKey(const KvScanOptions &options) const
{
    if (config_.key_prefix_.empty() || !options.start_key.empty())
    {
        return EncodeKey(options.start_key);
    }
    return options.reverse ? PrefixUpperBound() : config_.key_prefix_;
}

std::string TikvKvClient::EncodeScanEndKey(const KvScanOptions &options) const
{
    if (config_.key_prefix_.empty() || !options.end_key.empty())
    {
        return EncodeKey(options.end_key);
    }
    return options.reverse ? config_.key_prefix_ : PrefixUpperBound();
}

uint32_t TikvKvClient::EffectiveScanLimit(uint32_t requested_limit) const
{
    if (requested_limit > 0)
    {
        return requested_limit;
    }
    return config_.scan_batch_size_ == 0 ? 256 : config_.scan_batch_size_;
}

int TikvKvClient::RequestTimeoutSeconds() const
{
    return static_cast<int>(config_.request_timeout_seconds_ == 0
                                ? 5
                                : config_.request_timeout_seconds_);
}

void TikvKvClient::EnsureInitialized() const
{
    if (!cluster_)
    {
        throw std::logic_error("TiKV client is not initialized");
    }
}

void TikvKvClient::ClearLastError() const
{
    std::lock_guard<std::mutex> lock(error_mu_);
    last_error_.clear();
}

void TikvKvClient::SetLastError(const std::string &error) const
{
    std::lock_guard<std::mutex> lock(error_mu_);
    last_error_ = error;
}

}  // namespace EloqDS
