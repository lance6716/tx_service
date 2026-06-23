/**
 *    Copyright (C) 2025 EloqData Inc.
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

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "meter.h"
#include "metrics.h"

namespace metrics
{
inline const Name NAME_MONGO_COMMAND_TOTAL{"mongo_command_total"};
inline const Name NAME_MONGO_COMMAND_DURATION{"mongo_command_duration"};
inline const Name NAME_MONGO_COMMAND_AGGREGATED_TOTAL{
    "mongo_command_aggregated_total"};
inline const Name NAME_MONGO_COMMAND_AGGREGATED_DURATION{
    "mongo_command_aggregated_duration"};

inline const Name NAME_MONGO_STORAGE_OPERATION_TOTAL{
    "mongo_storage_operation_total"};
inline const Name NAME_MONGO_STORAGE_OPERATION_DURATION{
    "mongo_storage_operation_duration"};

inline bool enable_mongo_metrics{false};
inline size_t collect_mongo_command_duration_round{100};
inline size_t collect_mongo_storage_duration_round{100};
inline std::unique_ptr<Meter> mongo_meter{nullptr};

inline constexpr std::array<std::string_view, 36> MONGO_COMMAND_LABELS = {
    "find",
    "getMore",
    "insert",
    "update",
    "delete",
    "aggregate",
    "count",
    "distinct",
    "findAndModify",
    "create",
    "drop",
    "dropDatabase",
    "createIndexes",
    "dropIndexes",
    "listCollections",
    "listIndexes",
    "collStats",
    "dbStats",
    "serverStatus",
    "explain",
    "commitTransaction",
    "abortTransaction",
    "doTxn",
    "applyOps",
    "ping",
    "hello",
    "isMaster",
    "ismaster",
    "buildInfo",
    "connectionStatus",
    "authenticate",
    "saslStart",
    "saslContinue",
    "getParameter",
    "setParameter",
    "other"};

inline constexpr std::array<std::string_view, 21>
    MONGO_STORAGE_OPERATION_LABELS = {"catalog_find",
                                      "catalog_insert",
                                      "catalog_update",
                                      "catalog_delete",
                                      "catalog_scan",
                                      "engine_lock_collection",
                                      "engine_create_record_store",
                                      "engine_create_index",
                                      "engine_drop_ident",
                                      "record_find",
                                      "record_insert",
                                      "record_update",
                                      "record_delete",
                                      "record_scan",
                                      "index_insert",
                                      "index_delete",
                                      "index_seek",
                                      "index_next",
                                      "index_scan_open",
                                      "index_scan_batch",
                                      "other"};

inline constexpr std::array<std::pair<std::string_view, std::string_view>, 35>
    MONGO_COMMAND_ACCESS_TYPES = {
        std::pair<std::string_view, std::string_view>{"find", "read"},
        {"getMore", "read"},
        {"insert", "write"},
        {"update", "write"},
        {"delete", "write"},
        {"aggregate", "read"},
        {"count", "read"},
        {"distinct", "read"},
        {"findAndModify", "write"},
        {"create", "write"},
        {"drop", "write"},
        {"dropDatabase", "write"},
        {"createIndexes", "write"},
        {"dropIndexes", "write"},
        {"listCollections", "read"},
        {"listIndexes", "read"},
        {"collStats", "read"},
        {"dbStats", "read"},
        {"serverStatus", "read"},
        {"explain", "read"},
        {"commitTransaction", "write"},
        {"abortTransaction", "write"},
        {"doTxn", "write"},
        {"applyOps", "write"},
        {"ping", "admin"},
        {"hello", "admin"},
        {"isMaster", "admin"},
        {"ismaster", "admin"},
        {"buildInfo", "admin"},
        {"connectionStatus", "admin"},
        {"authenticate", "admin"},
        {"saslStart", "admin"},
        {"saslContinue", "admin"},
        {"getParameter", "admin"},
        {"setParameter", "admin"}};

inline std::array<std::atomic<size_t>, MONGO_COMMAND_LABELS.size()>
    mongo_command_current_rounds{};
inline std::array<std::atomic<size_t>, MONGO_STORAGE_OPERATION_LABELS.size()>
    mongo_storage_current_rounds{};

inline std::vector<std::string> MongoCommandLabelValues()
{
    std::vector<std::string> values;
    values.reserve(MONGO_COMMAND_LABELS.size());
    for (const auto label : MONGO_COMMAND_LABELS)
    {
        values.emplace_back(label);
    }
    return values;
}

inline std::vector<std::string> MongoStorageOperationLabelValues()
{
    std::vector<std::string> values;
    values.reserve(MONGO_STORAGE_OPERATION_LABELS.size());
    for (const auto label : MONGO_STORAGE_OPERATION_LABELS)
    {
        values.emplace_back(label);
    }
    return values;
}

inline size_t MongoCommandMetricIndex(std::string_view command)
{
    for (size_t i = 0; i < MONGO_COMMAND_LABELS.size(); ++i)
    {
        if (MONGO_COMMAND_LABELS[i] == command)
        {
            return i;
        }
    }
    return MONGO_COMMAND_LABELS.size() - 1;
}

inline size_t MongoStorageOperationMetricIndex(std::string_view operation)
{
    for (size_t i = 0; i < MONGO_STORAGE_OPERATION_LABELS.size(); ++i)
    {
        if (MONGO_STORAGE_OPERATION_LABELS[i] == operation)
        {
            return i;
        }
    }
    return MONGO_STORAGE_OPERATION_LABELS.size() - 1;
}

inline std::string_view NormalizeMongoCommandName(std::string_view command)
{
    return MONGO_COMMAND_LABELS[MongoCommandMetricIndex(command)];
}

inline std::string_view NormalizeMongoStorageOperation(
    std::string_view operation)
{
    return MONGO_STORAGE_OPERATION_LABELS[MongoStorageOperationMetricIndex(
        operation)];
}

inline std::string_view MongoCommandAccessType(std::string_view command)
{
    for (const auto &[label, access_type] : MONGO_COMMAND_ACCESS_TYPES)
    {
        if (label == command)
        {
            return access_type;
        }
    }
    return "other";
}

inline bool ShouldCollectRound(std::atomic<size_t> &round, size_t interval)
{
    const size_t current_round = round.fetch_add(1, std::memory_order_relaxed);
    return interval == 0 || (current_round % interval) == 0;
}

inline bool ShouldCollectMongoCommandDuration(std::string_view command)
{
    return ShouldCollectRound(
        mongo_command_current_rounds[MongoCommandMetricIndex(command)],
        collect_mongo_command_duration_round);
}

inline bool ShouldCollectMongoStorageDuration(std::string_view operation)
{
    return ShouldCollectRound(
        mongo_storage_current_rounds[MongoStorageOperationMetricIndex(
            operation)],
        collect_mongo_storage_duration_round);
}

inline void register_mongo_metrics(MetricsRegistry *metrics_registry,
                                   CommonLabels &common_labels)
{
    mongo_meter = std::make_unique<Meter>(metrics_registry, common_labels);

    mongo_meter->Register(
        NAME_MONGO_COMMAND_TOTAL,
        Type::Counter,
        {{"command", MongoCommandLabelValues()},
         {"result", std::vector<std::string>{"ok", "error"}}});
    mongo_meter->Register(NAME_MONGO_COMMAND_DURATION,
                          Type::Histogram,
                          {{"command", MongoCommandLabelValues()}});
    mongo_meter->Register(
        NAME_MONGO_COMMAND_AGGREGATED_TOTAL,
        Type::Counter,
        {{"access_type",
          std::vector<std::string>{"read", "write", "admin", "other"}},
         {"result", std::vector<std::string>{"ok", "error"}}});
    mongo_meter->Register(
        NAME_MONGO_COMMAND_AGGREGATED_DURATION,
        Type::Histogram,
        {{"access_type",
          std::vector<std::string>{"read", "write", "admin", "other"}}});

    mongo_meter->Register(NAME_MONGO_STORAGE_OPERATION_TOTAL,
                          Type::Counter,
                          {{"operation", MongoStorageOperationLabelValues()}});
    mongo_meter->Register(NAME_MONGO_STORAGE_OPERATION_DURATION,
                          Type::Histogram,
                          {{"operation", MongoStorageOperationLabelValues()}});
}

inline void CollectMongoCommandMetrics(std::string_view command,
                                       const char *result,
                                       uint64_t duration,
                                       bool collect_duration)
{
    if (!enable_metrics || !enable_mongo_metrics || mongo_meter == nullptr)
    {
        return;
    }

    command = NormalizeMongoCommandName(command);
    const std::string_view access_type = MongoCommandAccessType(command);
    mongo_meter->Collect(NAME_MONGO_COMMAND_TOTAL, 1, command, result);
    mongo_meter->Collect(
        NAME_MONGO_COMMAND_AGGREGATED_TOTAL, 1, access_type, result);

    if (collect_duration)
    {
        mongo_meter->Collect(NAME_MONGO_COMMAND_DURATION, duration, command);
        mongo_meter->Collect(
            NAME_MONGO_COMMAND_AGGREGATED_DURATION, duration, access_type);
    }
}

inline void CollectMongoStorageMetrics(std::string_view operation,
                                       uint64_t duration,
                                       bool collect_duration)
{
    if (!enable_metrics || !enable_mongo_metrics || mongo_meter == nullptr)
    {
        return;
    }

    operation = NormalizeMongoStorageOperation(operation);
    mongo_meter->Collect(NAME_MONGO_STORAGE_OPERATION_TOTAL, 1, operation);
    if (collect_duration)
    {
        mongo_meter->Collect(
            NAME_MONGO_STORAGE_OPERATION_DURATION, duration, operation);
    }
}

class MongoCommandMetricGuard
{
public:
    explicit MongoCommandMetricGuard(std::string_view command)
        : command_(NormalizeMongoCommandName(command))
    {
        if (enable_metrics && enable_mongo_metrics && mongo_meter != nullptr)
        {
            collect_duration_ = ShouldCollectMongoCommandDuration(command_);
            if (collect_duration_)
            {
                start_ = Clock::now();
            }
        }
    }

    ~MongoCommandMetricGuard()
    {
        if (!done_)
        {
            Done(std::uncaught_exceptions() > 0 ? "error" : "ok");
        }
    }

    void Done(const char *result)
    {
        if (done_)
        {
            return;
        }
        done_ = true;

        uint64_t duration = 0;
        if (collect_duration_)
        {
            duration = std::chrono::duration_cast<std::chrono::microseconds>(
                           Clock::now() - start_)
                           .count();
        }
        CollectMongoCommandMetrics(
            command_, result, duration, collect_duration_);
    }

private:
    std::string_view command_;
    TimePoint start_;
    bool collect_duration_{false};
    bool done_{false};
};

class MongoStorageMetricGuard
{
public:
    explicit MongoStorageMetricGuard(std::string_view operation)
        : operation_(NormalizeMongoStorageOperation(operation))
    {
        if (enable_metrics && enable_mongo_metrics && mongo_meter != nullptr)
        {
            collect_duration_ = ShouldCollectMongoStorageDuration(operation_);
            if (collect_duration_)
            {
                start_ = Clock::now();
            }
        }
    }

    ~MongoStorageMetricGuard()
    {
        uint64_t duration = 0;
        if (collect_duration_)
        {
            duration = std::chrono::duration_cast<std::chrono::microseconds>(
                           Clock::now() - start_)
                           .count();
        }
        CollectMongoStorageMetrics(operation_, duration, collect_duration_);
    }

private:
    std::string_view operation_;
    TimePoint start_;
    bool collect_duration_{false};
};
}  // namespace metrics
