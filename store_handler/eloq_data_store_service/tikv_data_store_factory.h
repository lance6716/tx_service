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

#include <glog/logging.h>

#include <memory>
#include <string>
#include <utility>

#include "data_store_factory.h"
#include "tikv_data_store.h"

namespace EloqDS
{

class TikvDataStoreFactory : public DataStoreFactory
{
public:
    explicit TikvDataStoreFactory(TikvConfig config)
        : config_(std::move(config))
    {
    }

    std::unique_ptr<DataStore> CreateDataStore(
        bool create_if_missing,
        uint32_t shard_id,
        DataStoreService *data_store_service,
        bool start_db = true,
        int64_t term = 0) override
    {
        (void) create_if_missing;

        auto ds =
            std::make_unique<TikvDataStore>(config_,
                                            shard_id,
                                            data_store_service);
        if (!ds->Initialize())
        {
            LOG(ERROR) << "Failed to initialize TiKV data store for shard "
                       << shard_id;
            return nullptr;
        }

        if (start_db && !ds->StartDB(term))
        {
            LOG(ERROR) << "Failed to start TiKV data store for shard "
                       << shard_id;
            return nullptr;
        }
        return ds;
    }

    DataStoreFactoryType DataStoreType() const override
    {
        return DataStoreFactoryType::TIKV_FACTORY;
    }

    std::string GetStoragePath() const override
    {
        return "";
    }

    std::string GetS3BucketName() const override
    {
        return "";
    }

    std::string GetS3ObjectPath() const override
    {
        return "";
    }

    std::string GetS3Region() const override
    {
        return "";
    }

    std::string GetS3EndpointUrl() const override
    {
        return "";
    }

    std::string GetAwsAccessKeyId() const override
    {
        return "";
    }

    std::string GetAwsSecretKey() const override
    {
        return "";
    }

    uint64_t GetSstFileCacheSize() const override
    {
        return 0;
    }

    bool IsCloudMode() const override
    {
        // TiKV is a shared external KV service from the DSS client's point of
        // view; data is not bound to a local RocksDB shard directory.
        return true;
    }

private:
    TikvConfig config_;
};

}  // namespace EloqDS
