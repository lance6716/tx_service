#include <gtest/gtest.h>
#include <pingcap/kv/Txn.h>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "internal_request.h"
#include "eloq_value_codec.h"
#include "tikv_data_store.h"

namespace EloqDS
{
// The smoke test passes nullptr as DataStoreService because shard ownership is
// irrelevant for single-node backend semantics. TikvDataStore still contains a
// defensive Shutdown() branch that references this method, so provide the only
// DataStoreService symbol needed by this standalone test binary.
void DataStoreService::ForceEraseScanIters(uint32_t shard_id)
{
    (void) shard_id;
}
}  // namespace EloqDS

namespace EloqDS::tests
{
namespace
{
using remote::DataStoreError;

std::vector<std::string> SplitEndpoints(std::string_view endpoints)
{
    std::vector<std::string> result;
    size_t start = 0;
    while (start <= endpoints.size())
    {
        size_t comma = endpoints.find(',', start);
        size_t end = comma == std::string_view::npos ? endpoints.size() : comma;
        std::string endpoint(endpoints.substr(start, end - start));
        endpoint.erase(endpoint.begin(),
                       std::find_if(endpoint.begin(),
                                    endpoint.end(),
                                    [](unsigned char ch) { return !std::isspace(ch); }));
        endpoint.erase(std::find_if(endpoint.rbegin(),
                                    endpoint.rend(),
                                    [](unsigned char ch) { return !std::isspace(ch); })
                           .base(),
                       endpoint.end());
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

uint64_t HostToBigEndian(uint64_t value)
{
    uint64_t result = 0;
    for (size_t i = 0; i < sizeof(value); ++i)
    {
        result = (result << 8) | ((value >> (i * 8)) & 0xff);
    }
    return result;
}

std::string EncodeArchiveKey(std::string_view table,
                             std::string_view key,
                             uint64_t commit_ts)
{
    const uint64_t be_commit_ts = HostToBigEndian(commit_ts);
    std::string archive_key;
    archive_key.reserve(table.size() + 1 + key.size() + 1 + sizeof(uint64_t));
    archive_key.append(table.data(), table.size());
    archive_key.push_back('/');
    archive_key.append(key.data(), key.size());
    archive_key.push_back('/');
    archive_key.append(reinterpret_cast<const char *>(&be_commit_ts),
                       sizeof(be_commit_ts));
    return archive_key;
}

std::string EncodeArchiveValue(bool is_deleted, std::string_view payload)
{
    std::string value;
    value.append(reinterpret_cast<const char *>(&is_deleted), sizeof(is_deleted));
    value.append(payload.data(), payload.size());
    return value;
}

uint32_t HashArchivePartition(std::string_view kv_table_name,
                              std::string_view logical_key)
{
    const size_t table_hash = std::hash<std::string_view>()(kv_table_name);
    const size_t key_hash = std::hash<std::string_view>()(logical_key);
    return static_cast<uint32_t>((table_hash ^ (key_hash << 1)) & 0x3ff);
}

std::string SerializeEloqDocRecord(bool is_deleted, std::string_view payload)
{
    std::string value;
    value.append(reinterpret_cast<const char *>(&is_deleted),
                 sizeof(is_deleted));
    if (is_deleted)
    {
        return value;
    }

    // Match DataStoreServiceClient::SerializeTxRecord for EloqDoc records with
    // empty unpack info and the BSON/encoded blob as payload:
    // [is_deleted][unpack_info_size][unpack_info][encoded_blob_size][blob].
    const size_t unpack_info_size = 0;
    value.append(reinterpret_cast<const char *>(&unpack_info_size),
                 sizeof(unpack_info_size));
    const size_t encoded_blob_size = payload.size();
    value.append(reinterpret_cast<const char *>(&encoded_blob_size),
                 sizeof(encoded_blob_size));
    value.append(payload.data(), payload.size());
    return value;
}

struct DecodedEloqDocRecord
{
    bool is_deleted{false};
    std::string payload;
};

DecodedEloqDocRecord DecodeEloqDocRecord(std::string_view value)
{
    EXPECT_GE(value.size(), sizeof(bool));
    DecodedEloqDocRecord decoded;
    size_t offset = 0;
    decoded.is_deleted = *reinterpret_cast<const bool *>(value.data() + offset);
    offset += sizeof(bool);
    if (decoded.is_deleted)
    {
        return decoded;
    }

    EXPECT_GE(value.size(), offset + sizeof(size_t));
    const size_t unpack_info_size =
        *reinterpret_cast<const size_t *>(value.data() + offset);
    offset += sizeof(size_t) + unpack_info_size;

    EXPECT_GE(value.size(), offset + sizeof(size_t));
    const size_t encoded_blob_size =
        *reinterpret_cast<const size_t *>(value.data() + offset);
    offset += sizeof(size_t);
    EXPECT_GE(value.size(), offset + encoded_blob_size);
    decoded.payload.assign(value.data() + offset, encoded_blob_size);
    return decoded;
}

std::string RecordWithType(char type, std::string_view payload)
{
    std::string record(1, type);
    record.append(payload.data(), payload.size());
    return record;
}

std::string PhysicalKey(const TikvConfig &config,
                        std::string_view table,
                        int32_t partition,
                        std::string_view key)
{
    std::string physical_key = config.key_prefix_;
    physical_key.append(table.data(), table.size());
    physical_key.push_back('/');
    physical_key.append(std::to_string(partition));
    physical_key.push_back('/');
    physical_key.append(key.data(), key.size());
    return physical_key;
}

struct SnapshotLookupResult
{
    bool found{false};
    bool is_deleted{false};
    std::string payload;
    uint64_t commit_ts{0};
};

class TestWriteRequest : public WriteRecordsRequest
{
public:
    struct Item
    {
        std::string key;
        std::string value;
        uint64_t ts{0};
        uint64_t ttl{0};
        WriteOpType op{WriteOpType::PUT};
    };

    TestWriteRequest(std::string table, int32_t partition, std::vector<Item> items)
        : table_(std::move(table)), partition_(partition), items_(std::move(items))
    {
    }

    void Clear() override {}

    size_t RecordsCount() const override { return items_.size(); }
    const std::string_view GetTableName() const override { return table_; }
    const std::string_view GetKeyPart(size_t index) const override
    {
        return items_.at(index).key;
    }
    uint16_t PartsCountPerKey() const override { return 1; }
    int32_t GetPartitionId() const override { return partition_; }
    uint32_t GetShardId() const override { return 0; }
    const std::string_view GetRecordPart(size_t index) const override
    {
        return items_.at(index).value;
    }
    uint16_t PartsCountPerRecord() const override { return 1; }
    uint64_t GetRecordTs(size_t index) const override { return items_.at(index).ts; }
    uint64_t GetRecordTtl(size_t index) const override { return items_.at(index).ttl; }
    WriteOpType KeyOpType(size_t index) const override { return items_.at(index).op; }
    bool SkipWal() const override { return false; }
    void SetFinish(const remote::CommonResult &result) override { result_ = result; }

    remote::CommonResult result_;

private:
    std::string table_;
    int32_t partition_;
    std::vector<Item> items_;
};

class TestReadRequest : public ReadRequest
{
public:
    TestReadRequest(std::string table, int32_t partition, std::string key)
        : table_(std::move(table)), partition_(partition), key_(std::move(key))
    {
    }

    void Clear() override {}

    const std::string_view GetTableName() const override { return table_; }
    const std::string_view GetKey() const override { return key_; }
    int32_t GetPartitionId() const override { return partition_; }
    uint32_t GetShardId() const override { return 0; }
    void SetRecord(std::string &&record) override { record_ = std::move(record); }
    void SetRecordTs(uint64_t record_ts) override { ts_ = record_ts; }
    void SetRecordTtl(uint64_t record_ttl) override { ttl_ = record_ttl; }
    void SetFinish(DataStoreError error_code) override { error_ = error_code; }

    DataStoreError error_{DataStoreError::NO_ERROR};
    std::string record_;
    uint64_t ts_{0};
    uint64_t ttl_{0};

private:
    std::string table_;
    int32_t partition_;
    std::string key_;
};

class TestDeleteRangeRequest : public DeleteRangeRequest
{
public:
    TestDeleteRangeRequest(std::string table,
                           int32_t partition,
                           std::string start_key,
                           std::string end_key)
        : table_(std::move(table)),
          partition_(partition),
          start_key_(std::move(start_key)),
          end_key_(std::move(end_key))
    {
    }

    void Clear() override {}

    const std::string_view GetTableName() const override { return table_; }
    int32_t GetPartitionId() const override { return partition_; }
    uint32_t GetShardId() const override { return 0; }
    const std::string_view GetStartKey() const override { return start_key_; }
    const std::string_view GetEndKey() const override { return end_key_; }
    bool SkipWal() const override { return false; }
    void SetFinish(const remote::CommonResult &result) override { result_ = result; }

    remote::CommonResult result_;

private:
    std::string table_;
    int32_t partition_;
    std::string start_key_;
    std::string end_key_;
};

class TestDropTableRequest : public DropTableRequest
{
public:
    explicit TestDropTableRequest(std::string table) : table_(std::move(table)) {}

    void Clear() override {}

    const std::string_view GetTableName() const override { return table_; }
    uint32_t GetShardId() const override { return 0; }
    void SetFinish(const remote::CommonResult &result) override { result_ = result; }

    remote::CommonResult result_;

private:
    std::string table_;
};

class TestFlushDataRequest : public FlushDataRequest
{
public:
    explicit TestFlushDataRequest(std::vector<std::string> tables)
        : tables_(std::move(tables))
    {
    }

    void Clear() override {}

    const std::vector<std::string> &GetKvTableNames() const override
    {
        return tables_;
    }
    uint32_t GetShardId() const override { return 0; }
    void SetFinish(const remote::CommonResult &result) override { result_ = result; }

    remote::CommonResult result_;

private:
    std::vector<std::string> tables_;
};

class TestBackupRequest : public CreateSnapshotForBackupRequest
{
public:
    void Clear() override {}

    uint32_t GetShardId() const override { return 0; }
    std::string_view GetBackupName() const override { return "tikv-smoke"; }
    uint64_t GetBackupTs() const override { return 1; }
    void AddBackupFile(const std::string &file) override { backup_files_.push_back(file); }
    void SetFinish(DataStoreError error_code, const std::string error_message) override
    {
        error_ = error_code;
        error_message_ = error_message;
    }

    std::vector<std::string> backup_files_;
    DataStoreError error_{DataStoreError::NO_ERROR};
    std::string error_message_;
};

class TestScanRequest : public ScanRequest
{
public:
    struct Item
    {
        std::string key;
        std::string value;
        uint64_t ts{0};
        uint64_t ttl{0};
    };

    TestScanRequest(std::string table,
                    int32_t partition,
                    std::string start_key,
                    std::string end_key,
                    bool inclusive_start,
                    bool inclusive_end,
                    bool scan_forward,
                    uint32_t batch_size,
                    std::string session_id = "",
                    bool generate_session_id = true)
        : session_id_(std::move(session_id)),
          table_(std::move(table)),
          partition_(partition),
          start_key_(std::move(start_key)),
          end_key_(std::move(end_key)),
          inclusive_start_(inclusive_start),
          inclusive_end_(inclusive_end),
          scan_forward_(scan_forward),
          batch_size_(batch_size),
          generate_session_id_(generate_session_id)
    {
    }

    void Clear() override {}

    const std::string_view GetTableName() const override { return table_; }
    int32_t GetPartitionId() const override { return partition_; }
    uint32_t GetShardId() const override { return 0; }
    const std::string_view GetStartKey() const override { return start_key_; }
    const std::string_view GetEndKey() const override { return end_key_; }
    bool InclusiveStart() const override { return inclusive_start_; }
    bool InclusiveEnd() const override { return inclusive_end_; }
    bool ScanForward() const override { return scan_forward_; }
    uint32_t BatchSize() const override { return batch_size_; }
    int GetSearchConditionsSize() const override
    {
        return static_cast<int>(conditions_.size());
    }
    const remote::SearchCondition *GetSearchConditions(int index) const override
    {
        return &conditions_.at(index);
    }
    void AddItem(std::string &&key, std::string &&value, uint64_t ts, uint64_t ttl) override
    {
        items_.push_back(Item{std::move(key), std::move(value), ts, ttl});
    }
    void SetSessionId(const std::string &session_id) override
    {
        session_id_ = session_id;
    }
    bool GenerateSessionId() const override { return generate_session_id_; }
    void ClearSessionId() override { session_id_.clear(); }
    const std::string &GetSessionId() override { return session_id_; }
    void SetFinish(DataStoreError error_code, const std::string error_message) override
    {
        error_ = error_code;
        error_message_ = error_message;
    }

    void AddTypeCondition(char type)
    {
        remote::SearchCondition condition;
        condition.set_field_name("type");
        condition.set_value(std::string(1, type));
        conditions_.push_back(std::move(condition));
    }

    DataStoreError error_{DataStoreError::NO_ERROR};
    std::string error_message_;
    std::vector<Item> items_;
    std::string session_id_;

private:
    std::string table_;
    int32_t partition_;
    std::string start_key_;
    std::string end_key_;
    bool inclusive_start_;
    bool inclusive_end_;
    bool scan_forward_;
    uint32_t batch_size_;
    bool generate_session_id_;
    std::vector<remote::SearchCondition> conditions_;
};

class TikvBackendSmokeTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        const char *pd_env = std::getenv("TIKV_PD_ENDPOINTS");
        config_.pd_endpoints_ = SplitEndpoints(pd_env == nullptr ? "127.0.0.1:2379" : pd_env);
        ASSERT_FALSE(config_.pd_endpoints_.empty());

        std::ostringstream prefix;
        prefix << "eloqdoc_tikv_smoke/" << std::chrono::steady_clock::now().time_since_epoch().count() << "/";
        config_.key_prefix_ = prefix.str();
        config_.scan_batch_size_ = 2;
        config_.request_timeout_seconds_ = 5;

        store_ = std::make_unique<TikvDataStore>(config_, 0, nullptr);
        ASSERT_TRUE(store_->Initialize());
        ASSERT_TRUE(store_->StartDB(1));
    }

    void TearDown() override
    {
        if (store_ != nullptr)
        {
            store_->Shutdown();
            store_.reset();
        }
    }

    void Write(std::string_view table,
               int32_t partition,
               std::vector<TestWriteRequest::Item> items)
    {
        TestWriteRequest req(std::string(table), partition, std::move(items));
        store_->BatchWriteRecords(&req);
        ASSERT_EQ(static_cast<DataStoreError>(req.result_.error_code()),
                  DataStoreError::NO_ERROR)
            << req.result_.error_msg();
    }

    void ExpectRead(std::string_view table,
                    int32_t partition,
                    std::string_view key,
                    DataStoreError expected_error,
                    std::string_view expected_record = "",
                    uint64_t expected_ts = 0)
    {
        TestReadRequest req(std::string(table), partition, std::string(key));
        store_->Read(&req);
        ASSERT_EQ(req.error_, expected_error);
        if (expected_error == DataStoreError::NO_ERROR)
        {
            EXPECT_EQ(req.record_, expected_record);
            EXPECT_EQ(req.ts_, expected_ts);
        }
    }

    SnapshotLookupResult SnapshotRecordAt(std::string_view base_table,
                                          int32_t base_partition,
                                          std::string_view key,
                                          uint64_t snapshot_ts)
    {
        TestReadRequest base_read(std::string(base_table),
                                  base_partition,
                                  std::string(key));
        store_->Read(&base_read);
        if (base_read.error_ == DataStoreError::NO_ERROR &&
            base_read.ts_ <= snapshot_ts)
        {
            DecodedEloqDocRecord decoded =
                DecodeEloqDocRecord(base_read.record_);
            return SnapshotLookupResult{!decoded.is_deleted,
                                        decoded.is_deleted,
                                        std::move(decoded.payload),
                                        base_read.ts_};
        }
        if (base_read.error_ != DataStoreError::NO_ERROR &&
            base_read.error_ != DataStoreError::KEY_NOT_FOUND)
        {
            ADD_FAILURE() << "unexpected base read error: "
                          << static_cast<int>(base_read.error_);
            return {};
        }

        return FetchVisibleArchive(base_table, key, snapshot_ts);
    }

    std::map<std::string, SnapshotLookupResult> SnapshotScan(
        std::string_view base_table,
        int32_t base_partition,
        uint64_t snapshot_ts)
    {
        std::map<std::string, SnapshotLookupResult> visible;
        std::string session_id;
        do
        {
            TestScanRequest req(std::string(base_table),
                                base_partition,
                                "",
                                "",
                                true,
                                false,
                                true,
                                2,
                                session_id);
            store_->ScanNext(&req);
            EXPECT_EQ(req.error_, DataStoreError::NO_ERROR)
                << req.error_message_;
            if (req.error_ != DataStoreError::NO_ERROR)
            {
                return visible;
            }

            for (const auto &item : req.items_)
            {
                SnapshotLookupResult result;
                if (item.ts <= snapshot_ts)
                {
                    DecodedEloqDocRecord decoded =
                        DecodeEloqDocRecord(item.value);
                    result = SnapshotLookupResult{!decoded.is_deleted,
                                                  decoded.is_deleted,
                                                  std::move(decoded.payload),
                                                  item.ts};
                }
                else
                {
                    result = FetchVisibleArchive(base_table,
                                                 item.key,
                                                 snapshot_ts);
                }

                if (result.found && !result.is_deleted)
                {
                    visible.emplace(item.key, std::move(result));
                }
            }
            session_id = req.session_id_;
        } while (!session_id.empty());

        return visible;
    }

    SnapshotLookupResult FetchVisibleArchive(std::string_view kv_table_name,
                                             std::string_view logical_key,
                                             uint64_t upper_bound_ts)
    {
        const uint32_t archive_partition =
            HashArchivePartition(kv_table_name, logical_key);
        TestScanRequest req("mvcc_archives",
                            static_cast<int32_t>(archive_partition),
                            EncodeArchiveKey(kv_table_name,
                                             logical_key,
                                             upper_bound_ts),
                            EncodeArchiveKey(kv_table_name, logical_key, 0),
                            true,
                            false,
                            false,
                            1);
        store_->ScanNext(&req);
        EXPECT_EQ(req.error_, DataStoreError::NO_ERROR) << req.error_message_;
        if (req.error_ != DataStoreError::NO_ERROR || req.items_.empty())
        {
            return {};
        }

        DecodedEloqDocRecord decoded = DecodeEloqDocRecord(req.items_[0].value);
        return SnapshotLookupResult{!decoded.is_deleted,
                                    decoded.is_deleted,
                                    std::move(decoded.payload),
                                    req.items_[0].ts};
    }

    void WriteArchive(std::string_view kv_table_name,
                      std::string_view key,
                      uint64_t commit_ts,
                      std::string_view serialized_record)
    {
        const uint32_t archive_partition =
            HashArchivePartition(kv_table_name, key);
        Write("mvcc_archives",
              static_cast<int32_t>(archive_partition),
              {{EncodeArchiveKey(kv_table_name, key, commit_ts),
                std::string(serialized_record),
                commit_ts,
                0,
                WriteOpType::PUT}});
    }

    void RestartStore()
    {
        ASSERT_NE(store_, nullptr);
        store_->Shutdown();
        store_.reset();

        store_ = std::make_unique<TikvDataStore>(config_, 0, nullptr);
        ASSERT_TRUE(store_->Initialize());
        ASSERT_TRUE(store_->StartDB(2));
    }

    TikvConfig config_;
    std::unique_ptr<TikvDataStore> store_;
};

TEST(TikvBackendFaultInjectionSmokeTest, EmptyPdEndpointsFailFast)
{
    TikvConfig bad_config;
    bad_config.pd_endpoints_.clear();
    bad_config.request_timeout_seconds_ = 1;

    TikvDataStore bad_store(bad_config, 0, nullptr);
    ASSERT_TRUE(bad_store.Initialize());
    EXPECT_FALSE(bad_store.StartDB(1));

    TestReadRequest read_req("unavailable", 1, "k");
    bad_store.Read(&read_req);
    EXPECT_EQ(read_req.error_, DataStoreError::DB_NOT_OPEN);
}

TEST_F(TikvBackendSmokeTest, PutReadDeleteRangeDropAndNoOpSemantics)
{
    const std::string table = "objects";
    Write(table,
          7,
          {{"a", RecordWithType('\x01', "alpha"), 10, 0, WriteOpType::PUT},
           {"b", RecordWithType('\x01', "beta"), 11, 0, WriteOpType::PUT},
           {"c", RecordWithType('\x02', "gamma"), 12, 0, WriteOpType::PUT}});

    ExpectRead(table,
               7,
               "a",
               DataStoreError::NO_ERROR,
               RecordWithType('\x01', "alpha"),
               10);

    Write(table, 7, {{"b", "", 0, 0, WriteOpType::DELETE}});
    ExpectRead(table, 7, "b", DataStoreError::KEY_NOT_FOUND);

    Write(table,
          7,
          {{"d", "delta", 20, 0, WriteOpType::PUT},
           {"e", "echo", 21, 0, WriteOpType::PUT},
           {"f", "foxtrot", 22, 0, WriteOpType::PUT},
           {"g", "golf", 23, 0, WriteOpType::PUT}});
    Write(table, 8, {{"z", "other-partition", 30, 0, WriteOpType::PUT}});

    TestDeleteRangeRequest bounded_delete(table, 7, "d", "f");
    store_->DeleteRange(&bounded_delete);
    ASSERT_EQ(static_cast<DataStoreError>(bounded_delete.result_.error_code()),
              DataStoreError::NO_ERROR)
        << bounded_delete.result_.error_msg();
    ExpectRead(table, 7, "d", DataStoreError::KEY_NOT_FOUND);
    ExpectRead(table, 7, "e", DataStoreError::KEY_NOT_FOUND);
    ExpectRead(table, 7, "f", DataStoreError::NO_ERROR, "foxtrot", 22);

    TestDeleteRangeRequest open_ended_delete(table, 7, "f", "");
    store_->DeleteRange(&open_ended_delete);
    ASSERT_EQ(static_cast<DataStoreError>(
                  open_ended_delete.result_.error_code()),
              DataStoreError::NO_ERROR)
        << open_ended_delete.result_.error_msg();
    ExpectRead(table, 7, "f", DataStoreError::KEY_NOT_FOUND);
    ExpectRead(table, 7, "g", DataStoreError::KEY_NOT_FOUND);
    ExpectRead(table, 8, "z", DataStoreError::NO_ERROR, "other-partition", 30);

    TestFlushDataRequest flush({table});
    store_->FlushData(&flush);
    EXPECT_EQ(static_cast<DataStoreError>(flush.result_.error_code()),
              DataStoreError::NO_ERROR);

    TestBackupRequest backup;
    store_->CreateSnapshotForBackup(&backup);
    EXPECT_EQ(backup.error_, DataStoreError::CREATE_SNAPSHOT_ERROR);
    EXPECT_NE(backup.error_message_.find("does not support"), std::string::npos);

    TestDropTableRequest drop(table);
    store_->DropTable(&drop);
    ASSERT_EQ(static_cast<DataStoreError>(drop.result_.error_code()),
              DataStoreError::NO_ERROR)
        << drop.result_.error_msg();
    ExpectRead(table, 8, "z", DataStoreError::KEY_NOT_FOUND);
}

TEST_F(TikvBackendSmokeTest, ForwardReverseScanPaginationAndTypeFiltering)
{
    const std::string table = "scan_objects";
    Write(table,
          1,
          {{"s1", RecordWithType('\x01', "one"), 101, 0, WriteOpType::PUT},
           {"s2", RecordWithType('\x02', "two"), 102, 0, WriteOpType::PUT},
           {"s3", RecordWithType('\x01', "three"), 103, 0, WriteOpType::PUT}});

    TestScanRequest first_page(table, 1, "", "", true, false, true, 1);
    first_page.AddTypeCondition('\x01');
    store_->ScanNext(&first_page);
    ASSERT_EQ(first_page.error_, DataStoreError::NO_ERROR) << first_page.error_message_;
    ASSERT_EQ(first_page.items_.size(), 1U);
    EXPECT_EQ(first_page.items_[0].key, "s1");
    ASSERT_FALSE(first_page.session_id_.empty());

    TestScanRequest second_page(table, 1, "", "", true, false, true, 1, first_page.session_id_);
    second_page.AddTypeCondition('\x01');
    store_->ScanNext(&second_page);
    ASSERT_EQ(second_page.error_, DataStoreError::NO_ERROR) << second_page.error_message_;
    ASSERT_EQ(second_page.items_.size(), 1U);
    EXPECT_EQ(second_page.items_[0].key, "s3");
    if (!second_page.session_id_.empty())
    {
        TestScanRequest end_page(table,
                                 1,
                                 "",
                                 "",
                                 true,
                                 false,
                                 true,
                                 1,
                                 second_page.session_id_);
        end_page.AddTypeCondition('\x01');
        store_->ScanNext(&end_page);
        ASSERT_EQ(end_page.error_, DataStoreError::NO_ERROR)
            << end_page.error_message_;
        EXPECT_TRUE(end_page.items_.empty());
        EXPECT_TRUE(end_page.session_id_.empty());
    }

    TestScanRequest reverse(table, 1, "", "", true, false, false, 10);
    store_->ScanNext(&reverse);
    ASSERT_EQ(reverse.error_, DataStoreError::NO_ERROR) << reverse.error_message_;
    ASSERT_EQ(reverse.items_.size(), 3U);
    EXPECT_EQ(reverse.items_[0].key, "s3");
    EXPECT_EQ(reverse.items_[1].key, "s2");
    EXPECT_EQ(reverse.items_[2].key, "s1");

    TestDropTableRequest drop(table);
    store_->DropTable(&drop);
    ASSERT_EQ(static_cast<DataStoreError>(drop.result_.error_code()),
              DataStoreError::NO_ERROR)
        << drop.result_.error_msg();
}

TEST_F(TikvBackendSmokeTest, ExpiredBaseTtlCleanupOnceDeletesBoundedKeys)
{
    const std::string table = "ttl_cleanup_objects";
    const int32_t partition = 13;
    const uint64_t now_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();

    Write(table,
          partition,
          {{"expired-a", "old-a", 701, now_ms - 1, WriteOpType::PUT},
           {"expired-b", "old-b", 702, now_ms - 1, WriteOpType::PUT},
           {"forever", "forever", 703, 0, WriteOpType::PUT},
           {"live", "live", 704, now_ms + 600000, WriteOpType::PUT},
           {"tombstone",
            SerializeEloqDocRecord(true, ""),
            705,
            now_ms - 1,
            WriteOpType::PUT}});

    ExpiredTtlCleanupRunResult first =
        store_->RunExpiredBaseTtlCleanupOnce(table, partition, "", 10, 1, now_ms);
    ASSERT_TRUE(first.ok) << first.error_message;
    EXPECT_FALSE(first.range_finished);
    EXPECT_EQ(first.scan_batch.expired_items, 1U);
    EXPECT_EQ(first.reread_items, 1U);
    EXPECT_EQ(first.delete_attempt_items, 1U);
    EXPECT_EQ(first.deleted_items, 1U);
    ASSERT_FALSE(first.next_cursor.empty());

    ExpiredTtlCleanupRunResult second = store_->RunExpiredBaseTtlCleanupOnce(
        table, partition, first.next_cursor, 10, 10, now_ms);
    ASSERT_TRUE(second.ok) << second.error_message;
    EXPECT_EQ(second.deleted_items, 1U);

    ExpiredTtlCandidateScanBatch remaining =
        store_->ScanExpiredBaseTtlCandidates(table, partition, "", 10, 10, now_ms);
    ASSERT_TRUE(remaining.ok) << remaining.error_message;
    EXPECT_TRUE(remaining.candidates.empty());

    ExpectRead(table,
               partition,
               "forever",
               DataStoreError::NO_ERROR,
               "forever",
               703);
    ExpectRead(table,
               partition,
               "live",
               DataStoreError::NO_ERROR,
               "live",
               704);

    TestScanRequest scan_after_cleanup(table,
                                       partition,
                                       "",
                                       "",
                                       true,
                                       false,
                                       true,
                                       10);
    store_->ScanNext(&scan_after_cleanup);
    ASSERT_EQ(scan_after_cleanup.error_, DataStoreError::NO_ERROR)
        << scan_after_cleanup.error_message_;
    bool tombstone_found = false;
    for (const auto &item : scan_after_cleanup.items_)
    {
        if (item.key == "tombstone")
        {
            tombstone_found = true;
            EXPECT_EQ(item.value, SerializeEloqDocRecord(true, ""));
            EXPECT_EQ(item.ts, 705U);
            EXPECT_EQ(item.ttl, now_ms - 1);
        }
    }
    EXPECT_TRUE(tombstone_found);

    const std::string archive_key =
        EncodeArchiveKey(table, "archived", 700);
    const int32_t archive_partition = static_cast<int32_t>(
        HashArchivePartition(table, "archived"));
    Write("mvcc_archives",
          archive_partition,
          {{archive_key, "archived-value", 700, now_ms - 1, WriteOpType::PUT}});

    ExpiredTtlCleanupRunResult archive_cleanup =
        store_->RunExpiredBaseTtlCleanupOnce(
            "mvcc_archives", archive_partition, "", 10, 10, now_ms);
    EXPECT_TRUE(archive_cleanup.ok);
    EXPECT_EQ(archive_cleanup.deleted_items, 0U);

    TestScanRequest archive_scan("mvcc_archives",
                                 archive_partition,
                                 "",
                                 "",
                                 true,
                                 false,
                                 true,
                                 10);
    store_->ScanNext(&archive_scan);
    ASSERT_EQ(archive_scan.error_, DataStoreError::NO_ERROR)
        << archive_scan.error_message_;
    ASSERT_EQ(archive_scan.items_.size(), 1U);
    EXPECT_EQ(archive_scan.items_[0].key, archive_key);

    TestDropTableRequest drop_base(table);
    store_->DropTable(&drop_base);
    ASSERT_EQ(static_cast<DataStoreError>(drop_base.result_.error_code()),
              DataStoreError::NO_ERROR)
        << drop_base.result_.error_msg();

    TestDropTableRequest drop_archives("mvcc_archives");
    store_->DropTable(&drop_archives);
    ASSERT_EQ(static_cast<DataStoreError>(drop_archives.result_.error_code()),
              DataStoreError::NO_ERROR)
        << drop_archives.result_.error_msg();
}

TEST_F(TikvBackendSmokeTest, ArchiveReverseScanFindsSnapshotVisibleVersion)
{
    const std::string archive_table = "mvcc_archives";
    const std::string kv_table = "base_table";
    const std::string logical_key = "doc42";
    const int32_t archive_partition = 3;

    Write(archive_table,
          archive_partition,
          {{EncodeArchiveKey(kv_table, logical_key, 100),
            EncodeArchiveValue(false, "v100"),
            100,
            0,
            WriteOpType::PUT},
           {EncodeArchiveKey(kv_table, logical_key, 200),
            EncodeArchiveValue(false, "v200"),
            200,
            0,
            WriteOpType::PUT},
           {EncodeArchiveKey(kv_table, logical_key, 300),
            EncodeArchiveValue(false, "v300"),
            300,
            0,
            WriteOpType::PUT}});

    TestScanRequest visible(archive_table,
                            archive_partition,
                            EncodeArchiveKey(kv_table, logical_key, 250),
                            EncodeArchiveKey(kv_table, logical_key, 0),
                            true,
                            false,
                            false,
                            1);
    store_->ScanNext(&visible);
    ASSERT_EQ(visible.error_, DataStoreError::NO_ERROR) << visible.error_message_;
    ASSERT_EQ(visible.items_.size(), 1U);
    EXPECT_EQ(visible.items_[0].key, EncodeArchiveKey(kv_table, logical_key, 200));
    EXPECT_EQ(visible.items_[0].ts, 200U);
    ASSERT_FALSE(visible.items_[0].value.empty());
    EXPECT_EQ(static_cast<bool>(visible.items_[0].value[0]), false);
    EXPECT_EQ(visible.items_[0].value.substr(1), "v200");

    TestDropTableRequest drop(archive_table);
    store_->DropTable(&drop);
    ASSERT_EQ(static_cast<DataStoreError>(drop.result_.error_code()),
              DataStoreError::NO_ERROR)
        << drop.result_.error_msg();
}

TEST_F(TikvBackendSmokeTest,
       ArchiveRetentionCleanupOnceDeletesOnlyScannerCandidates)
{
    const std::string table = "archive_retention_objects";
    const std::string key = "doc-retained";
    const int32_t archive_partition =
        static_cast<int32_t>(HashArchivePartition(table, key));

    WriteArchive(table, key, 100, SerializeEloqDocRecord(false, "archive-v100"));
    WriteArchive(table, key, 200, SerializeEloqDocRecord(false, "archive-v200"));
    WriteArchive(table, key, 250, SerializeEloqDocRecord(false, "archive-v250"));
    WriteArchive(table, key, 300, SerializeEloqDocRecord(false, "archive-v300"));

    ArchiveRetentionCleanupRunResult disabled =
        store_->RunArchiveRetentionCleanupOnce(archive_partition,
                                               "",
                                               10,
                                               10,
                                               UnknownArchiveCleanupWatermark());
    EXPECT_TRUE(disabled.ok);
    EXPECT_EQ(disabled.deleted_items, 0U);

    ArchiveRetentionCleanupRunResult first =
        store_->RunArchiveRetentionCleanupOnce(
            archive_partition, "", 10, 1, TxServiceArchiveCleanupWatermark(250));
    ASSERT_TRUE(first.ok) << first.error_message;
    EXPECT_FALSE(first.range_finished);
    EXPECT_EQ(first.scan_batch.candidate_items, 1U);
    EXPECT_EQ(first.delete_attempt_items, 1U);
    EXPECT_EQ(first.deleted_items, 1U);
    ASSERT_TRUE(first.scan_batch.has_carry_anchor);

    ArchiveRetentionCleanupRunResult second =
        store_->RunArchiveRetentionCleanupOnce(
            archive_partition,
            first.next_cursor,
            10,
            10,
            TxServiceArchiveCleanupWatermark(250),
            &first.scan_batch.carry_anchor);
    ASSERT_TRUE(second.ok) << second.error_message;
    EXPECT_EQ(second.deleted_items, 1U);

    ArchiveRetentionCandidateScanBatch remaining =
        store_->ScanArchiveRetentionCandidates(
            archive_partition, "", 10, 10, TxServiceArchiveCleanupWatermark(250));
    ASSERT_TRUE(remaining.ok) << remaining.error_message;
    EXPECT_TRUE(remaining.candidates.empty());

    SnapshotLookupResult at_watermark = FetchVisibleArchive(table, key, 250);
    ASSERT_TRUE(at_watermark.found);
    EXPECT_FALSE(at_watermark.is_deleted);
    EXPECT_EQ(at_watermark.commit_ts, 250U);
    EXPECT_EQ(at_watermark.payload, "archive-v250");

    SnapshotLookupResult after_watermark = FetchVisibleArchive(table, key, 350);
    ASSERT_TRUE(after_watermark.found);
    EXPECT_EQ(after_watermark.commit_ts, 300U);

    TestDropTableRequest drop("mvcc_archives");
    store_->DropTable(&drop);
    ASSERT_EQ(static_cast<DataStoreError>(drop.result_.error_code()),
              DataStoreError::NO_ERROR)
        << drop.result_.error_msg();
}

TEST_F(TikvBackendSmokeTest,
       TxServiceSnapshotReadSurvivesFlushAndStoreRestart)
{
    const std::string table = "eloqdoc_snapshot_read";
    const int32_t partition = 11;
    const std::string key = "doc-a";

    Write(table,
          partition,
          {{key,
            SerializeEloqDocRecord(false, "base-v300"),
            300,
            0,
            WriteOpType::PUT}});
    WriteArchive(table, key, 100, SerializeEloqDocRecord(false, "archive-v100"));
    WriteArchive(table, key, 200, SerializeEloqDocRecord(false, "archive-v200"));

    TestFlushDataRequest flush({table, "mvcc_archives"});
    store_->FlushData(&flush);
    ASSERT_EQ(static_cast<DataStoreError>(flush.result_.error_code()),
              DataStoreError::NO_ERROR)
        << flush.result_.error_msg();

    RestartStore();

    SnapshotLookupResult old = SnapshotRecordAt(table, partition, key, 250);
    ASSERT_TRUE(old.found);
    EXPECT_FALSE(old.is_deleted);
    EXPECT_EQ(old.commit_ts, 200U);
    EXPECT_EQ(old.payload, "archive-v200");

    SnapshotLookupResult missing_before_first_archive =
        SnapshotRecordAt(table, partition, key, 50);
    EXPECT_FALSE(missing_before_first_archive.found);

    SnapshotLookupResult latest = SnapshotRecordAt(table, partition, key, 350);
    ASSERT_TRUE(latest.found);
    EXPECT_FALSE(latest.is_deleted);
    EXPECT_EQ(latest.commit_ts, 300U);
    EXPECT_EQ(latest.payload, "base-v300");

    TestDropTableRequest drop_base(table);
    store_->DropTable(&drop_base);
    ASSERT_EQ(static_cast<DataStoreError>(drop_base.result_.error_code()),
              DataStoreError::NO_ERROR)
        << drop_base.result_.error_msg();

    TestDropTableRequest drop_archives("mvcc_archives");
    store_->DropTable(&drop_archives);
    ASSERT_EQ(static_cast<DataStoreError>(drop_archives.result_.error_code()),
              DataStoreError::NO_ERROR)
        << drop_archives.result_.error_msg();
}

TEST_F(TikvBackendSmokeTest,
       TxServiceSnapshotScanKeepsReadTimestampAcrossUpdates)
{
    const std::string table = "eloqdoc_snapshot_scan";
    const int32_t partition = 12;

    // Persist the post-T2 base state, plus the archive rows that TxService
    // writes during checkpoint for the pre-T2 versions. A snapshot scan at
    // read_ts=150 must still see the old versions of a/b, the unchanged c, and
    // must not see d because it was inserted after the snapshot timestamp.
    Write(table,
          partition,
          {{"a", SerializeEloqDocRecord(false, "a-v200"), 200, 0, WriteOpType::PUT},
           {"b", SerializeEloqDocRecord(false, "b-v210"), 210, 0, WriteOpType::PUT},
           {"c", SerializeEloqDocRecord(false, "c-v120"), 120, 0, WriteOpType::PUT},
           {"d", SerializeEloqDocRecord(false, "d-v220"), 220, 0, WriteOpType::PUT}});
    WriteArchive(table, "a", 100, SerializeEloqDocRecord(false, "a-v100"));
    WriteArchive(table, "b", 110, SerializeEloqDocRecord(false, "b-v110"));

    std::map<std::string, SnapshotLookupResult> at_150 =
        SnapshotScan(table, partition, 150);
    ASSERT_EQ(at_150.size(), 3U);
    ASSERT_TRUE(at_150.find("a") != at_150.end());
    ASSERT_TRUE(at_150.find("b") != at_150.end());
    ASSERT_TRUE(at_150.find("c") != at_150.end());
    EXPECT_TRUE(at_150.find("d") == at_150.end());
    EXPECT_EQ(at_150.at("a").commit_ts, 100U);
    EXPECT_EQ(at_150.at("a").payload, "a-v100");
    EXPECT_EQ(at_150.at("b").commit_ts, 110U);
    EXPECT_EQ(at_150.at("b").payload, "b-v110");
    EXPECT_EQ(at_150.at("c").commit_ts, 120U);
    EXPECT_EQ(at_150.at("c").payload, "c-v120");

    std::map<std::string, SnapshotLookupResult> at_250 =
        SnapshotScan(table, partition, 250);
    ASSERT_EQ(at_250.size(), 4U);
    EXPECT_EQ(at_250.at("a").commit_ts, 200U);
    EXPECT_EQ(at_250.at("a").payload, "a-v200");
    EXPECT_EQ(at_250.at("b").commit_ts, 210U);
    EXPECT_EQ(at_250.at("b").payload, "b-v210");
    EXPECT_EQ(at_250.at("c").commit_ts, 120U);
    EXPECT_EQ(at_250.at("c").payload, "c-v120");
    EXPECT_EQ(at_250.at("d").commit_ts, 220U);
    EXPECT_EQ(at_250.at("d").payload, "d-v220");

    TestDropTableRequest drop_base(table);
    store_->DropTable(&drop_base);
    ASSERT_EQ(static_cast<DataStoreError>(drop_base.result_.error_code()),
              DataStoreError::NO_ERROR)
        << drop_base.result_.error_msg();

    TestDropTableRequest drop_archives("mvcc_archives");
    store_->DropTable(&drop_archives);
    ASSERT_EQ(static_cast<DataStoreError>(drop_archives.result_.error_code()),
              DataStoreError::NO_ERROR)
        << drop_archives.result_.error_msg();
}

TEST_F(TikvBackendSmokeTest, TransactionConflictKeepsCommittedValue)
{
    const std::string table = "fault_txn_conflict";
    const int32_t partition = 21;
    const std::string key = "doc";
    const std::string physical_key =
        PhysicalKey(config_, table, partition, key);

    auto cluster = std::make_unique<pingcap::kv::Cluster>(
        config_.pd_endpoints_, config_.cluster_config_);
    pingcap::kv::Txn stale_txn(cluster.get());
    stale_txn.set(physical_key,
                  EloqValueCodec::EncodeValue("stale-value", 400, 0));

    Write(table,
          partition,
          {{key, "committed-value", 410, 0, WriteOpType::PUT}});

    EXPECT_THROW(stale_txn.commit(), pingcap::Exception);
    ExpectRead(table,
               partition,
               key,
               DataStoreError::NO_ERROR,
               "committed-value",
               410);

    TestDropTableRequest drop(table);
    store_->DropTable(&drop);
    ASSERT_EQ(static_cast<DataStoreError>(drop.result_.error_code()),
              DataStoreError::NO_ERROR)
        << drop.result_.error_msg();
}

TEST_F(TikvBackendSmokeTest, FailedBatchWriteLeavesNoPartialVisibleData)
{
    const std::string table = "fault_failed_batch";
    const int32_t partition = 22;
    const std::string first_key = "a";
    const std::string conflict_key = "z";
    const std::string physical_first =
        PhysicalKey(config_, table, partition, first_key);
    const std::string physical_conflict =
        PhysicalKey(config_, table, partition, conflict_key);
    const std::string split_key = PhysicalKey(config_, table, partition, "m");

    Write(table,
          partition,
          {{conflict_key, "committed-z", 600, 0, WriteOpType::PUT}});

    auto cluster = std::make_unique<pingcap::kv::Cluster>(
        config_.pd_endpoints_, config_.cluster_config_);
    cluster->splitRegion(split_key);

    pingcap::kv::Txn stale_batch(cluster.get());
    stale_batch.set(physical_first,
                    EloqValueCodec::EncodeValue("rolled-back-a", 610, 0));
    stale_batch.set(physical_conflict,
                    EloqValueCodec::EncodeValue("stale-z", 611, 0));

    // Make only the second region conflict after the stale batch has picked
    // its start_ts. The first region may already be prewritten before the
    // second region reports write-conflict, so this exercises 2PC cleanup.
    Write(table,
          partition,
          {{conflict_key, "committed-z-after-start", 620, 0, WriteOpType::PUT}});

    EXPECT_THROW(stale_batch.commit(), pingcap::Exception);
    ExpectRead(table,
               partition,
               first_key,
               DataStoreError::KEY_NOT_FOUND);
    ExpectRead(table,
               partition,
               conflict_key,
               DataStoreError::NO_ERROR,
               "committed-z-after-start",
               620);

    TestDropTableRequest drop(table);
    store_->DropTable(&drop);
    ASSERT_EQ(static_cast<DataStoreError>(drop.result_.error_code()),
              DataStoreError::NO_ERROR)
        << drop.result_.error_msg();
}

}  // namespace
}  // namespace EloqDS::tests
