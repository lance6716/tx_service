#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "internal_request.h"
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

std::string RecordWithType(char type, std::string_view payload)
{
    std::string record(1, type);
    record.append(payload.data(), payload.size());
    return record;
}

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

    TikvConfig config_;
    std::unique_ptr<TikvDataStore> store_;
};

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

}  // namespace
}  // namespace EloqDS::tests
