#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>

#include "tikv_kv_metrics.h"

namespace EloqDS::tests
{
namespace
{

TEST(TikvKvMetricsTest, OperationLabelsAreBoundedAndScoped)
{
    using tikv_metrics::Operation;

    EXPECT_STREQ(tikv_metrics::OperationLabel(Operation::Read), "read");
    EXPECT_STREQ(tikv_metrics::OperationLabel(Operation::Write), "write");
    EXPECT_STREQ(tikv_metrics::OperationLabel(Operation::DeleteKeys),
                 "delete_keys");
    EXPECT_STREQ(tikv_metrics::OperationLabel(Operation::Scan), "scan");
    EXPECT_STREQ(tikv_metrics::OperationLabel(Operation::RangeDelete),
                 "range_delete");
    EXPECT_STREQ(tikv_metrics::OperationLabel(Operation::Unknown), "unknown");
    EXPECT_STREQ(tikv_metrics::OperationLabel(static_cast<Operation>(-1)),
                 "unknown");

    EXPECT_STREQ(tikv_metrics::CurrentOperationLabel(), "unknown");
    {
        tikv_metrics::OperationScope read_scope(Operation::Read);
        EXPECT_STREQ(tikv_metrics::CurrentOperationLabel(), "read");
        {
            tikv_metrics::OperationScope scan_scope(Operation::Scan);
            EXPECT_STREQ(tikv_metrics::CurrentOperationLabel(), "scan");
        }
        EXPECT_STREQ(tikv_metrics::CurrentOperationLabel(), "read");
    }
    EXPECT_STREQ(tikv_metrics::CurrentOperationLabel(), "unknown");
}

TEST(TikvKvMetricsTest, BackoffTypeLabelsAreBounded)
{
    EXPECT_STREQ(tikv_metrics::BackoffTypeLabel(pingcap::kv::boTiKVRPC),
                 "tikv_rpc");
    EXPECT_STREQ(tikv_metrics::BackoffTypeLabel(pingcap::kv::boTxnLock),
                 "txn_lock");
    EXPECT_STREQ(tikv_metrics::BackoffTypeLabel(pingcap::kv::boRegionMiss),
                 "region_miss");
    EXPECT_STREQ(tikv_metrics::BackoffTypeLabel(pingcap::kv::boTiFlashRPC),
                 "tiflash_rpc");
    EXPECT_STREQ(tikv_metrics::BackoffTypeLabel(
                     static_cast<pingcap::kv::BackoffType>(-1)),
                 "unknown");
}

TEST(TikvKvMetricsTest, CollectBackoffMetricIsSafeWhenMetricsDisabled)
{
    const bool saved_enable_kv_metrics = metrics::enable_kv_metrics;
    auto saved_kv_meter = std::move(metrics::kv_meter);

    pingcap::kv::BackoffEvent event{pingcap::kv::boRegionMiss,
                                    1,
                                    1,
                                    pingcap::kv::GetMaxBackoff,
                                    -1,
                                    1,
                                    0,
                                    std::string(),
                                    false};

    metrics::enable_kv_metrics = false;
    metrics::kv_meter.reset();
    EXPECT_NO_THROW(tikv_metrics::CollectBackoffMetric(event));

    metrics::enable_kv_metrics = true;
    metrics::kv_meter.reset();
    EXPECT_NO_THROW(tikv_metrics::CollectBackoffMetric(event));

    metrics::enable_kv_metrics = saved_enable_kv_metrics;
    metrics::kv_meter = std::move(saved_kv_meter);
}

}  // namespace
}  // namespace EloqDS::tests
