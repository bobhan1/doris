// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "cloud/cloud_warm_up_manager.h"

#include <gtest/gtest.h>

#include <optional>
#include <unordered_set>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "gen_cpp/AgentService_types.h"

namespace doris {

class CloudWarmUpManagerFilterTest : public testing::Test {
public:
    CloudWarmUpManagerFilterTest() : _engine(CloudStorageEngine(EngineOptions {})) {}

protected:
    CloudStorageEngine _engine;
};

// Test EventDrivenJobFilter type alias semantics
TEST_F(CloudWarmUpManagerFilterTest, EventDrivenJobFilterNullopt) {
    EventDrivenJobFilter filter = std::nullopt;
    EXPECT_FALSE(filter.has_value());
}

TEST_F(CloudWarmUpManagerFilterTest, EventDrivenJobFilterWithTableIds) {
    EventDrivenJobFilter filter = std::unordered_set<int64_t> {100, 200, 300};
    EXPECT_TRUE(filter.has_value());
    EXPECT_EQ(3, filter->size());
    EXPECT_TRUE(filter->count(100) > 0);
    EXPECT_TRUE(filter->count(200) > 0);
    EXPECT_TRUE(filter->count(300) > 0);
    EXPECT_TRUE(filter->count(999) == 0);
}

TEST_F(CloudWarmUpManagerFilterTest, EventDrivenJobFilterEmpty) {
    EventDrivenJobFilter filter = std::unordered_set<int64_t> {};
    EXPECT_TRUE(filter.has_value());
    EXPECT_EQ(0, filter->size());
}

// Test set_event with table_ids (cluster-level, no table filter)
TEST_F(CloudWarmUpManagerFilterTest, SetEventWithoutTableIds) {
    CloudWarmUpManager manager(_engine);
    int64_t job_id = 1001;

    auto st = manager.set_event(job_id, TWarmUpEventType::LOAD, false, nullptr);
    EXPECT_TRUE(st.ok());
}

// Test set_event with table_ids (table-level filter)
TEST_F(CloudWarmUpManagerFilterTest, SetEventWithTableIds) {
    CloudWarmUpManager manager(_engine);
    int64_t job_id = 1002;
    std::vector<int64_t> table_ids = {10, 20, 30};

    auto st = manager.set_event(job_id, TWarmUpEventType::LOAD, false, &table_ids);
    EXPECT_TRUE(st.ok());
}

// Test set_event with empty table_ids (should set empty filter, not cluster-level)
// Scenario: all matched tables were deleted, should warm up nothing
TEST_F(CloudWarmUpManagerFilterTest, SetEventWithEmptyTableIds) {
    CloudWarmUpManager manager(_engine);
    int64_t job_id = 1003;
    std::vector<int64_t> table_ids = {};

    auto st = manager.set_event(job_id, TWarmUpEventType::LOAD, false, &table_ids);
    EXPECT_TRUE(st.ok());
}

// Test set_event clear
TEST_F(CloudWarmUpManagerFilterTest, SetEventClear) {
    CloudWarmUpManager manager(_engine);
    int64_t job_id = 1004;
    std::vector<int64_t> table_ids = {10, 20};

    // Set event first
    auto st = manager.set_event(job_id, TWarmUpEventType::LOAD, false, &table_ids);
    EXPECT_TRUE(st.ok());

    // Clear the event
    st = manager.set_event(job_id, TWarmUpEventType::LOAD, true);
    EXPECT_TRUE(st.ok());
}

// Test set_event update table_ids for existing job
TEST_F(CloudWarmUpManagerFilterTest, SetEventUpdateTableIds) {
    CloudWarmUpManager manager(_engine);
    int64_t job_id = 1005;

    // Set initial table_ids
    std::vector<int64_t> initial_ids = {10, 20};
    auto st = manager.set_event(job_id, TWarmUpEventType::LOAD, false, &initial_ids);
    EXPECT_TRUE(st.ok());

    // Update table_ids
    std::vector<int64_t> updated_ids = {30, 40, 50};
    st = manager.set_event(job_id, TWarmUpEventType::LOAD, false, &updated_ids);
    EXPECT_TRUE(st.ok());
}

// Test set_event with unsupported event type
TEST_F(CloudWarmUpManagerFilterTest, SetEventUnsupportedType) {
    CloudWarmUpManager manager(_engine);
    int64_t job_id = 1006;

    // Use a non-LOAD event type — should fail
    auto st = manager.set_event(job_id, TWarmUpEventType::QUERY, false, nullptr);
    EXPECT_FALSE(st.ok());
}

// Test multiple jobs with different filters
TEST_F(CloudWarmUpManagerFilterTest, MultipleJobsDifferentFilters) {
    CloudWarmUpManager manager(_engine);

    // Job 1: cluster-level (no filter)
    auto st = manager.set_event(2001, TWarmUpEventType::LOAD, false, nullptr);
    EXPECT_TRUE(st.ok());

    // Job 2: table-level filter
    std::vector<int64_t> table_ids = {100, 200};
    st = manager.set_event(2002, TWarmUpEventType::LOAD, false, &table_ids);
    EXPECT_TRUE(st.ok());

    // Clear job 1
    st = manager.set_event(2001, TWarmUpEventType::LOAD, true);
    EXPECT_TRUE(st.ok());

    // Job 2 should still be active
    st = manager.set_event(2002, TWarmUpEventType::LOAD, true);
    EXPECT_TRUE(st.ok());
}

} // namespace doris
