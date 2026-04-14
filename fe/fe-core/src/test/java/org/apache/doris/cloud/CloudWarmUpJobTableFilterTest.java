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

package org.apache.doris.cloud;

import org.apache.doris.cloud.CloudWarmUpJob.PersistedTableFilterRule;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tests for table-filter extensions in {@link CloudWarmUpJob}:
 * canonicalize(), rebuildOnTablesFilter(), hasTableFilter(), getJobInfo(),
 * getMatchedTablesString(), dynamic table ID tracking, SHOW WARM UP JOB columns.
 */
public class CloudWarmUpJobTableFilterTest {

    private static final int COL_JOB_ID = 0;
    private static final int COL_SRC = 1;
    private static final int COL_DST = 2;
    private static final int COL_STATUS = 3;
    private static final int COL_TYPE = 4;
    private static final int COL_SYNC_MODE = 5;
    private static final int COL_CREATE_TIME = 6;
    private static final int COL_START_TIME = 7;
    private static final int COL_FINISH_BATCH = 8;
    private static final int COL_ALL_BATCH = 9;
    private static final int COL_FINISH_TIME = 10;
    private static final int COL_ERR_MSG = 11;
    private static final int COL_TABLES = 12;
    private static final int COL_TABLE_FILTER = 13;
    private static final int COL_MATCHED_TABLES = 14;
    private static final int TOTAL_COLUMNS = 15;

    private PersistedTableFilterRule rule(String type, String pattern) {
        PersistedTableFilterRule r = new PersistedTableFilterRule();
        r.ruleType = type;
        r.pattern = pattern;
        return r;
    }

    private CloudWarmUpJob.Builder baseBuilder() {
        return new CloudWarmUpJob.Builder()
                .setJobId(1L)
                .setSrcClusterName("write_cg")
                .setDstClusterName("read_cg")
                .setJobType(CloudWarmUpJob.JobType.CLUSTER)
                .setSyncMode(CloudWarmUpJob.SyncMode.EVENT_DRIVEN);
    }

    // ===== canonicalize() =====

    @Test
    public void testCanonicalizeIncludeOnly() {
        List<PersistedTableFilterRule> rules = Arrays.asList(
                rule("INCLUDE", "dw.*"),
                rule("INCLUDE", "ods.*"));
        String expr = CloudWarmUpJob.canonicalize(rules);
        Assertions.assertEquals("{\"include\":[\"dw.*\",\"ods.*\"]}", expr);
    }

    @Test
    public void testCanonicalizeWithExclude() {
        List<PersistedTableFilterRule> rules = Arrays.asList(
                rule("INCLUDE", "ods.*"),
                rule("INCLUDE", "dw.*"),
                rule("EXCLUDE", "dw.tmp_*"));
        String expr = CloudWarmUpJob.canonicalize(rules);
        Assertions.assertEquals("{\"include\":[\"dw.*\",\"ods.*\"],\"exclude\":[\"dw.tmp_*\"]}", expr);
    }

    @Test
    public void testCanonicalizeOrderIndependentAndDedup() {
        // Different order + duplicates → same canonical form (FAQ: order doesn't matter)
        List<PersistedTableFilterRule> rules1 = Arrays.asList(
                rule("INCLUDE", "ods.*"), rule("INCLUDE", "dw.*"), rule("EXCLUDE", "dw.tmp_*"));
        List<PersistedTableFilterRule> rules2 = Arrays.asList(
                rule("EXCLUDE", "dw.tmp_*"), rule("INCLUDE", "dw.*"),
                rule("INCLUDE", "ods.*"), rule("INCLUDE", "ods.*"));
        Assertions.assertEquals(
                CloudWarmUpJob.canonicalize(rules1),
                CloudWarmUpJob.canonicalize(rules2));
    }

    @Test
    public void testCanonicalizeExcludeKeyAbsentWhenNoExcludes() {
        String expr = CloudWarmUpJob.canonicalize(Arrays.asList(rule("INCLUDE", "ods.*")));
        Assertions.assertFalse(expr.contains("exclude"));
    }

    @Test
    public void testCanonicalizeEmptyRules() {
        String expr = CloudWarmUpJob.canonicalize(new ArrayList<>());
        Assertions.assertEquals("{\"include\":[]}", expr);
    }

    // ===== rebuildOnTablesFilter() =====

    @Test
    public void testRebuildOnTablesFilter() {
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterRules(Arrays.asList(
                        rule("INCLUDE", "ods.*"), rule("EXCLUDE", "ods.tmp_*")))
                .setTableFilterExpr("{\"include\":[\"ods.*\"],\"exclude\":[\"ods.tmp_*\"]}")
                .build();
        job.rebuildOnTablesFilter();

        OnTablesFilter filter = job.getOnTablesFilter();
        Assertions.assertNotNull(filter);
        Assertions.assertTrue(filter.shouldWarmUp("ods", "orders"));
        Assertions.assertFalse(filter.shouldWarmUp("ods", "tmp_staging"));
        Assertions.assertFalse(filter.shouldWarmUp("dw", "something"));
    }

    @Test
    public void testRebuildOnTablesFilterNoRules() {
        CloudWarmUpJob job = baseBuilder().build();
        job.rebuildOnTablesFilter();
        Assertions.assertNull(job.getOnTablesFilter());
    }

    // ===== hasTableFilter() =====

    @Test
    public void testHasTableFilter() {
        CloudWarmUpJob withFilter = baseBuilder()
                .setTableFilterRules(Arrays.asList(rule("INCLUDE", "ods.*")))
                .build();
        Assertions.assertTrue(withFilter.hasTableFilter());

        CloudWarmUpJob withoutFilter = baseBuilder().build();
        Assertions.assertFalse(withoutFilter.hasTableFilter());
    }

    // ===== getJobInfo() — SHOW WARM UP JOB output =====

    @Test
    public void testGetJobInfoTableLevelJob() {
        // Scenario: user creates a table-level event-driven job and runs SHOW WARM UP JOB
        String filterExpr = "{\"include\":[\"ods.*\"],\"exclude\":[\"ods.tmp_*\"]}";
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterExpr(filterExpr)
                .setTableFilterRules(Arrays.asList(
                        rule("INCLUDE", "ods.*"), rule("EXCLUDE", "ods.tmp_*")))
                .setSyncEvent(CloudWarmUpJob.SyncEvent.LOAD)
                .build();
        // Simulate resolved table IDs
        Set<Long> tableIds = new HashSet<>(Arrays.asList(1001L, 1002L, 1003L));
        job.setCurrentTableIds(tableIds);

        List<String> info = job.getJobInfo();
        Assertions.assertEquals(TOTAL_COLUMNS, info.size());
        Assertions.assertEquals("1", info.get(COL_JOB_ID));
        Assertions.assertEquals("write_cg", info.get(COL_SRC));
        Assertions.assertEquals("read_cg", info.get(COL_DST));
        Assertions.assertEquals("PENDING", info.get(COL_STATUS));
        Assertions.assertEquals("CLUSTER", info.get(COL_TYPE));
        Assertions.assertTrue(info.get(COL_SYNC_MODE).contains("EVENT_DRIVEN"));
        Assertions.assertEquals(filterExpr, info.get(COL_TABLE_FILTER));
        // MatchedTables should contain sorted table IDs
        Assertions.assertEquals("1001, 1002, 1003", info.get(COL_MATCHED_TABLES));
    }

    @Test
    public void testGetJobInfoClusterLevelJob() {
        // Scenario: cluster-level job without ON TABLES — TableFilter and MatchedTables are empty
        CloudWarmUpJob job = baseBuilder()
                .setSyncEvent(CloudWarmUpJob.SyncEvent.LOAD)
                .build();
        List<String> info = job.getJobInfo();
        Assertions.assertEquals(TOTAL_COLUMNS, info.size());
        Assertions.assertEquals("", info.get(COL_TABLE_FILTER));
        Assertions.assertEquals("", info.get(COL_MATCHED_TABLES));
        Assertions.assertEquals("", info.get(COL_TABLES));
    }

    @Test
    public void testGetJobInfoMatchedTablesEmpty() {
        // Scenario: all matched tables have been dropped → MatchedTables becomes empty
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterExpr("{\"include\":[\"ods.*\"]}")
                .setTableFilterRules(Arrays.asList(rule("INCLUDE", "ods.*")))
                .build();
        // Initially had tables, now all dropped
        job.setCurrentTableIds(new HashSet<>());

        List<String> info = job.getJobInfo();
        Assertions.assertEquals("{\"include\":[\"ods.*\"]}", info.get(COL_TABLE_FILTER));
        Assertions.assertEquals("", info.get(COL_MATCHED_TABLES));
    }

    // ===== Dynamic table ID tracking (simulating create/drop/rename) =====

    @Test
    public void testDynamicTableIdTracking() {
        // Scenario: User guide says system re-evaluates every 60s.
        // - Initial: tables 1001, 1002 matched
        // - New table 1003 created → next refresh adds it
        // - Table 1001 dropped → next refresh removes it
        // - Table 1002 renamed (still matches) → stays
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterRules(Arrays.asList(rule("INCLUDE", "ods.*")))
                .setTableFilterExpr("{\"include\":[\"ods.*\"]}")
                .build();

        // Phase 1: initial resolution
        Set<Long> initial = new HashSet<>(Arrays.asList(1001L, 1002L));
        job.setCurrentTableIds(initial);
        Assertions.assertEquals(2, job.getCurrentTableIds().size());
        Assertions.assertTrue(job.getCurrentTableIds().contains(1001L));
        Assertions.assertTrue(job.getCurrentTableIds().contains(1002L));

        // Phase 2: new table created + old table dropped (simulate refresh)
        Set<Long> afterRefresh = new HashSet<>(Arrays.asList(1002L, 1003L));
        job.setCurrentTableIds(afterRefresh);
        Assertions.assertEquals(2, job.getCurrentTableIds().size());
        Assertions.assertFalse(job.getCurrentTableIds().contains(1001L));
        Assertions.assertTrue(job.getCurrentTableIds().contains(1003L));

        // Phase 3: all tables dropped → empty set (Job stays RUNNING per user guide)
        job.setCurrentTableIds(new HashSet<>());
        Assertions.assertTrue(job.getCurrentTableIds().isEmpty());
        // TableFilter expr is still there (job not cancelled)
        Assertions.assertTrue(job.hasTableFilter());
    }

    // ===== Builder validation =====

    @Test
    public void testBuilderMissingRequiredFieldsThrows() {
        Assertions.assertThrows(IllegalStateException.class, () -> {
            new CloudWarmUpJob.Builder().build();
        });
    }
}
