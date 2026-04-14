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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cloud.OnTablesFilter.TableFilterRule;
import org.apache.doris.cloud.OnTablesFilter.TableFilterRule.RuleType;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.datasource.InternalCatalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tests for CacheHotspotManager's table filter methods:
 * resolveTableIds() and refreshAllTableFilters().
 * Uses Mockito to mock Env.getCurrentInternalCatalog() with fake databases/tables.
 */
public class CacheHotspotManagerTableFilterTest {

    private MockedStatic<Env> envMock;
    private InternalCatalog mockCatalog;
    private CacheHotspotManager manager;
    private List<DatabaseIf<? extends TableIf>> databases;

    @BeforeEach
    public void setUp() {
        mockCatalog = Mockito.mock(InternalCatalog.class);
        envMock = Mockito.mockStatic(Env.class);
        envMock.when(Env::getCurrentInternalCatalog).thenReturn(mockCatalog);

        databases = new ArrayList<>();
        Mockito.when(mockCatalog.getAllDbs()).thenAnswer(inv -> databases);

        manager = new CacheHotspotManager(Mockito.mock(CloudSystemInfoService.class));
    }

    @AfterEach
    public void tearDown() {
        envMock.close();
    }

    @SuppressWarnings("unchecked")
    private DatabaseIf<TableIf> mockDb(String name, TableIf... tables) {
        DatabaseIf<TableIf> db = Mockito.mock(DatabaseIf.class);
        Mockito.when(db.getFullName()).thenReturn(name);
        Mockito.when(db.getTables()).thenReturn(Arrays.asList(tables));
        return db;
    }

    private TableIf mockTable(long id, String name) {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getId()).thenReturn(id);
        Mockito.when(table.getName()).thenReturn(name);
        return table;
    }

    private OnTablesFilter buildFilter(TableFilterRule... rules) {
        return new OnTablesFilter(Arrays.asList(rules));
    }

    // ===== resolveTableIds() =====

    @Test
    public void testResolveTableIdsBasicMatching() {
        // Scenario: INCLUDE 'ods.*' matches all tables in ods database
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "users"),
                mockTable(1003, "tmp_staging")));
        databases.add(mockDb("dw",
                mockTable(2001, "fact_sales")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));
        Set<Long> ids = manager.resolveTableIds(filter);

        Assertions.assertEquals(new HashSet<>(Arrays.asList(1001L, 1002L, 1003L)), ids);
    }

    @Test
    public void testResolveTableIdsWithExclude() {
        // Scenario: INCLUDE 'ods.*' EXCLUDE 'ods.tmp_*' — exclude tmp tables
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "tmp_staging"),
                mockTable(1003, "tmp_data")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"),
                new TableFilterRule(RuleType.EXCLUDE, "ods.tmp_*"));
        Set<Long> ids = manager.resolveTableIds(filter);

        Assertions.assertEquals(new HashSet<>(Arrays.asList(1001L)), ids);
    }

    @Test
    public void testResolveTableIdsMultipleDatabases() {
        // Scenario: INCLUDE 'ods.*', INCLUDE 'dw.fact_*' — match across two databases
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "users")));
        databases.add(mockDb("dw",
                mockTable(2001, "fact_sales"),
                mockTable(2002, "dim_product"),
                mockTable(2003, "fact_orders")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"),
                new TableFilterRule(RuleType.INCLUDE, "dw.fact_*"));
        Set<Long> ids = manager.resolveTableIds(filter);

        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(1001L, 1002L, 2001L, 2003L)), ids);
    }

    @Test
    public void testResolveTableIdsNoMatch() {
        // Scenario: pattern matches nothing → empty set
        databases.add(mockDb("ods", mockTable(1001, "orders")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "nonexistent.*"));
        Set<Long> ids = manager.resolveTableIds(filter);

        Assertions.assertTrue(ids.isEmpty());
    }

    @Test
    public void testResolveTableIdsNullFilter() {
        Set<Long> ids = manager.resolveTableIds(null);
        Assertions.assertTrue(ids.isEmpty());
    }

    @Test
    public void testResolveTableIdsDbNameWithPrefix() {
        // CacheHotspotManager strips "default_cluster:" prefix from db name
        databases.add(mockDb("default_cluster:ods",
                mockTable(1001, "orders")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));
        Set<Long> ids = manager.resolveTableIds(filter);

        Assertions.assertEquals(new HashSet<>(Arrays.asList(1001L)), ids);
    }

    // ===== resolveTableIds() with dynamic table changes =====

    @Test
    public void testResolveTableIdsAfterNewTableCreated() {
        // Initial: ods has orders. After new table created, re-resolve picks it up.
        DatabaseIf<TableIf> odsDb = mockDb("ods", mockTable(1001, "orders"));
        databases.add(odsDb);

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));

        Set<Long> ids1 = manager.resolveTableIds(filter);
        Assertions.assertEquals(1, ids1.size());

        // Simulate new table created: replace the db mock to include new table
        databases.clear();
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1004, "payments")));

        Set<Long> ids2 = manager.resolveTableIds(filter);
        Assertions.assertEquals(new HashSet<>(Arrays.asList(1001L, 1004L)), ids2);
    }

    @Test
    public void testResolveTableIdsAfterTableDropped() {
        // Initial: ods has orders and users. After orders dropped, re-resolve removes it.
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "users")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));

        Set<Long> ids1 = manager.resolveTableIds(filter);
        Assertions.assertEquals(2, ids1.size());

        databases.clear();
        databases.add(mockDb("ods", mockTable(1002, "users")));

        Set<Long> ids2 = manager.resolveTableIds(filter);
        Assertions.assertEquals(new HashSet<>(Arrays.asList(1002L)), ids2);
    }

    @Test
    public void testResolveTableIdsAfterTableRenamed() {
        // Scenario from user guide: INCLUDE 'db.order_*', rename order_2024→archive_2024 → stops matching
        databases.add(mockDb("db",
                mockTable(1001, "order_2024"),
                mockTable(1002, "order_2025")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "db.order_*"));

        Set<Long> ids1 = manager.resolveTableIds(filter);
        Assertions.assertEquals(new HashSet<>(Arrays.asList(1001L, 1002L)), ids1);

        // Rename order_2024 → archive_2024 (no longer matches order_*)
        databases.clear();
        databases.add(mockDb("db",
                mockTable(1001, "archive_2024"),
                mockTable(1002, "order_2025")));

        Set<Long> ids2 = manager.resolveTableIds(filter);
        Assertions.assertEquals(new HashSet<>(Arrays.asList(1002L)), ids2);
    }

    @Test
    public void testResolveTableIdsAfterAllTablesDropped() {
        // User guide: all matched tables dropped → empty set, Job stays RUNNING
        databases.add(mockDb("ods", mockTable(1001, "orders")));

        OnTablesFilter filter = buildFilter(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));

        Set<Long> ids1 = manager.resolveTableIds(filter);
        Assertions.assertEquals(1, ids1.size());

        databases.clear();
        databases.add(mockDb("ods"));  // empty database

        Set<Long> ids2 = manager.resolveTableIds(filter);
        Assertions.assertTrue(ids2.isEmpty());
    }

    // ===== refreshAllTableFilters() =====

    @Test
    public void testRefreshAllTableFiltersUpdatesJobTableIds() throws Exception {
        // Setup: create a job with table filter, add it via replayCloudWarmUpJob
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "users")));

        CloudWarmUpJob.PersistedTableFilterRule rule = new CloudWarmUpJob.PersistedTableFilterRule();
        rule.ruleType = "INCLUDE";
        rule.pattern = "ods.*";

        CloudWarmUpJob job = new CloudWarmUpJob.Builder()
                .setJobId(100L)
                .setSrcClusterName("write_cg")
                .setDstClusterName("read_cg")
                .setJobType(CloudWarmUpJob.JobType.CLUSTER)
                .setSyncMode(CloudWarmUpJob.SyncMode.EVENT_DRIVEN)
                .setTableFilterRules(Arrays.asList(rule))
                .setTableFilterExpr("{\"include\":[\"ods.*\"]}")
                .build();

        manager.replayCloudWarmUpJob(job);

        // Verify initial resolution picked up 2 tables
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(1001L, 1002L)),
                job.getCurrentTableIds());

        // Simulate new table created
        databases.clear();
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "users"),
                mockTable(1003, "payments")));

        manager.refreshAllTableFilters();

        // Verify job now has 3 table IDs
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(1001L, 1002L, 1003L)),
                job.getCurrentTableIds());
    }

    @Test
    public void testRefreshAllTableFiltersSkipsClusterLevelJob() throws Exception {
        // Cluster-level job (no table filter) should not be affected by refresh
        databases.add(mockDb("ods", mockTable(1001, "orders")));

        CloudWarmUpJob clusterJob = new CloudWarmUpJob.Builder()
                .setJobId(200L)
                .setSrcClusterName("write_cg")
                .setDstClusterName("read_cg")
                .setJobType(CloudWarmUpJob.JobType.CLUSTER)
                .setSyncMode(CloudWarmUpJob.SyncMode.EVENT_DRIVEN)
                .build();

        manager.replayCloudWarmUpJob(clusterJob);

        // currentTableIds should be empty (no table filter)
        Assertions.assertTrue(clusterJob.getCurrentTableIds().isEmpty());

        manager.refreshAllTableFilters();

        // Still empty after refresh — cluster-level jobs are skipped
        Assertions.assertTrue(clusterJob.getCurrentTableIds().isEmpty());
    }

    @Test
    public void testRefreshAllTableFiltersHandlesTableDrop() throws Exception {
        // Setup: job matching ods.*, initially 2 tables
        databases.add(mockDb("ods",
                mockTable(1001, "orders"),
                mockTable(1002, "users")));

        CloudWarmUpJob.PersistedTableFilterRule rule = new CloudWarmUpJob.PersistedTableFilterRule();
        rule.ruleType = "INCLUDE";
        rule.pattern = "ods.*";

        CloudWarmUpJob job = new CloudWarmUpJob.Builder()
                .setJobId(300L)
                .setSrcClusterName("write_cg")
                .setDstClusterName("read_cg")
                .setJobType(CloudWarmUpJob.JobType.CLUSTER)
                .setSyncMode(CloudWarmUpJob.SyncMode.EVENT_DRIVEN)
                .setTableFilterRules(Arrays.asList(rule))
                .setTableFilterExpr("{\"include\":[\"ods.*\"]}")
                .build();

        manager.replayCloudWarmUpJob(job);
        Assertions.assertEquals(2, job.getCurrentTableIds().size());

        // Drop one table
        databases.clear();
        databases.add(mockDb("ods", mockTable(1002, "users")));

        manager.refreshAllTableFilters();

        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(1002L)),
                job.getCurrentTableIds());
    }
}
