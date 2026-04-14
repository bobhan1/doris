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
import java.util.List;

public class CloudWarmUpJobTableFilterTest {

    private PersistedTableFilterRule rule(String type, String pattern) {
        PersistedTableFilterRule r = new PersistedTableFilterRule();
        r.ruleType = type;
        r.pattern = pattern;
        return r;
    }

    private CloudWarmUpJob.Builder baseBuilder() {
        return new CloudWarmUpJob.Builder()
                .setJobId(1L)
                .setSrcClusterName("src")
                .setDstClusterName("dst")
                .setJobType(CloudWarmUpJob.JobType.CLUSTER)
                .setSyncMode(CloudWarmUpJob.SyncMode.EVENT_DRIVEN);
    }

    @Test
    public void testCanonicalizeIncludeOnly() {
        List<PersistedTableFilterRule> rules = Arrays.asList(
                rule("INCLUDE", "dw.*"),
                rule("INCLUDE", "ods.*"));
        String expr = CloudWarmUpJob.canonicalize(rules);
        // Should be sorted alphabetically
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
    public void testCanonicalizeDeduplicate() {
        List<PersistedTableFilterRule> rules = Arrays.asList(
                rule("INCLUDE", "ods.*"),
                rule("INCLUDE", "ods.*"),
                rule("INCLUDE", "dw.*"));
        String expr = CloudWarmUpJob.canonicalize(rules);
        // Duplicates should be removed
        Assertions.assertEquals("{\"include\":[\"dw.*\",\"ods.*\"]}", expr);
    }

    @Test
    public void testCanonicalizeOrderIndependent() {
        List<PersistedTableFilterRule> rules1 = Arrays.asList(
                rule("INCLUDE", "ods.*"),
                rule("INCLUDE", "dw.*"),
                rule("EXCLUDE", "dw.tmp_*"));
        List<PersistedTableFilterRule> rules2 = Arrays.asList(
                rule("EXCLUDE", "dw.tmp_*"),
                rule("INCLUDE", "dw.*"),
                rule("INCLUDE", "ods.*"));
        Assertions.assertEquals(
                CloudWarmUpJob.canonicalize(rules1),
                CloudWarmUpJob.canonicalize(rules2));
    }

    @Test
    public void testCanonicalizeExcludeOnlyOmitsExcludeKey() {
        // When there are no excludes, the "exclude" key should be absent
        List<PersistedTableFilterRule> rules = Arrays.asList(
                rule("INCLUDE", "ods.*"));
        String expr = CloudWarmUpJob.canonicalize(rules);
        Assertions.assertFalse(expr.contains("exclude"));
    }

    @Test
    public void testCanonicalizeEmpty() {
        String expr = CloudWarmUpJob.canonicalize(new ArrayList<>());
        Assertions.assertEquals("{\"include\":[]}", expr);
    }

    @Test
    public void testRebuildOnTablesFilter() {
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterRules(Arrays.asList(
                        rule("INCLUDE", "ods.*"),
                        rule("EXCLUDE", "ods.tmp_*")))
                .setTableFilterExpr("{\"include\":[\"ods.*\"],\"exclude\":[\"ods.tmp_*\"]}")
                .build();

        // Before rebuild, onTablesFilter is null (transient field)
        job.rebuildOnTablesFilter();

        OnTablesFilter filter = job.getOnTablesFilter();
        Assertions.assertNotNull(filter);
        Assertions.assertTrue(filter.shouldWarmUp("ods", "orders"));
        Assertions.assertFalse(filter.shouldWarmUp("ods", "tmp_staging"));
        Assertions.assertFalse(filter.shouldWarmUp("dw", "something"));
    }

    @Test
    public void testRebuildOnTablesFilterEmpty() {
        CloudWarmUpJob job = baseBuilder().build();
        job.rebuildOnTablesFilter();
        Assertions.assertNull(job.getOnTablesFilter());
    }

    @Test
    public void testHasTableFilter() {
        CloudWarmUpJob jobWithFilter = baseBuilder()
                .setTableFilterRules(Arrays.asList(rule("INCLUDE", "ods.*")))
                .build();
        Assertions.assertTrue(jobWithFilter.hasTableFilter());

        CloudWarmUpJob jobWithoutFilter = baseBuilder().build();
        Assertions.assertFalse(jobWithoutFilter.hasTableFilter());
    }

    @Test
    public void testGetJobInfoHasTableFilterColumns() {
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterExpr("{\"include\":[\"ods.*\"]}")
                .setTableFilterRules(Arrays.asList(rule("INCLUDE", "ods.*")))
                .build();
        List<String> info = job.getJobInfo();
        // getJobInfo should have at least the base columns + 2 new columns (TableFilter, MatchedTables)
        // The last two entries are tableFilterExpr and currentTableIds
        Assertions.assertTrue(info.size() >= 2);
        // Second-to-last should be the filter expr
        String filterCol = info.get(info.size() - 2);
        Assertions.assertEquals("{\"include\":[\"ods.*\"]}", filterCol);
    }
}
