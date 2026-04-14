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

/**
 * Tests for table-filter extensions in {@link CloudWarmUpJob}:
 * canonicalize(), rebuildOnTablesFilter(), hasTableFilter(), getJobInfo().
 */
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
    public void testCanonicalizeDeduplicate() {
        List<PersistedTableFilterRule> rules = Arrays.asList(
                rule("INCLUDE", "ods.*"),
                rule("INCLUDE", "ods.*"),
                rule("INCLUDE", "dw.*"));
        String expr = CloudWarmUpJob.canonicalize(rules);
        Assertions.assertEquals("{\"include\":[\"dw.*\",\"ods.*\"]}", expr);
    }

    @Test
    public void testCanonicalizeIsOrderIndependent() {
        List<PersistedTableFilterRule> rules1 = Arrays.asList(
                rule("INCLUDE", "ods.*"), rule("INCLUDE", "dw.*"), rule("EXCLUDE", "dw.tmp_*"));
        List<PersistedTableFilterRule> rules2 = Arrays.asList(
                rule("EXCLUDE", "dw.tmp_*"), rule("INCLUDE", "dw.*"), rule("INCLUDE", "ods.*"));
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

    // ===== getJobInfo() =====

    @Test
    public void testGetJobInfoIncludesTableFilterColumns() {
        CloudWarmUpJob job = baseBuilder()
                .setTableFilterExpr("{\"include\":[\"ods.*\"]}")
                .setTableFilterRules(Arrays.asList(rule("INCLUDE", "ods.*")))
                .build();
        List<String> info = job.getJobInfo();
        // The last two columns are TableFilter and MatchedTables
        String tableFilterCol = info.get(info.size() - 2);
        Assertions.assertEquals("{\"include\":[\"ods.*\"]}", tableFilterCol);
    }

    @Test
    public void testGetJobInfoWithoutTableFilter() {
        CloudWarmUpJob job = baseBuilder().build();
        List<String> info = job.getJobInfo();
        // tableFilterExpr defaults to "" when not set
        String tableFilterCol = info.get(info.size() - 2);
        Assertions.assertEquals("", tableFilterCol);
    }

    // ===== Builder validation =====

    @Test
    public void testBuilderMissingRequiredFieldsThrows() {
        Assertions.assertThrows(IllegalStateException.class, () -> {
            new CloudWarmUpJob.Builder().build();
        });
    }
}
