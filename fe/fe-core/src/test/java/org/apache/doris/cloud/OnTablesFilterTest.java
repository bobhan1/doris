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

import org.apache.doris.cloud.OnTablesFilter.TableFilterRule;
import org.apache.doris.cloud.OnTablesFilter.TableFilterRule.RuleType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class OnTablesFilterTest {

    @Test
    public void testIncludeAllWildcard() {
        List<TableFilterRule> rules = Collections.singletonList(
                new TableFilterRule(RuleType.INCLUDE, "*.*"));
        OnTablesFilter filter = new OnTablesFilter(rules);
        Assertions.assertTrue(filter.shouldWarmUp("db1", "tbl1"));
        Assertions.assertTrue(filter.shouldWarmUp("db2", "xyz"));
    }

    @Test
    public void testIncludeSpecificDb() {
        List<TableFilterRule> rules = Collections.singletonList(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"));
        OnTablesFilter filter = new OnTablesFilter(rules);
        Assertions.assertTrue(filter.shouldWarmUp("ods", "orders"));
        Assertions.assertTrue(filter.shouldWarmUp("ods", "users"));
        Assertions.assertFalse(filter.shouldWarmUp("dw", "orders"));
    }

    @Test
    public void testIncludeSpecificTable() {
        List<TableFilterRule> rules = Collections.singletonList(
                new TableFilterRule(RuleType.INCLUDE, "ods.orders"));
        OnTablesFilter filter = new OnTablesFilter(rules);
        Assertions.assertTrue(filter.shouldWarmUp("ods", "orders"));
        Assertions.assertFalse(filter.shouldWarmUp("ods", "users"));
        Assertions.assertFalse(filter.shouldWarmUp("dw", "orders"));
    }

    @Test
    public void testExcludeWithInclude() {
        List<TableFilterRule> rules = Arrays.asList(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"),
                new TableFilterRule(RuleType.EXCLUDE, "ods.tmp_*"));
        OnTablesFilter filter = new OnTablesFilter(rules);
        Assertions.assertTrue(filter.shouldWarmUp("ods", "orders"));
        Assertions.assertFalse(filter.shouldWarmUp("ods", "tmp_staging"));
        Assertions.assertFalse(filter.shouldWarmUp("dw", "orders"));
    }

    @Test
    public void testMultipleIncludes() {
        List<TableFilterRule> rules = Arrays.asList(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"),
                new TableFilterRule(RuleType.INCLUDE, "dw.*"));
        OnTablesFilter filter = new OnTablesFilter(rules);
        Assertions.assertTrue(filter.shouldWarmUp("ods", "orders"));
        Assertions.assertTrue(filter.shouldWarmUp("dw", "fact_sales"));
        Assertions.assertFalse(filter.shouldWarmUp("staging", "temp"));
    }

    @Test
    public void testExcludeOnlyDoesNotMatch() {
        // With no INCLUDE rules, nothing matches
        List<TableFilterRule> rules = Collections.singletonList(
                new TableFilterRule(RuleType.EXCLUDE, "ods.tmp_*"));
        OnTablesFilter filter = new OnTablesFilter(rules);
        Assertions.assertFalse(filter.shouldWarmUp("ods", "orders"));
        Assertions.assertFalse(filter.shouldWarmUp("ods", "tmp_staging"));
    }

    @Test
    public void testQuestionMarkWildcard() {
        List<TableFilterRule> rules = Collections.singletonList(
                new TableFilterRule(RuleType.INCLUDE, "db?.orders"));
        OnTablesFilter filter = new OnTablesFilter(rules);
        Assertions.assertTrue(filter.shouldWarmUp("db1", "orders"));
        Assertions.assertTrue(filter.shouldWarmUp("dbA", "orders"));
        Assertions.assertFalse(filter.shouldWarmUp("db12", "orders"));
        Assertions.assertFalse(filter.shouldWarmUp("db", "orders"));
    }

    @Test
    public void testDotIsLiteral() {
        // Dot in glob should match literal dot, not any character
        List<TableFilterRule> rules = Collections.singletonList(
                new TableFilterRule(RuleType.INCLUDE, "ods.tbl"));
        OnTablesFilter filter = new OnTablesFilter(rules);
        Assertions.assertTrue(filter.shouldWarmUp("ods", "tbl"));
        // "odsXtbl" should NOT match "ods.tbl" because dot is literal
        Assertions.assertFalse(new TableFilterRule(RuleType.INCLUDE, "ods.tbl")
                .matches("odsXtbl"));
    }

    @Test
    public void testExcludeOverridesInclude() {
        // Include all tables in ods, but exclude ods.secret
        List<TableFilterRule> rules = Arrays.asList(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"),
                new TableFilterRule(RuleType.EXCLUDE, "ods.secret"));
        OnTablesFilter filter = new OnTablesFilter(rules);
        Assertions.assertTrue(filter.shouldWarmUp("ods", "orders"));
        Assertions.assertFalse(filter.shouldWarmUp("ods", "secret"));
    }

    @Test
    public void testGetRulesPartition() {
        List<TableFilterRule> rules = Arrays.asList(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"),
                new TableFilterRule(RuleType.EXCLUDE, "ods.tmp_*"),
                new TableFilterRule(RuleType.INCLUDE, "dw.*"));
        OnTablesFilter filter = new OnTablesFilter(rules);
        Assertions.assertEquals(2, filter.getIncludeRules().size());
        Assertions.assertEquals(1, filter.getExcludeRules().size());
        Assertions.assertEquals(3, filter.getAllRules().size());
    }

    @Test
    public void testEmptyRules() {
        OnTablesFilter filter = new OnTablesFilter(Collections.emptyList());
        Assertions.assertFalse(filter.shouldWarmUp("any", "table"));
    }

    @Test
    public void testToString() {
        List<TableFilterRule> rules = Arrays.asList(
                new TableFilterRule(RuleType.INCLUDE, "ods.*"),
                new TableFilterRule(RuleType.EXCLUDE, "ods.tmp_*"));
        OnTablesFilter filter = new OnTablesFilter(rules);
        String str = filter.toString();
        Assertions.assertTrue(str.contains("ods.*"));
        Assertions.assertTrue(str.contains("ods.tmp_*"));
    }

    @Test
    public void testRegexMetacharsEscaped() {
        // Patterns with regex metacharacters should be treated as literals
        List<TableFilterRule> rules = Collections.singletonList(
                new TableFilterRule(RuleType.INCLUDE, "db(1).tbl[2]"));
        OnTablesFilter filter = new OnTablesFilter(rules);
        Assertions.assertTrue(filter.shouldWarmUp("db(1)", "tbl[2]"));
        Assertions.assertFalse(filter.shouldWarmUp("db1", "tbl2"));
    }
}
