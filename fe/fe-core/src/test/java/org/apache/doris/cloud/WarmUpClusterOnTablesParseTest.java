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
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.WarmUpClusterCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

/**
 * Tests parsing of WARM UP CLUSTER ... ON TABLES (...) grammar.
 * Covers valid syntax, extracted rule types/patterns, and syntax errors.
 */
public class WarmUpClusterOnTablesParseTest {

    private static MockedStatic<ConnectContext> connectContextMock;

    @BeforeAll
    public static void init() {
        ConnectContext ctx = new ConnectContext();
        connectContextMock = Mockito.mockStatic(ConnectContext.class);
        connectContextMock.when(ConnectContext::get).thenReturn(ctx);
    }

    @AfterAll
    public static void tearDown() {
        connectContextMock.close();
    }

    private WarmUpClusterCommand parse(String sql) {
        NereidsParser parser = new NereidsParser();
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(WarmUpClusterCommand.class, plan);
        return (WarmUpClusterCommand) plan;
    }

    // ===== Valid syntax: ON TABLES clause is parsed correctly =====

    @Test
    public void testOnTablesSingleInclude() {
        WarmUpClusterCommand cmd = parse(
                "WARM UP CLUSTER dst WITH CLUSTER src "
                + "ON TABLES (INCLUDE 'ods.*') "
                + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')");
        List<TableFilterRule> rules = cmd.getOnTablesRules();
        Assertions.assertNotNull(rules);
        Assertions.assertEquals(1, rules.size());
        Assertions.assertEquals(RuleType.INCLUDE, rules.get(0).getRuleType());
        Assertions.assertEquals("ods.*", rules.get(0).getRawPattern());
    }

    @Test
    public void testOnTablesMultipleRules() {
        WarmUpClusterCommand cmd = parse(
                "WARM UP CLUSTER dst WITH CLUSTER src "
                + "ON TABLES (INCLUDE 'ods.*', INCLUDE 'dw.*', EXCLUDE 'dw.tmp_*') "
                + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')");
        List<TableFilterRule> rules = cmd.getOnTablesRules();
        Assertions.assertNotNull(rules);
        Assertions.assertEquals(3, rules.size());
        Assertions.assertEquals(RuleType.INCLUDE, rules.get(0).getRuleType());
        Assertions.assertEquals("ods.*", rules.get(0).getRawPattern());
        Assertions.assertEquals(RuleType.INCLUDE, rules.get(1).getRuleType());
        Assertions.assertEquals("dw.*", rules.get(1).getRawPattern());
        Assertions.assertEquals(RuleType.EXCLUDE, rules.get(2).getRuleType());
        Assertions.assertEquals("dw.tmp_*", rules.get(2).getRawPattern());
    }

    @Test
    public void testWithoutOnTablesClause() {
        WarmUpClusterCommand cmd = parse(
                "WARM UP CLUSTER dst WITH CLUSTER src "
                + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')");
        Assertions.assertNull(cmd.getOnTablesRules());
    }

    @Test
    public void testOnTablesWithForce() {
        WarmUpClusterCommand cmd = parse(
                "WARM UP CLUSTER dst WITH CLUSTER src FORCE "
                + "ON TABLES (INCLUDE 'ods.*') "
                + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')");
        Assertions.assertTrue(cmd.isForce());
        Assertions.assertNotNull(cmd.getOnTablesRules());
        Assertions.assertEquals(1, cmd.getOnTablesRules().size());
    }

    @Test
    public void testOnTablesWithComputeGroup() {
        WarmUpClusterCommand cmd = parse(
                "WARM UP COMPUTE GROUP dst WITH COMPUTE GROUP src "
                + "ON TABLES (INCLUDE 'db1.*') "
                + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')");
        Assertions.assertNotNull(cmd.getOnTablesRules());
        Assertions.assertEquals(1, cmd.getOnTablesRules().size());
    }

    // ===== Syntax errors =====

    @Test
    public void testOnTablesEmptyParensFails() {
        Assertions.assertThrows(ParseException.class, () ->
                parse("WARM UP CLUSTER dst WITH CLUSTER src "
                    + "ON TABLES () "
                    + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')"));
    }

    @Test
    public void testOnTablesMissingParensFails() {
        Assertions.assertThrows(ParseException.class, () ->
                parse("WARM UP CLUSTER dst WITH CLUSTER src "
                    + "ON TABLES INCLUDE 'ods.*' "
                    + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')"));
    }

    @Test
    public void testOnTablesMissingPatternFails() {
        Assertions.assertThrows(ParseException.class, () ->
                parse("WARM UP CLUSTER dst WITH CLUSTER src "
                    + "ON TABLES (INCLUDE) "
                    + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')"));
    }

    // ===== Validation logic in WarmUpClusterCommand =====

    @Test
    public void testOnTablesExcludeOnlyParsesButLacksInclude() {
        WarmUpClusterCommand cmd = parse(
                "WARM UP CLUSTER dst WITH CLUSTER src "
                + "ON TABLES (EXCLUDE 'ods.tmp_*') "
                + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')");
        List<TableFilterRule> rules = cmd.getOnTablesRules();
        Assertions.assertEquals(1, rules.size());
        Assertions.assertEquals(RuleType.EXCLUDE, rules.get(0).getRuleType());
        boolean hasInclude = rules.stream()
                .anyMatch(r -> r.getRuleType() == RuleType.INCLUDE);
        Assertions.assertFalse(hasInclude, "Exclude-only rules should have no INCLUDE");
    }

    @Test
    public void testOnTablesNonEventDrivenSyncModeParses() {
        WarmUpClusterCommand cmd = parse(
                "WARM UP CLUSTER dst WITH CLUSTER src "
                + "ON TABLES (INCLUDE 'ods.*') "
                + "PROPERTIES('sync_mode'='once')");
        Assertions.assertNotNull(cmd.getOnTablesRules());
        Assertions.assertEquals("once", cmd.getProperties().get("sync_mode"));
    }
}
