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

package org.apache.doris.catalog;

import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.DropDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.info.DropDatabaseInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.Tag;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ColocateTableTest {
    private static String runningDir = "fe/mocked/ColocateTableTest" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;
    private static String dbName = "testDb";
    private static String fullDbName = "" + dbName;
    private static String tableName1 = "t1";
    private static String tableName2 = "t2";
    private static String groupName = "group1";

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        connectContext = UtFrameUtils.createDefaultCtx();

    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Before
    public void createDb() throws Exception {
        String createDbStmtStr = "create database " + dbName;
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(connectContext, stmtExecutor);
        }
        Env.getCurrentEnv().setColocateTableIndex(new ColocateTableIndex());
    }

    @After
    public void dropDb() throws Exception {
        String dropDbStmtStr = "drop database " + dbName;
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(dropDbStmtStr);
        DropDatabaseCommand command = (DropDatabaseCommand) logicalPlan;
        DropDatabaseInfo dropDatabaseInfo = command.getDropDatabaseInfo();
        Env.getCurrentEnv().dropDb(
                dropDatabaseInfo.getCatalogName(),
                dropDatabaseInfo.getDatabaseName(),
                dropDatabaseInfo.isIfExists(),
                dropDatabaseInfo.isForce());
    }

    private static void createTable(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof CreateTableCommand) {
            ((CreateTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    private static void alterTable(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof AlterTableCommand) {
            ((AlterTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    @Test
    public void testCreateOneTable() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        ColocateTableIndex index = Env.getCurrentColocateIndex();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        long tableId = db.getTableOrMetaException(tableName1).getId();

        Assert.assertEquals(1, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(1, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(1, Deencapsulation.<Table<GroupId, Tag, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(1, Deencapsulation.<Map<GroupId, ColocateGroupSchema>>getField(index, "group2Schema").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertTrue(index.isColocateTable(tableId));

        Long dbId = db.getId();
        Assert.assertEquals(dbId, index.getGroup(tableId).dbId);

        GroupId groupId = index.getGroup(tableId);
        Map<Tag, List<List<Long>>> backendIds = index.getBackendsPerBucketSeq(groupId);
        Assert.assertEquals(1, backendIds.get(Tag.DEFAULT_BACKEND_TAG).get(0).size());

        String fullGroupName = GroupId.getFullGroupName(dbId, groupName);
        Assert.assertEquals(tableId, index.getTableIdByGroup(fullGroupName));
        ColocateGroupSchema groupSchema = index.getGroupSchema(fullGroupName);
        Assert.assertNotNull(groupSchema);
        Assert.assertEquals(dbId, groupSchema.getGroupId().dbId);
        Assert.assertEquals(1, groupSchema.getBucketsNum());
        Assert.assertEquals((short) 1, groupSchema.getReplicaAlloc().getTotalReplicaNum());
    }

    @Test
    public void testCreateTwoTableWithSameGroup() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        createTable("create table " + dbName + "." + tableName2 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        ColocateTableIndex index = Env.getCurrentColocateIndex();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        long firstTblId = db.getTableOrMetaException(tableName1).getId();
        long secondTblId = db.getTableOrMetaException(tableName2).getId();

        Assert.assertEquals(2, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(2, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(1, Deencapsulation.<Table<GroupId, Tag, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(1, Deencapsulation.<Map<GroupId, ColocateGroupSchema>>getField(index, "group2Schema").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertTrue(index.isColocateTable(firstTblId));
        Assert.assertTrue(index.isColocateTable(secondTblId));

        Assert.assertTrue(index.isSameGroup(firstTblId, secondTblId));

        // drop first
        index.removeTable(firstTblId);
        Assert.assertEquals(1, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(1, index.getAllGroupIds().size());
        Assert.assertEquals(1, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(1,
                Deencapsulation.<Table<GroupId, Tag, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertFalse(index.isColocateTable(firstTblId));
        Assert.assertTrue(index.isColocateTable(secondTblId));
        Assert.assertFalse(index.isSameGroup(firstTblId, secondTblId));

        // drop second
        index.removeTable(secondTblId);
        Assert.assertEquals(0, Deencapsulation.<Multimap<GroupId, Long>>getField(index, "group2Tables").size());
        Assert.assertEquals(0, index.getAllGroupIds().size());
        Assert.assertEquals(0, Deencapsulation.<Map<Long, GroupId>>getField(index, "table2Group").size());
        Assert.assertEquals(0,
                Deencapsulation.<Table<GroupId, Tag, List<List<Long>>>>getField(index, "group2BackendsPerBucketSeq").size());
        Assert.assertEquals(0, index.getUnstableGroupIds().size());

        Assert.assertFalse(index.isColocateTable(firstTblId));
        Assert.assertFalse(index.isColocateTable(secondTblId));
    }

    @Test
    public void testBucketNum() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables must have same bucket num: 2 should be 1");
        createTable("create table " + dbName + "." + tableName2 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 2\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

    }

    @Test
    public void testReplicationNum() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables must have same replication allocation: { tag.location.default: 2 }"
                + " should be { tag.location.default: 1 }");
        createTable("create table " + dbName + "." + tableName2 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"2\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");
    }

    @Test
    public void testDistributionColumnsSize() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables distribution columns size must be same: 1 should be 2");
        createTable("create table " + dbName + "." + tableName2 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");
    }

    @Test
    public void testDistributionColumnsType() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` int NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        expectedEx.expect(DdlException.class);
        expectedEx.expectMessage("Colocate tables distribution columns must have the same data type: k2(varchar(10)) should be int");
        createTable("create table " + dbName + "." + tableName2 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");
    }


    @Test
    public void testModifyGroupNameForBucketSeqInconsistent() throws Exception {
        createTable("create table " + dbName + "." + tableName1 + " (\n"
                + " `k1` int NULL COMMENT \"\",\n"
                + " `k2` varchar(10) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + " \"replication_num\" = \"1\",\n"
                + " \"colocate_with\" = \"" + groupName + "\"\n"
                + ");");

        ColocateTableIndex index = Env.getCurrentColocateIndex();
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        long tableId = db.getTableOrMetaException(tableName1).getId();
        GroupId groupId1 = index.getGroup(tableId);

        Map<Tag, List<List<Long>>> backendIds1 = index.getBackendsPerBucketSeq(groupId1);
        Assert.assertEquals(1, backendIds1.get(Tag.DEFAULT_BACKEND_TAG).get(0).size());

        // set same group name
        alterTable("ALTER TABLE " + dbName + "." + tableName1
                + " SET (" + "\"colocate_with\" = \"" + groupName + "\")");
        GroupId groupId2 = index.getGroup(tableId);

        // verify groupId group2BackendsPerBucketSeq
        Map<Tag, List<List<Long>>> backendIds2 = index.getBackendsPerBucketSeq(groupId2);
        Assert.assertEquals(1, backendIds2.get(Tag.DEFAULT_BACKEND_TAG).get(0).size());
        Assert.assertEquals(groupId1, groupId2);
        Assert.assertEquals(backendIds1, backendIds2);
    }
}
