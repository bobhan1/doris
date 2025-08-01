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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.thrift.TUniqueId;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class ConnectContextTest {
    @Mocked
    private StmtExecutor executor;
    @Mocked
    private SocketChannel socketChannel;
    @Mocked
    private Env env;
    @Mocked
    private ConnectScheduler connectScheduler;
    @Mocked
    private Auth auth;
    @Mocked
    private String qualifiedUser;

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testNormal() {
        ConnectContext ctx = new ConnectContext();

        // State
        Assert.assertNotNull(ctx.getState());

        // Capability
        Assert.assertEquals(MysqlCapability.DEFAULT_CAPABILITY, ctx.getServerCapability());
        ctx.setCapability(new MysqlCapability(10));
        Assert.assertEquals(new MysqlCapability(10), ctx.getCapability());

        // Kill flag
        Assert.assertFalse(ctx.isKilled());
        ctx.setKilled();
        Assert.assertTrue(ctx.isKilled());

        // Current db
        Assert.assertEquals("", ctx.getDatabase());
        ctx.setDatabase("testDb");
        Assert.assertEquals("testDb", ctx.getDatabase());

        // User
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("testUser", "%"));
        Assert.assertEquals("testUser", ctx.getQualifiedUser());

        // Serializer
        Assert.assertNotNull(ctx.getMysqlChannel().getSerializer());

        // Session variable
        Assert.assertNotNull(ctx.getSessionVariable());

        // connect scheduler
        Assert.assertNull(ctx.getConnectScheduler());
        ctx.setConnectScheduler(connectScheduler);
        Assert.assertNotNull(ctx.getConnectScheduler());

        // connection id
        ctx.setConnectionId(101);
        Assert.assertEquals(101, ctx.getConnectionId());

        // command
        ctx.setCommand(MysqlCommand.COM_PING);
        Assert.assertEquals(MysqlCommand.COM_PING, ctx.getCommand());

        // LoginTime
        ctx.loginTime = 1694002396223L;

        // Thread info
        Assert.assertNotNull(ctx.toThreadInfo(false));
        List<String> row = ctx.toThreadInfo(false).toRow(101, 1000, Optional.empty());
        Assert.assertEquals(15, row.size());
        Assert.assertEquals("Yes", row.get(0));
        Assert.assertEquals("101", row.get(1));
        Assert.assertEquals("testUser", row.get(2));
        Assert.assertEquals("", row.get(3));
        Assert.assertEquals("2023-09-06 20:13:16", row.get(4));
        Assert.assertEquals("internal", row.get(5));
        Assert.assertEquals("testDb", row.get(6));
        Assert.assertEquals("Ping", row.get(7));
        Assert.assertEquals("1", row.get(8));
        Assert.assertEquals("OK", row.get(9));
        Assert.assertEquals("", row.get(10));
        Assert.assertEquals("", row.get(11));

        // Start time
        Assert.assertEquals(0, ctx.getStartTime());
        ctx.setStartTime();
        Assert.assertNotSame(0, ctx.getStartTime());

        // query id
        ctx.setQueryId(new TUniqueId(100, 200));
        Assert.assertEquals(new TUniqueId(100, 200), ctx.queryId());

        // Catalog
        Assert.assertNull(ctx.getEnv());
        ctx.setEnv(env);
        Assert.assertNotNull(ctx.getEnv());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testSleepTimeout() {
        ConnectContext ctx = new ConnectContext();
        ctx.setCommand(MysqlCommand.COM_SLEEP);

        // sleep no time out
        ctx.setStartTime();
        Assert.assertFalse(ctx.isKilled());
        long now = ctx.getStartTime() + ctx.getSessionVariable().getWaitTimeoutS() * 1000L - 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Timeout
        ctx.setStartTime();
        now = ctx.getStartTime() + ctx.getSessionVariable().getWaitTimeoutS() * 1000L + 1;
        ctx.setExecutor(executor);
        ctx.checkTimeout(now);
        Assert.assertTrue(ctx.isKilled());

        // user query timeout
        ctx.setStartTime();
        now = ctx.getStartTime() + auth.getQueryTimeout(qualifiedUser) * 1000L + 1;
        ctx.setExecutor(executor);
        ctx.checkTimeout(now);
        Assert.assertTrue(ctx.isKilled());

        // Kill
        ctx.kill(true);
        Assert.assertTrue(ctx.isKilled());
        ctx.kill(false);
        Assert.assertTrue(ctx.isKilled());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testOtherTimeout() {
        ConnectContext ctx = new ConnectContext();
        ctx.setCommand(MysqlCommand.COM_QUERY);

        // sleep no time out
        Assert.assertFalse(ctx.isKilled());
        ctx.setExecutor(executor);
        long now = ctx.getExecTimeoutS() * 1000L - 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Timeout
        ctx.setExecutor(executor);
        now = ctx.getExecTimeoutS() * 1000L + 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Kill
        ctx.kill(true);
        Assert.assertTrue(ctx.isKilled());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testThreadLocal() {
        ConnectContext ctx = new ConnectContext();
        Assert.assertNull(ConnectContext.get());
        ctx.setThreadLocalInfo();
        Assert.assertNotNull(ConnectContext.get());
        Assert.assertEquals(ctx, ConnectContext.get());
    }

    @Test
    public void testGetMaxExecMemByte() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("a", "%"));
        context.setEnv(env);
        long sessionValue = 2097153L;
        long propertyValue = 2097154L;
        // only session
        context.getSessionVariable().setMaxExecMemByte(sessionValue);
        long result = context.getMaxExecMemByte();
        Assert.assertEquals(sessionValue, result);
        // has property
        new Expectations() {
            {
                auth.getExecMemLimit(anyString);
                minTimes = 0;
                result = propertyValue;
            }
        };
        result = context.getMaxExecMemByte();
        Assert.assertEquals(propertyValue, result);
    }

    @Test
    public void testGetQueryTimeoutS() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("a", "%"));
        context.setEnv(env);
        int sessionValue = 1;
        int propertyValue = 2;
        // only session
        context.getSessionVariable().setQueryTimeoutS(sessionValue);
        long result = context.getQueryTimeoutS();
        Assert.assertEquals(sessionValue, result);
        // has property
        new Expectations() {
            {
                auth.getQueryTimeout(anyString);
                minTimes = 0;
                result = propertyValue;
            }
        };
        result = context.getQueryTimeoutS();
        Assert.assertEquals(propertyValue, result);
    }

    @Test
    public void testInsertQueryTimeoutS() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("a", "%"));
        context.setEnv(env);
        int sessionValue = 1;
        int propertyValue = 2;
        // only session
        context.getSessionVariable().setInsertTimeoutS(sessionValue);
        long result = context.getInsertTimeoutS();
        Assert.assertEquals(sessionValue, result);
        // has property
        new Expectations() {
            {
                auth.getInsertTimeout(anyString);
                minTimes = 0;
                result = propertyValue;
            }
        };
        result = context.getInsertTimeoutS();
        Assert.assertEquals(propertyValue, result);
    }

    @Test
    public void testResetQueryId() {
        ConnectContext context = new ConnectContext();
        Assert.assertNull(context.queryId);
        Assert.assertNull(context.lastQueryId);

        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        context.setQueryId(queryId);
        Assert.assertEquals(queryId, context.queryId);
        Assert.assertNull(context.lastQueryId);

        context.resetQueryId();
        Assert.assertNull(context.queryId);
        Assert.assertEquals(queryId, context.lastQueryId);

        UUID uuid2 = UUID.randomUUID();
        TUniqueId queryId2 = new TUniqueId(uuid2.getMostSignificantBits(), uuid2.getLeastSignificantBits());
        context.setQueryId(queryId2);
        Assert.assertEquals(queryId2, context.queryId);
        Assert.assertEquals(queryId, context.lastQueryId);
    }
}
