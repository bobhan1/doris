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

package org.apache.doris.job.extensions.insert;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public class InsertTask extends AbstractTask {

    public static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("TaskId", ScalarType.createStringType()),
            new Column("JobId", ScalarType.createStringType()),
            new Column("JobName", ScalarType.createStringType()),
            new Column("Label", ScalarType.createStringType()),
            new Column("Status", ScalarType.createStringType()),
            new Column("ErrorMsg", ScalarType.createStringType()),
            new Column("CreateTime", ScalarType.createStringType()),
            new Column("StartTime", ScalarType.createStringType()),
            new Column("FinishTime", ScalarType.createStringType()),
            new Column("TrackingUrl", ScalarType.createStringType()),
            new Column("LoadStatistic", ScalarType.createStringType()),
            new Column("User", ScalarType.createStringType()));

    public static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    private String labelName;
    private InsertIntoTableCommand command;
    private StmtExecutor stmtExecutor;
    private ConnectContext ctx;
    private String sql;
    private String currentDb;
    @SerializedName(value = "uif")
    private UserIdentity userIdentity;
    private LoadStatistic loadStatistic;
    private AtomicBoolean isCanceled = new AtomicBoolean(false);
    private AtomicBoolean isFinished = new AtomicBoolean(false);
    private static final String LABEL_SPLITTER = "_";

    private FailMsg failMsg;
    @Getter
    private String trackingUrl;

    @Getter
    @Setter
    private LoadJob jobInfo;
    private TaskType taskType = TaskType.PENDING;
    private MergeType mergeType = MergeType.APPEND;

    /**
     * task merge type
     */
    enum MergeType {
        MERGE,
        APPEND,
        DELETE
    }

    /**
     * task type
     */
    enum TaskType {
        UNKNOWN, // this is only for ISSUE #2354
        PENDING,
        LOADING,
        FINISHED,
        FAILED,
        CANCELLED
    }

    public InsertTask(InsertIntoTableCommand insertInto,
                      ConnectContext ctx, StmtExecutor executor, LoadStatistic statistic) {
        this(insertInto.getLabelName().get(), insertInto, ctx, executor, statistic);
    }

    public InsertTask(String labelName, String currentDb, String sql, UserIdentity userIdentity) {
        this.labelName = labelName;
        this.sql = sql;
        this.currentDb = currentDb;
        this.userIdentity = userIdentity;
    }

    public InsertTask(String labelName, InsertIntoTableCommand insertInto,
                      ConnectContext ctx, StmtExecutor executor, LoadStatistic statistic) {
        this.labelName = labelName;
        this.command = insertInto;
        this.userIdentity = ctx.getCurrentUserIdentity();
        this.ctx = ctx;
        this.stmtExecutor = executor;
        this.loadStatistic = statistic;
    }

    public static ConnectContext makeConnectContext(UserIdentity userIdentity, String currentDb) {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCurrentUserIdentity(userIdentity);
        ctx.getState().reset();
        ctx.getState().setInternal(true);
        ctx.getState().setNereids(true);
        ctx.setThreadLocalInfo();
        TUniqueId queryId = generateQueryId();
        ctx.setQueryId(queryId);
        if (StringUtils.isNotEmpty(currentDb)) {
            ctx.setDatabase(currentDb);
        }
        return ctx;
    }

    public static StmtExecutor makeStmtExecutor(ConnectContext ctx) {
        return new StmtExecutor(ctx, (String) null);
    }

    @Override
    public void before() throws JobException {
        if (isCanceled.get()) {
            throw new JobException("Export executor has been canceled, task id: {}", getTaskId());
        }
        ctx = makeConnectContext(userIdentity, currentDb);
        StatementContext statementContext = new StatementContext();
        ctx.setStatementContext(statementContext);
        if (StringUtils.isNotEmpty(sql)) {
            NereidsParser parser = new NereidsParser();
            this.command = (InsertIntoTableCommand) parser.parseSingle(sql);
            this.command.setLabelName(Optional.of(getJobId() + LABEL_SPLITTER + getTaskId()));
            this.command.setJobId(getTaskId());
            stmtExecutor = new StmtExecutor(ctx, new LogicalPlanAdapter(command, ctx.getStatementContext()));
        }

        super.before();
    }

    @Override
    protected void closeOrReleaseResources() {
        if (null != stmtExecutor) {
            stmtExecutor = null;
        }
        if (null != command) {
            command = null;
        }
        if (null != ctx) {
            ctx = null;
        }
    }

    @Override
    public void run() throws JobException {
        try {
            if (isCanceled.get()) {
                log.info("task has been canceled, task id is {}", getTaskId());
                return;
            }
            command.runWithUpdateInfo(ctx, stmtExecutor, loadStatistic);
            if (ctx.getState().getStateType() != QueryState.MysqlStateType.OK) {
                throw new JobException(ctx.getState().getErrorMessage());
            }
        } catch (Exception e) {
            log.warn("execute insert task error, job id is {}, task id is {},sql is {}", getJobId(),
                    getTaskId(), sql, e);
            throw new JobException(Util.getRootCauseMessage(e));
        }
    }

    @Override
    public boolean onFail() throws JobException {
        if (isCanceled.get()) {
            return false;
        }
        isFinished.set(true);
        return super.onFail();
    }

    @Override
    public boolean onSuccess() throws JobException {
        isFinished.set(true);
        return super.onSuccess();
    }

    @Override
    protected void executeCancelLogic(boolean needWaitCancelComplete) {
        if (isFinished.get() || isCanceled.get()) {
            return;
        }
        isCanceled.getAndSet(true);
        if (null != stmtExecutor) {
            stmtExecutor.cancel(new Status(TStatusCode.CANCELLED, "insert task cancelled"));
        }
    }

    @Override
    public TRow getTvfInfo(String jobName) {
        TRow trow = new TRow();
        if (jobInfo == null) {
            // if task not start, load job is null,return pending task show info
            return getPendingTaskTVFInfo(jobName);
        }
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(jobInfo.getId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(jobName));
        trow.addToColumnValue(new TCell().setStringVal(getJobId() + LABEL_SPLITTER + getTaskId()));
        trow.addToColumnValue(new TCell().setStringVal(getStatus().name()));
        trow.addToColumnValue(new TCell().setStringVal(getErrorMsg()));
        // create time
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(null == getStartTimeMs() ? ""
                : TimeUtils.longToTimeString(getStartTimeMs())));
        // load end time
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getFinishTimeMs())));
        // tracking url
        trow.addToColumnValue(new TCell().setStringVal(trackingUrl));
        if (null != jobInfo.getLoadStatistic()) {
            trow.addToColumnValue(new TCell().setStringVal(jobInfo.getLoadStatistic().toJson()));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(""));
        }
        if (userIdentity == null) {
            trow.addToColumnValue(new TCell().setStringVal(""));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(userIdentity.getQualifiedUser()));
        }
        return trow;
    }

    // if task not start, load job is null,return pending task show info
    private TRow getPendingTaskTVFInfo(String jobName) {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getTaskId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(jobName));
        trow.addToColumnValue(new TCell().setStringVal(getJobId() + LABEL_SPLITTER + getTaskId()));
        trow.addToColumnValue(new TCell().setStringVal(getStatus().name()));
        trow.addToColumnValue(new TCell().setStringVal(getErrorMsg()));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(null == getStartTimeMs() ? ""
                : TimeUtils.longToTimeString(getStartTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(null == getFinishTimeMs() ? ""
                : TimeUtils.longToTimeString(getFinishTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(""));
        trow.addToColumnValue(new TCell().setStringVal(""));
        trow.addToColumnValue(new TCell().setStringVal(userIdentity.getQualifiedUser()));
        return trow;
    }

    private String getErrorMsg() {
        // err msg
        String errorMsg = "";
        if (failMsg != null) {
            errorMsg = failMsg.getMsg();
        }
        if (StringUtils.isNotBlank(getErrMsg())) {
            errorMsg = getErrMsg();
        }
        return errorMsg;
    }
}
