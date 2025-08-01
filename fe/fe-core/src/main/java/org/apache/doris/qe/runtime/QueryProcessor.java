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

package org.apache.doris.qe.runtime;

import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.qe.AbstractJobProcessor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.LimitUtils;
import org.apache.doris.qe.ResultReceiver;
import org.apache.doris.qe.ResultReceiverConsumer;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TReportExecStatusParams;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class QueryProcessor extends AbstractJobProcessor {
    private static final Logger LOG = LogManager.getLogger(QueryProcessor.class);

    // constant fields
    private final long limitRows;

    // mutable field
    private ResultReceiverConsumer receiverConsumer;

    private long numReceivedRows;

    public QueryProcessor(CoordinatorContext coordinatorContext, ResultReceiverConsumer consumer) {
        super(coordinatorContext);
        receiverConsumer = consumer;

        this.limitRows = coordinatorContext.fragments.get(0)
                .getPlanRoot()
                .getLimit();
    }

    public static QueryProcessor build(CoordinatorContext coordinatorContext) {
        PipelineDistributedPlan topFragment = coordinatorContext.topDistributedPlan;
        DataSink topDataSink = coordinatorContext.dataSink;
        Boolean enableParallelResultSink;
        if (topDataSink instanceof ResultSink) {
            enableParallelResultSink = coordinatorContext.queryOptions.isEnableParallelResultSink();
        } else {
            enableParallelResultSink = coordinatorContext.queryOptions.isEnableParallelOutfile();
        }

        List<AssignedJob> topInstances = topFragment.getInstanceJobs();
        List<ResultReceiver> receivers = Lists.newArrayListWithCapacity(topInstances.size());
        Map<Long, AssignedJob> distinctWorkerJobs = Maps.newLinkedHashMap();
        for (AssignedJob topInstance : topInstances) {
            distinctWorkerJobs.putIfAbsent(topInstance.getAssignedWorker().id(), topInstance);
        }

        boolean regenerateInstanceId = coordinatorContext.connectContext.consumeNeedRegenerateQueryId();
        for (AssignedJob topInstance : distinctWorkerJobs.values()) {
            if (regenerateInstanceId) {
                topInstance.resetInstanceId(coordinatorContext.connectContext.nextInstanceId());
            }
            DistributedPlanWorker topWorker = topInstance.getAssignedWorker();
            TNetworkAddress execBeAddr = new TNetworkAddress(topWorker.host(), topWorker.brpcPort());
            receivers.add(
                    new ResultReceiver(
                            coordinatorContext.queryId,
                            topInstance.instanceId(),
                            topWorker.id(),
                            execBeAddr,
                            coordinatorContext.timeoutDeadline.get(),
                            coordinatorContext.connectContext.getSessionVariable().getMaxMsgSizeOfResultReceiver(),
                            enableParallelResultSink
                    )
            );
        }
        ResultReceiverConsumer consumer = new ResultReceiverConsumer(receivers,
                coordinatorContext.timeoutDeadline.get());
        return new QueryProcessor(coordinatorContext, consumer);
    }

    @Override
    protected void doProcessReportExecStatus(TReportExecStatusParams params, SingleFragmentPipelineTask fragmentTask) {

    }

    public boolean isEos() {
        return receiverConsumer.isEos();
    }

    public RowBatch getNext() throws UserException, InterruptedException, TException, RpcException, ExecutionException {
        Status status = new Status();
        RowBatch resultBatch = receiverConsumer.getNext(status);
        if (!status.ok()) {
            LOG.warn("Query {} coordinator get next fail, {}, need cancel.",
                    DebugUtil.printId(coordinatorContext.queryId), status.getErrorMsg());
        }
        coordinatorContext.updateStatusIfOk(status);

        Status copyStatus = coordinatorContext.readCloneStatus();
        if (!copyStatus.ok()) {
            if (Strings.isNullOrEmpty(copyStatus.getErrorMsg())) {
                copyStatus.rewriteErrorMsg();
            }
            if (copyStatus.isRpcError()) {
                throw new RpcException(null, copyStatus.getErrorMsg());
            } else {
                String errMsg = copyStatus.getErrorMsg();
                LOG.warn("query failed: {}", errMsg);
                throw new UserException(errMsg);
            }
        }

        ConnectContext connectContext = coordinatorContext.connectContext;
        if (connectContext != null && connectContext.getSessionVariable().dryRunQuery) {
            // In BE: vmysql_result_writer.cpp:GetResultBatchCtx::on_close()
            //      statistics->set_returned_rows(returned_rows);
            // In a multi-mysql_result_writer scenario, since each mysql_result_writer will set this rows, in order
            // to avoid missing rows when dry_run_query = true, they should all be added up.
            numReceivedRows += resultBatch.getQueryStatistics().getReturnedRows();
        } else if (resultBatch.getBatch() != null) {
            numReceivedRows += resultBatch.getBatch().getRowsSize();
        }

        // if reached limit rows, cancel this query immediately
        // to avoid BE from reading more data.
        // ATTN: if change here, also need to change the same logic in Coordinator.getNext();
        boolean reachedLimit = LimitUtils.cancelIfReachLimit(
                resultBatch, limitRows, numReceivedRows, coordinatorContext::cancelSchedule);

        if (reachedLimit) {
            resultBatch.setEos(true);
        }
        return resultBatch;
    }

    public void cancel(Status cancelReason) {
        receiverConsumer.cancel(cancelReason);

        this.executionTask.ifPresent(sqlPipelineTask -> {
            for (MultiFragmentsPipelineTask fragmentsTask : sqlPipelineTask.getChildrenTasks().values()) {
                fragmentsTask.cancelExecute(cancelReason);
            }
        });
    }

    public long getNumReceivedRows() {
        return numReceivedRows;
    }
}
