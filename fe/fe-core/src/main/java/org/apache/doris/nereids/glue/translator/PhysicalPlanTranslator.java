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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.AnalyticWindow;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.GroupByClause.GroupingType;
import org.apache.doris.analysis.GroupingInfo;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.OrderByElement;
import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TableSample;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.es.EsExternalTable;
import org.apache.doris.datasource.es.source.EsScanNode;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.hive.source.HiveScanNode;
import org.apache.doris.datasource.hudi.source.HudiScanNode;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.source.IcebergScanNode;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.datasource.jdbc.sink.JdbcTableSink;
import org.apache.doris.datasource.jdbc.source.JdbcScanNode;
import org.apache.doris.datasource.lakesoul.LakeSoulExternalTable;
import org.apache.doris.datasource.lakesoul.source.LakeSoulScanNode;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.datasource.maxcompute.source.MaxComputeScanNode;
import org.apache.doris.datasource.odbc.source.OdbcScanNode;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.source.PaimonScanNode;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalTable;
import org.apache.doris.datasource.trinoconnector.source.TrinoConnectorScanNode;
import org.apache.doris.fs.DirectoryLister;
import org.apache.doris.fs.FileSystemDirectoryLister;
import org.apache.doris.fs.TransactionScopeCachingDirectoryListerFactory;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.processor.post.runtimefilterv2.RuntimeFilterV2;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecAllSingleton;
import org.apache.doris.nereids.properties.DistributionSpecAny;
import org.apache.doris.nereids.properties.DistributionSpecExecutionAny;
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHiveTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.DistributionSpecHiveTableSinkUnPartitioned;
import org.apache.doris.nereids.properties.DistributionSpecOlapTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.properties.DistributionSpecStorageAny;
import org.apache.doris.nereids.properties.DistributionSpecStorageGather;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow.WindowFrameGroup;
import org.apache.doris.nereids.rules.rewrite.MergeLimits;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.UnaryNode;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.PartitionTopnPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDictionarySink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEsScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHiveTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHudiScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSchemaScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.trees.plans.physical.TopnFilter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.AnalyticEvalNode;
import org.apache.doris.planner.AssertNumRowsNode;
import org.apache.doris.planner.BackendPartitionedSchemaScanNode;
import org.apache.doris.planner.CTEScanNode;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.DictionarySink;
import org.apache.doris.planner.EmptySetNode;
import org.apache.doris.planner.ExceptNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.GroupCommitBlockSink;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.HashJoinNode.DistributionMode;
import org.apache.doris.planner.HiveTableSink;
import org.apache.doris.planner.IcebergTableSink;
import org.apache.doris.planner.IntersectNode;
import org.apache.doris.planner.JoinNodeBase;
import org.apache.doris.planner.MaterializationNode;
import org.apache.doris.planner.MultiCastDataSink;
import org.apache.doris.planner.MultiCastPlanFragment;
import org.apache.doris.planner.NestedLoopJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PartitionSortNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.RepeatNode;
import org.apache.doris.planner.ResultFileSink;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SchemaScanNode;
import org.apache.doris.planner.SelectNode;
import org.apache.doris.planner.SetOperationNode;
import org.apache.doris.planner.SortNode;
import org.apache.doris.planner.TableFunctionNode;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.tablefunction.TableValuedFunctionIf;
import org.apache.doris.thrift.TFetchOption;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TResultSinkType;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Used to translate to physical plan generated by new optimizer to the plan fragments.
 * <STRONG>
 * ATTENTION:
 * Must always visit plan's children first when you implement a method to translate from PhysicalPlan to PlanNode.
 * </STRONG>
 */
public class PhysicalPlanTranslator extends DefaultPlanVisitor<PlanFragment, PlanTranslatorContext> {

    private static final Logger LOG = LogManager.getLogger(PhysicalPlanTranslator.class);
    private final StatsErrorEstimator statsErrorEstimator;
    private final PlanTranslatorContext context;

    private DirectoryLister directoryLister;

    public PhysicalPlanTranslator() {
        this(null, null);
    }

    public PhysicalPlanTranslator(PlanTranslatorContext context) {
        this(context, null);
    }

    public PhysicalPlanTranslator(PlanTranslatorContext context, StatsErrorEstimator statsErrorEstimator) {
        this.context = context;
        this.statsErrorEstimator = statsErrorEstimator;
    }

    /**
     * Translate Nereids Physical Plan tree to Stale Planner PlanFragment tree.
     *
     * @param physicalPlan Nereids Physical Plan tree
     * @return Stale Planner PlanFragment tree
     */
    public PlanFragment translatePlan(PhysicalPlan physicalPlan) {
        PlanFragment rootFragment = physicalPlan.accept(this, context);
        if (CollectionUtils.isEmpty(rootFragment.getOutputExprs())) {
            List<Expr> outputExprs = Lists.newArrayList();
            physicalPlan.getOutput().stream().map(Slot::getExprId)
                    .forEach(exprId -> outputExprs.add(context.findSlotRef(exprId)));
            rootFragment.setOutputExprs(outputExprs);
        }
        Collections.reverse(context.getPlanFragments());
        // TODO: maybe we need to trans nullable directly? and then we could remove call computeMemLayout
        context.getDescTable().computeMemLayout();
        if (context.getSessionVariable() != null && context.getSessionVariable().forbidUnknownColStats) {
            Set<ScanNode> scans = context.getScanNodeWithUnknownColumnStats();
            if (!scans.isEmpty()) {
                StringBuilder builder = new StringBuilder();
                scans.forEach(builder::append);
                throw new AnalysisException("tables with unknown column stats: " + builder);
            }
        }
        for (ScanNode scanNode : context.getScanNodes()) {
            try {
                scanNode.finalizeForNereids();
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        if (isSimpleQuery(physicalPlan)) {
            // the simple query maybe has two fragments or one fragment
            rootFragment.setForceSingleInstance();
            for (PlanFragment child : rootFragment.getChildren()) {
                child.setForceSingleInstance();
            }
        }
        return rootFragment;
    }

    /* ********************************************************************************************
     * distribute node
     * ******************************************************************************************** */

    @Override
    public PlanFragment visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute,
            PlanTranslatorContext context) {
        Plan upstream = distribute.child(); // now they're in one fragment but will be split by ExchangeNode.
        PlanFragment upstreamFragment = upstream.accept(this, context);
        List<List<Expr>> upstreamDistributeExprs = getDistributeExprs(upstream);

        DistributionSpec targetDistribution = distribute.getDistributionSpec();

        // TODO: why need set streaming here? should remove this.
        if (upstreamFragment.getPlanRoot() instanceof AggregationNode && upstream instanceof PhysicalHashAggregate) {
            PhysicalHashAggregate<?> hashAggregate = (PhysicalHashAggregate<?>) upstream;
            if (hashAggregate.getAggPhase() == AggPhase.LOCAL
                    && hashAggregate.getAggMode() == AggMode.INPUT_TO_BUFFER
                    && hashAggregate.getTopnPushInfo() == null) {
                AggregationNode aggregationNode = (AggregationNode) upstreamFragment.getPlanRoot();
                aggregationNode.setUseStreamingPreagg(hashAggregate.isMaybeUsingStream());
            }
        }
        // all PhysicalDistribute translate to ExchangeNode. upstream as input.
        ExchangeNode exchangeNode = new ExchangeNode(context.nextPlanNodeId(), upstreamFragment.getPlanRoot());
        updateLegacyPlanIdToPhysicalPlan(exchangeNode, distribute);
        List<ExprId> validOutputIds = distribute.getOutputExprIds();
        if (upstream instanceof PhysicalHashAggregate) {
            // we must add group by keys to output list,
            // otherwise we could not process aggregate's output without group by keys
            List<ExprId> keys = ((PhysicalHashAggregate<?>) upstream).getGroupByExpressions().stream()
                    .filter(SlotReference.class::isInstance)
                    .map(SlotReference.class::cast)
                    .map(SlotReference::getExprId)
                    .collect(Collectors.toList());
            keys.addAll(validOutputIds);
            validOutputIds = keys;
        }
        if (upstreamFragment instanceof MultiCastPlanFragment) {
            // TODO: remove this logic when we split to multi-window in logical window to physical window conversion
            MultiCastDataSink multiCastDataSink = (MultiCastDataSink) upstreamFragment.getSink();
            DataStreamSink dataStreamSink = multiCastDataSink.getDataStreamSinks().get(
                    multiCastDataSink.getDataStreamSinks().size() - 1);
            if (!(upstream instanceof PhysicalProject)) {
                List<Expr> projectionExprs = new ArrayList<>();
                PhysicalCTEConsumer consumer = getCTEConsumerChild(distribute);
                Preconditions.checkState(consumer != null, "consumer not found");
                for (Slot slot : distribute.getOutput()) {
                    projectionExprs.add(ExpressionTranslator.translate(consumer.getProducerSlot(slot), context));
                }
                TupleDescriptor projectionTuple = generateTupleDesc(distribute.getOutput(), null, context);
                dataStreamSink.setProjections(projectionExprs);
                dataStreamSink.setOutputTupleDesc(projectionTuple);
            }
        }
        // target data partition
        DataPartition targetDataPartition = toDataPartition(targetDistribution, validOutputIds, context);
        exchangeNode.setPartitionType(targetDataPartition.getType());
        exchangeNode.setChildrenDistributeExprLists(upstreamDistributeExprs);
        // its source partition is targetDataPartition. and outputPartition is UNPARTITIONED now, will be set when
        // visit its SinkNode
        PlanFragment downstreamFragment = new PlanFragment(context.nextFragmentId(), exchangeNode, targetDataPartition);
        if (targetDistribution instanceof DistributionSpecGather
                || targetDistribution instanceof DistributionSpecStorageGather) {
            // gather to one instance
            exchangeNode.setNumInstances(1);
        } else if (targetDistribution instanceof DistributionSpecAllSingleton) {
            // instances number = BE number now. assign one by one later.
            //ATTN: this number MAY BE CHANGED when we do distributing because when we finished physical planning,
            // we got the source table version. and in distribute planning, basing on the src version we may find
            // there's some BE whose dictionary already have newest data we dont have to reload.
            int aliveBENumber = Env.getCurrentSystemInfo().getAllClusterBackends(true).size();
            exchangeNode.setNumInstances(aliveBENumber);
        } else { // not change instances
            exchangeNode.setNumInstances(upstreamFragment.getPlanRoot().getNumInstances());
        }

        // process multicast sink
        if (upstreamFragment instanceof MultiCastPlanFragment) {
            MultiCastDataSink multiCastDataSink = (MultiCastDataSink) upstreamFragment.getSink();
            DataStreamSink dataStreamSink = multiCastDataSink.getDataStreamSinks().get(
                    multiCastDataSink.getDataStreamSinks().size() - 1);
            exchangeNode.updateTupleIds(dataStreamSink.getOutputTupleDesc());
            dataStreamSink.setExchNodeId(exchangeNode.getId());
            dataStreamSink.setOutputPartition(targetDataPartition);
            downstreamFragment.addChild(upstreamFragment);
            ((MultiCastPlanFragment) upstreamFragment).addToDest(exchangeNode);

            CTEScanNode cteScanNode = context.getCteScanNodeMap().get(upstreamFragment.getFragmentId());
            Preconditions.checkState(cteScanNode != null, "cte scan node is null");
            cteScanNode.setFragment(upstreamFragment);
            cteScanNode.setPlanNodeId(exchangeNode.getId());
            context.getRuntimeTranslator().ifPresent(runtimeFilterTranslator ->
                    runtimeFilterTranslator.getContext().getPlanNodeIdToCTEDataSinkMap()
                            .put(cteScanNode.getId(), dataStreamSink));
        } else {
            /*
             * FragmentA (NodeA) ---> FragmentB (NodeB)
             * ↓
             * FragmentA (NodeA -> DataStreamSink) ---> FragmentB (ExchangeNode -> NodeB)
             *                                ↓-----------------------↑
             */
            upstreamFragment.setDestination(exchangeNode);
            // by exchange, upstreamFragment transform itselves partition to exchange's partition
            upstreamFragment.setOutputPartition(targetDataPartition);
            DataStreamSink streamSink = new DataStreamSink(exchangeNode.getId());
            streamSink.setOutputPartition(targetDataPartition);
            upstreamFragment.setSink(streamSink);
        }

        context.addPlanFragment(downstreamFragment);
        return downstreamFragment;
    }

    /* ********************************************************************************************
     * sink Node, in lexicographical order
     * ******************************************************************************************** */

    @Override
    public PlanFragment visitPhysicalResultSink(PhysicalResultSink<? extends Plan> physicalResultSink,
            PlanTranslatorContext context) {
        PlanFragment planFragment = physicalResultSink.child().accept(this, context);
        TResultSinkType resultSinkType = context.getConnectContext() != null
                ? context.getConnectContext().getResultSinkType() : null;
        planFragment.setSink(new ResultSink(planFragment.getPlanRoot().getId(), resultSinkType));
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalDeferMaterializeResultSink(
            PhysicalDeferMaterializeResultSink<? extends Plan> sink,
            PlanTranslatorContext context) {
        PlanFragment planFragment = visitPhysicalResultSink(sink.getPhysicalResultSink(), context);
        TFetchOption fetchOption = sink.getOlapTable().generateTwoPhaseReadOption(sink.getSelectedIndexId());
        ((ResultSink) planFragment.getSink()).setFetchOption(fetchOption);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalDictionarySink(PhysicalDictionarySink<? extends Plan> dictionarySink,
            PlanTranslatorContext context) {
        // Scan(ABCD) DataStreamSink(ABCD) -> Exchange(ABCD) Sink(CB)
        // source partition is UNPARTITIONED set by exchange node.
        // TODO: after changed ABCD to DCB. check what exchange do here.
        PlanFragment rootFragment = dictionarySink.child().accept(this, context);
        rootFragment.setOutputPartition(DataPartition.UNPARTITIONED); // only used for explain string
        // set rootFragment output expr

        DictionarySink sink = new DictionarySink(dictionarySink.getDictionary(), dictionarySink.allowAdaptiveLoad(),
                dictionarySink.getCols().stream().map(Column::getName).collect(Collectors.toList()));
        rootFragment.setSink(sink);
        return rootFragment;
    }

    @Override
    public PlanFragment visitPhysicalOlapTableSink(PhysicalOlapTableSink<? extends Plan> olapTableSink,
            PlanTranslatorContext context) {
        PlanFragment rootFragment = olapTableSink.child().accept(this, context);
        rootFragment.setOutputPartition(DataPartition.UNPARTITIONED);

        HashSet<String> partialUpdateCols = new HashSet<>();
        boolean isPartialUpdate = olapTableSink.isPartialUpdate();
        if (isPartialUpdate) {
            for (Column col : olapTableSink.getCols()) {
                partialUpdateCols.add(col.getName());
            }
        }
        TupleDescriptor olapTuple = generateTupleDesc(olapTableSink.getTargetTableSlots(),
                olapTableSink.getTargetTable(), context);
        List<Expr> partitionExprs = olapTableSink.getPartitionExprList().stream()
                .map(e -> ExpressionTranslator.translate(e, context)).collect(Collectors.toList());
        Map<Long, Expr> syncMvWhereClauses = new HashMap<>();
        for (Map.Entry<Long, Expression> entry : olapTableSink.getSyncMvWhereClauses().entrySet()) {
            syncMvWhereClauses.put(entry.getKey(), ExpressionTranslator.translate(entry.getValue(), context));
        }
        OlapTableSink sink;
        // This statement is only used in the group_commit mode
        if (context.getConnectContext().isGroupCommit()) {
            sink = new GroupCommitBlockSink(olapTableSink.getTargetTable(), olapTuple,
                    olapTableSink.getTargetTable().getPartitionIds(), olapTableSink.isSingleReplicaLoad(),
                    partitionExprs, syncMvWhereClauses,
                    context.getSessionVariable().getGroupCommit(),
                    ConnectContext.get().getSessionVariable().getEnableInsertStrict() ? 0 : 1);
        } else {
            sink = new OlapTableSink(
                olapTableSink.getTargetTable(),
                olapTuple,
                olapTableSink.getPartitionIds().isEmpty() ? null : olapTableSink.getPartitionIds(),
                olapTableSink.isSingleReplicaLoad(), partitionExprs, syncMvWhereClauses
            );
        }
        sink.setPartialUpdateInputColumns(isPartialUpdate, partialUpdateCols);
        if (isPartialUpdate) {
            sink.setPartialUpdateNewRowPolicy(olapTableSink.getPartialUpdateNewRowPolicy());
        }
        rootFragment.setSink(sink);

        return rootFragment;
    }

    @Override
    public PlanFragment visitPhysicalHiveTableSink(PhysicalHiveTableSink<? extends Plan> hiveTableSink,
                                                   PlanTranslatorContext context) {
        PlanFragment rootFragment = hiveTableSink.child().accept(this, context);
        rootFragment.setOutputPartition(DataPartition.UNPARTITIONED);

        TupleDescriptor hiveTuple = context.generateTupleDesc();
        List<Column> targetTableColumns = hiveTableSink.getTargetTable().getFullSchema();
        for (Column column : targetTableColumns) {
            SlotDescriptor slotDesc = context.addSlotDesc(hiveTuple);
            slotDesc.setIsMaterialized(true);
            slotDesc.setType(column.getType());
            slotDesc.setColumn(column);
            slotDesc.setIsNullable(column.isAllowNull());
            slotDesc.setAutoInc(column.isAutoInc());
        }
        HiveTableSink sink = new HiveTableSink((HMSExternalTable) hiveTableSink.getTargetTable());
        rootFragment.setSink(sink);
        return rootFragment;
    }

    @Override
    public PlanFragment visitPhysicalIcebergTableSink(PhysicalIcebergTableSink<? extends Plan> icebergTableSink,
                                                      PlanTranslatorContext context) {
        PlanFragment rootFragment = icebergTableSink.child().accept(this, context);
        rootFragment.setOutputPartition(DataPartition.UNPARTITIONED);

        TupleDescriptor hiveTuple = context.generateTupleDesc();
        List<Column> targetTableColumns = icebergTableSink.getTargetTable().getFullSchema();
        for (Column column : targetTableColumns) {
            SlotDescriptor slotDesc = context.addSlotDesc(hiveTuple);
            slotDesc.setIsMaterialized(true);
            slotDesc.setType(column.getType());
            slotDesc.setColumn(column);
            slotDesc.setIsNullable(column.isAllowNull());
            slotDesc.setAutoInc(column.isAutoInc());
        }
        IcebergTableSink sink = new IcebergTableSink((IcebergExternalTable) icebergTableSink.getTargetTable());
        rootFragment.setSink(sink);
        return rootFragment;
    }

    @Override
    public PlanFragment visitPhysicalJdbcTableSink(PhysicalJdbcTableSink<? extends Plan> jdbcTableSink,
            PlanTranslatorContext context) {
        PlanFragment rootFragment = jdbcTableSink.child().accept(this, context);
        rootFragment.setOutputPartition(DataPartition.UNPARTITIONED);
        List<Column> targetTableColumns = jdbcTableSink.getCols();
        List<String> insertCols = targetTableColumns.stream()
                .map(Column::getName)
                .collect(Collectors.toList());

        JdbcTableSink sink = new JdbcTableSink(
                ((JdbcExternalTable) jdbcTableSink.getTargetTable()).getJdbcTable(),
                insertCols
        );
        rootFragment.setSink(sink);
        return rootFragment;
    }

    @Override
    public PlanFragment visitPhysicalFileSink(PhysicalFileSink<? extends Plan> fileSink,
            PlanTranslatorContext context) {
        PlanFragment sinkFragment = fileSink.child().accept(this, context);
        OutFileClause outFile = new OutFileClause(
                fileSink.getFilePath(),
                fileSink.getFormat(),
                fileSink.getProperties()
        );

        List<Expr> outputExprs = Lists.newArrayList();
        fileSink.getOutput().stream().map(Slot::getExprId)
                .forEach(exprId -> outputExprs.add(context.findSlotRef(exprId)));
        sinkFragment.setOutputExprs(outputExprs);

        // generate colLabels
        List<String> labels = fileSink.getOutput().stream().map(NamedExpression::getName).collect(Collectors.toList());

        // TODO: should not call legacy planner analyze in Nereids
        try {
            outFile.analyze(outputExprs, labels);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
        ResultFileSink resultFileSink = new ResultFileSink(sinkFragment.getPlanRoot().getId(), outFile,
                (ArrayList<String>) labels);

        sinkFragment.setSink(resultFileSink);
        return sinkFragment;
    }

    /* ********************************************************************************************
     * scan Node, in lexicographical order
     * ******************************************************************************************** */

    @Override
    public PlanFragment visitPhysicalFileScan(PhysicalFileScan fileScan, PlanTranslatorContext context) {
        List<Slot> slots = fileScan.getOutput();
        ExternalTable table = fileScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, table, context);

        SessionVariable sv = ConnectContext.get().getSessionVariable();
        // TODO(cmy): determine the needCheckColumnPriv param
        ScanNode scanNode;
        if (table instanceof HMSExternalTable) {
            if (directoryLister == null) {
                this.directoryLister = new TransactionScopeCachingDirectoryListerFactory(
                        Config.max_external_table_split_file_meta_cache_num).get(new FileSystemDirectoryLister());
            }
            switch (((HMSExternalTable) table).getDlaType()) {
                case ICEBERG:
                    scanNode = new IcebergScanNode(context.nextPlanNodeId(), tupleDescriptor, false, sv);
                    break;
                case HIVE:
                    scanNode = new HiveScanNode(context.nextPlanNodeId(), tupleDescriptor, false, sv, directoryLister);
                    HiveScanNode hiveScanNode = (HiveScanNode) scanNode;
                    hiveScanNode.setSelectedPartitions(fileScan.getSelectedPartitions());
                    if (fileScan.getTableSample().isPresent()) {
                        hiveScanNode.setTableSample(new TableSample(fileScan.getTableSample().get().isPercent,
                                fileScan.getTableSample().get().sampleValue, fileScan.getTableSample().get().seek));
                    }
                    break;
                default:
                    throw new RuntimeException("do not support DLA type " + ((HMSExternalTable) table).getDlaType());
            }
        } else if (table instanceof IcebergExternalTable) {
            scanNode = new IcebergScanNode(context.nextPlanNodeId(), tupleDescriptor, false, sv);
        } else if (table instanceof PaimonExternalTable) {
            scanNode = new PaimonScanNode(context.nextPlanNodeId(), tupleDescriptor, false, sv);
        } else if (table instanceof TrinoConnectorExternalTable) {
            scanNode = new TrinoConnectorScanNode(context.nextPlanNodeId(), tupleDescriptor, false, sv);
        } else if (table instanceof MaxComputeExternalTable) {
            scanNode = new MaxComputeScanNode(context.nextPlanNodeId(), tupleDescriptor,
                    fileScan.getSelectedPartitions(), false, sv);
        } else if (table instanceof LakeSoulExternalTable) {
            scanNode = new LakeSoulScanNode(context.nextPlanNodeId(), tupleDescriptor, false, sv);
        } else {
            throw new RuntimeException("do not support table type " + table.getType());
        }
        if (scanNode instanceof FileQueryScanNode) {
            FileQueryScanNode fileQueryScanNode = (FileQueryScanNode) scanNode;
            fileScan.getTableSnapshot().ifPresent(fileQueryScanNode::setQueryTableSnapshot);
            fileScan.getScanParams().ifPresent(fileQueryScanNode::setScanParams);
        }
        // translate rf v2 target
        List<RuntimeFilterV2> rfV2s = context.getRuntimeFilterV2Context()
                .getRuntimeFilterV2ByTargetPlan(fileScan);
        for (RuntimeFilterV2 rfV2 : rfV2s) {
            Expr targetExpr = rfV2.getTargetExpression().accept(ExpressionTranslator.INSTANCE, context);
            rfV2.setLegacyTargetNode(scanNode);
            rfV2.setLegacyTargetExpr(targetExpr);
        }
        return getPlanFragmentForPhysicalFileScan(fileScan, context, scanNode, table, tupleDescriptor);
    }

    @Override
    public PlanFragment visitPhysicalEmptyRelation(PhysicalEmptyRelation emptyRelation, PlanTranslatorContext context) {
        List<Slot> output = emptyRelation.getOutput();
        TupleDescriptor tupleDescriptor = generateTupleDesc(output, null, context);
        for (Slot slot : output) {
            SlotRef slotRef = context.findSlotRef(slot.getExprId());
            slotRef.setLabel(slot.getName());
        }

        ArrayList<TupleId> tupleIds = new ArrayList<>();
        tupleIds.add(tupleDescriptor.getId());
        EmptySetNode emptySetNode = new EmptySetNode(context.nextPlanNodeId(), tupleIds);
        emptySetNode.setNereidsId(emptyRelation.getId());
        context.getNereidsIdToPlanNodeIdMap().put(emptyRelation.getId(), emptySetNode.getId());
        PlanFragment planFragment = createPlanFragment(emptySetNode,
                DataPartition.UNPARTITIONED, emptyRelation);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), emptyRelation);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalEsScan(PhysicalEsScan esScan, PlanTranslatorContext context) {
        List<Slot> slots = esScan.getOutput();
        TableIf table = esScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, table, context);
        EsScanNode esScanNode = new EsScanNode(context.nextPlanNodeId(), tupleDescriptor,
                table instanceof EsExternalTable);
        esScanNode.setNereidsId(esScan.getId());
        context.getNereidsIdToPlanNodeIdMap().put(esScan.getId(), esScanNode.getId());
        Utils.execWithUncheckedException(esScanNode::init);
        context.addScanNode(esScanNode, esScan);
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getContext().getTargetListByScan(esScan).forEach(
                        expr -> runtimeFilterGenerator.translateRuntimeFilterTarget(expr, esScanNode, context)
                )
        );
        // translate rf v2 target
        List<RuntimeFilterV2> rfV2s = context.getRuntimeFilterV2Context()
                .getRuntimeFilterV2ByTargetPlan(esScan);
        for (RuntimeFilterV2 rfV2 : rfV2s) {
            Expr targetExpr = rfV2.getTargetExpression().accept(ExpressionTranslator.INSTANCE, context);
            rfV2.setLegacyTargetNode(esScanNode);
            rfV2.setLegacyTargetExpr(targetExpr);
        }
        context.getTopnFilterContext().translateTarget(esScan, esScanNode, context);
        DataPartition dataPartition = DataPartition.RANDOM;
        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), esScanNode, dataPartition);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), esScan);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalHudiScan(PhysicalHudiScan fileScan, PlanTranslatorContext context) {
        if (directoryLister == null) {
            this.directoryLister = new TransactionScopeCachingDirectoryListerFactory(
                    Config.max_external_table_split_file_meta_cache_num).get(new FileSystemDirectoryLister());
        }
        List<Slot> slots = fileScan.getOutput();
        ExternalTable table = fileScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, table, context);

        if (!(table instanceof HMSExternalTable) || ((HMSExternalTable) table).getDlaType() != DLAType.HUDI) {
            throw new RuntimeException("Invalid table type for Hudi scan: " + table.getType());
        }
        Preconditions.checkState(fileScan instanceof PhysicalHudiScan,
                "Invalid physical scan: " + fileScan.getClass().getSimpleName()
                        + " for Hudi table");
        PhysicalHudiScan hudiScan = (PhysicalHudiScan) fileScan;
        ScanNode scanNode = new HudiScanNode(context.nextPlanNodeId(), tupleDescriptor, false,
                hudiScan.getScanParams(), hudiScan.getIncrementalRelation(), ConnectContext.get().getSessionVariable(),
                directoryLister);
        if (fileScan.getTableSnapshot().isPresent()) {
            ((FileQueryScanNode) scanNode).setQueryTableSnapshot(fileScan.getTableSnapshot().get());
        }
        HudiScanNode hudiScanNode = (HudiScanNode) scanNode;
        hudiScanNode.setSelectedPartitions(fileScan.getSelectedPartitions());
        return getPlanFragmentForPhysicalFileScan(fileScan, context, scanNode, table, tupleDescriptor);
    }

    @NotNull
    private PlanFragment getPlanFragmentForPhysicalFileScan(PhysicalFileScan fileScan, PlanTranslatorContext context,
            ScanNode scanNode,
            ExternalTable table, TupleDescriptor tupleDescriptor) {
        scanNode.setNereidsId(fileScan.getId());
        context.getNereidsIdToPlanNodeIdMap().put(fileScan.getId(), scanNode.getId());
        scanNode.setPushDownAggNoGrouping(context.getRelationPushAggOp(fileScan.getRelationId()));

        TableName tableName = new TableName(null, "", "");
        TableRef ref = new TableRef(tableName, null, null);
        BaseTableRef tableRef = new BaseTableRef(ref, table, tableName);
        tupleDescriptor.setRef(tableRef);
        if (fileScan.getStats() != null) {
            scanNode.setCardinality((long) fileScan.getStats().getRowCount());
        }
        Utils.execWithUncheckedException(scanNode::init);
        context.addScanNode(scanNode, fileScan);
        ScanNode finalScanNode = scanNode;
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getContext().getTargetListByScan(fileScan).forEach(
                        expr -> runtimeFilterGenerator.translateRuntimeFilterTarget(expr, finalScanNode, context)
                )
        );
        // translate rf v2 target
        List<RuntimeFilterV2> rfV2s = context.getRuntimeFilterV2Context()
                .getRuntimeFilterV2ByTargetPlan(fileScan);
        for (RuntimeFilterV2 rfV2 : rfV2s) {
            Expr targetExpr = rfV2.getTargetExpression().accept(ExpressionTranslator.INSTANCE, context);
            rfV2.setLegacyTargetNode(scanNode);
            rfV2.setLegacyTargetExpr(targetExpr);
        }
        context.getTopnFilterContext().translateTarget(fileScan, scanNode, context);
        // Create PlanFragment
        DataPartition dataPartition = DataPartition.RANDOM;
        PlanFragment planFragment = createPlanFragment(scanNode, dataPartition, fileScan);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), fileScan);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalJdbcScan(PhysicalJdbcScan jdbcScan, PlanTranslatorContext context) {
        List<Slot> slots = jdbcScan.getOutput();
        TableIf table = jdbcScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, table, context);
        JdbcScanNode jdbcScanNode = new JdbcScanNode(context.nextPlanNodeId(), tupleDescriptor,
                table instanceof JdbcExternalTable);
        jdbcScanNode.setNereidsId(jdbcScan.getId());
        context.getNereidsIdToPlanNodeIdMap().put(jdbcScan.getId(), jdbcScanNode.getId());
        Utils.execWithUncheckedException(jdbcScanNode::init);
        context.addScanNode(jdbcScanNode, jdbcScan);
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getContext().getTargetListByScan(jdbcScan).forEach(
                        expr -> runtimeFilterGenerator.translateRuntimeFilterTarget(expr, jdbcScanNode, context)
                )
        );
        // translate rf v2 target
        List<RuntimeFilterV2> rfV2s = context.getRuntimeFilterV2Context()
                .getRuntimeFilterV2ByTargetPlan(jdbcScan);
        for (RuntimeFilterV2 rfV2 : rfV2s) {
            Expr targetExpr = rfV2.getTargetExpression().accept(ExpressionTranslator.INSTANCE, context);
            rfV2.setLegacyTargetNode(jdbcScanNode);
            rfV2.setLegacyTargetExpr(targetExpr);
        }

        context.getTopnFilterContext().translateTarget(jdbcScan, jdbcScanNode, context);
        DataPartition dataPartition = DataPartition.RANDOM;
        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), jdbcScanNode, dataPartition);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), jdbcScan);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalOdbcScan(PhysicalOdbcScan odbcScan, PlanTranslatorContext context) {
        List<Slot> slots = odbcScan.getOutput();
        TableIf table = odbcScan.getTable();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, table, context);
        OdbcScanNode odbcScanNode = new OdbcScanNode(context.nextPlanNodeId(), tupleDescriptor,
                (OdbcTable) table);
        odbcScanNode.setNereidsId(odbcScan.getId());
        context.getNereidsIdToPlanNodeIdMap().put(odbcScan.getId(), odbcScanNode.getId());
        Utils.execWithUncheckedException(odbcScanNode::init);
        context.addScanNode(odbcScanNode, odbcScan);
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getContext().getTargetListByScan(odbcScan).forEach(
                        expr -> runtimeFilterGenerator.translateRuntimeFilterTarget(expr, odbcScanNode, context)
                )
        );
        // translate rf v2 target
        List<RuntimeFilterV2> rfV2s = context.getRuntimeFilterV2Context()
                .getRuntimeFilterV2ByTargetPlan(odbcScan);
        for (RuntimeFilterV2 rfV2 : rfV2s) {
            Expr targetExpr = rfV2.getTargetExpression().accept(ExpressionTranslator.INSTANCE, context);
            rfV2.setLegacyTargetNode(odbcScanNode);
            rfV2.setLegacyTargetExpr(targetExpr);
        }
        context.getTopnFilterContext().translateTarget(odbcScan, odbcScanNode, context);
        context.getTopnFilterContext().translateTarget(odbcScan, odbcScanNode, context);
        DataPartition dataPartition = DataPartition.RANDOM;
        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), odbcScanNode, dataPartition);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), odbcScan);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalOlapScan(PhysicalOlapScan olapScan, PlanTranslatorContext context) {
        return computePhysicalOlapScan(olapScan, context);
    }

    private PlanFragment computePhysicalOlapScan(PhysicalOlapScan olapScan, PlanTranslatorContext context) {
        List<Slot> slots = olapScan.getOutput();
        OlapTable olapTable = olapScan.getTable();
        // generate real output tuple
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, olapTable, context);

        // put virtual column expr into slot desc
        Map<ExprId, Expression> slotToVirtualColumnMap = olapScan.getSlotToVirtualColumnMap();
        for (SlotDescriptor slotDescriptor : tupleDescriptor.getSlots()) {
            ExprId exprId = context.findExprId(slotDescriptor.getId());
            if (slotToVirtualColumnMap.containsKey(exprId)) {
                slotDescriptor.setVirtualColumn(ExpressionTranslator.translate(
                        slotToVirtualColumnMap.get(exprId), context));
                context.getVirtualColumnIds().add(slotDescriptor.getId());
            }
        }

        // generate base index tuple because this fragment partitioned expr relay on slots of based index
        if (olapScan.getSelectedIndexId() != olapScan.getTable().getBaseIndexId()) {
            generateTupleDesc(olapScan.getBaseOutputs(), olapTable, context);
        }

        OlapScanNode olapScanNode = new OlapScanNode(context.nextPlanNodeId(), tupleDescriptor, "OlapScanNode");
        olapScanNode.setNereidsId(olapScan.getId());
        context.getNereidsIdToPlanNodeIdMap().put(olapScan.getId(), olapScanNode.getId());
        // TODO: move all node set cardinality into one place
        if (olapScan.getStats() != null) {
            // NOTICE: we should not set stats row count
            //   because it is whole table cardinality and will break block rules.
            // olapScanNode.setCardinality((long) olapScan.getStats().getRowCount());
            if (context.getSessionVariable() != null && context.getSessionVariable().forbidUnknownColStats) {
                for (int i = 0; i < slots.size(); i++) {
                    SlotReference slot = (SlotReference) slots.get(i);
                    boolean inVisibleCol = slot.getOriginalColumn().isPresent()
                            && StatisticConstants.shouldIgnoreCol(olapTable, slot.getOriginalColumn().get());
                    if (olapScan.getStats().findColumnStatistics(slot).isUnKnown()
                            && !isComplexDataType(slot.getDataType())
                            && !StatisticConstants.isSystemTable(olapTable)
                            && !inVisibleCol) {
                        context.addUnknownStatsColumn(olapScanNode, tupleDescriptor.getSlots().get(i).getId());
                    }
                }
            }
        }
        // TODO: Do we really need tableName here?
        TableName tableName = new TableName(null, "", "");
        TableRef ref = new TableRef(tableName, null, null);
        BaseTableRef tableRef = new BaseTableRef(ref, olapTable, tableName);
        tupleDescriptor.setRef(tableRef);
        olapScanNode.setSelectedPartitionIds(olapScan.getSelectedPartitionIds());
        olapScanNode.setNereidsPrunedTabletIds(new LinkedHashSet<>(olapScan.getSelectedTabletIds()));
        if (olapScan.getTableSample().isPresent()) {
            olapScanNode.setTableSample(new TableSample(olapScan.getTableSample().get().isPercent,
                    olapScan.getTableSample().get().sampleValue, olapScan.getTableSample().get().seek));
        }

        // TODO:  remove this switch?
        switch (olapScan.getTable().getKeysType()) {
            case AGG_KEYS:
            case UNIQUE_KEYS:
            case DUP_KEYS:
                PreAggStatus preAgg = olapScan.getPreAggStatus();
                olapScanNode.setSelectedIndexInfo(olapScan.getSelectedIndexId(), preAgg.isOn(), preAgg.getOffReason());
                break;
            default:
                throw new RuntimeException("Not supported key type: " + olapScan.getTable().getKeysType());
        }

        // create scan range
        Utils.execWithUncheckedException(olapScanNode::init);
        // TODO: process collect scan node in one place
        context.addScanNode(olapScanNode, olapScan);
        // TODO: process translate runtime filter in one place
        //   use real plan node to present rf apply and rf generator
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterTranslator -> runtimeFilterTranslator.getContext().getTargetListByScan(olapScan)
                        .forEach(expr -> runtimeFilterTranslator.translateRuntimeFilterTarget(
                                expr, olapScanNode, context)
                        )
        );
        // translate rf v2 target
        List<RuntimeFilterV2> rfV2s = context.getRuntimeFilterV2Context()
                .getRuntimeFilterV2ByTargetPlan(olapScan);
        for (RuntimeFilterV2 rfV2 : rfV2s) {
            Expr targetExpr = rfV2.getTargetExpression().accept(ExpressionTranslator.INSTANCE, context);
            rfV2.setLegacyTargetNode(olapScanNode);
            rfV2.setLegacyTargetExpr(targetExpr);
        }
        context.getTopnFilterContext().translateTarget(olapScan, olapScanNode, context);
        olapScanNode.setPushDownAggNoGrouping(context.getRelationPushAggOp(olapScan.getRelationId()));
        // Create PlanFragment
        // TODO: use a util function to convert distribution to DataPartition
        DataPartition dataPartition = DataPartition.RANDOM;
        if (olapScan.getDistributionSpec() instanceof DistributionSpecHash) {
            DistributionSpecHash distributionSpecHash = (DistributionSpecHash) olapScan.getDistributionSpec();
            List<Expr> partitionExprs = distributionSpecHash.getOrderedShuffledColumns().stream()
                    .map(context::findSlotRef).collect(Collectors.toList());
            dataPartition = new DataPartition(TPartitionType.HASH_PARTITIONED, partitionExprs);
        }
        if (olapScan.getStats() != null) {
            olapScanNode.setCardinality((long) olapScan.getStats().getRowCount());
        }
        // TODO: maybe we could have a better way to create fragment
        PlanFragment planFragment = createPlanFragment(olapScanNode, dataPartition, olapScan);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), olapScan);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalDeferMaterializeOlapScan(
            PhysicalDeferMaterializeOlapScan deferMaterializeOlapScan, PlanTranslatorContext context) {
        PlanFragment planFragment = visitPhysicalOlapScan(deferMaterializeOlapScan.getPhysicalOlapScan(), context);
        OlapScanNode olapScanNode = (OlapScanNode) planFragment.getPlanRoot();
        TupleDescriptor tupleDescriptor = context.getTupleDesc(olapScanNode.getTupleId());
        for (SlotDescriptor slotDescriptor : tupleDescriptor.getSlots()) {
            if (deferMaterializeOlapScan.getDeferMaterializeSlotIds()
                    .contains(context.findExprId(slotDescriptor.getId()))) {
                slotDescriptor.setNeedMaterialize(false);
            }
        }
        context.createSlotDesc(tupleDescriptor, deferMaterializeOlapScan.getColumnIdSlot());
        context.getTopnFilterContext().translateTarget(deferMaterializeOlapScan, olapScanNode, context);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRowRelation,
            PlanTranslatorContext context) {
        List<Slot> slots = oneRowRelation.getLogicalProperties().getOutput();
        TupleDescriptor oneRowTuple = generateTupleDesc(slots, null, context);

        List<Expr> legacyExprs = Lists.newArrayList();
        List<Expression> expressionList = Lists.newArrayList();
        for (NamedExpression namedExpression : oneRowRelation.getProjects()) {
            legacyExprs.add(ExpressionTranslator.translate(namedExpression, context));
            expressionList.add(namedExpression);
        }

        for (int i = 0; i < legacyExprs.size(); i++) {
            SlotDescriptor slotDescriptor = oneRowTuple.getSlots().get(i);
            Expr expr = legacyExprs.get(i);
            slotDescriptor.setSourceExpr(expr);
            slotDescriptor.setIsNullable(slots.get(i).nullable());
        }

        UnionNode unionNode = new UnionNode(context.nextPlanNodeId(), oneRowTuple.getId());
        unionNode.setNereidsId(oneRowRelation.getId());
        context.getNereidsIdToPlanNodeIdMap().put(oneRowRelation.getId(), unionNode.getId());
        unionNode.setCardinality(1L);
        List<List<Expression>> constExpressionList = Lists.newArrayList();
        constExpressionList.add(expressionList);
        finalizeForSetOperationNode(unionNode, oneRowTuple.getSlots(), new ArrayList<>(),
                constExpressionList, new ArrayList<>(), context);

        PlanFragment planFragment = createPlanFragment(unionNode, DataPartition.UNPARTITIONED, oneRowRelation);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), oneRowRelation);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalSchemaScan(PhysicalSchemaScan schemaScan, PlanTranslatorContext context) {
        TableIf table = schemaScan.getTable();
        List<Slot> slots = ImmutableList.copyOf(schemaScan.getOutput());
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, table, context);

        // For the information_schema.rowsets table, the scan fragment needs to be sent to all BEs.
        // For other information_schema tables, the scan fragment only needs to be sent to one of the BEs.
        SchemaScanNode scanNode = null;
        if (BackendPartitionedSchemaScanNode.isBackendPartitionedSchemaTable(
                table.getName())) {
            scanNode = new BackendPartitionedSchemaScanNode(context.nextPlanNodeId(), table, tupleDescriptor,
                schemaScan.getSchemaCatalog().orElse(null), schemaScan.getSchemaDatabase().orElse(null),
                schemaScan.getSchemaTable().orElse(null));
        } else {
            scanNode = new SchemaScanNode(context.nextPlanNodeId(), tupleDescriptor,
                schemaScan.getSchemaCatalog().orElse(null), schemaScan.getSchemaDatabase().orElse(null),
                schemaScan.getSchemaTable().orElse(null));
        }
        scanNode.setNereidsId(schemaScan.getId());
        context.getNereidsIdToPlanNodeIdMap().put(schemaScan.getId(), scanNode.getId());
        SchemaScanNode finalScanNode = scanNode;
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getContext().getTargetListByScan(schemaScan)
                        .forEach(expr -> runtimeFilterGenerator
                                .translateRuntimeFilterTarget(expr, finalScanNode, context)
                )
        );
        // translate rf v2 target
        List<RuntimeFilterV2> rfV2s = context.getRuntimeFilterV2Context()
                .getRuntimeFilterV2ByTargetPlan(schemaScan);
        for (RuntimeFilterV2 rfV2 : rfV2s) {
            Expr targetExpr = rfV2.getTargetExpression().accept(ExpressionTranslator.INSTANCE, context);
            rfV2.setLegacyTargetNode(scanNode);
            rfV2.setLegacyTargetExpr(targetExpr);
        }
        context.addScanNode(scanNode, schemaScan);
        PlanFragment planFragment = createPlanFragment(scanNode, DataPartition.RANDOM, schemaScan);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), schemaScan);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalTVFRelation(PhysicalTVFRelation tvfRelation, PlanTranslatorContext context) {
        List<Slot> slots = tvfRelation.getLogicalProperties().getOutput();
        TupleDescriptor tupleDescriptor = generateTupleDesc(slots, tvfRelation.getFunction().getTable(), context);

        TableValuedFunctionIf catalogFunction = tvfRelation.getFunction().getCatalogFunction();
        SessionVariable sv = ConnectContext.get().getSessionVariable();
        ScanNode scanNode = catalogFunction.getScanNode(context.nextPlanNodeId(), tupleDescriptor, sv);
        scanNode.setNereidsId(tvfRelation.getId());
        context.getNereidsIdToPlanNodeIdMap().put(tvfRelation.getId(), scanNode.getId());
        Utils.execWithUncheckedException(scanNode::init);
        context.getRuntimeTranslator().ifPresent(
                runtimeFilterGenerator -> runtimeFilterGenerator.getContext().getTargetListByScan(tvfRelation)
                        .forEach(expr -> runtimeFilterGenerator.translateRuntimeFilterTarget(expr, scanNode, context)
                )
        );
        context.addScanNode(scanNode, tvfRelation);

        // TODO: it is weird update label in this way
        // set label for explain
        for (Slot slot : slots) {
            String tableColumnName = TableValuedFunctionIf.TVF_TABLE_PREFIX + tvfRelation.getFunction().getName()
                    + "." + slot.getName();
            context.findSlotRef(slot.getExprId()).setLabel(tableColumnName);
        }

        PlanFragment planFragment = createPlanFragment(scanNode, DataPartition.RANDOM, tvfRelation);
        context.addPlanFragment(planFragment);
        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), tvfRelation);
        return planFragment;
    }


    /* ********************************************************************************************
     * other Node, in lexicographical order, ignore algorithm name. for example, HashAggregate -> Aggregate
     * ******************************************************************************************** */

    /**
     * Translate Agg.
     */
    @Override
    public PlanFragment visitPhysicalHashAggregate(
            PhysicalHashAggregate<? extends Plan> aggregate,
            PlanTranslatorContext context) {

        PlanFragment inputPlanFragment = aggregate.child(0).accept(this, context);
        List<List<Expr>> distributeExprLists = getDistributeExprs(aggregate.child(0));

        List<Expression> groupByExpressions = aggregate.getGroupByExpressions();
        List<NamedExpression> outputExpressions = aggregate.getOutputExpressions();

        // 1. generate slot reference for each group expression
        List<SlotReference> groupSlots = collectGroupBySlots(groupByExpressions, outputExpressions);
        ArrayList<Expr> execGroupingExpressions = Lists.newArrayListWithCapacity(groupByExpressions.size());
        for (Expression e : groupByExpressions) {
            Expr result = ExpressionTranslator.translate(e, context);
            if (result == null) {
                throw new RuntimeException("translate " + e + " failed");
            }
            execGroupingExpressions.add(result);
        }
        // 2. collect agg expressions and generate agg function to slot reference map
        List<Slot> aggFunctionOutput = Lists.newArrayList();
        ArrayList<FunctionCallExpr> execAggregateFunctions = Lists.newArrayListWithCapacity(outputExpressions.size());
        for (NamedExpression o : outputExpressions) {
            if (o.containsType(AggregateExpression.class)) {
                aggFunctionOutput.add(o.toSlot());

                o.foreach(c -> {
                    if (c instanceof AggregateExpression) {
                        execAggregateFunctions.add(
                                (FunctionCallExpr) ExpressionTranslator.translate((AggregateExpression) c, context)
                        );
                    }
                });
            }
        }

        // 3. generate output tuple
        List<Slot> slotList = Lists.newArrayList();
        TupleDescriptor outputTupleDesc;
        slotList.addAll(groupSlots);
        slotList.addAll(aggFunctionOutput);
        outputTupleDesc = generateTupleDesc(slotList, null, context);

        List<Integer> aggFunOutputIds = ImmutableList.of();
        if (!aggFunctionOutput.isEmpty()) {
            aggFunOutputIds = Lists.newArrayListWithCapacity(outputTupleDesc.getSlots().size() - groupSlots.size());
            ArrayList<SlotDescriptor> slots = outputTupleDesc.getSlots();
            for (int i = groupSlots.size(); i < slots.size(); i++) {
                aggFunOutputIds.add(slots.get(i).getId().asInt());
            }
        }
        boolean isPartial = aggregate.getAggregateParam().aggMode.productAggregateBuffer;
        AggregateInfo aggInfo = AggregateInfo.create(execGroupingExpressions, execAggregateFunctions,
                aggFunOutputIds, isPartial, outputTupleDesc, outputTupleDesc, aggregate.getAggPhase().toExec());
        AggregationNode aggregationNode = new AggregationNode(context.nextPlanNodeId(),
                inputPlanFragment.getPlanRoot(), aggInfo);

        aggregationNode.setChildrenDistributeExprLists(distributeExprLists);

        aggregationNode.setNereidsId(aggregate.getId());
        context.getNereidsIdToPlanNodeIdMap().put(aggregate.getId(), aggregationNode.getId());
        if (!aggregate.getAggMode().isFinalPhase) {
            aggregationNode.unsetNeedsFinalize();
        }

        switch (aggregate.getAggPhase()) {
            case LOCAL:
                // we should set is useStreamingAgg when has exchange,
                // so the `aggregationNode.setUseStreamingPreagg()` in the visitPhysicalDistribute
                break;
            case DISTINCT_LOCAL:
                aggregationNode.setIntermediateTuple();
                break;
            case GLOBAL:
            case DISTINCT_GLOBAL:
                break;
            default:
                throw new RuntimeException("Unsupported agg phase: " + aggregate.getAggPhase());
        }

        // in pipeline engine, we use parallel scan by default, but it broke the rule of data distribution
        // so, if we do final phase or merge without exchange.
        // we need turn of parallel scan to ensure to get correct result.
        PlanNode leftMostNode = inputPlanFragment.getPlanRoot();
        while (!leftMostNode.getChildren().isEmpty() && !(leftMostNode instanceof ExchangeNode)) {
            leftMostNode = leftMostNode.getChild(0);
        }
        // TODO: nereids forbid all parallel scan under aggregate temporary, because nereids could generate
        //  so complex aggregate plan than legacy planner, and should add forbid parallel scan hint when
        //  generate physical aggregate plan.
        //  There is one exception, we use some precondition in optimizer, input to buffer always require any for input,
        //  so when agg mode is INPUT_TO_BUFFER, we do not forbid parallel scan
        if (aggregate.getAggregateParam().aggMode != AggMode.INPUT_TO_BUFFER) {
            inputPlanFragment.setHasColocatePlanNode(true);
            // Set colocate info in agg node. This is a hint for local shuffling to decide which type of
            // local exchanger will be used.
            aggregationNode.setColocate(true);

            Plan child = aggregate.child();
            // we should set colocate = true, when the same LogicalAggregate generate two PhysicalHashAggregates
            // in one fragment:
            //
            // agg(merge finalize)   <- current, set colocate = true
            //          |
            // agg(update serialize) <- child, also set colocate = true
            if (aggregate.getAggregateParam().aggMode.consumeAggregateBuffer
                    && child instanceof PhysicalHashAggregate
                    && !((PhysicalHashAggregate<Plan>) child).getAggregateParam().aggMode.consumeAggregateBuffer
                    && inputPlanFragment.getPlanRoot() instanceof AggregationNode) {
                AggregationNode childAgg = (AggregationNode) inputPlanFragment.getPlanRoot();
                childAgg.setColocate(true);
            }
        }
        if (aggregate.getTopnPushInfo() != null) {
            List<Expr> orderingExprs = Lists.newArrayList();
            List<Boolean> ascOrders = Lists.newArrayList();
            List<Boolean> nullsFirstParams = Lists.newArrayList();
            aggregate.getTopnPushInfo().orderkeys.forEach(k -> {
                orderingExprs.add(ExpressionTranslator.translate(k.getExpr(), context));
                ascOrders.add(k.isAsc());
                nullsFirstParams.add(k.isNullFirst());
            });
            SortInfo sortInfo = new SortInfo(orderingExprs, ascOrders, nullsFirstParams, outputTupleDesc);
            aggregationNode.setSortByGroupKey(sortInfo);
            if (aggregationNode.getLimit() == -1) {
                aggregationNode.setLimit(aggregate.getTopnPushInfo().limit);
            }
        } else {
            aggregationNode.setSortByGroupKey(null);
        }
        setPlanRoot(inputPlanFragment, aggregationNode, aggregate);
        if (aggregate.getStats() != null) {
            aggregationNode.setCardinality((long) aggregate.getStats().getRowCount());
        }
        updateLegacyPlanIdToPhysicalPlan(inputPlanFragment.getPlanRoot(), aggregate);
        return inputPlanFragment;
    }

    @Override
    public PlanFragment visitPhysicalStorageLayerAggregate(
            PhysicalStorageLayerAggregate storageLayerAggregate, PlanTranslatorContext context) {
        Preconditions.checkState((storageLayerAggregate.getRelation() instanceof PhysicalOlapScan
                        || storageLayerAggregate.getRelation() instanceof PhysicalFileScan),
                "PhysicalStorageLayerAggregate only support PhysicalOlapScan and PhysicalFileScan: "
                        + storageLayerAggregate.getRelation().getClass().getName());

        TPushAggOp pushAggOp;
        switch (storageLayerAggregate.getAggOp()) {
            case COUNT:
                pushAggOp = TPushAggOp.COUNT;
                break;
            case COUNT_ON_MATCH:
                pushAggOp = TPushAggOp.COUNT_ON_INDEX;
                break;
            case MIN_MAX:
                pushAggOp = TPushAggOp.MINMAX;
                break;
            case MIX:
                pushAggOp = TPushAggOp.MIX;
                break;
            default:
                throw new AnalysisException("Unsupported storage layer aggregate: "
                        + storageLayerAggregate.getAggOp());
        }

        if (storageLayerAggregate.getRelation() instanceof PhysicalFileScan
                && pushAggOp.equals(TPushAggOp.COUNT)
                && !ConnectContext.get().getSessionVariable().isEnableCountPushDownForExternalTable()) {
            pushAggOp = TPushAggOp.NONE;
        }

        context.setRelationPushAggOp(
                storageLayerAggregate.getRelation().getRelationId(), pushAggOp);

        PlanFragment planFragment = storageLayerAggregate.getRelation().accept(this, context);

        updateLegacyPlanIdToPhysicalPlan(planFragment.getPlanRoot(), storageLayerAggregate);
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
            PlanTranslatorContext context) {
        PlanFragment currentFragment = assertNumRows.child().accept(this, context);
        List<List<Expr>> distributeExprLists = getDistributeExprs(assertNumRows.child());

        // we need convert all columns to nullable in AssertNumRows node
        // create a tuple for AssertNumRowsNode
        TupleDescriptor tupleDescriptor = context.generateTupleDesc();
        // create assertNode
        AssertNumRowsNode assertNumRowsNode = new AssertNumRowsNode(context.nextPlanNodeId(),
                currentFragment.getPlanRoot(),
                ExpressionTranslator.translateAssert(assertNumRows.getAssertNumRowsElement()), true, tupleDescriptor);
        assertNumRowsNode.setChildrenDistributeExprLists(distributeExprLists);
        assertNumRowsNode.setNereidsId(assertNumRows.getId());
        context.getNereidsIdToPlanNodeIdMap().put(assertNumRows.getId(), assertNumRowsNode.getId());

        // collect all child output slots
        List<TupleDescriptor> childTuples = context.getTupleDesc(currentFragment.getPlanRoot());
        List<SlotDescriptor> childSlotDescriptors = childTuples.stream()
                .map(TupleDescriptor::getSlots)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        // create output slot based on child output
        Map<ExprId, SlotReference> childOutputMap = Maps.newHashMap();
        assertNumRows.child().getOutput().stream()
                .map(SlotReference.class::cast)
                .forEach(s -> childOutputMap.put(s.getExprId(), s));
        List<SlotDescriptor> slotDescriptors = Lists.newArrayList();
        for (SlotDescriptor slot : childSlotDescriptors) {
            SlotReference sf = childOutputMap.get(context.findExprId(slot.getId()));
            SlotDescriptor sd = context.createSlotDesc(tupleDescriptor, sf, slot.getParent().getTable());
            slotDescriptors.add(sd);
        }

        // set all output slot nullable
        slotDescriptors.forEach(sd -> sd.setIsNullable(true));

        addPlanRoot(currentFragment, assertNumRowsNode, assertNumRows);
        return currentFragment;
    }

    /**
     * NOTICE: Must translate left, which it's the producer of consumer.
     */
    @Override
    public PlanFragment visitPhysicalCTEAnchor(PhysicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
            PlanTranslatorContext context) {
        cteAnchor.child(0).accept(this, context);
        return cteAnchor.child(1).accept(this, context);
    }

    @Override
        public PlanFragment visitPhysicalCTEConsumer(PhysicalCTEConsumer cteConsumer,
            PlanTranslatorContext context) {
        CTEId cteId = cteConsumer.getCteId();

        MultiCastPlanFragment multiCastFragment = (MultiCastPlanFragment) context.getCteProduceFragments().get(cteId);
        Preconditions.checkState(multiCastFragment.getSink() instanceof MultiCastDataSink,
                "invalid multiCastFragment");

        MultiCastDataSink multiCastDataSink = (MultiCastDataSink) multiCastFragment.getSink();
        Preconditions.checkState(multiCastDataSink != null, "invalid multiCastDataSink");

        PhysicalCTEProducer<?> cteProducer = context.getCteProduceMap().get(cteId);
        Preconditions.checkState(cteProducer != null, "invalid cteProducer");

        context.getCteConsumerMap().put(cteId, cteConsumer);
        // set datasink to multicast data sink but do not set target now
        // target will be set when translate distribute
        DataStreamSink streamSink = new DataStreamSink();
        streamSink.setFragment(multiCastFragment);
        multiCastDataSink.getDataStreamSinks().add(streamSink);
        multiCastDataSink.getDestinations().add(Lists.newArrayList());

        // update expr to slot mapping
        TupleDescriptor tupleDescriptor = null;
        for (Slot producerSlot : cteProducer.getOutput()) {
            SlotRef slotRef = context.findSlotRef(producerSlot.getExprId());
            tupleDescriptor = slotRef.getDesc().getParent();
            for (Slot consumerSlot : cteConsumer.getProducerToConsumerSlotMap().get(producerSlot)) {
                context.addExprIdSlotRefPair(consumerSlot.getExprId(), slotRef);
            }
        }
        CTEScanNode cteScanNode = new CTEScanNode(tupleDescriptor);
        context.getRuntimeTranslator().ifPresent(runtimeFilterTranslator ->
                    runtimeFilterTranslator.getContext().getTargetListByScan(cteConsumer).forEach(
                            expr -> runtimeFilterTranslator.translateRuntimeFilterTarget(expr, cteScanNode, context)));
        // translate rf v2 target
        List<RuntimeFilterV2> rfV2s = context.getRuntimeFilterV2Context()
                .getRuntimeFilterV2ByTargetPlan(cteConsumer);
        for (RuntimeFilterV2 rfV2 : rfV2s) {
            Expr targetExpr = rfV2.getTargetExpression().accept(ExpressionTranslator.INSTANCE, context);
            rfV2.setLegacyTargetNode(cteScanNode);
            rfV2.setLegacyTargetExpr(targetExpr);
        }
        context.getCteScanNodeMap().put(multiCastFragment.getFragmentId(), cteScanNode);

        return multiCastFragment;
    }

    @Override
    public PlanFragment visitPhysicalCTEProducer(PhysicalCTEProducer<? extends Plan> cteProducer,
            PlanTranslatorContext context) {
        PlanFragment child = cteProducer.child().accept(this, context);
        CTEId cteId = cteProducer.getCteId();
        context.getPlanFragments().remove(child);

        MultiCastPlanFragment multiCastPlanFragment = new MultiCastPlanFragment(child);
        MultiCastDataSink multiCastDataSink = new MultiCastDataSink();
        multiCastPlanFragment.setSink(multiCastDataSink);

        List<Expr> outputs = cteProducer.getOutput().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());

        multiCastPlanFragment.setOutputExprs(outputs);
        context.getCteProduceFragments().put(cteId, multiCastPlanFragment);
        context.getCteProduceMap().put(cteId, cteProducer);
        context.getPlanFragments().add(multiCastPlanFragment);
        return child;
    }

    @Override
    public PlanFragment visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, PlanTranslatorContext context) {
        if (filter.child(0) instanceof AbstractPhysicalJoin) {
            AbstractPhysicalJoin<?, ?> join = (AbstractPhysicalJoin<?, ?>) filter.child();
            join.addFilterConjuncts(filter.getConjuncts());
        }
        PlanFragment inputFragment = filter.child(0).accept(this, context);

        // process multicast sink
        if (inputFragment instanceof MultiCastPlanFragment) {
            MultiCastDataSink multiCastDataSink = (MultiCastDataSink) inputFragment.getSink();
            DataStreamSink dataStreamSink = multiCastDataSink.getDataStreamSinks().get(
                    multiCastDataSink.getDataStreamSinks().size() - 1);
            if (CollectionUtils.isNotEmpty(dataStreamSink.getConjuncts())
                    || CollectionUtils.isNotEmpty(dataStreamSink.getProjections())) {
                String errMsg = "generate invalid plan \n" + filter.treeString();
                LOG.warn(errMsg);
                throw new AnalysisException(errMsg);
            }
            filter.getConjuncts().stream()
                    .map(e -> ExpressionTranslator.translate(e, context))
                    .forEach(dataStreamSink::addConjunct);
            return inputFragment;
        }

        PlanNode planNode = inputFragment.getPlanRoot();
        // the three nodes don't support conjuncts, need create a SelectNode to filter data
        if (planNode instanceof ExchangeNode || planNode instanceof SortNode || planNode instanceof UnionNode) {
            SelectNode selectNode = new SelectNode(context.nextPlanNodeId(), planNode);
            selectNode.setNereidsId(filter.getId());
            context.getNereidsIdToPlanNodeIdMap().put(filter.getId(), selectNode.getId());
            addConjunctsToPlanNode(filter, selectNode, context);
            addPlanRoot(inputFragment, selectNode, filter);
        } else {
            if (!(filter.child(0) instanceof AbstractPhysicalJoin)) {
                // already have filter on this node, we should not override it, so need a new node
                if (!planNode.getConjuncts().isEmpty()
                        // already have project on this node, filter need execute after project, so need a new node
                        || CollectionUtils.isNotEmpty(planNode.getProjectList())
                        // already have limit on this node, filter need execute after limit, so need a new node
                        || planNode.hasLimit()) {
                    planNode = new SelectNode(context.nextPlanNodeId(), planNode);
                    planNode.setNereidsId(filter.getId());
                    // NOTE: can't collect planNode.getId() on filter's child, such as scan node
                    // since if the filter is embedded into scan, the id mapping relation is not correct
                    // i.e, the physical filter's nereids's id will be mapped to final plan's scan node
                    context.getNereidsIdToPlanNodeIdMap().put(filter.getId(), planNode.getId());
                    addPlanRoot(inputFragment, planNode, filter);
                }
                addConjunctsToPlanNode(filter, planNode, context);
            }
        }
        updateLegacyPlanIdToPhysicalPlan(inputFragment.getPlanRoot(), filter);
        // in ut, filter.stats may be null
        if (filter.getStats() != null) {
            inputFragment.getPlanRoot().setCardinalityAfterFilter((long) filter.getStats().getRowCount());
        }
        return inputFragment;
    }

    @Override
    public PlanFragment visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate,
            PlanTranslatorContext context) {
        PlanFragment currentFragment = generate.child().accept(this, context);
        ArrayList<Expr> functionCalls = generate.getGenerators().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toCollection(ArrayList::new));
        TupleDescriptor tupleDescriptor = generateTupleDesc(generate.getGeneratorOutput(), null, context);
        List<TupleId> childOutputTupleIds = currentFragment.getPlanRoot().getOutputTupleIds();
        if (childOutputTupleIds == null || childOutputTupleIds.isEmpty()) {
            childOutputTupleIds = currentFragment.getPlanRoot().getTupleIds();
        }
        List<SlotId> outputSlotIds = Stream.concat(childOutputTupleIds.stream(),
                        Stream.of(tupleDescriptor.getId()))
                .map(id -> context.getTupleDesc(id).getSlots())
                .flatMap(List::stream)
                .map(SlotDescriptor::getId)
                .collect(Collectors.toList());
        TableFunctionNode tableFunctionNode = new TableFunctionNode(context.nextPlanNodeId(),
                currentFragment.getPlanRoot(), tupleDescriptor.getId(), functionCalls, outputSlotIds);
        tableFunctionNode.setNereidsId(generate.getId());
        context.getNereidsIdToPlanNodeIdMap().put(generate.getId(), tableFunctionNode.getId());
        addPlanRoot(currentFragment, tableFunctionNode, generate);
        return currentFragment;
    }

    /**
     * the contract of hash join node with BE
     * 1. hash join contains 3 types of predicates:
     *   a. equal join conjuncts
     *   b. other join conjuncts
     *   c. other predicates (denoted by filter conjuncts in the rest of comments)
     * <p>
     * 2. hash join contains 3 tuple descriptors
     *   a. input tuple descriptors, corresponding to the left child output and right child output.
     *      If its column is selected, it will be displayed in explain by `tuple ids`.
     *      for example, select L.* from L join R on ..., because no column from R are selected, tuple ids only
     *      contains output tuple of L.
     *      equal join conjuncts is bound on input tuple descriptors.
     * <p>
     *   b.intermediate tuple.
     *      This tuple describes schema of the output block after evaluating equal join conjuncts
     *      and other join conjuncts.
     * <p>
     *      Other join conjuncts currently is bound on intermediate tuple. There are some historical reason, and it
     *      should be bound on input tuple in the future.
     * <p>
     *      filter conjuncts will be evaluated on the intermediate tuple. That means the input block of filter is
     *      described by intermediate tuple, and hence filter conjuncts should be bound on intermediate tuple.
     * <p>
     *      In order to be compatible with old version, intermediate tuple is not pruned. For example, intermediate
     *      tuple contains all slots from both sides of children. After probing hash-table, BE does not need to
     *      materialize all slots in intermediate tuple. The slots in HashJoinNode.hashOutputSlotIds will be
     *      materialized by BE. If `hashOutputSlotIds` is empty, all slots will be materialized.
     * <p>
     *      In case of outer join, the slots in intermediate should be set nullable.
     *      For example,
     *      select L.*, R.* from L left outer join R on ...
     *      All slots from R in intermediate tuple should be nullable.
     * <p>
     *   c. output tuple
     *      This describes the schema of hash join output block.
     * 3. Intermediate tuple
     *      for BE performance reason, the slots in intermediate tuple
     *      depends on the join type and other join conjuncts.
     *      In general, intermediate tuple contains all slots of both children, except one case.
     *      For left-semi/left-ant (right-semi/right-semi) join without other join conjuncts, intermediate tuple
     *      only contains left (right) children output slots.
     *
     */
    @Override
    public PlanFragment visitPhysicalHashJoin(
            PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin,
            PlanTranslatorContext context) {
        Preconditions.checkArgument(hashJoin.left() instanceof PhysicalPlan,
                "HashJoin's left child should be PhysicalPlan");
        Preconditions.checkArgument(hashJoin.right() instanceof PhysicalPlan,
                "HashJoin's left child should be PhysicalPlan");
        PhysicalHashJoin<PhysicalPlan, PhysicalPlan> physicalHashJoin
                = (PhysicalHashJoin<PhysicalPlan, PhysicalPlan>) hashJoin;
        // NOTICE: We must visit from right to left, to ensure the last fragment is root fragment
        PlanFragment rightFragment = hashJoin.child(1).accept(this, context);
        PlanFragment leftFragment = hashJoin.child(0).accept(this, context);
        List<List<Expr>> distributeExprLists = getDistributeExprs(physicalHashJoin.left(), physicalHashJoin.right());

        if (JoinUtils.shouldNestedLoopJoin(hashJoin)) {
            throw new RuntimeException("Physical hash join could not execute without equal join condition.");
        }

        PlanNode leftPlanRoot = leftFragment.getPlanRoot();
        PlanNode rightPlanRoot = rightFragment.getPlanRoot();
        JoinType joinType = hashJoin.getJoinType();

        List<Expr> execEqConjuncts = hashJoin.getHashJoinConjuncts().stream()
                .map(EqualPredicate.class::cast)
                .map(e -> JoinUtils.swapEqualToForChildrenOrder(e, hashJoin.left().getOutputSet()))
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());
        List<Expr> markConjuncts = ImmutableList.of();
        boolean isHashJoinConjunctsEmpty = hashJoin.getHashJoinConjuncts().isEmpty();
        boolean isMarkJoinConjunctsEmpty = hashJoin.getMarkJoinConjuncts().isEmpty();
        JoinOperator joinOperator = JoinType.toJoinOperator(joinType);
        if (isHashJoinConjunctsEmpty) {
            // if hash join conjuncts is empty, means mark join conjuncts must be EqualPredicate
            // BE should use mark join conjuncts to build hash table
            Preconditions.checkState(!isMarkJoinConjunctsEmpty, "mark join conjuncts should not be empty.");
            markConjuncts = hashJoin.getMarkJoinConjuncts().stream()
                    .map(EqualPredicate.class::cast)
                    .map(e -> JoinUtils.swapEqualToForChildrenOrder(e, hashJoin.left().getOutputSet()))
                    .map(e -> ExpressionTranslator.translate(e, context))
                    .collect(Collectors.toList());
            // in order to process semi/anti join with no hash conjunct and having mark conjunct effeciently
            // we can use mark conjunct as hash conjunct with slight different behavior if meets null values
            // so we use null aware semi/anti join to indicate it's a null aware hash conjunct
            // it's unnecessary to introduce new join type like NULL_AWARE_LEFT_SEMI_JOIN in nereids
            // so we translate the join type here to let be known if the hash conjunct is null aware
            if (joinOperator == JoinOperator.LEFT_ANTI_JOIN) {
                joinOperator = JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
            } else if (joinOperator == JoinOperator.LEFT_SEMI_JOIN) {
                joinOperator = JoinOperator.NULL_AWARE_LEFT_SEMI_JOIN;
            }
        }

        HashJoinNode hashJoinNode = new HashJoinNode(context.nextPlanNodeId(), leftPlanRoot,
                rightPlanRoot, joinOperator, execEqConjuncts, Lists.newArrayList(), markConjuncts,
                null, null, null, hashJoin.isMarkJoin());
        hashJoinNode.setNereidsId(hashJoin.getId());
        context.getNereidsIdToPlanNodeIdMap().put(hashJoin.getId(), hashJoinNode.getId());
        hashJoinNode.setChildrenDistributeExprLists(distributeExprLists);
        PlanFragment currentFragment = connectJoinNode(hashJoinNode, leftFragment, rightFragment, context, hashJoin);

        if (JoinUtils.shouldColocateJoin(physicalHashJoin)) {
            // TODO: add reason
            hashJoinNode.setColocate(true, "");
            leftFragment.setHasColocatePlanNode(true);
        } else if (JoinUtils.shouldBroadcastJoin(physicalHashJoin)) {
            Preconditions.checkState(rightPlanRoot instanceof ExchangeNode,
                    "right child of broadcast join must be ExchangeNode but it is " + rightFragment.getPlanRoot());
            Preconditions.checkState(rightFragment.getChildren().size() == 1,
                    "right child of broadcast join must have 1 child, but meet " + rightFragment.getChildren().size());
            ((ExchangeNode) rightPlanRoot).setRightChildOfBroadcastHashJoin(true);
            hashJoinNode.setDistributionMode(DistributionMode.BROADCAST);
        } else if (JoinUtils.shouldBucketShuffleJoin(physicalHashJoin)) {
            hashJoinNode.setDistributionMode(DistributionMode.BUCKET_SHUFFLE);
        } else {
            hashJoinNode.setDistributionMode(DistributionMode.PARTITIONED);
        }

        // Nereids does not care about output order of join,
        // but BE need left child's output must be before right child's output.
        // So we need to swap the output order of left and right child if necessary.
        // TODO: revert this after Nereids could ensure the output order is correct.
        List<TupleDescriptor> leftTuples = context.getTupleDesc(leftPlanRoot);
        List<SlotDescriptor> leftSlotDescriptors = leftTuples.stream()
                .map(TupleDescriptor::getSlots)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        List<TupleDescriptor> rightTuples = context.getTupleDesc(rightPlanRoot);
        List<SlotDescriptor> rightSlotDescriptors = rightTuples.stream()
                .map(TupleDescriptor::getSlots)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        Map<ExprId, SlotReference> outputSlotReferenceMap = hashJoin.getOutput().stream()
                .map(SlotReference.class::cast)
                .collect(Collectors.toMap(Slot::getExprId, s -> s, (existing, replacement) -> existing));
        List<SlotReference> outputSlotReferences = Stream.concat(leftTuples.stream(), rightTuples.stream())
                .map(TupleDescriptor::getSlots)
                .flatMap(Collection::stream)
                .map(sd -> context.findExprId(sd.getId()))
                .map(outputSlotReferenceMap::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        Map<ExprId, SlotReference> hashOutputSlotReferenceMap = Maps.newHashMap(outputSlotReferenceMap);

        hashJoin.getOtherJoinConjuncts()
                .stream()
                .flatMap(e -> e.getInputSlots().stream())
                .map(SlotReference.class::cast)
                .forEach(s -> hashOutputSlotReferenceMap.put(s.getExprId(), s));
        if (!isHashJoinConjunctsEmpty && !isMarkJoinConjunctsEmpty) {
            // if hash join conjuncts is NOT empty, mark join conjuncts would be processed like other conjuncts
            // BE should deal with mark join conjuncts differently, its result is 3 value bool(true, false, null)
            hashJoin.getMarkJoinConjuncts()
                    .stream()
                    .flatMap(e -> e.getInputSlots().stream())
                    .map(SlotReference.class::cast)
                    .forEach(s -> hashOutputSlotReferenceMap.put(s.getExprId(), s));
        }
        hashJoin.getFilterConjuncts().stream()
                .flatMap(e -> e.getInputSlots().stream())
                .map(SlotReference.class::cast)
                .forEach(s -> hashOutputSlotReferenceMap.put(s.getExprId(), s));

        Map<ExprId, SlotReference> leftChildOutputMap = hashJoin.left().getOutput().stream()
                .map(SlotReference.class::cast)
                .collect(Collectors.toMap(Slot::getExprId, s -> s, (existing, replacement) -> existing));
        Map<ExprId, SlotReference> rightChildOutputMap = hashJoin.right().getOutput().stream()
                .map(SlotReference.class::cast)
                .collect(Collectors.toMap(Slot::getExprId, s -> s, (existing, replacement) -> existing));

        // translate runtime filter
        context.getRuntimeTranslator().ifPresent(runtimeFilterTranslator -> physicalHashJoin.getRuntimeFilters()
                .forEach(filter -> runtimeFilterTranslator.createLegacyRuntimeFilter(filter, hashJoinNode, context)));

        // make intermediate tuple
        List<SlotDescriptor> leftIntermediateSlotDescriptor = Lists.newArrayList();
        List<SlotDescriptor> rightIntermediateSlotDescriptor = Lists.newArrayList();
        TupleDescriptor intermediateDescriptor = context.generateTupleDesc();

        if (hashJoin.getOtherJoinConjuncts().isEmpty() && (isHashJoinConjunctsEmpty != isMarkJoinConjunctsEmpty)
                && (joinType == JoinType.LEFT_ANTI_JOIN
                || joinType == JoinType.LEFT_SEMI_JOIN
                || joinType == JoinType.NULL_AWARE_LEFT_ANTI_JOIN)) {
            for (SlotDescriptor leftSlotDescriptor : leftSlotDescriptors) {
                if (!leftSlotDescriptor.isMaterialized()) {
                    continue;
                }
                SlotReference sf = leftChildOutputMap.get(context.findExprId(leftSlotDescriptor.getId()));
                SlotDescriptor sd;
                if (sf == null && leftSlotDescriptor.getColumn().getName().equals(Column.ROWID_COL)) {
                    // TODO: temporary code for two phase read, should remove it after refactor
                    sd = context.getDescTable().copySlotDescriptor(intermediateDescriptor, leftSlotDescriptor);
                } else {
                    sd = context.createSlotDesc(intermediateDescriptor, sf, leftSlotDescriptor.getParent().getTable());
                    if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                        hashJoinNode.addSlotIdToHashOutputSlotIds(leftSlotDescriptor.getId());
                        hashJoinNode.getHashOutputExprSlotIdMap().put(sf.getExprId(), leftSlotDescriptor.getId());
                    }
                }
                leftIntermediateSlotDescriptor.add(sd);
            }
        } else if (hashJoin.getOtherJoinConjuncts().isEmpty() && (isHashJoinConjunctsEmpty != isMarkJoinConjunctsEmpty)
                && (joinType == JoinType.RIGHT_ANTI_JOIN || joinType == JoinType.RIGHT_SEMI_JOIN)) {
            for (SlotDescriptor rightSlotDescriptor : rightSlotDescriptors) {
                if (!rightSlotDescriptor.isMaterialized()) {
                    continue;
                }
                SlotReference sf = rightChildOutputMap.get(context.findExprId(rightSlotDescriptor.getId()));
                SlotDescriptor sd;
                if (sf == null && rightSlotDescriptor.getColumn().getName().equals(Column.ROWID_COL)) {
                    // TODO: temporary code for two phase read, should remove it after refactor
                    sd = context.getDescTable().copySlotDescriptor(intermediateDescriptor, rightSlotDescriptor);
                } else {
                    sd = context.createSlotDesc(intermediateDescriptor, sf, rightSlotDescriptor.getParent().getTable());
                    if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                        hashJoinNode.addSlotIdToHashOutputSlotIds(rightSlotDescriptor.getId());
                        hashJoinNode.getHashOutputExprSlotIdMap().put(sf.getExprId(), rightSlotDescriptor.getId());
                    }
                }
                rightIntermediateSlotDescriptor.add(sd);
            }
        } else {
            for (SlotDescriptor leftSlotDescriptor : leftSlotDescriptors) {
                if (!leftSlotDescriptor.isMaterialized()) {
                    continue;
                }
                SlotReference sf = leftChildOutputMap.get(context.findExprId(leftSlotDescriptor.getId()));
                SlotDescriptor sd;
                if (sf == null && leftSlotDescriptor.getColumn().getName().equals(Column.ROWID_COL)) {
                    // TODO: temporary code for two phase read, should remove it after refactor
                    sd = context.getDescTable().copySlotDescriptor(intermediateDescriptor, leftSlotDescriptor);
                } else {
                    sd = context.createSlotDesc(intermediateDescriptor, sf, leftSlotDescriptor.getParent().getTable());
                    // sd = context.createSlotDesc(intermediateDescriptor, sf);
                    if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                        hashJoinNode.addSlotIdToHashOutputSlotIds(leftSlotDescriptor.getId());
                        hashJoinNode.getHashOutputExprSlotIdMap().put(sf.getExprId(), leftSlotDescriptor.getId());
                    }
                }
                leftIntermediateSlotDescriptor.add(sd);
            }
            for (SlotDescriptor rightSlotDescriptor : rightSlotDescriptors) {
                if (!rightSlotDescriptor.isMaterialized()) {
                    continue;
                }
                SlotReference sf = rightChildOutputMap.get(context.findExprId(rightSlotDescriptor.getId()));
                SlotDescriptor sd;
                if (sf == null && rightSlotDescriptor.getColumn().getName().equals(Column.ROWID_COL)) {
                    // TODO: temporary code for two phase read, should remove it after refactor
                    sd = context.getDescTable().copySlotDescriptor(intermediateDescriptor, rightSlotDescriptor);
                } else {
                    sd = context.createSlotDesc(intermediateDescriptor, sf, rightSlotDescriptor.getParent().getTable());
                    if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                        hashJoinNode.addSlotIdToHashOutputSlotIds(rightSlotDescriptor.getId());
                        hashJoinNode.getHashOutputExprSlotIdMap().put(sf.getExprId(), rightSlotDescriptor.getId());
                    }
                }
                rightIntermediateSlotDescriptor.add(sd);
            }
        }

        if (hashJoin.getMarkJoinSlotReference().isPresent()) {
            SlotReference sf = hashJoin.getMarkJoinSlotReference().get();
            outputSlotReferences.add(sf);
            context.createSlotDesc(intermediateDescriptor, sf);
            if (hashOutputSlotReferenceMap.get(sf.getExprId()) != null) {
                SlotRef markJoinSlotId = context.findSlotRef(sf.getExprId());
                Preconditions.checkState(markJoinSlotId != null);
                hashJoinNode.addSlotIdToHashOutputSlotIds(markJoinSlotId.getSlotId());
                hashJoinNode.getHashOutputExprSlotIdMap().put(sf.getExprId(), markJoinSlotId.getSlotId());
            }
        }

        // set slots as nullable for outer join
        if (joinType == JoinType.LEFT_OUTER_JOIN || joinType == JoinType.FULL_OUTER_JOIN) {
            rightIntermediateSlotDescriptor.forEach(sd -> sd.setIsNullable(true));
        }
        if (joinType == JoinType.RIGHT_OUTER_JOIN || joinType == JoinType.FULL_OUTER_JOIN) {
            leftIntermediateSlotDescriptor.forEach(sd -> sd.setIsNullable(true));
        }

        // Constant expr will cause be crash.
        // But EliminateJoinCondition and Expression Rewrite already eliminate true literal.
        List<Expr> otherJoinConjuncts = hashJoin.getOtherJoinConjuncts()
                .stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());

        hashJoin.getFilterConjuncts().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .forEach(hashJoinNode::addConjunct);

        hashJoinNode.setOtherJoinConjuncts(otherJoinConjuncts);

        if (!isHashJoinConjunctsEmpty && !isMarkJoinConjunctsEmpty) {
            // add mark join conjuncts to hash join node
            List<Expr> markJoinConjuncts = hashJoin.getMarkJoinConjuncts()
                    .stream()
                    .map(e -> ExpressionTranslator.translate(e, context))
                    .collect(Collectors.toList());
            hashJoinNode.setMarkJoinConjuncts(markJoinConjuncts);
        }

        hashJoinNode.setvIntermediateTupleDescList(Lists.newArrayList(intermediateDescriptor));

        if (hashJoin.isShouldTranslateOutput()) {
            // translate output expr on intermediate tuple
            List<Expr> srcToOutput = outputSlotReferences.stream()
                    .map(e -> ExpressionTranslator.translate(e, context))
                    .collect(Collectors.toList());

            TupleDescriptor outputDescriptor = context.generateTupleDesc();
            outputSlotReferences.forEach(s -> context.createSlotDesc(outputDescriptor, s));

            hashJoinNode.setOutputTupleDesc(outputDescriptor);
            hashJoinNode.setProjectList(srcToOutput);
        }
        if (hashJoin.getStats() != null) {
            hashJoinNode.setCardinality((long) hashJoin.getStats().getRowCount());
        }
        updateLegacyPlanIdToPhysicalPlan(currentFragment.getPlanRoot(), hashJoin);
        return currentFragment;
    }

    @Override
    public PlanFragment visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin,
            PlanTranslatorContext context) {
        // NOTICE: We must visit from right to left, to ensure the last fragment is root fragment
        // TODO: we should add a helper method to wrap this logic.
        //   Maybe something like private List<PlanFragment> postOrderVisitChildren(
        //       PhysicalPlan plan, PlanVisitor visitor, Context context).
        PlanFragment rightFragment = nestedLoopJoin.child(1).accept(this, context);
        PlanFragment leftFragment = nestedLoopJoin.child(0).accept(this, context);
        List<List<Expr>> distributeExprLists = getDistributeExprs(nestedLoopJoin.child(0), nestedLoopJoin.child(1));
        PlanNode leftFragmentPlanRoot = leftFragment.getPlanRoot();
        PlanNode rightFragmentPlanRoot = rightFragment.getPlanRoot();

        if (!JoinUtils.shouldNestedLoopJoin(nestedLoopJoin)) {
            throw new RuntimeException("Physical nested loop join could not execute with equal join condition.");
        }

        List<TupleDescriptor> leftTuples = context.getTupleDesc(leftFragmentPlanRoot);
        List<TupleDescriptor> rightTuples = context.getTupleDesc(rightFragmentPlanRoot);
        List<TupleId> tupleIds = Stream.concat(leftTuples.stream(), rightTuples.stream())
                .map(TupleDescriptor::getId)
                .collect(Collectors.toList());

        JoinType joinType = nestedLoopJoin.getJoinType();

        NestedLoopJoinNode nestedLoopJoinNode = new NestedLoopJoinNode(context.nextPlanNodeId(),
                leftFragmentPlanRoot, rightFragmentPlanRoot, tupleIds, JoinType.toJoinOperator(joinType),
                null, null, null, nestedLoopJoin.isMarkJoin());
        nestedLoopJoinNode.setNereidsId(nestedLoopJoin.getId());
        context.getNereidsIdToPlanNodeIdMap().put(nestedLoopJoin.getId(), nestedLoopJoinNode.getId());
        nestedLoopJoinNode.setChildrenDistributeExprLists(distributeExprLists);
        if (nestedLoopJoin.getStats() != null) {
            nestedLoopJoinNode.setCardinality((long) nestedLoopJoin.getStats().getRowCount());
        }
        nestedLoopJoinNode.setChild(0, leftFragment.getPlanRoot());
        nestedLoopJoinNode.setChild(1, rightFragment.getPlanRoot());
        setPlanRoot(leftFragment, nestedLoopJoinNode, nestedLoopJoin);
        // TODO: what's this? do we really need to set this?
        rightFragment.getPlanRoot().setCompactData(false);
        context.mergePlanFragment(rightFragment, leftFragment);
        for (PlanFragment rightChild : rightFragment.getChildren()) {
            leftFragment.addChild(rightChild);
        }
        // translate runtime filter
        context.getRuntimeTranslator().ifPresent(runtimeFilterTranslator -> {
            List<RuntimeFilter> filters = nestedLoopJoin.getRuntimeFilters();
            filters.forEach(filter -> runtimeFilterTranslator
                    .createLegacyRuntimeFilter(filter, nestedLoopJoinNode, context));
            if (filters.stream().anyMatch(filter -> filter.getType() == TRuntimeFilterType.BITMAP)) {
                nestedLoopJoinNode.setOutputLeftSideOnly(true);
            }
        });

        Map<ExprId, SlotReference> leftChildOutputMap = nestedLoopJoin.child(0).getOutput().stream()
                .map(SlotReference.class::cast)
                .collect(Collectors.toMap(Slot::getExprId, s -> s, (existing, replacement) -> existing));
        Map<ExprId, SlotReference> rightChildOutputMap = nestedLoopJoin.child(1).getOutput().stream()
                .map(SlotReference.class::cast)
                .collect(Collectors.toMap(Slot::getExprId, s -> s, (existing, replacement) -> existing));
        // make intermediate tuple
        List<SlotDescriptor> leftIntermediateSlotDescriptor = Lists.newArrayList();
        List<SlotDescriptor> rightIntermediateSlotDescriptor = Lists.newArrayList();
        TupleDescriptor intermediateDescriptor = context.generateTupleDesc();

        // Nereids does not care about output order of join,
        // but BE need left child's output must be before right child's output.
        // So we need to swap the output order of left and right child if necessary.
        // TODO: revert this after Nereids could ensure the output order is correct.
        List<SlotDescriptor> leftSlotDescriptors = leftTuples.stream()
                .map(TupleDescriptor::getSlots)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        List<SlotDescriptor> rightSlotDescriptors = rightTuples.stream()
                .map(TupleDescriptor::getSlots)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        Map<ExprId, SlotReference> outputSlotReferenceMap = Maps.newHashMap();

        nestedLoopJoin.getOutput().stream()
                .map(SlotReference.class::cast)
                .forEach(s -> outputSlotReferenceMap.put(s.getExprId(), s));
        nestedLoopJoin.getFilterConjuncts().stream()
                .flatMap(e -> e.getInputSlots().stream())
                .map(SlotReference.class::cast)
                .forEach(s -> outputSlotReferenceMap.put(s.getExprId(), s));
        List<SlotReference> outputSlotReferences = Stream.concat(leftTuples.stream(), rightTuples.stream())
                .map(TupleDescriptor::getSlots)
                .flatMap(Collection::stream)
                .map(sd -> context.findExprId(sd.getId()))
                .map(outputSlotReferenceMap::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        // TODO: because of the limitation of be, the VNestedLoopJoinNode will output column from both children
        // in the intermediate tuple, so fe have to do the same, if be fix the problem, we can change it back.
        for (SlotDescriptor leftSlotDescriptor : leftSlotDescriptors) {
            if (!leftSlotDescriptor.isMaterialized()) {
                continue;
            }
            SlotReference sf = leftChildOutputMap.get(context.findExprId(leftSlotDescriptor.getId()));
            SlotDescriptor sd;
            if (sf == null && leftSlotDescriptor.getColumn().getName().equals(Column.ROWID_COL)) {
                // TODO: temporary code for two phase read, should remove it after refactor
                sd = context.getDescTable().copySlotDescriptor(intermediateDescriptor, leftSlotDescriptor);
            } else {
                sd = context.createSlotDesc(intermediateDescriptor, sf, leftSlotDescriptor.getParent().getTable());
            }
            leftIntermediateSlotDescriptor.add(sd);
        }
        for (SlotDescriptor rightSlotDescriptor : rightSlotDescriptors) {
            if (!rightSlotDescriptor.isMaterialized()) {
                continue;
            }
            SlotReference sf = rightChildOutputMap.get(context.findExprId(rightSlotDescriptor.getId()));
            SlotDescriptor sd;
            if (sf == null && rightSlotDescriptor.getColumn().getName().equals(Column.ROWID_COL)) {
                // TODO: temporary code for two phase read, should remove it after refactor
                sd = context.getDescTable().copySlotDescriptor(intermediateDescriptor, rightSlotDescriptor);
            } else {
                sd = context.createSlotDesc(intermediateDescriptor, sf, rightSlotDescriptor.getParent().getTable());
            }
            rightIntermediateSlotDescriptor.add(sd);
        }

        if (nestedLoopJoin.getMarkJoinSlotReference().isPresent()) {
            outputSlotReferences.add(nestedLoopJoin.getMarkJoinSlotReference().get());
            context.createSlotDesc(intermediateDescriptor, nestedLoopJoin.getMarkJoinSlotReference().get());
        }

        // set slots as nullable for outer join
        if (joinType == JoinType.LEFT_OUTER_JOIN || joinType == JoinType.FULL_OUTER_JOIN) {
            rightIntermediateSlotDescriptor.forEach(sd -> sd.setIsNullable(true));
        }
        if (joinType == JoinType.RIGHT_OUTER_JOIN || joinType == JoinType.FULL_OUTER_JOIN) {
            leftIntermediateSlotDescriptor.forEach(sd -> sd.setIsNullable(true));
        }

        nestedLoopJoinNode.setvIntermediateTupleDescList(Lists.newArrayList(intermediateDescriptor));

        List<Expr> joinConjuncts = nestedLoopJoin.getOtherJoinConjuncts().stream()
                .filter(e -> !nestedLoopJoin.isBitmapRuntimeFilterCondition(e))
                .map(e -> ExpressionTranslator.translate(e, context)).collect(Collectors.toList());

        if (!nestedLoopJoin.isBitMapRuntimeFilterConditionsEmpty() && joinConjuncts.isEmpty()) {
            // left semi join need at least one conjunct. otherwise left-semi-join fallback to cross-join
            joinConjuncts.add(new BoolLiteral(true));
        }

        nestedLoopJoinNode.setJoinConjuncts(joinConjuncts);

        if (!nestedLoopJoin.getOtherJoinConjuncts().isEmpty()) {
            List<Expr> markJoinConjuncts = nestedLoopJoin.getMarkJoinConjuncts().stream()
                    .map(e -> ExpressionTranslator.translate(e, context)).collect(Collectors.toList());
            nestedLoopJoinNode.setMarkJoinConjuncts(markJoinConjuncts);
        }

        nestedLoopJoin.getFilterConjuncts().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .forEach(nestedLoopJoinNode::addConjunct);

        if (nestedLoopJoin.isShouldTranslateOutput()) {
            // translate output expr on intermediate tuple
            List<Expr> srcToOutput = outputSlotReferences.stream()
                    .map(e -> ExpressionTranslator.translate(e, context))
                    .collect(Collectors.toList());

            TupleDescriptor outputDescriptor = context.generateTupleDesc();
            outputSlotReferences.forEach(s -> context.createSlotDesc(outputDescriptor, s));

            nestedLoopJoinNode.setOutputTupleDesc(outputDescriptor);
            nestedLoopJoinNode.setProjectList(srcToOutput);
        }
        if (nestedLoopJoin.getStats() != null) {
            nestedLoopJoinNode.setCardinality((long) nestedLoopJoin.getStats().getRowCount());
        }
        updateLegacyPlanIdToPhysicalPlan(leftFragment.getPlanRoot(), nestedLoopJoin);
        return leftFragment;
    }

    @Override
    public PlanFragment visitPhysicalLimit(PhysicalLimit<? extends Plan> physicalLimit, PlanTranslatorContext context) {
        PlanFragment inputFragment = physicalLimit.child(0).accept(this, context);
        PlanNode child = inputFragment.getPlanRoot();

        if (physicalLimit.getPhase().isLocal()) {
            long newLimit = MergeLimits.mergeLimit(physicalLimit.getLimit(), physicalLimit.getOffset(),
                    child.getLimit());
            child.setLimit(newLimit);
            if (newLimit != -1
                    && child instanceof AggregationNode && physicalLimit.child() instanceof PhysicalHashAggregate) {
                PhysicalHashAggregate<? extends Plan> agg
                        = (PhysicalHashAggregate<? extends Plan>) physicalLimit.child();
                if (agg.isDistinct()) {
                    if (agg.child(0) instanceof PhysicalDistribute
                            && agg.child(0).child(0) instanceof PhysicalHashAggregate
                            && ((Aggregate) agg.child(0).child(0)).isDistinct()
                            && child.getChild(0) instanceof ExchangeNode
                            && child.getChild(0).getChild(0) instanceof AggregationNode) {
                        child.getChild(0).getChild(0).setLimit(newLimit);
                    }
                }
            }
        } else if (physicalLimit.getPhase().isGlobal()) {
            if (!(child instanceof ExchangeNode)) {
                ExchangeNode exchangeNode = new ExchangeNode(context.nextPlanNodeId(), child);
                exchangeNode.setLimit(physicalLimit.getLimit());
                exchangeNode.setOffset(physicalLimit.getOffset());
                exchangeNode.setPartitionType(TPartitionType.UNPARTITIONED);
                exchangeNode.setNumInstances(1);

                PlanFragment fragment = new PlanFragment(context.nextFragmentId(), exchangeNode,
                        DataPartition.UNPARTITIONED);
                inputFragment.setDestination(exchangeNode);
                inputFragment.setOutputPartition(DataPartition.UNPARTITIONED);

                DataStreamSink sink = new DataStreamSink(exchangeNode.getId());
                sink.setOutputPartition(DataPartition.UNPARTITIONED);
                inputFragment.setSink(sink);

                context.addPlanFragment(fragment);
                inputFragment = fragment;
            } else {
                ExchangeNode exchangeNode = (ExchangeNode) child;
                exchangeNode.setLimit(MergeLimits.mergeLimit(physicalLimit.getLimit(), physicalLimit.getOffset(),
                        exchangeNode.getLimit()));
                exchangeNode.setOffset(MergeLimits.mergeOffset(physicalLimit.getOffset(), exchangeNode.getOffset()));
            }
        }

        updateLegacyPlanIdToPhysicalPlan(inputFragment.getPlanRoot(), physicalLimit);
        return inputFragment;
    }

    @Override
    public PlanFragment visitPhysicalPartitionTopN(PhysicalPartitionTopN<? extends Plan> partitionTopN,
            PlanTranslatorContext context) {
        PlanFragment inputFragment = partitionTopN.child(0).accept(this, context);
        List<List<Expr>> distributeExprLists = getDistributeExprs(partitionTopN.child(0));
        PartitionSortNode partitionSortNode = translatePartitionSortNode(
                partitionTopN, inputFragment.getPlanRoot(), context);
        partitionSortNode.setChildrenDistributeExprLists(distributeExprLists);
        addPlanRoot(inputFragment, partitionSortNode, partitionTopN);
        // in pipeline engine, we use parallel scan by default, but it broke the rule of data distribution
        // we need turn of parallel scan to ensure to get correct result.
        if (partitionTopN.getPhase() == PartitionTopnPhase.ONE_PHASE_GLOBAL_PTOPN
                && findOlapScanNodesByPassExchangeAndJoinNode(inputFragment.getPlanRoot())) {
            inputFragment.setHasColocatePlanNode(true);
        }
        return inputFragment;
    }

    // TODO: generate expression mapping when be project could do in ExecNode.
    @Override
    public PlanFragment visitPhysicalProject(PhysicalProject<? extends Plan> project, PlanTranslatorContext context) {
        if (project.child(0) instanceof AbstractPhysicalJoin) {
            ((AbstractPhysicalJoin<?, ?>) project.child(0)).setShouldTranslateOutput(false);
        }
        if (project.child(0) instanceof PhysicalFilter) {
            if (project.child(0).child(0) instanceof AbstractPhysicalJoin) {
                ((AbstractPhysicalJoin<?, ?>) project.child(0).child(0)).setShouldTranslateOutput(false);
            }
        }

        PlanFragment inputFragment = project.child(0).accept(this, context);
        PlanNode inputPlanNode = inputFragment.getPlanRoot();
        // this means already have project on this node, filter need execute after project, so need a new node
        if (CollectionUtils.isNotEmpty(inputPlanNode.getProjectList())) {
            SelectNode selectNode = new SelectNode(context.nextPlanNodeId(), inputPlanNode);
            selectNode.setNereidsId(project.getId());
            context.getNereidsIdToPlanNodeIdMap().put(project.getId(), selectNode.getId());
            addPlanRoot(inputFragment, selectNode, project);
            inputPlanNode = selectNode;
        }

        List<Expr> projectionExprs = null;
        List<Expr> allProjectionExprs = Lists.newArrayList();
        List<Slot> slots = null;
        // TODO FE/BE do not support multi-layer-project on MultiDataSink now.
        if (project.hasMultiLayerProjection()
                && !(inputFragment instanceof MultiCastPlanFragment)
                // TODO support for two phase read with project, remove it after refactor
                && !(project.child() instanceof PhysicalDeferMaterializeTopN)
                && !(project.child() instanceof PhysicalDeferMaterializeOlapScan
                || (project.child() instanceof PhysicalFilter
                && ((PhysicalFilter<?>) project.child()).child() instanceof PhysicalDeferMaterializeOlapScan))) {
            int layerCount = project.getMultiLayerProjects().size();
            for (int i = 0; i < layerCount; i++) {
                List<NamedExpression> layer = project.getMultiLayerProjects().get(i);

                projectionExprs = new ArrayList<>(layer.size());
                slots = new ArrayList<>(layer.size());
                for (int j = 0; j < layer.size(); j++) {
                    NamedExpression layerExpr = layer.get(j);
                    projectionExprs.add(ExpressionTranslator.translate(layerExpr, context));
                    slots.add(layerExpr.toSlot());
                }

                if (i < layerCount - 1) {
                    inputPlanNode.addIntermediateProjectList(projectionExprs);
                    TupleDescriptor projectionTuple = generateTupleDesc(slots, null, context);
                    inputPlanNode.addIntermediateOutputTupleDescList(projectionTuple);
                }
                allProjectionExprs.addAll(projectionExprs);
            }
        } else {
            List<NamedExpression> projects = project.getProjects();
            int projectNum = projects.size();
            projectionExprs = new ArrayList<>(projectNum);
            slots = new ArrayList<>(projectNum);
            for (int j = 0; j < projectNum; j++) {
                NamedExpression layerExpr = projects.get(j);
                projectionExprs.add(ExpressionTranslator.translate(layerExpr, context));
                slots.add(layerExpr.toSlot());
            }
            allProjectionExprs.addAll(projectionExprs);
        }
        // process multicast sink
        if (inputFragment instanceof MultiCastPlanFragment) {
            MultiCastDataSink multiCastDataSink = (MultiCastDataSink) inputFragment.getSink();
            DataStreamSink dataStreamSink = multiCastDataSink.getDataStreamSinks().get(
                    multiCastDataSink.getDataStreamSinks().size() - 1);
            if (CollectionUtils.isNotEmpty(dataStreamSink.getProjections())) {
                String errMsg = "generate invalid plan \n" + project.treeString();
                LOG.warn(errMsg);
                throw new AnalysisException(errMsg);
            }
            TupleDescriptor projectionTuple = generateTupleDesc(slots, null, context);
            dataStreamSink.setProjections(projectionExprs);
            dataStreamSink.setOutputTupleDesc(projectionTuple);
            return inputFragment;
        }

        List<Expr> conjuncts = inputPlanNode.getConjuncts();
        Set<SlotId> requiredSlotIdSet = Sets.newHashSet();
        for (Expr expr : allProjectionExprs) {
            Expr.extractSlots(expr, requiredSlotIdSet);
        }
        Set<SlotId> requiredByProjectSlotIdSet = Sets.newHashSet(requiredSlotIdSet);
        for (Expr expr : conjuncts) {
            Expr.extractSlots(expr, requiredSlotIdSet);
        }
        // For hash join node, use vSrcToOutputSMap to describe the expression calculation, use
        // vIntermediateTupleDescList as input, and set vOutputTupleDesc as the final output.
        // TODO: HashJoinNode's be implementation is not support projection yet, remove this after when supported.
        if (inputPlanNode instanceof JoinNodeBase) {
            TupleDescriptor tupleDescriptor = generateTupleDesc(slots, null, context);
            JoinNodeBase joinNode = (JoinNodeBase) inputPlanNode;
            joinNode.setOutputTupleDesc(tupleDescriptor);
            joinNode.setProjectList(projectionExprs);
            // prune the hashOutputSlotIds
            if (joinNode instanceof HashJoinNode) {
                Set<SlotId> oldHashOutputSlotIds = Sets.newHashSet(((HashJoinNode) joinNode).getHashOutputSlotIds());
                ((HashJoinNode) joinNode).getHashOutputSlotIds().clear();
                Set<ExprId> requiredExprIds = Sets.newHashSet();
                Set<SlotId> requiredOtherConjunctsSlotIdSet = Sets.newHashSet();
                List<Expr> otherConjuncts = ((HashJoinNode) joinNode).getOtherJoinConjuncts();
                for (Expr expr : otherConjuncts) {
                    Expr.extractSlots(expr, requiredOtherConjunctsSlotIdSet);
                }
                if (!((HashJoinNode) joinNode).getEqJoinConjuncts().isEmpty()
                        && !((HashJoinNode) joinNode).getMarkJoinConjuncts().isEmpty()) {
                    List<Expr> markConjuncts = ((HashJoinNode) joinNode).getMarkJoinConjuncts();
                    for (Expr expr : markConjuncts) {
                        Expr.extractSlots(expr, requiredOtherConjunctsSlotIdSet);
                    }
                }
                requiredOtherConjunctsSlotIdSet.forEach(e -> requiredExprIds.add(context.findExprId(e)));
                requiredSlotIdSet.forEach(e -> requiredExprIds.add(context.findExprId(e)));
                for (ExprId exprId : requiredExprIds) {
                    SlotId slotId = ((HashJoinNode) joinNode).getHashOutputExprSlotIdMap().get(exprId);
                    // Preconditions.checkState(slotId != null);
                    if (slotId != null) {
                        ((HashJoinNode) joinNode).addSlotIdToHashOutputSlotIds(slotId);
                    }
                }
                if (((HashJoinNode) joinNode).getHashOutputSlotIds().isEmpty()) {
                    // In FE, if all columns are pruned, hash output slots are empty.
                    // On the contrary, BE will keep all columns if hash output slots are empty.
                    // Currently BE will keep this behavior in order to be compatible with older planner.
                    // So we have to workaround this in FE by keeping at least one slot in oldHashOutputSlotIds.
                    // TODO: Remove this code when old planner is deleted and BE changes to be consistent with FE.
                    for (SlotId slotId : oldHashOutputSlotIds) {
                        ((HashJoinNode) joinNode).addSlotIdToHashOutputSlotIds(slotId);
                        break;
                    }
                }
            }
            return inputFragment;
        }

        if (inputPlanNode instanceof TableFunctionNode) {
            TableFunctionNode tableFunctionNode = (TableFunctionNode) inputPlanNode;
            tableFunctionNode.setOutputSlotIds(Lists.newArrayList(requiredSlotIdSet));
        }

        if (inputPlanNode instanceof ScanNode) {
            // TODO support for two phase read with project, remove this if after refactor
            if (!(project.child() instanceof PhysicalDeferMaterializeOlapScan
                    || (project.child() instanceof PhysicalFilter
                    && ((PhysicalFilter<?>) project.child()).child() instanceof PhysicalDeferMaterializeOlapScan))) {
                TupleDescriptor projectionTuple = generateTupleDesc(slots,
                        ((ScanNode) inputPlanNode).getTupleDesc().getTable(), context);
                inputPlanNode.setProjectList(projectionExprs);
                inputPlanNode.setOutputTupleDesc(projectionTuple);
            }
            if (inputPlanNode instanceof OlapScanNode) {
                ((OlapScanNode) inputPlanNode).updateRequiredSlots(context, requiredByProjectSlotIdSet);
            }
            updateScanSlotsMaterialization((ScanNode) inputPlanNode, requiredSlotIdSet,
                    requiredByProjectSlotIdSet, context);
        } else {
            if (project.child() instanceof PhysicalDeferMaterializeTopN) {
                inputFragment.setOutputExprs(allProjectionExprs);
            } else {
                TupleDescriptor tupleDescriptor = generateTupleDesc(slots, null, context);
                inputPlanNode.setProjectList(projectionExprs);
                inputPlanNode.setOutputTupleDesc(tupleDescriptor);
            }
        }
        return inputFragment;
    }

    /**
     * Returns a new fragment with a UnionNode as its root. The data partition of the
     * returned fragment and how the data of the child fragments is consumed depends on the
     * data partitions of the child fragments:
     * - All child fragments are unpartitioned or partitioned: The returned fragment has an
     *   UNPARTITIONED or RANDOM data partition, respectively. The UnionNode absorbs the
     *   plan trees of all child fragments.
     * - Mixed partitioned/unpartitioned child fragments: The returned fragment is
     *   RANDOM partitioned. The plan trees of all partitioned child fragments are absorbed
     *   into the UnionNode. All unpartitioned child fragments are connected to the
     *   UnionNode via a RANDOM exchange, and remain unchanged otherwise.
     */
    @Override
    public PlanFragment visitPhysicalSetOperation(
            PhysicalSetOperation setOperation, PlanTranslatorContext context) {
        List<PlanFragment> childrenFragments = new ArrayList<>();
        for (Plan plan : setOperation.children()) {
            childrenFragments.add(plan.accept(this, context));
        }

        TupleDescriptor setTuple = generateTupleDesc(setOperation.getOutput(), null, context);
        List<SlotDescriptor> outputSlotDescs = new ArrayList<>(setTuple.getSlots());

        SetOperationNode setOperationNode;
        // create setOperationNode
        if (setOperation instanceof PhysicalUnion) {
            setOperationNode = new UnionNode(context.nextPlanNodeId(), setTuple.getId());
        } else if (setOperation instanceof PhysicalExcept) {
            setOperationNode = new ExceptNode(context.nextPlanNodeId(), setTuple.getId());
        } else if (setOperation instanceof PhysicalIntersect) {
            setOperationNode = new IntersectNode(context.nextPlanNodeId(), setTuple.getId());
        } else {
            throw new RuntimeException("not support set operation type " + setOperation);
        }
        setOperationNode.setNereidsId(setOperation.getId());
        List<List<Expression>> resultExpressionLists = Lists.newArrayList();
        context.getNereidsIdToPlanNodeIdMap().put(setOperation.getId(), setOperationNode.getId());
        for (List<SlotReference> regularChildrenOutput : setOperation.getRegularChildrenOutputs()) {
            resultExpressionLists.add(new ArrayList<>(regularChildrenOutput));
        }

        List<List<Expression>> constExpressionLists = Lists.newArrayList();
        if (setOperation instanceof PhysicalUnion) {
            for (List<NamedExpression> unionConsts : ((PhysicalUnion) setOperation).getConstantExprsList()) {
                constExpressionLists.add(new ArrayList<>(unionConsts));
            }
        }

        for (PlanFragment childFragment : childrenFragments) {
            setOperationNode.addChild(childFragment.getPlanRoot());
        }
        finalizeForSetOperationNode(setOperationNode, outputSlotDescs, outputSlotDescs,
                constExpressionLists, resultExpressionLists, context);

        PlanFragment setOperationFragment;
        if (childrenFragments.isEmpty()) {
            setOperationFragment = createPlanFragment(setOperationNode,
                    DataPartition.UNPARTITIONED, setOperation);
            context.addPlanFragment(setOperationFragment);
        } else {
            int childrenSize = childrenFragments.size();
            setOperationFragment = childrenFragments.get(childrenSize - 1);
            for (int i = childrenSize - 2; i >= 0; i--) {
                context.mergePlanFragment(childrenFragments.get(i), setOperationFragment);
                for (PlanFragment child : childrenFragments.get(i).getChildren()) {
                    setOperationFragment.addChild(child);
                }
            }
            setPlanRoot(setOperationFragment, setOperationNode, setOperation);
        }

        // in pipeline engine, we use parallel scan by default, but it broke the rule of data distribution
        // we need turn of parallel scan to ensure to get correct result.
        // TODO: nereids forbid all parallel scan under PhysicalSetOperation temporary
        if (!setOperation.getPhysicalProperties().equals(PhysicalProperties.ANY)
                && findOlapScanNodesByPassExchangeAndJoinNode(setOperationFragment.getPlanRoot())) {
            setOperationFragment.setHasColocatePlanNode(true);
            setOperationNode.setColocate(true);
        }

        return setOperationFragment;
    }

    @Override
    public PlanFragment visitPhysicalIntersect(PhysicalIntersect intersect, PlanTranslatorContext context) {
        PlanFragment fragment = visitPhysicalSetOperation(intersect, context);
        RunTimeFilterTranslatorV2.INSTANCE.createLegacyRuntimeFilters(
                fragment.getPlanRoot(),
                intersect.getRuntimeFiltersV2(),
                context);

        return fragment;
    }

    @Override
    public PlanFragment visitPhysicalExcept(PhysicalExcept except, PlanTranslatorContext context) {
        PlanFragment fragment = visitPhysicalSetOperation(except, context);
        RunTimeFilterTranslatorV2.INSTANCE.createLegacyRuntimeFilters(
                fragment.getPlanRoot(),
                except.getRuntimeFiltersV2(),
                context);

        return fragment;
    }

    /*-
     * Physical sort:
     * 1. Build sortInfo
     *    There are two types of slotRef:
     *    one is generated by the previous node, collectively called old.
     *    the other is newly generated by the sort node, collectively called new.
     *    Filling of sortInfo related data structures,
     *    a. ordering use newSlotRef.
     *    b. sortTupleSlotExprs use oldSlotRef.
     * 2. Create sortNode
     * 3. Create mergeFragment
     * TODO: When the slotRef of sort is currently generated,
     *       it will be based on the expression in select and orderBy expression in to ensure the uniqueness of slotRef.
     *       But eg:
     *       select a+1 from table order by a+1;
     *       the expressions of the two are inconsistent.
     *       The former will perform an additional Alias.
     *       Currently we cannot test whether this will have any effect.
     *       After a+1 can be parsed , reprocessing.
     */
    @Override
    public PlanFragment visitPhysicalQuickSort(PhysicalQuickSort<? extends Plan> sort,
            PlanTranslatorContext context) {
        PlanFragment inputFragment = sort.child(0).accept(this, context);
        List<List<Expr>> distributeExprLists = getDistributeExprs(sort.child(0));

        // 2. According to the type of sort, generate physical plan
        if (!sort.getSortPhase().isMerge()) {
            // For localSort or Gather->Sort, we just need to add sortNode
            SortNode sortNode = translateSortNode(sort, inputFragment.getPlanRoot(), context);
            sortNode.setChildrenDistributeExprLists(distributeExprLists);
            addPlanRoot(inputFragment, sortNode, sort);
        } else {
            // For mergeSort, we need to push sortInfo to exchangeNode
            if (!(inputFragment.getPlanRoot() instanceof ExchangeNode)) {
                // if there is no exchange node for mergeSort
                //   e.g., localSort -> mergeSort
                // It means the local has satisfied the Gather property. We can just ignore mergeSort
                return inputFragment;
            }
            SortNode sortNode = (SortNode) inputFragment.getPlanRoot().getChild(0);
            ((ExchangeNode) inputFragment.getPlanRoot()).setMergeInfo(sortNode.getSortInfo());
            if (inputFragment.hasChild(0) && inputFragment.getChild(0).getSink() != null) {
                inputFragment.getChild(0).getSink().setMerge(true);
            }
            sortNode.setMergeByExchange();
            sortNode.setChildrenDistributeExprLists(distributeExprLists);
        }
        return inputFragment;
    }

    @Override
    public PlanFragment visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, PlanTranslatorContext context) {
        PlanFragment inputFragment = topN.child(0).accept(this, context);
        List<List<Expr>> distributeExprLists = getDistributeExprs(topN.child(0));
        // 2. According to the type of sort, generate physical plan
        if (!topN.getSortPhase().isMerge()) {
            // For localSort or Gather->Sort, we just need to add TopNNode
            SortNode sortNode = translateSortNode(topN, inputFragment.getPlanRoot(), context);
            sortNode.setOffset(topN.getOffset());
            sortNode.setLimit(topN.getLimit());
            if (context.getTopnFilterContext().isTopnFilterSource(topN)) {
                context.getTopnFilterContext().translateSource(topN, sortNode);
                TopnFilter filter = context.getTopnFilterContext().getTopnFilter(topN);
                List<Pair<Integer, Integer>> targets = new ArrayList<>();
                for (Map.Entry<ScanNode, Expr> entry : filter.legacyTargets.entrySet()) {
                    Set<SlotRef> inputSlots = entry.getValue().getInputSlotRef();
                    if (inputSlots.size() != 1) {
                        LOG.warn("topn filter targets error: " + inputSlots);
                    } else {
                        SlotRef slot = inputSlots.iterator().next();
                        targets.add(Pair.of(entry.getKey().getId().asInt(),
                                (slot.getDesc().getId().asInt())));
                    }
                }
                sortNode.setTopnFilterTargets(targets);
            }
            // push sort to scan opt
            if (sortNode.getChild(0) instanceof OlapScanNode) {
                OlapScanNode scanNode = ((OlapScanNode) sortNode.getChild(0));
                if (checkPushSort(sortNode, scanNode.getOlapTable())) {
                    SortInfo sortInfo = sortNode.getSortInfo();
                    scanNode.setSortInfo(sortInfo);
                    scanNode.getSortInfo().setSortTupleSlotExprs(sortNode.getResolvedTupleExprs());
                    for (Expr expr : sortInfo.getOrderingExprs()) {
                        scanNode.getSortInfo().addMaterializedOrderingExpr(expr);
                    }
                    if (sortNode.getOffset() > 0) {
                        scanNode.setSortLimit(sortNode.getLimit() + sortNode.getOffset());
                    } else {
                        scanNode.setSortLimit(sortNode.getLimit());
                    }
                }
            }
            sortNode.setChildrenDistributeExprLists(distributeExprLists);
            addPlanRoot(inputFragment, sortNode, topN);
        } else {
            // For mergeSort, we need to push sortInfo to exchangeNode
            if (!(inputFragment.getPlanRoot() instanceof ExchangeNode)) {
                // if there is no exchange node for mergeSort
                //   e.g., mergeTopN -> localTopN
                // It means the local has satisfied the Gather property. We can just ignore mergeSort
                inputFragment.getPlanRoot().setOffset(topN.getOffset());
                inputFragment.getPlanRoot().setLimit(topN.getLimit());
                return inputFragment;
            }
            ExchangeNode exchangeNode = (ExchangeNode) inputFragment.getPlanRoot();
            exchangeNode.setChildrenDistributeExprLists(distributeExprLists);
            exchangeNode.setMergeInfo(((SortNode) exchangeNode.getChild(0)).getSortInfo());
            if (inputFragment.hasChild(0) && inputFragment.getChild(0).getSink() != null) {
                inputFragment.getChild(0).getSink().setMerge(true);
            }
            exchangeNode.setLimit(topN.getLimit());
            exchangeNode.setOffset(topN.getOffset());
            ((SortNode) exchangeNode.getChild(0)).setMergeByExchange();
        }
        updateLegacyPlanIdToPhysicalPlan(inputFragment.getPlanRoot(), topN);
        return inputFragment;
    }

    @Override
    public PlanFragment visitPhysicalDeferMaterializeTopN(PhysicalDeferMaterializeTopN<? extends Plan> topN,
            PlanTranslatorContext context) {
        PlanFragment planFragment = visitPhysicalTopN(topN.getPhysicalTopN(), context);
        if (planFragment.getPlanRoot() instanceof SortNode) {
            SortNode sortNode = (SortNode) planFragment.getPlanRoot();
            sortNode.setUseTwoPhaseReadOpt(true);
            sortNode.getSortInfo().setUseTwoPhaseRead();
            if (context.getTopnFilterContext().isTopnFilterSource(topN)) {
                context.getTopnFilterContext().translateSource(topN, sortNode);
            }
            TupleDescriptor tupleDescriptor = sortNode.getSortInfo().getSortTupleDescriptor();
            for (SlotDescriptor slotDescriptor : tupleDescriptor.getSlots()) {
                if (topN.getDeferMaterializeSlotIds()
                        .contains(context.findExprId(slotDescriptor.getId()))) {
                    slotDescriptor.setNeedMaterialize(false);
                }
            }
        }
        return planFragment;
    }

    @Override
    public PlanFragment visitPhysicalRepeat(PhysicalRepeat<? extends Plan> repeat, PlanTranslatorContext context) {
        PlanFragment inputPlanFragment = repeat.child(0).accept(this, context);
        List<List<Expr>> distributeExprLists = getDistributeExprs(repeat.child(0));

        Set<VirtualSlotReference> sortedVirtualSlots = repeat.getSortedVirtualSlots();
        TupleDescriptor virtualSlotsTuple =
                generateTupleDesc(ImmutableList.copyOf(sortedVirtualSlots), null, context);

        ImmutableSet<Expression> flattenGroupingSetExprs = ImmutableSet.copyOf(
                ExpressionUtils.flatExpressions(repeat.getGroupingSets()));

        List<Slot> aggregateFunctionUsedSlots = repeat.getOutputExpressions()
                .stream()
                .filter(output -> !(output instanceof VirtualSlotReference))
                .filter(output -> !flattenGroupingSetExprs.contains(output))
                .distinct()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());

        // keep flattenGroupingSetExprs comes first
        List<Expr> preRepeatExprs = Stream.concat(flattenGroupingSetExprs.stream(), aggregateFunctionUsedSlots.stream())
                .map(expr -> ExpressionTranslator.translate(expr, context)).collect(ImmutableList.toImmutableList());

        // outputSlots's order need same with preRepeatExprs
        List<Slot> outputSlots = Stream
                .concat(repeat.getOutputExpressions().stream()
                        .filter(output -> flattenGroupingSetExprs.contains(output)),
                        repeat.getOutputExpressions().stream()
                                .filter(output -> !flattenGroupingSetExprs.contains(output)).distinct())
                .map(NamedExpression::toSlot).collect(ImmutableList.toImmutableList());

        // NOTE: we should first translate preRepeatExprs, then generate output tuple,
        //       or else the preRepeatExprs can not find the bottom slotRef and throw
        //       exception: invalid slot id
        TupleDescriptor outputTuple = generateTupleDesc(outputSlots, null, context);

        // cube and rollup already convert to grouping sets in LogicalPlanBuilder.withAggregate()
        GroupingInfo groupingInfo = new GroupingInfo(
                GroupingType.GROUPING_SETS, virtualSlotsTuple, outputTuple, preRepeatExprs);

        List<Set<Integer>> repeatSlotIdList = repeat.computeRepeatSlotIdList(getSlotIds(outputTuple));
        Set<Integer> allSlotId = repeatSlotIdList.stream()
                .flatMap(Set::stream)
                .collect(ImmutableSet.toImmutableSet());

        RepeatNode repeatNode = new RepeatNode(context.nextPlanNodeId(),
                inputPlanFragment.getPlanRoot(), groupingInfo, repeatSlotIdList,
                allSlotId, repeat.computeVirtualSlotValues(sortedVirtualSlots));
        repeatNode.setNereidsId(repeat.getId());
        context.getNereidsIdToPlanNodeIdMap().put(repeat.getId(), repeatNode.getId());
        repeatNode.setChildrenDistributeExprLists(distributeExprLists);
        addPlanRoot(inputPlanFragment, repeatNode, repeat);
        updateLegacyPlanIdToPhysicalPlan(inputPlanFragment.getPlanRoot(), repeat);
        return inputPlanFragment;
    }

    @Override
    public PlanFragment visitPhysicalWindow(PhysicalWindow<? extends Plan> physicalWindow,
            PlanTranslatorContext context) {
        PlanFragment inputPlanFragment = physicalWindow.child(0).accept(this, context);
        List<List<Expr>> distributeExprLists = getDistributeExprs(physicalWindow.child(0));

        // 1. translate to old optimizer variable
        // variable in Nereids
        WindowFrameGroup windowFrameGroup = physicalWindow.getWindowFrameGroup();
        List<Expression> partitionKeyList = Lists.newArrayList(windowFrameGroup.getPartitionKeys());
        List<OrderExpression> orderKeyList = windowFrameGroup.getOrderKeys();
        List<NamedExpression> windowFunctionList = windowFrameGroup.getGroups();
        WindowFrame windowFrame = windowFrameGroup.getWindowFrame();

        // partition by clause
        List<Expr> partitionExprs = partitionKeyList.stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());

        // order by clause
        List<OrderByElement> orderByElements = orderKeyList.stream()
                .map(orderKey -> new OrderByElement(
                        ExpressionTranslator.translate(orderKey.child(), context),
                        orderKey.isAsc(), orderKey.isNullFirst()))
                .collect(Collectors.toList());

        // function calls
        List<Expr> analyticFnCalls = windowFunctionList.stream()
                .map(e -> {
                    Expression function = e.child(0).child(0);
                    if (function instanceof AggregateFunction) {
                        AggregateParam param = AggregateParam.LOCAL_RESULT;
                        function = new AggregateExpression((AggregateFunction) function, param);
                    }
                    return ExpressionTranslator.translate(function, context);
                })
                .map(FunctionCallExpr.class::cast)
                .peek(fnCall -> {
                    fnCall.setIsAnalyticFnCall(true);
                    ((org.apache.doris.catalog.AggregateFunction) fnCall.getFn()).setIsAnalyticFn(true);
                })
                .collect(Collectors.toList());

        // analytic window
        AnalyticWindow analyticWindow = physicalWindow.translateWindowFrame(windowFrame, context);

        // 2. get bufferedTupleDesc from SortNode and compute isNullableMatched
        Map<ExprId, SlotRef> bufferedSlotRefForWindow = getBufferedSlotRefForWindow(windowFrameGroup, context);
        TupleDescriptor bufferedTupleDesc = context.getBufferedTupleForWindow();

        // generate predicates to check if the exprs of partitionKeys and orderKeys have matched isNullable between
        // sortNode and analyticNode
        Expr partitionExprsIsNullableMatched = partitionExprs.isEmpty() ? null : windowExprsHaveMatchedNullable(
                partitionKeyList, partitionExprs, bufferedSlotRefForWindow);

        Expr orderElementsIsNullableMatched = orderByElements.isEmpty() ? null : windowExprsHaveMatchedNullable(
                orderKeyList.stream().map(UnaryNode::child).collect(Collectors.toList()),
                orderByElements.stream().map(OrderByElement::getExpr).collect(Collectors.toList()),
                bufferedSlotRefForWindow);

        // 3. generate tupleDesc
        List<Slot> windowSlotList = windowFunctionList.stream()
                .map(NamedExpression::toSlot)
                .collect(Collectors.toList());
        TupleDescriptor outputTupleDesc = generateTupleDesc(windowSlotList, null, context);

        // 4. generate AnalyticEvalNode
        AnalyticEvalNode analyticEvalNode = new AnalyticEvalNode(
                context.nextPlanNodeId(),
                inputPlanFragment.getPlanRoot(),
                analyticFnCalls,
                partitionExprs,
                orderByElements,
                analyticWindow,
                outputTupleDesc,
                outputTupleDesc,
                partitionExprsIsNullableMatched,
                orderElementsIsNullableMatched,
                bufferedTupleDesc
        );
        analyticEvalNode.setNereidsId(physicalWindow.getId());
        context.getNereidsIdToPlanNodeIdMap().put(physicalWindow.getId(), analyticEvalNode.getId());
        analyticEvalNode.setChildrenDistributeExprLists(distributeExprLists);
        PlanNode root = inputPlanFragment.getPlanRoot();
        if (root instanceof SortNode) {
            ((SortNode) root).setIsAnalyticSort(true);
        }
        inputPlanFragment.addPlanRoot(analyticEvalNode);

        // in pipeline engine, we use parallel scan by default, but it broke the rule of data distribution
        // we need turn of parallel scan to ensure to get correct result.
        // TODO: nereids forbid all parallel scan under PhysicalSetOperation temporary
        if (findOlapScanNodesByPassExchangeAndJoinNode(inputPlanFragment.getPlanRoot())) {
            inputPlanFragment.setHasColocatePlanNode(true);
            analyticEvalNode.setColocate(true);
            if (root instanceof SortNode) {
                ((SortNode) root).setColocate(true);
            }
        }
        return inputPlanFragment;
    }

    @Override
    public PlanFragment visitPhysicalLazyMaterialize(PhysicalLazyMaterialize<? extends Plan> materialize,
            PlanTranslatorContext context) {
        PlanFragment inputPlanFragment = materialize.child(0).accept(this, context);
        TupleDescriptor materializeTupleDesc = generateTupleDesc(materialize.getOutput(), null, context);

        MaterializationNode materializeNode = new MaterializationNode(context.nextPlanNodeId(), materializeTupleDesc,
                inputPlanFragment.getPlanRoot());

        List<Expr> rowIds = materialize.getRowIds().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());
        materializeNode.setRowIds(rowIds);

        materializeNode.setLazyColumns(materialize.getLazyColumns());
        materializeNode.setLocations(materialize.getLazySlotLocations());
        materializeNode.setIdxs(materialize.getlazyTableIdxs());

        List<Boolean> rowStoreFlags = new ArrayList<>();
        for (CatalogRelation relation : materialize.getRelations()) {
            rowStoreFlags.add(shouldUseRowStore(relation));
        }
        materializeNode.setRowStoreFlags(rowStoreFlags);

        materializeNode.setTopMaterializeNode(context.isTopMaterializeNode());
        if (context.isTopMaterializeNode()) {
            context.setTopMaterializeNode(false);
        }

        inputPlanFragment.addPlanRoot(materializeNode);
        return inputPlanFragment;
    }

    private boolean shouldUseRowStore(CatalogRelation rel) {
        boolean useRowStore = false;
        if (rel instanceof PhysicalOlapScan) {
            OlapTable olapTable = ((PhysicalOlapScan) rel).getTable();
            useRowStore = olapTable.storeRowColumn()
                    && CollectionUtils.isEmpty(olapTable.getTableProperty().getCopiedRowStoreColumns());
        }
        return useRowStore;
    }

    @Override
    public PlanFragment visitPhysicalLazyMaterializeOlapScan(PhysicalLazyMaterializeOlapScan lazyScan,
            PlanTranslatorContext context) {
        PlanFragment planFragment = computePhysicalOlapScan(lazyScan.getScan(), context);
        OlapScanNode olapScanNode = (OlapScanNode) planFragment.getPlanRoot();
        // set lazy materialized context
        olapScanNode.setIsTopnLazyMaterialize(true);
        olapScanNode.setGlobalRowIdColumn(lazyScan.getRowId().getOriginalColumn().get());
        Set<SlotId> scanIds = lazyScan.getOutput().stream().map(NamedExpression::getExprId)
                .map(context::findSlotRef).filter(Objects::nonNull).map(SlotRef::getSlotId)
                .collect(Collectors.toSet());

        for (SlotDescriptor slot : olapScanNode.getTupleDesc().getSlots()) {
            if (!scanIds.contains(slot.getId())) {
                slot.setIsMaterialized(false);
            }
        }
        context.createSlotDesc(olapScanNode.getTupleDesc(), lazyScan.getRowId(), lazyScan.getTable());
        for (Slot slot : lazyScan.getOutput()) {
            if (((SlotReference) slot).getOriginalColumn().isPresent()) {
                olapScanNode.addTopnLazyMaterializeOutputColumns(((SlotReference) slot).getOriginalColumn().get());
            }
        }
        // translate rf v2 target
        List<RuntimeFilterV2> rfV2s = context.getRuntimeFilterV2Context()
                .getRuntimeFilterV2ByTargetPlan(lazyScan);
        for (RuntimeFilterV2 rfV2 : rfV2s) {
            Expr targetExpr = rfV2.getTargetExpression().accept(ExpressionTranslator.INSTANCE, context);
            rfV2.setLegacyTargetNode(olapScanNode);
            rfV2.setLegacyTargetExpr(targetExpr);
        }
        context.getTopnFilterContext().translateTarget(lazyScan, olapScanNode, context);

        return planFragment;
    }

    /* ********************************************************************************************
     * private functions
     * ******************************************************************************************** */

    private PartitionSortNode translatePartitionSortNode(PhysicalPartitionTopN<? extends Plan> partitionTopN,
            PlanNode childNode, PlanTranslatorContext context) {
        List<Expr> partitionExprs = partitionTopN.getPartitionKeys().stream()
                .map(e -> ExpressionTranslator.translate(e, context))
                .collect(Collectors.toList());
        // partition key should on child tuple, sort key should on partition top's tuple
        TupleDescriptor sortTuple = generateTupleDesc(partitionTopN.child().getOutput(), null, context);
        List<Expr> orderingExprs = Lists.newArrayList();
        List<Boolean> ascOrders = Lists.newArrayList();
        List<Boolean> nullsFirstParams = Lists.newArrayList();
        List<OrderKey> orderKeys = partitionTopN.getOrderKeys();
        orderKeys.forEach(k -> {
            orderingExprs.add(ExpressionTranslator.translate(k.getExpr(), context));
            ascOrders.add(k.isAsc());
            nullsFirstParams.add(k.isNullFirst());
        });
        SortInfo sortInfo = new SortInfo(orderingExprs, ascOrders, nullsFirstParams, sortTuple);
        PartitionSortNode partitionSortNode = new PartitionSortNode(context.nextPlanNodeId(), childNode,
                partitionTopN.getFunction(), partitionExprs, sortInfo, partitionTopN.hasGlobalLimit(),
                partitionTopN.getPartitionLimit(), partitionTopN.getPhase());
        partitionSortNode.setNereidsId(partitionTopN.getId());
        context.getNereidsIdToPlanNodeIdMap().put(partitionTopN.getId(), partitionSortNode.getId());
        if (partitionTopN.getStats() != null) {
            partitionSortNode.setCardinality((long) partitionTopN.getStats().getRowCount());
        }
        updateLegacyPlanIdToPhysicalPlan(partitionSortNode, partitionTopN);
        return partitionSortNode;
    }

    private SortNode translateSortNode(AbstractPhysicalSort<? extends Plan> sort, PlanNode childNode,
            PlanTranslatorContext context) {
        TupleDescriptor sortTuple = generateTupleDesc(sort.child().getOutput(), null, context);
        List<Expr> orderingExprs = Lists.newArrayList();
        List<Boolean> ascOrders = Lists.newArrayList();
        List<Boolean> nullsFirstParams = Lists.newArrayList();
        List<OrderKey> orderKeys = sort.getOrderKeys();
        orderKeys.forEach(k -> {
            orderingExprs.add(ExpressionTranslator.translate(k.getExpr(), context));
            ascOrders.add(k.isAsc());
            nullsFirstParams.add(k.isNullFirst());
        });
        SortInfo sortInfo = new SortInfo(orderingExprs, ascOrders, nullsFirstParams, sortTuple);
        SortNode sortNode = new SortNode(context.nextPlanNodeId(), childNode, sortInfo, sort instanceof PhysicalTopN);
        sortNode.setNereidsId(sort.getId());
        context.getNereidsIdToPlanNodeIdMap().put(sort.getId(), sortNode.getId());
        if (sort.getStats() != null) {
            sortNode.setCardinality((long) sort.getStats().getRowCount());
        }
        updateLegacyPlanIdToPhysicalPlan(sortNode, sort);
        return sortNode;
    }

    private void updateScanSlotsMaterialization(ScanNode scanNode,
            Set<SlotId> requiredSlotIdSet, Set<SlotId> requiredByProjectSlotIdSet,
            PlanTranslatorContext context) {
        Set<SlotId> requiredWithVirtualColumns = Sets.newHashSet(requiredSlotIdSet);
        for (SlotDescriptor virtualSlot : scanNode.getTupleDesc().getSlots()) {
            Expr virtualColumn = virtualSlot.getVirtualColumn();
            if (virtualColumn == null) {
                continue;
            }
            Set<Expr> slotRefs = Sets.newHashSet();
            virtualColumn.collect(e -> e instanceof SlotRef, slotRefs);
            Set<SlotId> virtualColumnInputSlotIds = slotRefs.stream()
                    .filter(s -> s instanceof SlotRef)
                    .map(s -> (SlotRef) s)
                    .map(SlotRef::getSlotId)
                    .collect(Collectors.toSet());
            requiredWithVirtualColumns.addAll(virtualColumnInputSlotIds);
        }
        // TODO: use smallest slot if do not need any slot in upper node
        SlotDescriptor smallest = scanNode.getTupleDesc().getSlots().get(0);
        scanNode.getTupleDesc().getSlots().removeIf(s -> !requiredWithVirtualColumns.contains(s.getId()));
        if (scanNode.getTupleDesc().getSlots().isEmpty()) {
            scanNode.getTupleDesc().getSlots().add(smallest);
        }
        if (context.getSessionVariable() != null
                && context.getSessionVariable().forbidUnknownColStats
                && !StatisticConstants.isSystemTable(scanNode.getTupleDesc().getTable())) {
            for (SlotId slotId : requiredByProjectSlotIdSet) {
                if (context.isColumnStatsUnknown(scanNode, slotId)) {
                    String colName = scanNode.getTupleDesc().getSlot(slotId.asInt()).getColumn().getName();
                    throw new AnalysisException("meet unknown column stats: " + colName);
                }
            }
            context.removeScanFromStatsUnknownColumnsMap(scanNode);
        }
    }

    private void addConjunctsToPlanNode(PhysicalFilter<? extends Plan> filter,
            PlanNode planNode,
            PlanTranslatorContext context) {
        for (Expression conjunct : filter.getConjuncts()) {
            for (Expression singleConjunct : ExpressionUtils.extractConjunctionToSet(conjunct)) {
                planNode.addConjunct(ExpressionTranslator.translate(singleConjunct, context));
            }
        }
        updateLegacyPlanIdToPhysicalPlan(planNode, filter);
    }

    private TupleDescriptor generateTupleDesc(List<Slot> slotList, TableIf table, PlanTranslatorContext context) {
        TupleDescriptor tupleDescriptor = context.generateTupleDesc();
        tupleDescriptor.setTable(table);
        for (Slot slot : slotList) {
            context.createSlotDesc(tupleDescriptor, (SlotReference) slot, table);
        }
        return tupleDescriptor;
    }

    private PlanFragment connectJoinNode(HashJoinNode hashJoinNode, PlanFragment leftFragment,
            PlanFragment rightFragment, PlanTranslatorContext context, AbstractPlan join) {
        hashJoinNode.setChild(0, leftFragment.getPlanRoot());
        hashJoinNode.setChild(1, rightFragment.getPlanRoot());
        setPlanRoot(leftFragment, hashJoinNode, join);
        context.mergePlanFragment(rightFragment, leftFragment);
        for (PlanFragment rightChild : rightFragment.getChildren()) {
            leftFragment.addChild(rightChild);
        }
        return leftFragment;
    }

    private List<SlotReference> collectGroupBySlots(List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions) {
        List<SlotReference> groupSlots = Lists.newArrayList();
        Set<VirtualSlotReference> virtualSlotReferences = groupByExpressions.stream()
                .filter(VirtualSlotReference.class::isInstance)
                .map(VirtualSlotReference.class::cast)
                .collect(Collectors.toSet());
        for (Expression e : groupByExpressions) {
            if (e instanceof SlotReference && outputExpressions.stream().anyMatch(o -> o.anyMatch(e::equals))) {
                groupSlots.add((SlotReference) e);
            } else if (e instanceof SlotReference && !virtualSlotReferences.isEmpty()) {
                // When there is a virtualSlot, it is a groupingSets scenario,
                // and the original exprId should be retained at this time.
                groupSlots.add((SlotReference) e);
            } else {
                groupSlots.add(new SlotReference(e.toSql(), e.getDataType(), e.nullable(), ImmutableList.of()));
            }
        }
        return groupSlots;
    }

    private List<Integer> getSlotIds(TupleDescriptor tupleDescriptor) {
        return tupleDescriptor.getSlots()
                .stream()
                .map(slot -> slot.getId().asInt())
                .collect(ImmutableList.toImmutableList());
    }

    private Map<ExprId, SlotRef> getBufferedSlotRefForWindow(WindowFrameGroup windowFrameGroup,
                                                             PlanTranslatorContext context) {
        Map<ExprId, SlotRef> bufferedSlotRefForWindow = context.getBufferedSlotRefForWindow();

        // set if absent
        windowFrameGroup.getPartitionKeys().stream()
                .map(NamedExpression.class::cast)
                .forEach(expression -> {
                    ExprId exprId = expression.getExprId();
                    bufferedSlotRefForWindow.putIfAbsent(exprId, context.findSlotRef(exprId));
                });
        windowFrameGroup.getOrderKeys().stream()
                .map(UnaryNode::child)
                .map(NamedExpression.class::cast)
                .forEach(expression -> {
                    ExprId exprId = expression.getExprId();
                    bufferedSlotRefForWindow.putIfAbsent(exprId, context.findSlotRef(exprId));
                });
        return bufferedSlotRefForWindow;
    }

    private Expr windowExprsHaveMatchedNullable(List<Expression> expressions, List<Expr> exprs,
                                                Map<ExprId, SlotRef> bufferedSlotRef) {
        Map<ExprId, Expr> exprIdToExpr = Maps.newHashMap();
        for (int i = 0; i < expressions.size(); i++) {
            NamedExpression expression = (NamedExpression) expressions.get(i);
            exprIdToExpr.put(expression.getExprId(), exprs.get(i));
        }
        return windowExprsHaveMatchedNullable(exprIdToExpr, bufferedSlotRef, expressions, 0, expressions.size());
    }

    private Expr windowExprsHaveMatchedNullable(Map<ExprId, Expr> exprIdToExpr, Map<ExprId, SlotRef> exprIdToSlotRef,
                                                List<Expression> expressions, int i, int size) {
        if (i > size - 1) {
            return new BoolLiteral(true);
        }

        ExprId exprId = ((NamedExpression) expressions.get(i)).getExprId();
        Expr lhs = exprIdToExpr.get(exprId);
        Expr rhs = exprIdToSlotRef.get(exprId);

        Expr bothNull = new CompoundPredicate(CompoundPredicate.Operator.AND,
                new IsNullPredicate(lhs, false, true), new IsNullPredicate(rhs, false, true));
        Expr lhsEqRhsNotNull = new CompoundPredicate(CompoundPredicate.Operator.AND,
                new CompoundPredicate(CompoundPredicate.Operator.AND,
                        new IsNullPredicate(lhs, true, true), new IsNullPredicate(rhs, true, true)),
                new BinaryPredicate(BinaryPredicate.Operator.EQ, lhs, rhs,
                        Type.BOOLEAN, NullableMode.DEPEND_ON_ARGUMENT));

        Expr remainder = windowExprsHaveMatchedNullable(exprIdToExpr, exprIdToSlotRef, expressions, i + 1, size);
        return new CompoundPredicate(CompoundPredicate.Operator.AND,
                new CompoundPredicate(CompoundPredicate.Operator.OR, bothNull, lhsEqRhsNotNull), remainder);
    }

    private PlanFragment createPlanFragment(PlanNode planNode, DataPartition dataPartition, AbstractPlan physicalPlan) {
        PlanFragment planFragment = new PlanFragment(context.nextFragmentId(), planNode, dataPartition);
        updateLegacyPlanIdToPhysicalPlan(planNode, physicalPlan);
        return planFragment;
    }

    // TODO: refactor this, call it every where is not a good way
    private void setPlanRoot(PlanFragment fragment, PlanNode planNode, AbstractPlan physicalPlan) {
        fragment.setPlanRoot(planNode);
        updateLegacyPlanIdToPhysicalPlan(planNode, physicalPlan);
    }

    // TODO: refactor this, call it every where is not a good way
    private void addPlanRoot(PlanFragment fragment, PlanNode planNode, AbstractPlan physicalPlan) {
        fragment.addPlanRoot(planNode);
        updateLegacyPlanIdToPhysicalPlan(planNode, physicalPlan);
    }

    private DataPartition toDataPartition(DistributionSpec distributionSpec/* target distribution */,
                    List<ExprId> childOutputIds, PlanTranslatorContext context) {
        if (distributionSpec instanceof DistributionSpecAny
                || distributionSpec instanceof DistributionSpecStorageAny
                || distributionSpec instanceof DistributionSpecExecutionAny) {
            return DataPartition.RANDOM;
        } else if (distributionSpec instanceof DistributionSpecGather // gather to one. will set instance later
                // gather to one which has its storage. not useful now.
                || distributionSpec instanceof DistributionSpecStorageGather
                || distributionSpec instanceof DistributionSpecReplicated // broadcast to all
                || distributionSpec instanceof DistributionSpecAllSingleton // broadcast to all. one BE one instance
        ) {
            // broadcast to all (if only one, one equals all)
            return DataPartition.UNPARTITIONED;
        } else if (distributionSpec instanceof DistributionSpecHash) {
            DistributionSpecHash distributionSpecHash = (DistributionSpecHash) distributionSpec;
            List<Expr> partitionExprs = Lists.newArrayList();
            for (int i = 0; i < distributionSpecHash.getEquivalenceExprIds().size(); i++) {
                Set<ExprId> equivalenceExprId = distributionSpecHash.getEquivalenceExprIds().get(i);
                for (ExprId exprId : equivalenceExprId) {
                    if (childOutputIds.contains(exprId)) {
                        partitionExprs.add(context.findSlotRef(exprId));
                        break;
                    }
                }
                if (partitionExprs.size() != i + 1) {
                    throw new RuntimeException("Cannot translate DistributionSpec to DataPartition,"
                            + " DistributionSpec: " + distributionSpec
                            + ", child output: " + childOutputIds);
                }
            }
            TPartitionType partitionType;
            switch (distributionSpecHash.getShuffleType()) {
                case STORAGE_BUCKETED:
                    partitionType = TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED;
                    break;
                case EXECUTION_BUCKETED:
                    partitionType = TPartitionType.HASH_PARTITIONED;
                    break;
                case NATURAL:
                default:
                    throw new RuntimeException("Do not support shuffle type: "
                            + distributionSpecHash.getShuffleType());
            }
            return new DataPartition(partitionType, partitionExprs);
        } else if (distributionSpec instanceof DistributionSpecOlapTableSinkHashPartitioned) {
            return DataPartition.TABLET_ID;
        } else if (distributionSpec instanceof DistributionSpecHiveTableSinkHashPartitioned) {
            DistributionSpecHiveTableSinkHashPartitioned partitionSpecHash =
                    (DistributionSpecHiveTableSinkHashPartitioned) distributionSpec;
            List<Expr> partitionExprs = Lists.newArrayList();
            List<ExprId> partitionExprIds = partitionSpecHash.getOutputColExprIds();
            for (ExprId partitionExprId : partitionExprIds) {
                if (childOutputIds.contains(partitionExprId)) {
                    partitionExprs.add(context.findSlotRef(partitionExprId));
                }
            }
            return new DataPartition(TPartitionType.HIVE_TABLE_SINK_HASH_PARTITIONED, partitionExprs);
        } else if (distributionSpec instanceof DistributionSpecHiveTableSinkUnPartitioned) {
            return new DataPartition(TPartitionType.HIVE_TABLE_SINK_UNPARTITIONED);
        } else {
            throw new RuntimeException("Unknown DistributionSpec: " + distributionSpec);
        }
    }

    // TODO: refactor this, call it every where is not a good way
    private void updateLegacyPlanIdToPhysicalPlan(PlanNode planNode, AbstractPlan physicalPlan) {
        if (statsErrorEstimator != null) {
            statsErrorEstimator.updateLegacyPlanIdToPhysicalPlan(planNode, physicalPlan);
        }
    }

    private void injectRowIdColumnSlot(TupleDescriptor tupleDesc) {
        SlotDescriptor slotDesc = context.addSlotDesc(tupleDesc);
        if (LOG.isDebugEnabled()) {
            LOG.debug("inject slot {}", slotDesc);
        }
        String name = Column.ROWID_COL;
        Column col = new Column(name, Type.STRING, false, null, false, "", "rowid column");
        slotDesc.setType(Type.STRING);
        slotDesc.setColumn(col);
        slotDesc.setIsNullable(false);
        slotDesc.setIsMaterialized(true);
    }

    /**
     * topN opt: using storage data ordering to accelerate topn operation.
     * refer pr: optimize topn query if order by columns is prefix of sort keys of table (#10694)
     */
    private boolean checkPushSort(SortNode sortNode, OlapTable olapTable) {
        // Ensure limit is less than threshold
        if (sortNode.getLimit() <= 0
                || sortNode.getLimit() > context.getSessionVariable().topnOptLimitThreshold) {
            return false;
        }

        if (sortNode.getSortInfo().getOrderingExprs().stream().anyMatch(e -> e.getType().isComplexType())) {
            return false;
        }

        // Ensure all isAscOrder is same, ande length != 0. Can't be z-order.
        if (sortNode.getSortInfo().getIsAscOrder().stream().distinct().count() != 1 || olapTable.isZOrderSort()) {
            return false;
        }

        // Tablet's order by key only can be the front part of schema.
        // Like: schema: a.b.c.d.e.f.g order by key: a.b.c (no a,b,d)
        // Do **prefix match** to check if order by key can be pushed down.
        // olap order by key: a.b.c.d
        // sort key: (a) (a,b) (a,b,c) (a,b,c,d) is ok
        //           (a,c) (a,c,d), (a,c,b) (a,c,f) (a,b,c,d,e)is NOT ok
        List<Expr> sortExprs = sortNode.getSortInfo().getOrderingExprs();
        List<Boolean> nullsFirsts = sortNode.getSortInfo().getNullsFirst();
        List<Boolean> isAscOrders = sortNode.getSortInfo().getIsAscOrder();
        if (sortExprs.size() > olapTable.getDataSortInfo().getColNum()) {
            return false;
        }
        List<Column> sortKeyColumns = new ArrayList<>(olapTable.getFullSchema());
        if (olapTable.getEnableUniqueKeyMergeOnWrite()) {
            Map<Integer, Column> clusterKeyMap = new TreeMap<>();
            for (Column column : olapTable.getFullSchema()) {
                if (column.getClusterKeyId() != -1) {
                    clusterKeyMap.put(column.getClusterKeyId(), column);
                }
            }
            if (!clusterKeyMap.isEmpty()) {
                sortKeyColumns.clear();
                sortKeyColumns.addAll(clusterKeyMap.values());
            }
        }
        for (int i = 0; i < sortExprs.size(); i++) {
            // sort key.
            Column sortColumn = sortKeyColumns.get(i);
            // sort slot.
            Expr sortExpr = sortExprs.get(i);
            if (sortExpr instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) sortExpr;
                if (sortColumn.equals(slotRef.getColumn())) {
                    // [ORDER BY DESC NULLS FIRST] or [ORDER BY ASC NULLS LAST] can not be optimized
                    // to only read file tail, since NULLS is at file head but data is at tail
                    if (sortColumn.isAllowNull() && nullsFirsts.get(i) != isAscOrders.get(i)) {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        return true;
    }

    private List<Expr> translateToLegacyConjuncts(Set<Expression> conjuncts) {
        List<Expr> outputExprs = Lists.newArrayList();
        if (conjuncts != null) {
            conjuncts.stream()
                    .map(e -> ExpressionTranslator.translate(e, context))
                    .forEach(outputExprs::add);
        }
        return outputExprs;
    }

    private boolean isComplexDataType(DataType dataType) {
        return dataType instanceof ArrayType || dataType instanceof MapType || dataType instanceof JsonType
                || dataType instanceof StructType;
    }

    private PhysicalCTEConsumer getCTEConsumerChild(PhysicalPlan root) {
        if (root == null) {
            return null;
        } else if (root instanceof PhysicalCTEConsumer) {
            return (PhysicalCTEConsumer) root;
        } else if (root.children().size() != 1) {
            return null;
        } else {
            return getCTEConsumerChild((PhysicalPlan) root.child(0));
        }
    }

    private boolean findOlapScanNodesByPassExchangeAndJoinNode(PlanNode root) {
        if (root instanceof OlapScanNode) {
            return true;
        } else if (!(root instanceof JoinNodeBase || root instanceof ExchangeNode)) {
            return root.getChildren().stream().anyMatch(child -> findOlapScanNodesByPassExchangeAndJoinNode(child));
        }
        return false;
    }

    private List<List<Expr>> getDistributeExprs(Plan ... children) {
        List<List<Expr>> distributeExprLists = Lists.newArrayList();
        for (Plan child : children) {
            DistributionSpec spec = ((PhysicalPlan) child).getPhysicalProperties().getDistributionSpec();
            distributeExprLists.add(getDistributeExpr(child.getOutputExprIds(), spec));
        }
        return distributeExprLists;
    }

    private List<Expr> getDistributeExpr(List<ExprId> childOutputIds, DistributionSpec spec) {
        if (spec instanceof DistributionSpecHash) {
            DistributionSpecHash distributionSpecHash = (DistributionSpecHash) spec;
            List<Expr> partitionExprs = Lists.newArrayList();
            for (int i = 0; i < distributionSpecHash.getEquivalenceExprIds().size(); i++) {
                Set<ExprId> equivalenceExprId = distributionSpecHash.getEquivalenceExprIds().get(i);
                for (ExprId exprId : equivalenceExprId) {
                    if (childOutputIds.contains(exprId)) {
                        partitionExprs.add(context.findSlotRef(exprId));
                        break;
                    }
                }
            }
            return partitionExprs;
        }
        return Lists.newArrayList();
    }

    private void finalizeForSetOperationNode(SetOperationNode node,
                                                List<SlotDescriptor> constExprSlots,
                                                List<SlotDescriptor> resultExprSlots,
                                                List<List<Expression>> constExpressionLists,
                                                List<List<Expression>> resultExpressionLists,
                                                PlanTranslatorContext context) {
        if (node == null || constExprSlots == null || resultExprSlots == null
                || constExpressionLists == null || resultExpressionLists == null || context == null) {
            return;
        }

        List<List<Expr>> materializedConstExprLists = Lists.newArrayList();
        for (List<Expression> constExpressionList : constExpressionLists) {
            Preconditions.checkState(constExpressionList.size() == constExprSlots.size());
            List<Expr> exprList = Lists.newArrayList();
            for (Expression expression : constExpressionList) {
                exprList.add(ExpressionTranslator.translate(expression, context));
            }
            materializedConstExprLists.add(exprList);
        }
        node.setMaterializedConstExprLists(materializedConstExprLists);

        List<List<Expr>> materializedResultExprLists = Lists.newArrayList();
        for (int i = 0; i < resultExpressionLists.size(); ++i) {
            List<Expression> resultExpressionList = resultExpressionLists.get(i);
            List<Expr> exprList = Lists.newArrayList();
            Preconditions.checkState(resultExpressionList.size() == resultExprSlots.size());
            for (int j = 0; j < resultExpressionList.size(); ++j) {
                if (resultExprSlots.get(j).isMaterialized()) {
                    exprList.add(ExpressionTranslator.translate(resultExpressionList.get(j), context));
                    // TODO: reconsider this, we may change nullable info in previous nereids rules not here.
                    resultExprSlots.get(j)
                            .setIsNullable(resultExprSlots.get(j).getIsNullable() || exprList.get(j).isNullable());
                }
            }
            materializedResultExprLists.add(exprList);
        }
        node.setMaterializedResultExprLists(materializedResultExprLists);
        Preconditions.checkState(node.getMaterializedResultExprLists().size() == node.getChildren().size());
    }

    // matching the simple query:
    // 1. select xxx from tbl
    // 2. select xxx from tbl where xxx=yyy
    private boolean isSimpleQuery(PhysicalPlan root) {
        if (!(root instanceof PhysicalResultSink)) {
            return false;
        }
        Plan child = root.child(0);
        if (child instanceof PhysicalLimit) {
            child = child.child(0);
        }
        if (child instanceof PhysicalDistribute) {
            child = child.child(0);
        }
        if (child instanceof PhysicalLimit) {
            child = child.child(0);
        }
        if (child instanceof PhysicalProject) {
            child = child.child(0);
        }
        if (child instanceof PhysicalFilter) {
            child = child.child(0);
        }
        return child instanceof PhysicalRelation;
    }
}
