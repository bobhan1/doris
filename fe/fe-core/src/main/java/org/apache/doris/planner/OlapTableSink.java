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

package org.apache.doris.planner;

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugPointUtil.DebugPoint;
import org.apache.doris.nereids.trees.plans.commands.insert.OlapInsertCommandContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TNodeInfo;
import org.apache.doris.thrift.TOlapTableIndex;
import org.apache.doris.thrift.TOlapTableIndexSchema;
import org.apache.doris.thrift.TOlapTableIndexTablets;
import org.apache.doris.thrift.TOlapTableLocationParam;
import org.apache.doris.thrift.TOlapTablePartition;
import org.apache.doris.thrift.TOlapTablePartitionParam;
import org.apache.doris.thrift.TOlapTableSchemaParam;
import org.apache.doris.thrift.TOlapTableSink;
import org.apache.doris.thrift.TPaloNodesInfo;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TTabletLocation;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class OlapTableSink extends DataSink {
    private static final Logger LOG = LogManager.getLogger(OlapTableSink.class);

    // input variables
    private OlapTable dstTable;
    private TupleDescriptor tupleDescriptor;
    // specified partition ids.
    private List<Long> partitionIds;
    // partial update input columns
    private TUniqueKeyUpdateMode uniqueKeyUpdateMode = TUniqueKeyUpdateMode.UPSERT;
    private HashSet<String> partialUpdateInputColumns;
    private TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy = TPartialUpdateNewRowPolicy.APPEND;

    // set after init called
    protected TDataSink tDataSink;

    private boolean singleReplicaLoad;

    private boolean isStrictMode = false;
    private long txnId = -1;

    private List<Expr> partitionExprs;
    private Map<Long, Expr> syncMvWhereClauses;

    private TOlapTableSchemaParam tOlapTableSchemaParam;
    private TOlapTablePartitionParam tOlapTablePartitionParam;
    private List<TOlapTableLocationParam> tOlapTableLocationParams;

    public OlapTableSink(OlapTable dstTable, TupleDescriptor tupleDescriptor, List<Long> partitionIds,
            boolean singleReplicaLoad) {
        this.dstTable = dstTable;
        this.tupleDescriptor = tupleDescriptor;
        this.partitionIds = partitionIds;
        this.singleReplicaLoad = singleReplicaLoad;
    }

    // new constructor for nereids
    public OlapTableSink(OlapTable dstTable, TupleDescriptor tupleDescriptor, List<Long> partitionIds,
                         boolean singleReplicaLoad, List<Expr> partitionExprs, Map<Long, Expr> syncMvWhereClauses) {
        this.dstTable = dstTable;
        this.tupleDescriptor = tupleDescriptor;
        this.partitionIds = partitionIds;
        this.singleReplicaLoad = singleReplicaLoad;
        this.partitionExprs = partitionExprs;
        this.syncMvWhereClauses = syncMvWhereClauses;
    }

    public void init(TUniqueId loadId, long txnId, long dbId, long loadChannelTimeoutS, int sendBatchParallelism,
            boolean loadToSingleTablet, boolean isStrictMode, long txnExpirationS) throws AnalysisException {
        TOlapTableSink tSink = new TOlapTableSink();
        tSink.setLoadId(loadId);
        tSink.setTxnId(txnId);
        tSink.setDbId(dbId);
        tSink.setBaseSchemaVersion(dstTable.getBaseSchemaVersion());
        tSink.setLoadChannelTimeoutS(loadChannelTimeoutS);
        tSink.setSendBatchParallelism(sendBatchParallelism);
        tSink.setWriteFileCache(ConnectContext.get() != null
                ? !ConnectContext.get().getSessionVariable().isDisableFileCache()
                : false);
        this.isStrictMode = isStrictMode;
        this.txnId = txnId;
        if (loadToSingleTablet && !(dstTable.getDefaultDistributionInfo() instanceof RandomDistributionInfo)) {
            throw new AnalysisException(
                    "if load_to_single_tablet set to true," + " the olap table must be with random distribution");
        }
        tSink.setLoadToSingleTablet(loadToSingleTablet);
        tSink.setTxnTimeoutS(txnExpirationS);
        String vaultId = dstTable.getStorageVaultId();
        if (vaultId != null && !vaultId.isEmpty()) {
            tSink.setStorageVaultId(vaultId);
        }
        tDataSink = new TDataSink(getDataSinkType());
        tDataSink.setOlapTableSink(tSink);

        if (partitionIds == null) {
            partitionIds = dstTable.getPartitionIds();
            if (partitionIds.isEmpty() && dstTable.getPartitionInfo().enableAutomaticPartition() == false) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_EMPTY_PARTITION_IN_TABLE, dstTable.getName());
            }
        }
        for (Long partitionId : partitionIds) {
            Partition part = dstTable.getPartition(partitionId);
            if (part == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_PARTITION, partitionId, dstTable.getName());
            }
        }

        if (singleReplicaLoad && dstTable.getStorageFormat() == TStorageFormat.V1) {
            // Single replica load not supported by TStorageFormat.V1
            singleReplicaLoad = false;
            LOG.warn("Single replica load not supported by TStorageFormat.V1. table: {}", dstTable.getName());
        }
        if (dstTable.getEnableUniqueKeyMergeOnWrite()) {
            singleReplicaLoad = false;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Single replica load not supported by merge-on-write table: {}", dstTable.getName());
            }
        }
    }

    // init for nereids insert into
    public void init(TUniqueId loadId, long txnId, long dbId, long loadChannelTimeoutS,
            int sendBatchParallelism, boolean loadToSingleTablet, boolean isStrictMode,
            long txnExpirationS, OlapInsertCommandContext olapInsertCtx) throws UserException {
        init(loadId, txnId, dbId, loadChannelTimeoutS, sendBatchParallelism, loadToSingleTablet,
                isStrictMode, txnExpirationS);
        for (Long partitionId : partitionIds) {
            Partition partition = dstTable.getPartition(partitionId);
            if (dstTable.getIndexNumber() != partition.getMaterializedIndices(IndexExtState.ALL).size()) {
                throw new UserException(
                        "table's index number not equal with partition's index number. table's index number="
                                + dstTable.getIndexIdToMeta().size() + ", partition's index number="
                                + partition.getMaterializedIndices(IndexExtState.ALL).size());
            }
        }

        TOlapTableSink tSink = tDataSink.getOlapTableSink();
        tOlapTableSchemaParam = createSchema(tSink.getDbId(), dstTable);
        tOlapTablePartitionParam = createPartition(tSink.getDbId(), dstTable);
        for (TOlapTablePartition partition : tOlapTablePartitionParam.getPartitions()) {
            partition.setTotalReplicaNum(dstTable.getPartitionTotalReplicasNum(partition.getId()));
            partition.setLoadRequiredReplicaNum(dstTable.getLoadRequiredReplicaNum(partition.getId()));
        }
        tOlapTableLocationParams = createLocation(tSink.getDbId(), dstTable);

        tSink.setTableId(dstTable.getId());
        tSink.setTupleId(tupleDescriptor.getId().asInt());
        int numReplicas = dstTable.getTableProperty().getReplicaAllocation().getTotalReplicaNum();
        tSink.setNumReplicas(numReplicas);
        tSink.setNeedGenRollup(dstTable.shouldLoadToNewRollup());
        tSink.setSchema(tOlapTableSchemaParam);
        tSink.setPartition(tOlapTablePartitionParam);
        tSink.setLocation(tOlapTableLocationParams.get(0));
        if (singleReplicaLoad) {
            tSink.setSlaveLocation(tOlapTableLocationParams.get(1));
        }
        tSink.setWriteSingleReplica(singleReplicaLoad);
        tSink.setNodesInfo(createPaloNodesInfo());

        if (!olapInsertCtx.isAllowAutoPartition()) {
            setAutoPartition(false);
        }
        if (olapInsertCtx.isAutoDetectOverwrite()) {
            setAutoDetectOverwite(true);
            setOverwriteGroupId(olapInsertCtx.getOverwriteGroupId());
        }
    }

    // init for nereids stream load
    public void init(TUniqueId loadId, long txnId, long dbId, long loadChannelTimeoutS,
            int sendBatchParallelism, boolean loadToSingleTablet, boolean isStrictMode,
            long txnExpirationS, TUniqueKeyUpdateMode uniquekeyUpdateMode,
            TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy,
            HashSet<String> partialUpdateInputColumns) throws UserException {
        setPartialUpdateInfo(uniquekeyUpdateMode, partialUpdateInputColumns);
        if (uniquekeyUpdateMode != TUniqueKeyUpdateMode.UPSERT) {
            setPartialUpdateNewRowPolicy(partialUpdateNewKeyPolicy);
        }
        init(loadId, txnId, dbId, loadChannelTimeoutS, sendBatchParallelism, loadToSingleTablet,
                isStrictMode, txnExpirationS);
        for (Long partitionId : partitionIds) {
            Partition partition = dstTable.getPartition(partitionId);
            if (dstTable.getIndexNumber() != partition.getMaterializedIndices(IndexExtState.ALL).size()) {
                throw new UserException(
                        "table's index number not equal with partition's index number. table's index number="
                                + dstTable.getIndexIdToMeta().size() + ", partition's index number="
                                + partition.getMaterializedIndices(IndexExtState.ALL).size());
            }
        }

        TOlapTableSink tSink = tDataSink.getOlapTableSink();
        tOlapTableSchemaParam = createSchema(tSink.getDbId(), dstTable);
        tOlapTablePartitionParam = createPartition(tSink.getDbId(), dstTable);
        for (TOlapTablePartition partition : tOlapTablePartitionParam.getPartitions()) {
            partition.setTotalReplicaNum(dstTable.getPartitionTotalReplicasNum(partition.getId()));
            partition.setLoadRequiredReplicaNum(dstTable.getLoadRequiredReplicaNum(partition.getId()));
        }
        tOlapTableLocationParams = createLocation(tSink.getDbId(), dstTable);

        tSink.setTableId(dstTable.getId());
        tSink.setTupleId(tupleDescriptor.getId().asInt());
        int numReplicas = dstTable.getTableProperty().getReplicaAllocation().getTotalReplicaNum();
        tSink.setNumReplicas(numReplicas);
        tSink.setNeedGenRollup(dstTable.shouldLoadToNewRollup());
        tSink.setSchema(tOlapTableSchemaParam);
        tSink.setPartition(tOlapTablePartitionParam);
        tSink.setLocation(tOlapTableLocationParams.get(0));
        if (singleReplicaLoad) {
            tSink.setSlaveLocation(tOlapTableLocationParams.get(1));
        }
        tSink.setWriteSingleReplica(singleReplicaLoad);
        tSink.setNodesInfo(createPaloNodesInfo());
    }

    public TOlapTableSchemaParam getOlapTableSchemaParam() {
        return tOlapTableSchemaParam;
    }

    public TOlapTablePartitionParam getOlapTablePartitionParam() {
        return tOlapTablePartitionParam;
    }

    public List<TOlapTableLocationParam> getOlapTableLocationParams() {
        return tOlapTableLocationParams;
    }

    public void setPartialUpdateInputColumns(boolean isPartialUpdate, HashSet<String> columns) {
        if (isPartialUpdate) {
            this.uniqueKeyUpdateMode = TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS;
            this.partialUpdateInputColumns = columns;
        }
    }

    public void setPartialUpdateInfo(TUniqueKeyUpdateMode uniqueKeyUpdateMode, HashSet<String> columns) {
        this.uniqueKeyUpdateMode = uniqueKeyUpdateMode;
        if (uniqueKeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS) {
            this.partialUpdateInputColumns = columns;
        }
    }

    public void setPartialUpdateNewRowPolicy(TPartialUpdateNewRowPolicy policy) {
        this.partialUpdateNewKeyPolicy = policy;
    }

    public void updateLoadId(TUniqueId newLoadId) {
        tDataSink.getOlapTableSink().setLoadId(newLoadId);
    }

    public void setAutoPartition(boolean var) {
        tDataSink.getOlapTableSink().getPartition().setEnableAutomaticPartition(var);
    }

    public void setAutoDetectOverwite(boolean var) {
        tDataSink.getOlapTableSink().getPartition().setEnableAutoDetectOverwrite(var);
    }

    public void setOverwriteGroupId(long var) {
        tDataSink.getOlapTableSink().getPartition().setOverwriteGroupId(var);
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "OLAP TABLE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        strBuilder.append(prefix + "  TUPLE ID: " + tupleDescriptor.getId() + "\n");
        strBuilder.append(prefix + "  " + DataPartition.RANDOM.getExplainString(explainLevel));
        boolean isPartialUpdate = uniqueKeyUpdateMode != TUniqueKeyUpdateMode.UPSERT;
        strBuilder.append(prefix + "  IS_PARTIAL_UPDATE: " + isPartialUpdate);
        if (isPartialUpdate) {
            strBuilder.append("\n");
            if (uniqueKeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS) {
                strBuilder.append(prefix + "  PARTIAL_UPDATE_MODE: UPDATE_FIXED_COLUMNS");
            } else {
                strBuilder.append(prefix + "  PARTIAL_UPDATE_MODE: UPDATE_FLEXIBLE_COLUMNS");
            }
            strBuilder.append("\n" + prefix + "  PARTIAL_UPDATE_NEW_KEY_BEHAVIOR: " + partialUpdateNewKeyPolicy);
        }
        return strBuilder.toString();
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return DataPartition.RANDOM;
    }

    @Override
    protected TDataSink toThrift() {
        return tDataSink;
    }

    private TOlapTableSchemaParam createSchema(long dbId, OlapTable table) throws AnalysisException {
        TOlapTableSchemaParam schemaParam = new TOlapTableSchemaParam();
        schemaParam.setDbId(dbId);
        schemaParam.setTableId(table.getId());
        schemaParam.setVersion(table.getIndexMetaByIndexId(table.getBaseIndexId()).getSchemaVersion());
        schemaParam.setIsStrictMode(isStrictMode);

        schemaParam.tuple_desc = tupleDescriptor.toThrift();
        for (SlotDescriptor slotDesc : tupleDescriptor.getSlots()) {
            schemaParam.addToSlotDescs(slotDesc.toThrift());
        }

        for (Map.Entry<Long, MaterializedIndexMeta> pair : table.getIndexIdToMeta().entrySet()) {
            MaterializedIndexMeta indexMeta = pair.getValue();
            List<String> columns = Lists.newArrayList();
            List<TColumn> columnsDesc = Lists.newArrayList();
            List<TOlapTableIndex> indexDesc = Lists.newArrayList();
            columns.addAll(indexMeta.getSchema().stream().map(Column::getNonShadowName).collect(Collectors.toList()));
            for (Column column : indexMeta.getSchema()) {
                TColumn tColumn = column.toThrift();
                // When schema change is doing, some modified column has prefix in name. Columns here
                // is for the schema in rowset meta, which should be no column with shadow prefix.
                // So we should remove the shadow prefix here.
                if (column.getName().startsWith(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
                    tColumn.setColumnName(column.getNonShadowName());
                }
                column.setIndexFlag(tColumn, table);
                columnsDesc.add(tColumn);
            }
            List<Index> indexes = indexMeta.getIndexes();
            if (indexes.size() == 0 && pair.getKey() == table.getBaseIndexId()) {
                // for compatible with old version befor 2.0-beta
                // if indexMeta.getIndexes() is empty, use table.getIndexes()
                indexes = table.getIndexes();
            }
            for (Index index : indexes) {
                TOlapTableIndex tIndex = index.toThrift(index.getColumnUniqueIds(table.getBaseSchema()));
                indexDesc.add(tIndex);
            }
            TOlapTableIndexSchema indexSchema = new TOlapTableIndexSchema(pair.getKey(), columns,
                    indexMeta.getSchemaHash());
            Expr whereClause = indexMeta.getWhereClause();
            if (whereClause != null) {
                Expr expr = syncMvWhereClauses.getOrDefault(pair.getKey(), null);
                if (expr == null) {
                    throw new AnalysisException(String.format("%s is not analyzed", whereClause.toSqlWithoutTbl()));
                }
                indexSchema.setWhereClause(expr.treeToThrift());
            }
            indexSchema.setColumnsDesc(columnsDesc);
            indexSchema.setIndexesDesc(indexDesc);
            schemaParam.addToIndexes(indexSchema);
        }
        setPartialUpdateInfoForParam(schemaParam, table, uniqueKeyUpdateMode);
        schemaParam.setInvertedIndexFileStorageFormat(table.getInvertedIndexFileStorageFormat());
        return schemaParam;
    }

    private void setPartialUpdateInfoForParam(TOlapTableSchemaParam schemaParam, OlapTable table,
            TUniqueKeyUpdateMode uniqueKeyUpdateMode) throws AnalysisException {
        // for backward compatibility
        schemaParam.setIsPartialUpdate(uniqueKeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS);
        schemaParam.setUniqueKeyUpdateMode(uniqueKeyUpdateMode);
        if (uniqueKeyUpdateMode != TUniqueKeyUpdateMode.UPSERT) {
            if (table.getState() == OlapTable.OlapTableState.ROLLUP
                    || table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
                throw new AnalysisException("Can't do partial update when table is doing schema change.");
            }
            schemaParam.setPartialUpdateNewKeyPolicy(partialUpdateNewKeyPolicy);
        }
        if (uniqueKeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS && table.getSequenceMapCol() != null) {
            Column seqMapCol = table.getFullSchema().stream()
                    .filter(col -> col.getName().equalsIgnoreCase(table.getSequenceMapCol()))
                    .findFirst().get();
            schemaParam.setSequenceMapColUniqueId(seqMapCol.getUniqueId());
        }
        if (uniqueKeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS) {
            for (String s : partialUpdateInputColumns) {
                schemaParam.addToPartialUpdateInputColumns(s);
            }
            for (Column col : table.getFullSchema()) {
                if (col.isAutoInc()) {
                    schemaParam.setAutoIncrementColumn(col.getName());
                    schemaParam.setAutoIncrementColumnUniqueId(col.getUniqueId());
                }
            }
        }
    }

    private List<String> getDistColumns(DistributionInfo distInfo) throws UserException {
        List<String> distColumns = Lists.newArrayList();
        switch (distInfo.getType()) {
            case HASH: {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distInfo;
                for (Column column : hashDistributionInfo.getDistributionColumns()) {
                    distColumns.add(column.getName());
                }
                break;
            }
            case RANDOM: {
                // RandomDistributionInfo doesn't have distributedColumns
                break;
            }
            default:
                throw new UserException("unsupported distributed type, type=" + distInfo.getType());
        }
        return distColumns;
    }

    private PartitionItem createDummyPartitionItem(PartitionType partType) throws UserException {
        if (partType == PartitionType.LIST) {
            return ListPartitionItem.DUMMY_ITEM;
        } else if (partType == PartitionType.RANGE) {
            return RangePartitionItem.DUMMY_ITEM;
        } else {
            throw new UserException("unsupported partition for OlapTable, partition=" + partType);
        }
    }

    private TOlapTablePartitionParam createDummyPartition(long dbId, OlapTable table,
            TOlapTablePartitionParam partitionParam, PartitionInfo partitionInfo, PartitionType partType)
            throws UserException {
        partitionParam.setEnableAutomaticPartition(true);
        // these partitions only use in locations. not find partition.
        partitionParam.setPartitionsIsFake(true);

        // set columns
        for (Column partCol : partitionInfo.getPartitionColumns()) {
            partitionParam.addToPartitionColumns(partCol.getName());
        }

        int partColNum = partitionInfo.getPartitionColumns().size();

        TOlapTablePartition fakePartition = new TOlapTablePartition();
        fakePartition.setId(0);
        // set partition keys
        setPartitionKeys(fakePartition, createDummyPartitionItem(partType), partColNum);

        for (Long indexId : table.getIndexIdToMeta().keySet()) {
            fakePartition.addToIndexes(new TOlapTableIndexTablets(indexId, Arrays.asList(0L)));
            fakePartition.setNumBuckets(1);
        }
        fakePartition.setIsMutable(true);

        DistributionInfo distInfo = table.getDefaultDistributionInfo();
        partitionParam.setDistributedColumns(getDistColumns(distInfo));
        partitionParam.addToPartitions(fakePartition);

        ArrayList<Expr> exprSource = partitionInfo.getPartitionExprs();
        if (exprSource != null && !exprSource.isEmpty()) {
            if (exprSource.size() != partitionExprs.size()) {
                throw new UserException(String.format("%s is not analyzed", exprSource));
            }
            partitionParam.setPartitionFunctionExprs(Expr.treesToThrift(partitionExprs));
        }

        return partitionParam;
    }

    private TOlapTablePartitionParam createPartition(long dbId, OlapTable table)
            throws UserException {
        TOlapTablePartitionParam partitionParam = new TOlapTablePartitionParam();
        PartitionInfo partitionInfo = table.getPartitionInfo();
        boolean enableAutomaticPartition = partitionInfo.enableAutomaticPartition();
        PartitionType partType = table.getPartitionInfo().getType();
        partitionParam.setDbId(dbId);
        partitionParam.setTableId(table.getId());
        partitionParam.setVersion(0);
        partitionParam.setPartitionType(partType.toThrift());

        // create shadow partition for empty auto partition table. only use in this load.
        if (enableAutomaticPartition && partitionIds.isEmpty()) {
            return createDummyPartition(dbId, table, partitionParam, partitionInfo, partType);
        }

        switch (partType) {
            case LIST:
            case RANGE: {
                for (Column partCol : partitionInfo.getPartitionColumns()) {
                    partitionParam.addToPartitionColumns(partCol.getName());
                }

                int partColNum = partitionInfo.getPartitionColumns().size();
                DistributionInfo selectedDistInfo = null;

                for (Long partitionId : partitionIds) {
                    Partition partition = table.getPartition(partitionId);
                    TOlapTablePartition tPartition = new TOlapTablePartition();
                    tPartition.setId(partition.getId());
                    // set partition keys
                    setPartitionKeys(tPartition, partitionInfo.getItem(partition.getId()), partColNum);

                    for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                        tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                                index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
                        tPartition.setNumBuckets(index.getTablets().size());
                    }
                    tPartition.setIsMutable(table.getPartitionInfo().getIsMutable(partitionId));
                    if (partition.getDistributionInfo().getType() == DistributionInfoType.RANDOM) {
                        int tabletIndex;
                        if (tDataSink != null && tDataSink.type == TDataSinkType.GROUP_COMMIT_BLOCK_SINK) {
                            tabletIndex = 0;
                        } else {
                            tabletIndex = Env.getCurrentEnv().getTabletLoadIndexRecorderMgr()
                                    .getCurrentTabletLoadIndex(dbId, table.getId(), partition);
                        }
                        tPartition.setLoadTabletIdx(tabletIndex);
                    }

                    partitionParam.addToPartitions(tPartition);

                    DistributionInfo distInfo = partition.getDistributionInfo();
                    if (selectedDistInfo == null) {
                        partitionParam.setDistributedColumns(getDistColumns(distInfo));
                        selectedDistInfo = distInfo;
                    } else {
                        if (selectedDistInfo.getType() != distInfo.getType()) {
                            throw new UserException("different distribute types in two different partitions, type1="
                                    + selectedDistInfo.getType() + ", type2=" + distInfo.getType());
                        }
                    }
                }
                // for auto create partition by function expr, there is no any partition firstly,
                // But this is required in thrift struct.
                if (enableAutomaticPartition && partitionIds.isEmpty()) {
                    partitionParam.setDistributedColumns(getDistColumns(table.getDefaultDistributionInfo()));
                    partitionParam.setPartitions(new ArrayList<TOlapTablePartition>());
                }

                ArrayList<Expr> exprSource = partitionInfo.getPartitionExprs();
                if (enableAutomaticPartition && exprSource != null && !exprSource.isEmpty()) {
                    if (exprSource.size() != partitionExprs.size()) {
                        throw new UserException(String.format("%s is not analyzed", exprSource));
                    }
                    partitionParam.setPartitionFunctionExprs(Expr.treesToThrift(partitionExprs));
                }

                partitionParam.setEnableAutomaticPartition(enableAutomaticPartition);
                break;
            }
            case UNPARTITIONED: {
                // there is no partition columns for single partition
                Preconditions.checkArgument(table.getPartitions().size() == 1,
                        "Number of table partitions is not 1 for unpartitioned table, partitionNum="
                                + table.getPartitions().size());
                Partition partition;
                if (partitionIds != null && partitionIds.size() == 1) {
                    partition = table.getPartition(partitionIds.get(0));
                } else {
                    partition = table.getPartitions().iterator().next();
                }

                TOlapTablePartition tPartition = new TOlapTablePartition();
                tPartition.setId(partition.getId());
                tPartition.setIsMutable(table.getPartitionInfo().getIsMutable(partition.getId()));
                // No lowerBound and upperBound for this range
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                            index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
                    tPartition.setNumBuckets(index.getTablets().size());
                }

                if (partition.getDistributionInfo().getType() == DistributionInfoType.RANDOM) {
                    int tabletIndex;
                    if (tDataSink != null && tDataSink.type == TDataSinkType.GROUP_COMMIT_BLOCK_SINK) {
                        tabletIndex = 0;
                    } else {
                        tabletIndex = Env.getCurrentEnv().getTabletLoadIndexRecorderMgr()
                                .getCurrentTabletLoadIndex(dbId, table.getId(), partition);
                    }
                    tPartition.setLoadTabletIdx(tabletIndex);
                }
                partitionParam.addToPartitions(tPartition);
                partitionParam.setDistributedColumns(getDistColumns(partition.getDistributionInfo()));
                partitionParam.setEnableAutomaticPartition(false);
                break;
            }
            default: {
                throw new UserException("unsupported partition for OlapTable, partition=" + partType);
            }
        }
        return partitionParam;
    }

    public static void setPartitionKeys(TOlapTablePartition tPartition, PartitionItem partitionItem, int partColNum)
            throws UserException {
        if (partitionItem instanceof RangePartitionItem) {
            Range<PartitionKey> range = partitionItem.getItems();
            // set start keys. min value is a REAL value. should be legal.
            if (range.hasLowerBound() && !range.lowerEndpoint().isMinValue()) {
                for (int i = 0; i < partColNum; i++) {
                    tPartition.addToStartKeys(range.lowerEndpoint().getKeys().get(i).treeToThrift().getNodes().get(0));
                }
            }
            // TODO: support real MaxLiteral in thrift.
            // now we dont send it to BE. if BE meet it, treat it as default value.
            // see VOlapTablePartition's ctor in tablet_info.h
            if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
                for (int i = 0; i < partColNum; i++) {
                    tPartition.addToEndKeys(range.upperEndpoint().getKeys().get(i).treeToThrift().getNodes().get(0));
                }
            }
        } else if (partitionItem instanceof ListPartitionItem) {
            List<PartitionKey> partitionKeys = partitionItem.getItems();
            // set in keys
            for (PartitionKey partitionKey : partitionKeys) {
                List<TExprNode> tExprNodes = new ArrayList<>();
                for (int i = 0; i < partColNum; i++) {
                    LiteralExpr literalExpr = partitionKey.getKeys().get(i);
                    if (literalExpr.isNullLiteral()) {
                        tExprNodes.add(NullLiteral.create(literalExpr.getType()).treeToThrift().getNodes().get(0));
                    } else {
                        tExprNodes.add(literalExpr.treeToThrift().getNodes().get(0));
                    }
                }
                tPartition.addToInKeys(tExprNodes);
                tPartition.setIsDefaultPartition(partitionItem.isDefaultPartition());
            }
        }
    }

    public List<TOlapTableLocationParam> createDummyLocation(OlapTable table) throws UserException {
        TOlapTableLocationParam locationParam = new TOlapTableLocationParam();
        TOlapTableLocationParam slaveLocationParam = new TOlapTableLocationParam();

        final long fakeTabletId = 0;
        SystemInfoService clusterInfo = Env.getCurrentSystemInfo();
        List<Long> aliveBe = clusterInfo.getAllBackendIds(true);
        if (aliveBe.isEmpty()) {
            throw new UserException(InternalErrorCode.REPLICA_FEW_ERR, "no available BE in cluster");
        }
        for (int i = 0; i < table.getIndexNumber(); i++) {
            // only one fake tablet here
            Long[] nodes = aliveBe.toArray(new Long[0]);
            Random random = new SecureRandom();
            int nodeIndex = random.nextInt(nodes.length);
            if (singleReplicaLoad) {
                List<Long> slaveBe = aliveBe;
                locationParam.addToTablets(new TTabletLocation(fakeTabletId,
                        Arrays.asList(nodes[nodeIndex])));

                slaveBe.remove(nodeIndex);
                slaveLocationParam.addToTablets(new TTabletLocation(fakeTabletId,
                        slaveBe));
            } else {
                locationParam.addToTablets(new TTabletLocation(fakeTabletId,
                        Arrays.asList(nodes[nodeIndex]))); // just one fake location is enough

                LOG.info("created dummy location tablet_id={}, be_id={}", fakeTabletId, nodes[nodeIndex]);
            }
        }

        return Arrays.asList(locationParam, slaveLocationParam);
    }

    private List<TOlapTableLocationParam> createLocation(long dbId, OlapTable table) throws UserException {
        if (table.getPartitionInfo().enableAutomaticPartition() && partitionIds.isEmpty()) {
            return createDummyLocation(table);
        }

        TOlapTableLocationParam locationParam = new TOlapTableLocationParam();
        TOlapTableLocationParam slaveLocationParam = new TOlapTableLocationParam();
        // BE id -> path hash
        Multimap<Long, Long> allBePathsMap = HashMultimap.create();
        for (long partitionId : partitionIds) {
            Partition partition = table.getPartition(partitionId);
            int loadRequiredReplicaNum = table.getLoadRequiredReplicaNum(partition.getId());
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                // we should ensure the replica backend is alive
                // otherwise, there will be a 'unknown node id, id=xxx' error for stream load
                for (Tablet tablet : index.getTablets()) {
                    StringBuilder errMsgBuilder = new StringBuilder();
                    Multimap<Long, Long> bePathsMap = HashMultimap.create();
                    try {
                        bePathsMap = tablet.getNormalReplicaBackendPathMap();
                        if (bePathsMap.keySet().size() < loadRequiredReplicaNum) {
                            errMsgBuilder.append("tablet ").append(tablet.getId())
                                    .append(" alive replica num ").append(bePathsMap.keySet().size())
                                    .append(" < load required replica num ").append(loadRequiredReplicaNum)
                                    .append(", alive backends: [")
                                    .append(StringUtils.join(bePathsMap.keySet(), ","))
                                    .append("]");
                            if (!Config.isCloudMode()) {
                                // in cloud mode, partition get visible version is a rpc,
                                // and each cluster has only one replica, no need to detail the replicas in cloud mode.
                                errMsgBuilder.append(", detail: ")
                                        .append(tablet.getDetailsStatusForQuery(partition.getVisibleVersion()));
                            }
                            long now = System.currentTimeMillis();
                            long lastLoadFailedTime = tablet.getLastLoadFailedTime();
                            tablet.setLastLoadFailedTime(now);
                            if (now - lastLoadFailedTime >= 5000L) {
                                Env.getCurrentEnv().getTabletScheduler().tryAddRepairTablet(
                                        tablet, dbId, table, partition, index, 0);
                            }
                            throw new UserException(InternalErrorCode.REPLICA_FEW_ERR, errMsgBuilder.toString());
                        }
                    } catch (ComputeGroupException e) {
                        LOG.warn("failed to get replica backend path for tablet " + tablet.getId(), e);
                        errMsgBuilder.append(", ").append(e.toString());
                        throw new UserException(InternalErrorCode.INTERNAL_ERR, errMsgBuilder.toString());
                    }
                    if (!Config.isCloudMode()) {
                        debugWriteRandomChooseSink(tablet, partition.getVisibleVersion(), bePathsMap);
                    }
                    if (bePathsMap.keySet().isEmpty()) {
                        throw new UserException(InternalErrorCode.REPLICA_FEW_ERR,
                                "tablet " + tablet.getId() + " no available replica");
                    }

                    if (singleReplicaLoad) {
                        Long[] nodes = bePathsMap.keySet().toArray(new Long[0]);
                        Random random = new SecureRandom();
                        Long masterNode = nodes[random.nextInt(nodes.length)];
                        Multimap<Long, Long> slaveBePathsMap = bePathsMap;
                        slaveBePathsMap.removeAll(masterNode);
                        locationParam.addToTablets(new TTabletLocation(tablet.getId(),
                                Lists.newArrayList(Sets.newHashSet(masterNode))));
                        slaveLocationParam.addToTablets(new TTabletLocation(tablet.getId(),
                                Lists.newArrayList(slaveBePathsMap.keySet())));
                    } else {
                        locationParam.addToTablets(new TTabletLocation(tablet.getId(),
                                Lists.newArrayList(bePathsMap.keySet())));
                    }
                    allBePathsMap.putAll(bePathsMap);
                }
            }
        }

        // for partition by function expr, there is no any partition firstly, But this is required in thrift struct.
        if (partitionIds.isEmpty()) {
            locationParam.setTablets(new ArrayList<TTabletLocation>());
            slaveLocationParam.setTablets(new ArrayList<TTabletLocation>());
        }
        // check if disk capacity reach limit
        // this is for load process, so use high water mark to check
        Status st = Env.getCurrentSystemInfo().checkExceedDiskCapacityLimit(allBePathsMap, true);
        if (!st.ok()) {
            throw new DdlException(st.getErrorMsg());
        }
        return Arrays.asList(locationParam, slaveLocationParam);
    }

    private void debugWriteRandomChooseSink(Tablet tablet, long version, Multimap<Long, Long> bePathsMap) {
        DebugPoint debugPoint = DebugPointUtil.getDebugPoint("OlapTableSink.write_random_choose_sink");
        if (debugPoint == null) {
            return;
        }

        boolean needCatchup = debugPoint.param("needCatchUp", false);
        int sinkNum = debugPoint.param("sinkNum", 0);
        if (sinkNum == 0) {
            sinkNum = new SecureRandom().nextInt() % bePathsMap.size() + 1;
        }
        List<Long> candidatePaths = tablet.getReplicas().stream()
                .filter(replica -> !needCatchup || replica.getVersion() >= version)
                .map(Replica::getPathHash)
                .collect(Collectors.toList());
        if (sinkNum > 0 && sinkNum < candidatePaths.size()) {
            Collections.shuffle(candidatePaths);
            while (candidatePaths.size() > sinkNum) {
                candidatePaths.remove(candidatePaths.size() - 1);
            }
        }

        Multimap<Long, Long> result = HashMultimap.create();
        bePathsMap.forEach((tabletId, pathHash) -> {
            if (candidatePaths.contains(pathHash)) {
                result.put(tabletId, pathHash);
            }
        });

        bePathsMap.clear();
        bePathsMap.putAll(result);
    }

    public TPaloNodesInfo createPaloNodesInfo() {
        TPaloNodesInfo nodesInfo = new TPaloNodesInfo();
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        for (Long id : systemInfoService.getAllBackendIds(false)) {
            Backend backend = systemInfoService.getBackend(id);
            nodesInfo.addToNodes(new TNodeInfo(backend.getId(), 0, backend.getHost(), backend.getBrpcPort()));
        }
        return nodesInfo;
    }

    protected TDataSinkType getDataSinkType() {
        return TDataSinkType.OLAP_TABLE_SINK;
    }

    public OlapTable getDstTable() {
        return dstTable;
    }

    public TupleDescriptor getTupleDescriptor() {
        return tupleDescriptor;
    }

    public long getTxnId() {
        return txnId;
    }
}
