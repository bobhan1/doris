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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.AllPartitionDesc;
import org.apache.doris.analysis.CreateMTMVStmt;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.ListPartitionDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.RangePartitionDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.View;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVPropertyUtil;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.info.BaseViewInfo.AnalyzerForCreateView;
import org.apache.doris.nereids.trees.plans.commands.info.BaseViewInfo.PlanSlotFinder;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * MTMV info in creating MTMV.
 */
public class CreateMTMVInfo {
    public static final Logger LOG = LogManager.getLogger(CreateMTMVInfo.class);
    public static final String MTMV_PLANER_DISABLE_RULES = "OLAP_SCAN_PARTITION_PRUNE,PRUNE_EMPTY_PARTITION,"
            + "ELIMINATE_GROUP_BY_KEY_BY_UNIFORM";
    private final boolean ifNotExists;
    private final TableNameInfo mvName;
    private List<String> keys;
    private final String comment;
    private final DistributionDescriptor distribution;
    private Map<String, String> properties;
    private Map<String, String> mvProperties = Maps.newHashMap();

    private final LogicalPlan logicalQuery;
    private String querySql;
    private final MTMVRefreshInfo refreshInfo;
    private List<ColumnDefinition> columns = Lists.newArrayList();
    private final List<SimpleColumnDefinition> simpleColumnDefinitions;
    private final MTMVPartitionDefinition mvPartitionDefinition;
    private PartitionDesc partitionDesc;
    private MTMVRelation relation;
    private MTMVPartitionInfo mvPartitionInfo;

    /**
     * constructor for create MTMV
     */
    public CreateMTMVInfo(boolean ifNotExists, TableNameInfo mvName,
            List<String> keys, String comment,
            DistributionDescriptor distribution, Map<String, String> properties,
            LogicalPlan logicalQuery, String querySql,
            MTMVRefreshInfo refreshInfo,
            List<SimpleColumnDefinition> simpleColumnDefinitions,
            MTMVPartitionDefinition mvPartitionDefinition) {
        this.ifNotExists = Objects.requireNonNull(ifNotExists, "require ifNotExists object");
        this.mvName = Objects.requireNonNull(mvName, "require mvName object");
        this.keys = Utils.copyRequiredList(keys);
        this.comment = comment;
        this.distribution = Objects.requireNonNull(distribution, "require distribution object");
        this.properties = Objects.requireNonNull(properties, "require properties object");
        this.logicalQuery = Objects.requireNonNull(logicalQuery, "require logicalQuery object");
        this.querySql = Objects.requireNonNull(querySql, "require querySql object");
        this.refreshInfo = Objects.requireNonNull(refreshInfo, "require refreshInfo object");
        this.simpleColumnDefinitions = Objects
                .requireNonNull(simpleColumnDefinitions, "require simpleColumnDefinitions object");
        this.mvPartitionDefinition = Objects
                .requireNonNull(mvPartitionDefinition, "require mtmvPartitionInfo object");
    }

    /**
     * analyze create table info
     */
    public void analyze(ConnectContext ctx) throws Exception {
        // analyze table name
        mvName.analyze(ctx);
        if (!InternalCatalog.INTERNAL_CATALOG_NAME.equals(mvName.getCtl())) {
            throw new AnalysisException("Only support creating asynchronous materialized views in internal catalog");
        }
        if (ctx.getSessionVariable().isInDebugMode()) {
            throw new AnalysisException("Create materialized view fail, because is in debug mode");
        }
        try {
            FeNameFormat.checkTableName(mvName.getTbl());
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, mvName.getCtl(), mvName.getDb(),
                mvName.getTbl(), PrivPredicate.CREATE)) {
            String message = ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("CREATE",
                    ctx.getQualifiedUser(), ctx.getRemoteIP(),
                    mvName.getDb() + ": " + mvName.getTbl());
            throw new AnalysisException(message);
        }
        analyzeProperties();
        analyzeQuery(ctx, this.mvProperties);
        // analyze column
        final boolean finalEnableMergeOnWrite = false;
        Set<String> keysSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        keysSet.addAll(keys);
        validateColumns(this.columns, keysSet, finalEnableMergeOnWrite);
        if (distribution == null) {
            throw new AnalysisException("Create async materialized view should contain distribution desc");
        }

        if (properties == null) {
            properties = Maps.newHashMap();
        }

        CreateTableInfo.maybeRewriteByAutoBucket(distribution, properties);

        // analyze distribute
        Map<String, ColumnDefinition> columnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        columns.forEach(c -> columnMap.put(c.getName(), c));
        distribution.updateCols(columns.get(0).getName());
        distribution.validate(columnMap, KeysType.DUP_KEYS);
        refreshInfo.validate();

        analyzeProperties();
        rewriteQuerySql(ctx);
    }

    /**validate column name*/
    public void validateColumns(List<ColumnDefinition> columns, Set<String> keysSet,
            boolean finalEnableMergeOnWrite) throws UserException {
        Set<String> colSets = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ColumnDefinition col : columns) {
            if (!colSets.add(col.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col.getName());
            }
            col.validate(true, keysSet, Sets.newHashSet(), finalEnableMergeOnWrite, KeysType.DUP_KEYS);
        }
    }

    private void rewriteQuerySql(ConnectContext ctx) {
        analyzeAndFillRewriteSqlMap(querySql, ctx);
        querySql = BaseViewInfo.rewriteSql(ctx.getStatementContext().getIndexInSqlToString(), querySql);
    }

    private void analyzeAndFillRewriteSqlMap(String sql, ConnectContext ctx) {
        StatementContext stmtCtx = ctx.getStatementContext();
        LogicalPlan parsedViewPlan = new NereidsParser().parseForCreateView(sql);
        if (parsedViewPlan instanceof UnboundResultSink) {
            parsedViewPlan = (LogicalPlan) ((UnboundResultSink<?>) parsedViewPlan).child();
        }
        CascadesContext viewContextForStar = CascadesContext.initContext(
                stmtCtx, parsedViewPlan, PhysicalProperties.ANY);
        AnalyzerForCreateView analyzerForStar = new AnalyzerForCreateView(viewContextForStar);
        analyzerForStar.analyze();
        Plan analyzedPlan = viewContextForStar.getRewritePlan();
        // Traverse all slots in the plan, and add the slot's location information
        // and the fully qualified replacement string to the indexInSqlToString of the StatementContext.
        analyzedPlan.accept(PlanSlotFinder.INSTANCE, ctx.getStatementContext());
    }

    private void analyzeProperties() {
        properties = PropertyAnalyzer.getInstance().rewriteOlapProperties(mvName.getCtl(), mvName.getDb(), properties);
        if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(properties)) {
            throw new AnalysisException("Not support dynamic partition properties on async materialized view");
        }
        for (String key : MTMVPropertyUtil.MV_PROPERTY_KEYS) {
            if (properties.containsKey(key)) {
                MTMVPropertyUtil.analyzeProperty(key, properties.get(key));
                mvProperties.put(key, properties.get(key));
                properties.remove(key);
            }
        }
    }

    /**
     * analyzeQuery
     */
    public void analyzeQuery(ConnectContext ctx, Map<String, String> mvProperties) {
        try (StatementContext statementContext = ctx.getStatementContext()) {
            NereidsPlanner planner = new NereidsPlanner(statementContext);
            // this is for expression column name infer when not use alias
            LogicalSink<Plan> logicalSink = new UnboundResultSink<>(logicalQuery);
            // Should not make table without data to empty relation when analyze the related table,
            // so add disable rules
            Set<String> tempDisableRules = ctx.getSessionVariable().getDisableNereidsRuleNames();
            ctx.getSessionVariable().setDisableNereidsRules(CreateMTMVInfo.MTMV_PLANER_DISABLE_RULES);
            statementContext.invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
            Plan plan;
            try {
                // must disable constant folding by be, because be constant folding may return wrong type
                ctx.getSessionVariable().setVarOnce(SessionVariable.ENABLE_FOLD_CONSTANT_BY_BE, "false");
                plan = planner.planWithLock(logicalSink, PhysicalProperties.ANY, ExplainLevel.ALL_PLAN);
            } finally {
                // after operate, roll back the disable rules
                ctx.getSessionVariable().setDisableNereidsRules(String.join(",", tempDisableRules));
                statementContext.invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
            }
            // can not contain VIEW or MTMV
            analyzeBaseTables(planner.getAnalyzedPlan());
            // can not contain Random function
            analyzeExpressions(planner.getAnalyzedPlan(), mvProperties);
            // can not contain partition or tablets
            boolean containTableQueryOperator = MaterializedViewUtils.containTableQueryOperator(
                    planner.getAnalyzedPlan());
            if (containTableQueryOperator) {
                throw new AnalysisException("can not contain invalid expression");
            }

            Set<TableIf> baseTables = Sets.newHashSet(statementContext.getTables().values());
            for (TableIf table : baseTables) {
                if (table.isTemporary()) {
                    throw new AnalysisException("do not support create materialized view on temporary table ("
                        + Util.getTempTableDisplayName(table.getName()) + ")");
                }
            }
            getRelation(baseTables, ctx);
            this.mvPartitionInfo = mvPartitionDefinition.analyzeAndTransferToMTMVPartitionInfo(planner);
            this.partitionDesc = generatePartitionDesc(ctx);
            columns = MTMVPlanUtil.generateColumns(plan, ctx, mvPartitionInfo.getPartitionCol(),
                    (distribution == null || CollectionUtils.isEmpty(distribution.getCols())) ? Sets.newHashSet()
                            : Sets.newHashSet(distribution.getCols()),
                    simpleColumnDefinitions, properties);
            analyzeKeys();
        }
    }

    private void analyzeKeys() {
        boolean enableDuplicateWithoutKeysByDefault = false;
        try {
            if (properties != null) {
                enableDuplicateWithoutKeysByDefault =
                        PropertyAnalyzer.analyzeEnableDuplicateWithoutKeysByDefault(properties);
            }
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
        if (keys.isEmpty() && !enableDuplicateWithoutKeysByDefault) {
            keys = Lists.newArrayList();
            int keyLength = 0;
            for (ColumnDefinition column : columns) {
                DataType type = column.getType();
                Type catalogType = column.getType().toCatalogDataType();
                keyLength += catalogType.getIndexSize();
                if (keys.size() >= FeConstants.shortkey_max_column_count
                        || keyLength > FeConstants.shortkey_maxsize_bytes) {
                    if (keys.isEmpty() && type.isStringLikeType()) {
                        keys.add(column.getName());
                        column.setIsKey(true);
                    }
                    break;
                }
                if (column.getAggType() != null) {
                    break;
                }
                if (!catalogType.couldBeShortKey()) {
                    break;
                }
                keys.add(column.getName());
                column.setIsKey(true);
                if (type.isVarcharType()) {
                    break;
                }
            }
        }
    }

    // Should use analyzed plan for collect views and tables
    private void getRelation(Set<TableIf> tables, ConnectContext ctx) {
        this.relation = MTMVPlanUtil.generateMTMVRelation(tables, ctx);
    }

    private PartitionDesc generatePartitionDesc(ConnectContext ctx) {
        if (mvPartitionInfo.getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return null;
        }
        MTMVRelatedTableIf relatedTable = MTMVUtil.getRelatedTable(mvPartitionInfo.getRelatedTableInfo());
        List<AllPartitionDesc> allPartitionDescs = null;
        try {
            allPartitionDescs = MTMVPartitionUtil
                    .getPartitionDescsByRelatedTable(properties, mvPartitionInfo, mvProperties);
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        if (allPartitionDescs.size() > ctx.getSessionVariable().getCreateTablePartitionMaxNum()) {
            throw new AnalysisException(String.format(
                    "The number of partitions to be created is [%s], exceeding the maximum value of [%s]. "
                            + "Creating too many partitions can be time-consuming. If necessary, "
                            + "You can set the session variable 'create_table_partition_max_num' to a larger value.",
                    allPartitionDescs.size(), ctx.getSessionVariable().getCreateTablePartitionMaxNum()));
        }
        try {
            PartitionType type = relatedTable.getPartitionType(Optional.empty());
            if (type == PartitionType.RANGE) {
                return new RangePartitionDesc(Lists.newArrayList(mvPartitionInfo.getPartitionCol()),
                        allPartitionDescs);
            } else if (type == PartitionType.LIST) {
                return new ListPartitionDesc(Lists.newArrayList(mvPartitionInfo.getPartitionCol()),
                        allPartitionDescs);
            } else {
                return null;
            }
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    private void analyzeBaseTables(Plan plan) {
        List<Object> subQuerys = plan.collectToList(node -> node instanceof LogicalSubQueryAlias);
        for (Object subquery : subQuerys) {
            List<String> qualifier = ((LogicalSubQueryAlias) subquery).getQualifier();
            if (!CollectionUtils.isEmpty(qualifier) && qualifier.size() == 3) {
                try {
                    TableIf table = Env.getCurrentEnv().getCatalogMgr()
                            .getCatalogOrAnalysisException(qualifier.get(0))
                            .getDbOrAnalysisException(qualifier.get(1)).getTableOrAnalysisException(qualifier.get(2));
                    if (table instanceof View) {
                        throw new AnalysisException("can not contain VIEW");
                    }
                } catch (org.apache.doris.common.AnalysisException e) {
                    LOG.warn(e.getMessage(), e);
                }
            }
        }
    }

    private void analyzeExpressions(Plan plan, Map<String, String> mvProperties) {
        boolean enableNondeterministicFunction = Boolean.parseBoolean(
                mvProperties.get(PropertyAnalyzer.PROPERTIES_ENABLE_NONDETERMINISTIC_FUNCTION));
        if (enableNondeterministicFunction) {
            return;
        }
        List<Expression> functionCollectResult = MaterializedViewUtils.extractNondeterministicFunction(plan);
        if (!CollectionUtils.isEmpty(functionCollectResult)) {
            throw new AnalysisException(String.format(
                    "can not contain nonDeterministic expression, the expression is %s. "
                            + "Should add 'enable_nondeterministic_function'  = 'true' property "
                            + "when create materialized view if you know the property real meaning entirely",
                    functionCollectResult.stream().map(Expression::toString).collect(Collectors.joining(","))));
        }
    }

    /**
     * translate to catalog CreateMultiTableMaterializedViewStmt
     */
    public CreateMTMVStmt translateToLegacyStmt() {
        TableName tableName = mvName.transferToTableName();
        KeysDesc keysDesc = new KeysDesc(KeysType.DUP_KEYS, keys);
        List<Column> catalogColumns = columns.stream()
                .map(ColumnDefinition::translateToCatalogStyle)
                .collect(Collectors.toList());
        return new CreateMTMVStmt(ifNotExists, tableName, catalogColumns, refreshInfo, keysDesc,
                distribution.translateToCatalogStyle(), properties, mvProperties, querySql, comment,
                partitionDesc, mvPartitionInfo, relation);
    }

}
