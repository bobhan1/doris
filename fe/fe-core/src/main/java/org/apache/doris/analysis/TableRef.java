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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/TableRef.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * Superclass of all table references, including references to views, base tables
 * (Hdfs, HBase or DataSource tables), and nested collections. Contains the join
 * specification. An instance of a TableRef (and not a subclass thereof) represents
 * an unresolved table reference that must be resolved during analysis. All resolved
 * table references are subclasses of TableRef.
 *
 * The analysis of table refs follows a two-step process:
 *
 * 1. Resolution: A table ref's path is resolved and then the generic TableRef is
 * replaced by a concrete table ref (a BaseTableRef, CollectionTableRef or ViewRef)
 * in the originating stmt and that is given the resolved path. This step is driven by
 * Analyzer.resolveTableRef().
 *
 * 2. Analysis/registration: After resolution, the concrete table ref is analyzed
 * to register a tuple descriptor for its resolved path and register other table-ref
 * specific state with the analyzer (e.g., whether it is outer/semi joined, etc.).
 *
 * Therefore, subclasses of TableRef should never call the analyze() of its superclass.
 *
 * TODO for 2.3: The current TableRef class hierarchy and the related two-phase analysis
 * feels convoluted and is hard to follow. We should reorganize the TableRef class
 * structure for clarity of analysis and avoid a table ref 'switching genders' in between
 * resolution and registration.
 *
 * TODO for 2.3: Rename this class to CollectionRef and re-consider the naming and
 * structure of all subclasses.
 */
public class TableRef implements ParseNode {
    private static final Logger LOG = LogManager.getLogger(TableRef.class);
    @SerializedName("n")
    protected TableName name;
    // Legal aliases of this table ref. Contains the explicit alias as its sole element if
    // there is one. Otherwise, contains the two implicit aliases. Implicit aliases are set
    // in the c'tor of the corresponding resolved table ref (subclasses of TableRef) during
    // analysis. By convention, for table refs with multiple implicit aliases, aliases_[0]
    // contains the fully-qualified implicit alias to ensure that aliases_[0] always
    // uniquely identifies this table ref regardless of whether it has an explicit alias.
    @SerializedName("a")
    protected String[] aliases;
    protected List<Long> sampleTabletIds;
    // Indicates whether this table ref is given an explicit alias,
    protected boolean hasExplicitAlias;
    protected JoinOperator joinOp;
    protected boolean isInBitmap;
    // for mark join
    protected boolean isMark;
    // we must record mark tuple name for re-analyze
    protected String markTupleName;

    protected List<String> usingColNames;
    protected ArrayList<LateralViewRef> lateralViewRefs;
    protected Expr onClause;
    // the ref to the left of us, if we're part of a JOIN clause
    protected TableRef leftTblRef;
    protected TableSample tableSample;

    // true if this TableRef has been analyzed; implementing subclass should set it to true
    // at the end of analyze() call.
    protected boolean isAnalyzed;
    // Lists of table ref ids and materialized tuple ids of the full sequence of table
    // refs up to and including this one. These ids are cached during analysis because
    // we may alter the chain of table refs during plan generation, but we still rely
    // on the original list of ids for correct predicate assignment.
    // Populated in analyzeJoin().
    protected List<TupleId> allTableRefIds = Lists.newArrayList();
    protected List<TupleId> allMaterializedTupleIds = Lists.newArrayList();
    // ///////////////////////////////////////
    // BEGIN: Members that need to be reset()
    // All physical tuple ids that this table ref is correlated with:
    // Tuple ids of root descriptors from outer query blocks that this table ref
    // (if a CollectionTableRef) or contained CollectionTableRefs (if an InlineViewRef)
    // are rooted at. Populated during analysis.
    protected List<TupleId> correlatedTupleIds = Lists.newArrayList();
    // analysis output
    protected TupleDescriptor desc;
    @SerializedName("p")
    private PartitionNames partitionNames = null;
    private ArrayList<String> joinHints;
    private ArrayList<String> sortHints;
    private ArrayList<String> commonHints; //The Hints is set by user
    private boolean isForcePreAggOpened;
    // set after analyzeJoinHints(); true if explicitly set via hints
    private boolean isBroadcastJoin;
    private boolean isPartitionJoin;
    private String sortColumn = null;

    private TableSnapshot tableSnapshot;

    private TableScanParams scanParams;

    // END: Members that need to be reset()
    // ///////////////////////////////////////

    public TableRef() {
        // for persist
    }

    public TableRef(TableName name, String alias) {
        this(name, alias, null);
    }

    public TableRef(TableName name, String alias, PartitionNames partitionNames) {
        this(name, alias, partitionNames, null);
    }

    public TableRef(TableName name, String alias, PartitionNames partitionNames, ArrayList<String> commonHints) {
        this(name, alias, partitionNames, null, null, commonHints);
    }

    /**
     * This method construct TableRef.
     */
    public TableRef(TableName name, String alias, PartitionNames partitionNames, ArrayList<Long> sampleTabletIds,
                    TableSample tableSample, ArrayList<String> commonHints) {
        this(name, alias, partitionNames, sampleTabletIds, tableSample, commonHints, null);
    }

    public TableRef(TableName name, String alias, PartitionNames partitionNames, ArrayList<Long> sampleTabletIds,
                    TableSample tableSample, ArrayList<String> commonHints, TableSnapshot tableSnapshot) {
        this(name, alias, partitionNames, sampleTabletIds, tableSample, commonHints, tableSnapshot, null);
    }

    public TableRef(TableName name, String alias, PartitionNames partitionNames,
                    ArrayList<Long> sampleTabletIds, TableSample tableSample, ArrayList<String> commonHints,
                    TableSnapshot tableSnapshot, TableScanParams scanParams) {
        this.name = name;
        if (alias != null) {
            if (Env.isStoredTableNamesLowerCase()) {
                alias = alias.toLowerCase();
            }
            aliases = new String[]{alias};
            hasExplicitAlias = true;
        } else {
            hasExplicitAlias = false;
        }
        this.partitionNames = partitionNames;
        this.sampleTabletIds = sampleTabletIds;
        this.tableSample = tableSample;
        this.commonHints = commonHints;
        this.tableSnapshot = tableSnapshot;
        this.scanParams = scanParams;
        isAnalyzed = false;
    }

    // Only used to clone
    // this will reset all the 'analyzed' stuff
    protected TableRef(TableRef other) {
        name = other.name;
        aliases = other.aliases;
        hasExplicitAlias = other.hasExplicitAlias;
        joinOp = other.joinOp;
        isMark = other.isMark;
        markTupleName = other.markTupleName;
        // NOTE: joinHints and sortHints maybe changed after clone. so we new one List.
        joinHints =
                (other.joinHints != null) ? Lists.newArrayList(other.joinHints) : null;
        sortHints =
                (other.sortHints != null) ? Lists.newArrayList(other.sortHints) : null;
        onClause = (other.onClause != null) ? other.onClause.clone().reset() : null;
        partitionNames = (other.partitionNames != null) ? new PartitionNames(other.partitionNames) : null;
        tableSnapshot = (other.tableSnapshot != null) ? new TableSnapshot(other.tableSnapshot) : null;
        scanParams = other.scanParams;
        tableSample = (other.tableSample != null) ? new TableSample(other.tableSample) : null;
        commonHints = other.commonHints;

        usingColNames =
                (other.usingColNames != null) ? Lists.newArrayList(other.usingColNames) : null;
        // The table ref links are created at the statement level, so cloning a set of linked
        // table refs is the responsibility of the statement.
        leftTblRef = null;
        isAnalyzed = other.isAnalyzed;
        allTableRefIds = Lists.newArrayList(other.allTableRefIds);
        allMaterializedTupleIds = Lists.newArrayList(other.allMaterializedTupleIds);
        correlatedTupleIds = Lists.newArrayList(other.correlatedTupleIds);
        desc = other.desc;
        lateralViewRefs = null;
        if (other.lateralViewRefs != null) {
            lateralViewRefs = Lists.newArrayList();
            for (LateralViewRef viewRef : other.lateralViewRefs) {
                lateralViewRefs.add((LateralViewRef) viewRef.clone());
            }
        }
        this.sampleTabletIds = other.sampleTabletIds;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    @Override
    public void analyze() throws AnalysisException, UserException {
        ErrorReport.reportAnalysisException(ErrorCode.ERR_UNRESOLVED_TABLE_REF, tableRefToSql());
    }

    @Override
    public String toSql() {
        if (joinOp == null) {
            // prepend "," if we're part of a sequence of table refs w/o an
            // explicit JOIN clause
            return (leftTblRef != null ? ", " : "") + tableRefToSql();
        }

        StringBuilder output = new StringBuilder(" " + joinOpToSql() + " ");
        if (joinHints != null && !joinHints.isEmpty()) {
            output.append("[").append(Joiner.on(", ").join(joinHints)).append("] ");
        }
        output.append(tableRefToSql()).append(" ");
        if (usingColNames != null) {
            output.append("USING (").append(Joiner.on(", ").join(usingColNames)).append(")");
        } else if (onClause != null) {
            output.append("ON ").append(onClause.toSql());
        }
        return output.toString();
    }

    /**
     * Creates and returns a empty TupleDescriptor registered with the analyzer. The
     * returned tuple descriptor must have its source table set via descTbl.setTable()).
     * This method is called from the analyzer when registering this table reference.
     */
    public TupleDescriptor createTupleDescriptor() throws AnalysisException {
        ErrorReport.reportAnalysisException(ErrorCode.ERR_UNRESOLVED_TABLE_REF, tableRefToSql());
        return null;
    }

    public JoinOperator getJoinOp() {
        // if it's not explicitly set, we're doing an inner join
        return (joinOp == null ? JoinOperator.INNER_JOIN : joinOp);
    }

    public void setJoinOp(JoinOperator op) {
        this.joinOp = op;
    }

    public boolean isInBitmap() {
        return isInBitmap;
    }

    public void setInBitmap(boolean inBitmap) {
        isInBitmap = inBitmap;
    }

    public boolean isMark() {
        return isMark;
    }

    public String getMarkTupleName() {
        return markTupleName;
    }

    public void setMark(TupleDescriptor markTuple) {
        this.isMark = markTuple != null;
        if (isMark) {
            this.markTupleName = markTuple.getAlias();
        } else {
            this.markTupleName = null;
        }
    }

    public Expr getOnClause() {
        return onClause;
    }

    public void setOnClause(Expr e) {
        this.onClause = e;
    }

    public TableName getName() {
        return name;
    }

    public List<Long> getSampleTabletIds() {
        return sampleTabletIds;
    }

    public ArrayList<String> getCommonHints() {
        return commonHints;
    }

    public TableSample getTableSample() {
        return tableSample;
    }

    public TableSnapshot getTableSnapshot() {
        return tableSnapshot;
    }

    public Boolean haveDesc() {
        return desc != null;
    }

    public TableScanParams getScanParams() {
        return scanParams;
    }

    /**
     * This method should only be called after the TableRef has been analyzed.
     */
    public TupleDescriptor getDesc() {
        Preconditions.checkState(isAnalyzed);
        // after analyze(), desc should be set.
        Preconditions.checkState(desc != null);
        return desc;
    }

    /**
     * This method should only be called after the TableRef has been analyzed.
     */
    public TupleId getId() {
        Preconditions.checkState(isAnalyzed);
        // after analyze(), desc should be set.
        Preconditions.checkState(desc != null);
        return desc.getId();
    }

    /**
     * Return the list of materialized tuple ids from the TableRef.
     * This method should only be called after the TableRef has been analyzed.
     */
    public List<TupleId> getMaterializedTupleIds() {
        // This function should only be called after analyze().
        Preconditions.checkState(isAnalyzed);
        Preconditions.checkNotNull(desc);
        return desc.getId().asList();
    }

    /**
     * Return the list of tuple ids materialized by the full sequence of
     * table refs up to this one.
     */
    public List<TupleId> getAllMaterializedTupleIds() {
        if (leftTblRef != null) {
            List<TupleId> result = Lists.newArrayList(leftTblRef.getAllMaterializedTupleIds());
            result.addAll(getMaterializedTupleIds());
            return result;
        } else {
            return getMaterializedTupleIds();
        }
    }

    /**
     * Returns true if this table ref has a resolved path that is rooted at a registered
     * tuple descriptor, false otherwise.
     */
    public boolean isRelative() {
        return false;
    }

    /**
     * Indicates if this TableRef directly or indirectly references another TableRef from
     * an outer query block.
     */
    public boolean isCorrelated() {
        return !correlatedTupleIds.isEmpty();
    }

    public TableIf getTable() {
        return desc.getTable();
    }

    public List<String> getUsingClause() {
        return this.usingColNames;
    }

    public void setUsingClause(List<String> colNames) {
        this.usingColNames = colNames;
    }

    public TableRef getLeftTblRef() {
        return leftTblRef;
    }

    public void setLeftTblRef(TableRef leftTblRef) {
        this.leftTblRef = leftTblRef;
    }

    public ArrayList<String> getJoinHints() {
        return joinHints;
    }

    public void setJoinHints(ArrayList<String> hints) {
        this.joinHints = hints;
    }

    public boolean hasJoinHints() {
        return CollectionUtils.isNotEmpty(joinHints);
    }

    public boolean isBroadcastJoin() {
        return isBroadcastJoin;
    }

    public boolean isPartitionJoin() {
        return isPartitionJoin;
    }

    public boolean isForcePreAggOpened() {
        return isForcePreAggOpened;
    }

    public void setSortHints(ArrayList<String> hints) {
        this.sortHints = hints;
    }

    public String getSortColumn() {
        return sortColumn;
    }

    public ArrayList<LateralViewRef> getLateralViewRefs() {
        return lateralViewRefs;
    }

    public void setLateralViewRefs(ArrayList<LateralViewRef> lateralViewRefs) {
        this.lateralViewRefs = lateralViewRefs;
    }

    private String joinOpToSql() {
        Preconditions.checkState(joinOp != null);
        switch (joinOp) {
            case INNER_JOIN:
                return "INNER JOIN";
            case LEFT_OUTER_JOIN:
                return "LEFT OUTER JOIN";
            case LEFT_SEMI_JOIN:
                return "LEFT SEMI JOIN";
            case LEFT_ANTI_JOIN:
                return "LEFT ANTI JOIN";
            case RIGHT_SEMI_JOIN:
                return "RIGHT SEMI JOIN";
            case RIGHT_ANTI_JOIN:
                return "RIGHT ANTI JOIN";
            case RIGHT_OUTER_JOIN:
                return "RIGHT OUTER JOIN";
            case FULL_OUTER_JOIN:
                return "FULL OUTER JOIN";
            case CROSS_JOIN:
                return "CROSS JOIN";
            case NULL_AWARE_LEFT_ANTI_JOIN:
                return "NULL AWARE LEFT ANTI JOIN";
            default:
                return "bad join op: " + joinOp;
        }
    }

    /**
     * Return the list of table ref ids of the full sequence of table refs up to
     * and including this one.
     */
    public List<TupleId> getAllTableRefIds() {
        Preconditions.checkState(isAnalyzed);
        return allTableRefIds;
    }

    /**
     * Return the table ref presentation to be used in the toSql string
     */
    // tbl1
    // tbl1 alias_tbl1
    // tbl1 alias_tbl1 lateral view explode_split(k1, ",") tmp1 as e1
    // (select xxx from xxx) t1 alias_tbl1 xxx
    public String tableRefToSql() {
        String tblName = tableNameToSql();
        if (lateralViewRefs != null) {
            for (LateralViewRef viewRef : lateralViewRefs) {
                tblName += " " + viewRef.toSql();
            }
        }
        if (partitionNames != null) {
            StringJoiner sj = new StringJoiner(",", "", " ");
            for (String partName : partitionNames.getPartitionNames()) {
                sj.add(partName);
            }
            return tblName + " PARTITION(" + sj.toString() + ")";
        }
        return tblName;
    }

    protected String tableNameToSql() {
        String aliasSql = null;
        String alias = getExplicitAlias();
        if (alias != null) {
            aliasSql = ToSqlUtils.getIdentSql(alias);
        }
        String tblName = name.toSql() + ((aliasSql != null) ? " " + aliasSql : "");
        return tblName;
    }

    public String tableRefToDigest() {
        return tableRefToSql();
    }

    public String toDigest() {
        if (joinOp == null) {
            // prepend "," if we're part of a sequence of table refs w/o an
            // explicit JOIN clause
            return (leftTblRef != null ? ", " : "") + tableRefToDigest();
        }

        StringBuilder output = new StringBuilder(" " + joinOpToSql() + " ");
        if (joinHints != null && !joinHints.isEmpty()) {
            output.append("[").append(Joiner.on(", ").join(joinHints)).append("] ");
        }
        output.append(tableRefToDigest()).append(" ");
        if (usingColNames != null) {
            output.append("USING (").append(Joiner.on(", ").join(usingColNames)).append(")");
        } else if (onClause != null) {
            output.append("ON ").append(onClause.toDigest());
        }
        return output.toString();
    }

    public String getAlias() {
        if (!hasExplicitAlias()) {
            return name.toString();
        }
        return getUniqueAlias();
    }

    public TableName getAliasAsName() {
        if (hasExplicitAlias()) {
            return new TableName(null, null, getUniqueAlias());
        }
        return name;
    }

    /**
     * Returns all legal aliases of this table ref.
     */
    public String[] getAliases() {
        return aliases;
    }

    /**
     * Returns the explicit alias or the fully-qualified implicit alias. The returned alias
     * is guaranteed to be unique (i.e., column/field references against the alias cannot
     * be ambiguous).
     */
    public String getUniqueAlias() {
        return aliases[0];
    }

    /**
     * Returns true if this table ref has an explicit alias.
     * Note that getAliases().length() == 1 does not imply an explicit alias because
     * nested collection refs have only a single implicit alias.
     */
    public boolean hasExplicitAlias() {
        return hasExplicitAlias;
    }

    /**
     * Returns the explicit alias if this table ref has one, null otherwise.
     */
    public String getExplicitAlias() {
        if (hasExplicitAlias()) {
            return getUniqueAlias();
        }
        return null;
    }

    public boolean isAnalyzed() {
        return isAnalyzed;
    }

    public boolean isResolved() {
        return !getClass().equals(TableRef.class);
    }

    /**
     * Return the list of tuple ids of the full sequence of table refs up to this one.
     */
    public List<TupleId> getAllTupleIds() {
        Preconditions.checkState(isAnalyzed);
        if (leftTblRef != null) {
            List<TupleId> result = leftTblRef.getAllTupleIds();
            result.add(desc.getId());
            return result;
        } else {
            return Lists.newArrayList(desc.getId());
        }
    }

    /**
     * Set this table's context-dependent join attributes from the given table.
     * Does not clone the attributes.
     */
    protected void setJoinAttrs(TableRef other) {
        this.joinOp = other.joinOp;
        this.isMark = other.isMark;
        this.markTupleName = other.markTupleName;
        this.joinHints = other.joinHints;
        // this.tableHints_ = other.tableHints_;
        this.onClause = other.onClause;
        this.usingColNames = other.usingColNames;
    }

    public void reset() {
        isAnalyzed = false;
        //  resolvedPath_ = null;
        if (usingColNames != null) {
            // The using col names are converted into an on-clause predicate during analysis,
            // so unset the on-clause here.
            onClause = null;
        } else if (onClause != null) {
            onClause.reset();
        }
        leftTblRef = null;
        allTableRefIds.clear();
        allMaterializedTupleIds.clear();
        correlatedTupleIds.clear();
        desc = null;
        if (lateralViewRefs != null) {
            for (LateralViewRef lateralViewRef : lateralViewRefs) {
                lateralViewRef.reset();
            }
        }
    }

    /**
     * Returns a deep clone of this table ref without also cloning the chain of table refs.
     * Sets leftTblRef_ in the returned clone to null.
     */
    @Override
    public TableRef clone() {
        return new TableRef(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        if (partitionNames != null) {
            sb.append(partitionNames.toSql());
        }
        if (aliases != null && aliases.length > 0) {
            sb.append(" AS ").append(aliases[0]);
        }
        return sb.toString();
    }

    public void setPartitionNames(PartitionNames partitionNames) {
        this.partitionNames = partitionNames;
    }

    public void setName(TableName name) {
        this.name = name;
    }
}
