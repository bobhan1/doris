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

import org.apache.doris.alter.Alter;
import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.alter.AlterJobV2.JobType;
import org.apache.doris.alter.MaterializedViewHandler;
import org.apache.doris.alter.QuotaType;
import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.alter.SystemHandler;
import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.AddPartitionLikeClause;
import org.apache.doris.analysis.AdminSetPartitionVersionStmt;
import org.apache.doris.analysis.AlterMultiPartitionClause;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.ColumnRenameClause;
import org.apache.doris.analysis.CreateFunctionStmt;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.CreateTableLikeStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.DropFunctionStmt;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.InstallPluginStmt;
import org.apache.doris.analysis.ModifyDistributionClause;
import org.apache.doris.analysis.PartitionRenameClause;
import org.apache.doris.analysis.RecoverDbStmt;
import org.apache.doris.analysis.RecoverPartitionStmt;
import org.apache.doris.analysis.RecoverTableStmt;
import org.apache.doris.analysis.ReplacePartitionClause;
import org.apache.doris.analysis.RollupRenameClause;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableRenameClause;
import org.apache.doris.analysis.UninstallPluginStmt;
import org.apache.doris.backup.BackupHandler;
import org.apache.doris.backup.RestoreJob;
import org.apache.doris.binlog.BinlogGcer;
import org.apache.doris.binlog.BinlogManager;
import org.apache.doris.blockrule.SqlBlockRuleMgr;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MetaIdGenerator.IdGeneratorBuffer;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Replica.ReplicaStatus;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.clone.ColocateTableCheckerAndBalancer;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.clone.TabletChecker;
import org.apache.doris.clone.TabletScheduler;
import org.apache.doris.clone.TabletSchedulerStat;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.ConfigException;
import org.apache.doris.common.DNSCache;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.LogUtils;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.lock.MonitoredReentrantLock;
import org.apache.doris.common.publish.TopicPublisher;
import org.apache.doris.common.publish.TopicPublisherThread;
import org.apache.doris.common.publish.WorkloadGroupPublisher;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.HttpURLUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.SmallFileMgr;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.consistency.ConsistencyChecker;
import org.apache.doris.cooldown.CooldownConfHandler;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalMetaIdMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.SplitSourceManager;
import org.apache.doris.datasource.es.EsExternalCatalog;
import org.apache.doris.datasource.es.EsRepository;
import org.apache.doris.datasource.hive.HiveTransactionMgr;
import org.apache.doris.datasource.hive.event.MetastoreEventsProcessor;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.deploy.DeployManager;
import org.apache.doris.deploy.impl.AmbariDeployManager;
import org.apache.doris.deploy.impl.K8sDeployManager;
import org.apache.doris.deploy.impl.LocalFileDeployManager;
import org.apache.doris.dictionary.DictionaryManager;
import org.apache.doris.encryption.KeyManager;
import org.apache.doris.event.EventProcessor;
import org.apache.doris.event.ReplacePartitionEvent;
import org.apache.doris.ha.BDBHA;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.ha.HAProtocol;
import org.apache.doris.ha.MasterInfo;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.meta.MetaBaseAction;
import org.apache.doris.httpv2.rest.RestApiStatusCode;
import org.apache.doris.indexpolicy.IndexPolicyMgr;
import org.apache.doris.insertoverwrite.InsertOverwriteManager;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.journal.bdbje.Timestamp;
import org.apache.doris.load.DeleteHandler;
import org.apache.doris.load.ExportJob;
import org.apache.doris.load.ExportJobState;
import org.apache.doris.load.ExportMgr;
import org.apache.doris.load.GroupCommitManager;
import org.apache.doris.load.StreamLoadRecordMgr;
import org.apache.doris.load.loadv2.LoadEtlChecker;
import org.apache.doris.load.loadv2.LoadJobScheduler;
import org.apache.doris.load.loadv2.LoadLoadingChecker;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.loadv2.ProgressManager;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.load.routineload.RoutineLoadScheduler;
import org.apache.doris.load.routineload.RoutineLoadTaskScheduler;
import org.apache.doris.master.Checkpoint;
import org.apache.doris.master.MetaHelper;
import org.apache.doris.master.PartitionInfoCollector;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mtmv.MTMVAlterOpType;
import org.apache.doris.mtmv.MTMVPartitionExprFactory;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVRefreshPartitionSnapshot;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVService;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mysql.authenticate.AuthenticateType;
import org.apache.doris.mysql.authenticate.AuthenticatorManager;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.jobs.load.LabelProcessor;
import org.apache.doris.nereids.stats.HboPlanStatisticsManager;
import org.apache.doris.nereids.trees.plans.commands.AdminSetFrontendConfigCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminSetPartitionVersionCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminSetReplicaStatusCommand;
import org.apache.doris.nereids.trees.plans.commands.AdminSetReplicaVersionCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterSystemCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.AlterViewCommand;
import org.apache.doris.nereids.trees.plans.commands.AnalyzeCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelAlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelBackupCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelBuildIndexCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateMaterializedViewCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateViewCommand;
import org.apache.doris.nereids.trees.plans.commands.DropCatalogRecycleBinCommand.IdType;
import org.apache.doris.nereids.trees.plans.commands.DropMTMVCommand;
import org.apache.doris.nereids.trees.plans.commands.DropMaterializedViewCommand;
import org.apache.doris.nereids.trees.plans.commands.TruncateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.UninstallPluginCommand;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMTMVPropertyInfo;
import org.apache.doris.nereids.trees.plans.commands.info.AlterMTMVRefreshInfo;
import org.apache.doris.nereids.trees.plans.commands.info.AlterViewInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableLikeInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateViewInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.persist.AutoIncrementIdUpdateLog;
import org.apache.doris.persist.BackendReplicasInfo;
import org.apache.doris.persist.BackendTabletsInfo;
import org.apache.doris.persist.BinlogGcInfo;
import org.apache.doris.persist.CleanQueryStatsInfo;
import org.apache.doris.persist.CreateDbInfo;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.DropDbInfo;
import org.apache.doris.persist.DropPartitionInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.GlobalVarPersistInfo;
import org.apache.doris.persist.ModifyPartitionInfo;
import org.apache.doris.persist.ModifyTableDefaultDistributionBucketNumOperationLog;
import org.apache.doris.persist.ModifyTablePropertyOperationLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.PartitionPersistInfo;
import org.apache.doris.persist.RecoverInfo;
import org.apache.doris.persist.RefreshExternalTableInfo;
import org.apache.doris.persist.ReplacePartitionOperationLog;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.persist.SetPartitionVersionOperationLog;
import org.apache.doris.persist.SetReplicaStatusOperationLog;
import org.apache.doris.persist.SetReplicaVersionOperationLog;
import org.apache.doris.persist.SetTableStatusOperationLog;
import org.apache.doris.persist.Storage;
import org.apache.doris.persist.StorageInfo;
import org.apache.doris.persist.TableInfo;
import org.apache.doris.persist.TablePropertyInfo;
import org.apache.doris.persist.TableRenameColumnInfo;
import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.persist.meta.MetaHeader;
import org.apache.doris.persist.meta.MetaReader;
import org.apache.doris.persist.meta.MetaWriter;
import org.apache.doris.planner.TabletLoadIndexRecorderMgr;
import org.apache.doris.plsql.metastore.PlsqlManager;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.plugin.PluginMgr;
import org.apache.doris.policy.PolicyMgr;
import org.apache.doris.qe.AuditEventProcessor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.FEOpExecutor;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.JournalObservable;
import org.apache.doris.qe.QueryCancelWorker;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.resource.AdmissionControl;
import org.apache.doris.resource.Tag;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;
import org.apache.doris.resource.workloadgroup.WorkloadGroupChecker;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;
import org.apache.doris.resource.workloadschedpolicy.WorkloadSchedPolicyMgr;
import org.apache.doris.resource.workloadschedpolicy.WorkloadSchedPolicyPublisher;
import org.apache.doris.scheduler.manager.TransientTaskManager;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.statistics.AnalysisManager;
import org.apache.doris.statistics.FollowerColumnSender;
import org.apache.doris.statistics.StatisticsAutoCollector;
import org.apache.doris.statistics.StatisticsCache;
import org.apache.doris.statistics.StatisticsCleaner;
import org.apache.doris.statistics.StatisticsJobAppender;
import org.apache.doris.statistics.query.QueryStats;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.HeartbeatMgr;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.CleanUDFCacheTask;
import org.apache.doris.task.CompactionTask;
import org.apache.doris.task.MasterTaskExecutor;
import org.apache.doris.task.PriorityMasterTaskExecutor;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TFrontendInfo;
import org.apache.doris.thrift.TGetMetaDBMeta;
import org.apache.doris.thrift.TGetMetaIndexMeta;
import org.apache.doris.thrift.TGetMetaPartitionMeta;
import org.apache.doris.thrift.TGetMetaReplicaMeta;
import org.apache.doris.thrift.TGetMetaResult;
import org.apache.doris.thrift.TGetMetaTableMeta;
import org.apache.doris.thrift.TGetMetaTabletMeta;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.transaction.DbUsedDataQuotaInfoCollector;
import org.apache.doris.transaction.GlobalExternalTransactionInfoMgr;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.PublishVersionDaemon;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 * A singleton class can also be seen as an entry point of Doris.
 * All manager classes can be obtained through this class.
 */
public class Env {
    private static final Logger LOG = LogManager.getLogger(Env.class);
    // 0 ~ 9999 used for qe
    public static final long NEXT_ID_INIT_VALUE = 10000;
    private static final int HTTP_TIMEOUT_SECOND = Config.sync_image_timeout_second;
    private static final int STATE_CHANGE_CHECK_INTERVAL_MS = 100;
    private static final int REPLAY_INTERVAL_MS = 1;
    private static final String BDB_DIR = "/bdb";
    public static final String IMAGE_DIR = "/image";
    public static final String CLIENT_NODE_HOST_KEY = "CLIENT_NODE_HOST";
    public static final String CLIENT_NODE_PORT_KEY = "CLIENT_NODE_PORT";

    private String metaDir;
    private String bdbDir;
    protected String imageDir;

    private MetaContext metaContext;
    private long epoch = 0;

    // Lock to perform atomic modification on map like 'idToDb' and 'fullNameToDb'.
    // These maps are all thread safe, we only use lock to perform atomic operations.
    // Operations like Get or Put do not need lock.
    // We use fair ReentrantLock to avoid starvation. Do not use this lock in critical code pass
    // because fair lock has poor performance.
    // Using QueryableReentrantLock to print owner thread in debug mode.
    private MonitoredReentrantLock lock;

    private CatalogMgr catalogMgr;
    private GlobalFunctionMgr globalFunctionMgr;
    protected LoadManager loadManager;
    private ProgressManager progressManager;
    private StreamLoadRecordMgr streamLoadRecordMgr;
    private TabletLoadIndexRecorderMgr tabletLoadIndexRecorderMgr;
    private RoutineLoadManager routineLoadManager;
    private GroupCommitManager groupCommitManager;
    private SqlBlockRuleMgr sqlBlockRuleMgr;
    private ExportMgr exportMgr;
    private Alter alter;
    private ConsistencyChecker consistencyChecker;
    private BackupHandler backupHandler;
    private PublishVersionDaemon publishVersionDaemon;
    private DeleteHandler deleteHandler;
    private DbUsedDataQuotaInfoCollector dbUsedDataQuotaInfoCollector;
    private PartitionInfoCollector partitionInfoCollector;
    private CooldownConfHandler cooldownConfHandler;
    private ExternalMetaIdMgr externalMetaIdMgr;
    private MetastoreEventsProcessor metastoreEventsProcessor;

    private JobManager<? extends AbstractJob<?, ?>, ?> jobManager;
    private LabelProcessor labelProcessor;
    private TransientTaskManager transientTaskManager;

    private MasterDaemon labelCleaner; // To clean old LabelInfo, ExportJobInfos
    private MasterDaemon txnCleaner; // To clean aborted or timeout txns
    private Daemon feDiskUpdater;  // Update fe disk info
    private Daemon replayer;
    private Daemon timePrinter;
    private Daemon listener;

    private ColumnIdFlushDaemon columnIdFlusher;

    protected boolean isFirstTimeStartUp = false;
    protected boolean isElectable;
    // set to true after finished replay all meta and ready to serve
    // set to false when catalog is not ready.
    private AtomicBoolean isReady = new AtomicBoolean(false);
    // set to true after http server start
    private AtomicBoolean httpReady = new AtomicBoolean(false);
    // set to true if FE can offer READ service.
    // canRead can be true even if isReady is false.
    // for example: OBSERVER transfer to UNKNOWN, then isReady will be set to false, but canRead can still be true
    private AtomicBoolean canRead = new AtomicBoolean(false);
    private String toMasterProgress = "";
    private BlockingQueue<FrontendNodeType> typeTransferQueue;

    // node name is used for bdbje NodeName.
    protected String nodeName;
    protected FrontendNodeType role;
    protected FrontendNodeType feType;
    // replica and observer use this value to decide provide read service or not
    private long synchronizedTimeMs;
    private MasterInfo masterInfo;

    private MetaIdGenerator idGenerator = new MetaIdGenerator(NEXT_ID_INIT_VALUE);

    private EditLog editLog;
    protected int clusterId;
    protected String token;
    // For checkpoint and observer memory replayed marker
    private AtomicLong replayedJournalId;

    private static Env CHECKPOINT = null;
    private static long checkpointThreadId = -1;
    private Checkpoint checkpointer;
    protected List<HostInfo> helperNodes = Lists.newArrayList();
    protected HostInfo selfNode = null;

    // node name -> Frontend
    protected ConcurrentHashMap<String, Frontend> frontends;
    // removed frontends' name. used for checking if name is duplicated in bdbje
    private ConcurrentLinkedQueue<String> removedFrontends;

    private HAProtocol haProtocol = null;

    private JournalObservable journalObservable;

    protected SystemInfoService systemInfo;
    private HeartbeatMgr heartbeatMgr;
    private FESessionMgr feSessionMgr;
    private TemporaryTableMgr temporaryTableMgr;
    // alive session of current fe
    private Set<String> aliveSessionSet;
    private TabletInvertedIndex tabletInvertedIndex;
    private ColocateTableIndex colocateTableIndex;

    private CatalogRecycleBin recycleBin;
    private FunctionSet functionSet;

    // for nereids
    private FunctionRegistry functionRegistry;

    private MetaReplayState metaReplayState;

    private BrokerMgr brokerMgr;
    private ResourceMgr resourceMgr;
    private StorageVaultMgr storageVaultMgr;

    private GlobalTransactionMgrIface globalTransactionMgr;

    private DeployManager deployManager;

    private MasterDaemon tabletStatMgr;

    private Auth auth;
    private AccessControllerManager accessManager;

    private AuthenticatorManager authenticatorManager;

    private DomainResolver domainResolver;

    private TabletSchedulerStat stat;

    private TabletScheduler tabletScheduler;

    private TabletChecker tabletChecker;

    // Thread pools for pending and loading task, separately
    private MasterTaskExecutor pendingLoadTaskScheduler;
    private PriorityMasterTaskExecutor<LoadTask> loadingLoadTaskScheduler;

    protected LoadJobScheduler loadJobScheduler;

    private LoadEtlChecker loadEtlChecker;
    private LoadLoadingChecker loadLoadingChecker;

    private RoutineLoadScheduler routineLoadScheduler;

    private RoutineLoadTaskScheduler routineLoadTaskScheduler;

    private SmallFileMgr smallFileMgr;

    private DynamicPartitionScheduler dynamicPartitionScheduler;

    private PluginMgr pluginMgr;

    private AuditEventProcessor auditEventProcessor;

    private RefreshManager refreshManager;

    private PolicyMgr policyMgr;

    private IndexPolicyMgr indexPolicyMgr;

    private AnalysisManager analysisManager;

    private HboPlanStatisticsManager hboPlanStatisticsManager;

    private ExternalMetaCacheMgr extMetaCacheMgr;

    private AtomicLong stmtIdCounter;

    private WorkloadGroupMgr workloadGroupMgr;

    private ComputeGroupMgr computeGroupMgr;

    private WorkloadSchedPolicyMgr workloadSchedPolicyMgr;

    private WorkloadRuntimeStatusMgr workloadRuntimeStatusMgr;

    private AdmissionControl admissionControl;

    private QueryStats queryStats;

    private StatisticsCleaner statisticsCleaner;

    private PlsqlManager plsqlManager;

    private BinlogManager binlogManager;

    private BinlogGcer binlogGcer;

    private QueryCancelWorker queryCancelWorker;

    private StatisticsAutoCollector statisticsAutoCollector;

    private StatisticsJobAppender statisticsJobAppender;

    private FollowerColumnSender followerColumnSender;

    private HiveTransactionMgr hiveTransactionMgr;

    private TopicPublisherThread topicPublisherThread;

    private WorkloadGroupChecker workloadGroupCheckerThread;

    private MTMVService mtmvService;
    private EventProcessor eventProcessor;

    private InsertOverwriteManager insertOverwriteManager;

    private DNSCache dnsCache;

    private final NereidsSqlCacheManager sqlCacheManager;

    private final NereidsSortedPartitionsCacheManager sortedPartitionsCacheManager;

    private final SplitSourceManager splitSourceManager;

    private final GlobalExternalTransactionInfoMgr globalExternalTransactionInfoMgr;

    private final List<String> forceSkipJournalIds = Arrays.asList(Config.force_skip_journal_ids);

    // all sessions' last heartbeat time of all fe
    private static volatile Map<String, Long> sessionReportTimeMap = new HashMap<>();

    private TokenManager tokenManager;

    private DictionaryManager dictionaryManager;

    private KeyManager keyManager;

    // if a config is relative to a daemon thread. record the relation here. we will proactively change interval of it.
    private final Map<String, Supplier<MasterDaemon>> configtoThreads = ImmutableMap
            .of("dynamic_partition_check_interval_seconds", this::getDynamicPartitionScheduler);

    public List<TFrontendInfo> getFrontendInfos() {
        List<TFrontendInfo> res = new ArrayList<>();

        for (Frontend fe : frontends.values()) {
            TFrontendInfo feInfo = new TFrontendInfo();
            feInfo.setCoordinatorAddress(new TNetworkAddress(fe.getHost(), fe.getRpcPort()));
            feInfo.setProcessUuid(fe.getProcessUUID());
            res.add(feInfo);
        }

        return res;
    }

    public List<Frontend> getFrontends(FrontendNodeType nodeType) {
        if (nodeType == null) {
            // get all
            return Lists.newArrayList(frontends.values());
        }

        List<Frontend> result = Lists.newArrayList();
        for (Frontend frontend : frontends.values()) {
            if (frontend.getRole() == nodeType) {
                result.add(frontend);
            }
        }

        return result;
    }

    public List<String> getRemovedFrontendNames() {
        return Lists.newArrayList(removedFrontends);
    }

    public JournalObservable getJournalObservable() {
        return journalObservable;
    }

    public SystemInfoService getClusterInfo() {
        return this.systemInfo;
    }

    private HeartbeatMgr getHeartbeatMgr() {
        return this.heartbeatMgr;
    }

    public TabletInvertedIndex getTabletInvertedIndex() {
        return this.tabletInvertedIndex;
    }

    // only for test
    public void setColocateTableIndex(ColocateTableIndex colocateTableIndex) {
        this.colocateTableIndex = colocateTableIndex;
    }

    public ColocateTableIndex getColocateTableIndex() {
        return this.colocateTableIndex;
    }

    private CatalogRecycleBin getRecycleBin() {
        return this.recycleBin;
    }

    public MetaReplayState getMetaReplayState() {
        return metaReplayState;
    }

    public DynamicPartitionScheduler getDynamicPartitionScheduler() {
        return this.dynamicPartitionScheduler;
    }

    public CatalogMgr getCatalogMgr() {
        return catalogMgr;
    }

    public ExternalMetaCacheMgr getExtMetaCacheMgr() {
        return extMetaCacheMgr;
    }

    public CatalogIf getCurrentCatalog() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            return catalogMgr.getInternalCatalog();
        }
        return ctx.getCurrentCatalog();
    }

    public InternalCatalog getInternalCatalog() {
        return catalogMgr.getInternalCatalog();
    }

    public static InternalCatalog getCurrentInternalCatalog() {
        return getCurrentEnv().getInternalCatalog();
    }

    public BinlogManager getBinlogManager() {
        return binlogManager;
    }

    private static class SingletonHolder {
        private static final Env INSTANCE = EnvFactory.getInstance().createEnv(false);
    }

    private Env() {
        this(false);
    }

    // if isCheckpointCatalog is true, it means that we should not collect thread pool metric
    public Env(boolean isCheckpointCatalog) {
        this.catalogMgr = new CatalogMgr();
        this.routineLoadManager = EnvFactory.getInstance().createRoutineLoadManager();
        this.groupCommitManager = new GroupCommitManager();
        this.sqlBlockRuleMgr = new SqlBlockRuleMgr();
        this.exportMgr = new ExportMgr();
        this.alter = new Alter();
        this.consistencyChecker = new ConsistencyChecker();
        this.lock = new MonitoredReentrantLock(true);
        this.backupHandler = new BackupHandler(this);
        this.metaDir = Config.meta_dir;
        this.publishVersionDaemon = new PublishVersionDaemon();
        this.deleteHandler = new DeleteHandler();
        this.dbUsedDataQuotaInfoCollector = new DbUsedDataQuotaInfoCollector();
        this.partitionInfoCollector = new PartitionInfoCollector();
        if (Config.enable_storage_policy) {
            this.cooldownConfHandler = new CooldownConfHandler();
        }
        this.externalMetaIdMgr = new ExternalMetaIdMgr();
        this.metastoreEventsProcessor = new MetastoreEventsProcessor();
        this.jobManager = new JobManager<>();
        this.labelProcessor = new LabelProcessor();
        this.transientTaskManager = new TransientTaskManager();

        this.replayedJournalId = new AtomicLong(0L);
        this.stmtIdCounter = new AtomicLong(0L);
        this.isElectable = false;
        this.synchronizedTimeMs = 0;
        this.feType = FrontendNodeType.INIT;
        this.typeTransferQueue = Queues.newLinkedBlockingDeque();

        this.role = FrontendNodeType.UNKNOWN;
        this.frontends = new ConcurrentHashMap<>();
        this.removedFrontends = new ConcurrentLinkedQueue<>();

        this.journalObservable = new JournalObservable();
        this.masterInfo = new MasterInfo();

        this.systemInfo = EnvFactory.getInstance().createSystemInfoService();
        this.heartbeatMgr = new HeartbeatMgr(systemInfo, !isCheckpointCatalog);
        this.feSessionMgr = new FESessionMgr();
        this.temporaryTableMgr = new TemporaryTableMgr();
        this.aliveSessionSet = new HashSet<>();
        this.tabletInvertedIndex = new TabletInvertedIndex();
        this.colocateTableIndex = new ColocateTableIndex();
        this.recycleBin = new CatalogRecycleBin();
        this.functionSet = new FunctionSet();
        this.functionSet.init();

        this.functionRegistry = new FunctionRegistry();

        this.metaReplayState = new MetaReplayState();

        this.brokerMgr = new BrokerMgr();
        this.resourceMgr = new ResourceMgr();
        this.storageVaultMgr = new StorageVaultMgr(systemInfo);

        this.globalTransactionMgr = EnvFactory.getInstance().createGlobalTransactionMgr(this);

        this.tabletStatMgr = EnvFactory.getInstance().createTabletStatMgr();

        this.auth = new Auth();
        this.accessManager = new AccessControllerManager(auth);
        this.authenticatorManager = new AuthenticatorManager(AuthenticateType.getAuthTypeConfigString());
        this.domainResolver = new DomainResolver(auth);

        this.metaContext = new MetaContext();
        this.metaContext.setThreadLocalInfo();

        this.stat = new TabletSchedulerStat();
        this.tabletScheduler = new TabletScheduler(this, systemInfo, tabletInvertedIndex, stat,
                Config.tablet_rebalancer_type);
        this.tabletChecker = new TabletChecker(this, systemInfo, tabletScheduler, stat);

        // The pendingLoadTaskScheduler's queue size should not less than Config.desired_max_waiting_jobs.
        // So that we can guarantee that all submitted load jobs can be scheduled without being starved.
        this.pendingLoadTaskScheduler = new MasterTaskExecutor("pending-load-task-scheduler",
                Config.async_pending_load_task_pool_size, Config.desired_max_waiting_jobs, !isCheckpointCatalog);
        // The loadingLoadTaskScheduler's queue size is unlimited, so that it can receive all loading tasks
        // created after pending tasks finish. And don't worry about the high concurrency, because the
        // concurrency is limited by Config.desired_max_waiting_jobs and Config.async_loading_load_task_pool_size.
        this.loadingLoadTaskScheduler = new PriorityMasterTaskExecutor<>("loading-load-task-scheduler",
                Config.async_loading_load_task_pool_size, LoadTask.COMPARATOR, LoadTask.class, !isCheckpointCatalog);

        this.loadJobScheduler = new LoadJobScheduler();
        this.loadManager = EnvFactory.getInstance().createLoadManager(loadJobScheduler);
        this.progressManager = new ProgressManager();
        this.streamLoadRecordMgr = new StreamLoadRecordMgr("stream_load_record_manager",
                Config.fetch_stream_load_record_interval_second * 1000L);
        this.tabletLoadIndexRecorderMgr = new TabletLoadIndexRecorderMgr();
        this.loadEtlChecker = new LoadEtlChecker(loadManager);
        this.loadLoadingChecker = new LoadLoadingChecker(loadManager);
        this.routineLoadScheduler = new RoutineLoadScheduler(routineLoadManager);
        this.routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);

        this.smallFileMgr = new SmallFileMgr();

        this.dynamicPartitionScheduler = new DynamicPartitionScheduler("DynamicPartitionScheduler",
                Config.dynamic_partition_check_interval_seconds * 1000L);

        this.metaDir = Config.meta_dir;
        this.bdbDir = this.metaDir + BDB_DIR;
        this.imageDir = this.metaDir + IMAGE_DIR;

        this.pluginMgr = new PluginMgr();
        this.auditEventProcessor = new AuditEventProcessor(this.pluginMgr);
        this.refreshManager = new RefreshManager();
        this.policyMgr = new PolicyMgr();
        this.indexPolicyMgr = new IndexPolicyMgr();
        this.extMetaCacheMgr = new ExternalMetaCacheMgr(isCheckpointCatalog);
        this.analysisManager = new AnalysisManager();
        this.hboPlanStatisticsManager = new HboPlanStatisticsManager();
        this.statisticsCleaner = new StatisticsCleaner();
        this.statisticsAutoCollector = new StatisticsAutoCollector();
        this.statisticsJobAppender = new StatisticsJobAppender();
        this.globalFunctionMgr = new GlobalFunctionMgr();
        this.workloadGroupMgr = new WorkloadGroupMgr();
        this.computeGroupMgr = new ComputeGroupMgr(systemInfo);
        this.workloadSchedPolicyMgr = new WorkloadSchedPolicyMgr();
        this.workloadRuntimeStatusMgr = new WorkloadRuntimeStatusMgr();
        this.admissionControl = new AdmissionControl(systemInfo);
        this.queryStats = new QueryStats();
        this.hiveTransactionMgr = new HiveTransactionMgr();
        this.plsqlManager = new PlsqlManager();
        this.binlogManager = new BinlogManager();
        this.binlogGcer = new BinlogGcer();
        this.columnIdFlusher = new ColumnIdFlushDaemon();
        this.queryCancelWorker = new QueryCancelWorker(systemInfo);
        this.topicPublisherThread = new TopicPublisherThread(
                "TopicPublisher", Config.publish_topic_info_interval_ms, systemInfo);
        this.workloadGroupCheckerThread = new WorkloadGroupChecker();
        this.mtmvService = new MTMVService();
        this.eventProcessor = new EventProcessor(mtmvService);
        this.insertOverwriteManager = new InsertOverwriteManager();
        this.dnsCache = new DNSCache();
        this.sqlCacheManager = new NereidsSqlCacheManager();
        this.sortedPartitionsCacheManager = new NereidsSortedPartitionsCacheManager();
        this.splitSourceManager = new SplitSourceManager();
        this.globalExternalTransactionInfoMgr = new GlobalExternalTransactionInfoMgr();
        this.tokenManager = new TokenManager();
        this.dictionaryManager = new DictionaryManager();
        this.keyManager = new KeyManager();
    }

    public static Map<String, Long> getSessionReportTimeMap() {
        return sessionReportTimeMap;
    }

    public void registerTempTableAndSession(Table table) {
        if (ConnectContext.get() != null) {
            ConnectContext.get().addTempTableToDB(table.getQualifiedDbName(), table.getName());
        }

        refreshSession(Util.getTempTableSessionId(table.getName()));
    }

    public void unregisterTempTable(Table table) {
        if (ConnectContext.get() != null) {
            ConnectContext.get().removeTempTableFromDB(table.getQualifiedDbName(), table.getName());
        }
    }

    private void refreshSession(String sessionId) {
        sessionReportTimeMap.put(sessionId, System.currentTimeMillis());
    }

    public void checkAndRefreshSession(String sessionId) {
        if (sessionReportTimeMap.containsKey(sessionId)) {
            sessionReportTimeMap.put(sessionId, System.currentTimeMillis());
        }
    }

    public void refreshAllAliveSession() {
        for (String sessionId : sessionReportTimeMap.keySet()) {
            refreshSession(sessionId);
        }
    }

    public static void destroyCheckpoint() {
        if (CHECKPOINT != null) {
            CHECKPOINT = null;
        }
    }

    public static Env getCurrentEnv() {
        if (isCheckpointThread()) {
            // only checkpoint thread it self will goes here.
            // so no need to care about the thread safe.
            if (CHECKPOINT == null) {
                CHECKPOINT = EnvFactory.getInstance().createEnv(true);
            }
            return CHECKPOINT;
        } else {
            return SingletonHolder.INSTANCE;
        }
    }

    // NOTICE: in most case, we should use getCurrentEnv() to get the right catalog.
    // but in some cases, we should get the serving catalog explicitly.
    public static Env getServingEnv() {
        return SingletonHolder.INSTANCE;
    }

    public BrokerMgr getBrokerMgr() {
        return brokerMgr;
    }

    public ResourceMgr getResourceMgr() {
        return resourceMgr;
    }

    public StorageVaultMgr getStorageVaultMgr() {
        return storageVaultMgr;
    }

    public static GlobalTransactionMgrIface getCurrentGlobalTransactionMgr() {
        return getCurrentEnv().globalTransactionMgr;
    }

    public GlobalTransactionMgrIface getGlobalTransactionMgr() {
        return globalTransactionMgr;
    }

    public PluginMgr getPluginMgr() {
        return pluginMgr;
    }

    public Auth getAuth() {
        return auth;
    }

    public AccessControllerManager getAccessManager() {
        return accessManager;
    }

    public AuthenticatorManager getAuthenticatorManager() {
        return authenticatorManager;
    }

    public MTMVService getMtmvService() {
        return mtmvService;
    }

    public EventProcessor getEventProcessor() {
        return eventProcessor;
    }

    public InsertOverwriteManager getInsertOverwriteManager() {
        return insertOverwriteManager;
    }

    public TabletScheduler getTabletScheduler() {
        return tabletScheduler;
    }

    public TabletChecker getTabletChecker() {
        return tabletChecker;
    }

    public AuditEventProcessor getAuditEventProcessor() {
        return auditEventProcessor;
    }

    public ComputeGroupMgr getComputeGroupMgr() {
        return computeGroupMgr;
    }

    public WorkloadGroupMgr getWorkloadGroupMgr() {
        return workloadGroupMgr;
    }

    public WorkloadSchedPolicyMgr getWorkloadSchedPolicyMgr() {
        return workloadSchedPolicyMgr;
    }

    public WorkloadRuntimeStatusMgr getWorkloadRuntimeStatusMgr() {
        return workloadRuntimeStatusMgr;
    }

    public AdmissionControl getAdmissionControl() {
        return admissionControl;
    }

    public ExternalMetaIdMgr getExternalMetaIdMgr() {
        return externalMetaIdMgr;
    }

    public MetastoreEventsProcessor getMetastoreEventsProcessor() {
        return metastoreEventsProcessor;
    }

    public PlsqlManager getPlsqlManager() {
        return plsqlManager;
    }

    // use this to get correct ClusterInfoService instance
    public static SystemInfoService getCurrentSystemInfo() {
        return getCurrentEnv().getClusterInfo();
    }

    public static HeartbeatMgr getCurrentHeartbeatMgr() {
        return getCurrentEnv().getHeartbeatMgr();
    }

    // use this to get correct TabletInvertedIndex instance
    public static TabletInvertedIndex getCurrentInvertedIndex() {
        return getCurrentEnv().getTabletInvertedIndex();
    }

    // use this to get correct ColocateTableIndex instance
    public static ColocateTableIndex getCurrentColocateIndex() {
        return getCurrentEnv().getColocateTableIndex();
    }

    public static CatalogRecycleBin getCurrentRecycleBin() {
        return getCurrentEnv().getRecycleBin();
    }

    // use this to get correct env's journal version
    public static int getCurrentEnvJournalVersion() {
        if (MetaContext.get() == null) {
            return FeMetaVersion.VERSION_CURRENT;
        }
        return MetaContext.get().getMetaVersion();
    }

    public static final boolean isCheckpointThread() {
        return Thread.currentThread().getId() == checkpointThreadId;
    }

    public static PluginMgr getCurrentPluginMgr() {
        return getCurrentEnv().getPluginMgr();
    }

    public static AuditEventProcessor getCurrentAuditEventProcessor() {
        return getCurrentEnv().getAuditEventProcessor();
    }

    // For unit test only
    public Checkpoint getCheckpointer() {
        return checkpointer;
    }

    public HiveTransactionMgr getHiveTransactionMgr() {
        return hiveTransactionMgr;
    }

    public static HiveTransactionMgr getCurrentHiveTransactionMgr() {
        return getCurrentEnv().getHiveTransactionMgr();
    }

    public DNSCache getDnsCache() {
        return dnsCache;
    }

    public List<String> getForceSkipJournalIds() {
        return forceSkipJournalIds;
    }

    // Use tryLock to avoid potential dead lock
    private boolean tryLock(boolean mustLock) {
        while (true) {
            try {
                if (!lock.tryLock(Config.catalog_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
                    // to see which thread held this lock for long time.
                    Thread owner = lock.getOwner();
                    if (owner != null) {
                        LOG.info("env lock is held by: {}", Util.dumpThread(owner, 10));
                    }

                    if (mustLock) {
                        continue;
                    } else {
                        return false;
                    }
                }
                return true;
            } catch (InterruptedException e) {
                LOG.warn("got exception while getting env lock", e);
                if (mustLock) {
                    continue;
                } else {
                    return lock.isHeldByCurrentThread();
                }
            }
        }
    }

    private void unlock() {
        if (lock.isHeldByCurrentThread()) {
            this.lock.unlock();
        }
    }

    public String getBdbDir() {
        return bdbDir;
    }

    public String getImageDir() {
        return imageDir;
    }

    public void initialize(String[] args) throws Exception {
        // set meta dir first.
        // we already set these variables in constructor. but Catalog is a singleton class.
        // so they may be set before Config is initialized.
        // set them here again to make sure these variables use values in fe.conf.
        this.metaDir = Config.meta_dir;
        this.bdbDir = this.metaDir + BDB_DIR;
        this.imageDir = this.metaDir + IMAGE_DIR;

        // 0. get local node and helper node info
        getSelfHostPort();
        getHelperNodes(args);

        // 1. check and create dirs and files
        File meta = new File(metaDir);
        if (!meta.exists()) {
            LOG.warn("Doris' meta dir {} does not exist." + " You need to create it before starting FE",
                    meta.getAbsolutePath());
            throw new Exception(meta.getAbsolutePath() + " does not exist, will exit");
        }

        if (Config.edit_log_type.equalsIgnoreCase("bdb")) {
            File bdbDir = new File(this.bdbDir);
            if (!bdbDir.exists()) {
                bdbDir.mkdirs();
            }
        }
        File imageDir = new File(this.imageDir);
        if (!imageDir.exists()) {
            imageDir.mkdirs();
        }

        // init plugin manager
        pluginMgr.init();
        auditEventProcessor.start();

        // 2. get cluster id and role (Observer or Follower)
        if (!Config.enable_check_compatibility_mode) {
            checkDeployMode();
            getClusterIdAndRole();
        } else {
            isElectable = true;
            role = FrontendNodeType.FOLLOWER;
            nodeName = genFeNodeName(selfNode.getHost(),
                    selfNode.getPort(), false /* new style */);
        }

        // 3. Load image first and replay edits
        this.editLog = new EditLog(nodeName);
        loadImage(this.imageDir); // load image file
        editLog.open(); // open bdb env
        this.globalTransactionMgr.setEditLog(editLog);
        this.idGenerator.setEditLog(editLog);

        if (Config.enable_check_compatibility_mode) {
            replayJournalsAndExit();
        }

        // 4. create load and export job label cleaner thread
        createLabelCleaner();

        // 5. create txn cleaner thread
        createTxnCleaner();

        // 6. start state listener thread
        startStateListener();

        // 7. create fe disk updater
        createFeDiskUpdater();

        if (!Config.edit_log_type.equalsIgnoreCase("bdb")) {
            // If not using bdb, we need to notify the FE type transfer manually.
            notifyNewFETypeTransfer(FrontendNodeType.MASTER);
        }
        queryCancelWorker.start();

        StmtExecutor.initBlockSqlAstNames();
    }

    // wait until FE is ready.
    public void waitForReady() throws InterruptedException {
        long counter = 0;
        while (true) {
            if (isReady()) {
                LOG.info("catalog is ready. FE type: {}", feType);
                break;
            }

            Thread.sleep(100);
            if (counter++ % 100 == 0) {
                String reason = editLog == null ? "editlog is null" : editLog.getNotReadyReason();
                LOG.info("wait catalog to be ready. feType:{} isReady:{}, counter:{} reason: {}",
                        feType, isReady.get(), counter, reason);
            }
        }
    }

    public boolean isReady() {
        return isReady.get();
    }

    public boolean isHttpReady() {
        return httpReady.get();
    }

    public void setHttpReady(boolean httpReady) {
        this.httpReady.set(httpReady);
    }

    protected boolean isStartFromEmpty() {
        File roleFile = new File(this.imageDir, Storage.ROLE_FILE);
        File versionFile = new File(this.imageDir, Storage.VERSION_FILE);

        return !roleFile.exists() && !versionFile.exists();
    }

    private void getClusterIdFromStorage(Storage storage) throws IOException {
        clusterId = storage.getClusterID();
        if (Config.cluster_id != -1 && Config.cluster_id != this.clusterId) {
            LOG.warn("Configured cluster_id {} does not match stored cluster_id {}. "
                     + "This may indicate a configuration error.",
                     Config.cluster_id, this.clusterId);
            throw new IOException("Configured cluster_id does not match stored cluster_id. "
                                + "Please check your configuration.");
        }
    }

    protected void getClusterIdAndRole() throws IOException {
        File roleFile = new File(this.imageDir, Storage.ROLE_FILE);
        File versionFile = new File(this.imageDir, Storage.VERSION_FILE);

        // if helper node is point to self, or there is ROLE and VERSION file in local.
        // get the node type from local
        if (isMyself() || (roleFile.exists() && versionFile.exists())) {

            if (!isMyself()) {
                LOG.info("find ROLE and VERSION file in local, ignore helper nodes: {}", helperNodes);
            }

            // check file integrity, if has.
            if ((roleFile.exists() && !versionFile.exists()) || (!roleFile.exists() && versionFile.exists())) {
                throw new IOException("role file and version file must both exist or both not exist. "
                        + "please specific one helper node to recover. will exit.");
            }

            // ATTN:
            // If the version file and role file does not exist and the helper node is itself,
            // this should be the very beginning startup of the cluster, so we create ROLE and VERSION file,
            // set isFirstTimeStartUp to true, and add itself to frontends list.
            // If ROLE and VERSION file is deleted for some reason, we may arbitrarily start this node as
            // FOLLOWER, which may cause UNDEFINED behavior.
            // Everything may be OK if the origin role is exactly FOLLOWER,
            // but if not, FE process will exit somehow.
            Storage storage = new Storage(this.imageDir);
            if (!roleFile.exists()) {
                // The very first time to start the first node of the cluster.
                // It should became a Master node (Master node's role is also FOLLOWER, which means electable)

                // For compatibility. Because this is the very first time to start, so we arbitrarily choose
                // a new name for this node
                role = FrontendNodeType.FOLLOWER;
                nodeName = genFeNodeName(selfNode.getHost(),
                        selfNode.getPort(), false /* new style */);
                storage.writeFrontendRoleAndNodeName(role, nodeName);
                LOG.info("very first time to start this node. role: {}, node name: {}", role.name(), nodeName);
            } else {
                role = storage.getRole();
                if (role == FrontendNodeType.REPLICA) {
                    // for compatibility
                    role = FrontendNodeType.FOLLOWER;
                }

                nodeName = storage.getNodeName();
                if (Strings.isNullOrEmpty(nodeName)) {
                    // In normal case, if ROLE file exist, role and nodeName should both exist.
                    // But we will get a empty nodeName after upgrading.
                    // So for forward compatibility, we use the "old-style" way of naming: "ip_port",
                    // and update the ROLE file.
                    nodeName = genFeNodeName(selfNode.getHost(), selfNode.getPort(), true/* old style */);
                    storage.writeFrontendRoleAndNodeName(role, nodeName);
                    LOG.info("forward compatibility. role: {}, node name: {}", role.name(), nodeName);
                }
                // Notice:
                // With the introduction of FQDN, the nodeName is no longer bound to an IP address,
                // so consistency is no longer checked here. Otherwise, the startup will fail.
            }

            Preconditions.checkNotNull(role);
            Preconditions.checkNotNull(nodeName);

            if (!versionFile.exists()) {
                clusterId = Config.cluster_id == -1 ? Storage.newClusterID() : Config.cluster_id;
                token = Strings.isNullOrEmpty(Config.auth_token) ? Storage.newToken() : Config.auth_token;
                storage = new Storage(clusterId, token, this.imageDir);
                storage.writeClusterIdAndToken();

                isFirstTimeStartUp = true;
                Frontend self = new Frontend(role, nodeName, selfNode.getHost(),
                        selfNode.getPort());
                // Set self alive to true, the BDBEnvironment.getReplicationGroupAdmin() will rely on this to get
                // helper node, before the heartbeat thread is started.
                self.setIsAlive(true);
                // We don't need to check if frontends already contains self.
                // frontends must be empty cause no image is loaded and no journal is replayed yet.
                // And this frontend will be persisted later after opening bdbje environment.
                frontends.put(nodeName, self);
                LOG.info("add self frontend: {}", self);
            } else {
                getClusterIdFromStorage(storage);
                if (storage.getToken() == null) {
                    token = Strings.isNullOrEmpty(Config.auth_token) ? Storage.newToken() : Config.auth_token;
                    LOG.info("refresh new token");
                    storage.setToken(token);
                    storage.writeClusterIdAndToken();
                } else {
                    token = storage.getToken();
                }
                isFirstTimeStartUp = false;
            }
        } else {
            // try to get role and node name from helper node,
            // this loop will not end until we get certain role type and name
            while (true) {
                if (!getFeNodeTypeAndNameFromHelpers()) {
                    LOG.warn("current node {} is not added to the group. please add it first. "
                            + "sleep 5 seconds and retry, current helper nodes: {}", selfNode, helperNodes);
                    try {
                        Thread.sleep(5000);
                        continue;
                    } catch (InterruptedException e) {
                        LOG.warn("", e);
                        System.exit(-1);
                    }
                }

                if (role == FrontendNodeType.REPLICA) {
                    // for compatibility
                    role = FrontendNodeType.FOLLOWER;
                }
                break;
            }

            Preconditions.checkState(helperNodes.size() == 1);
            Preconditions.checkNotNull(role);
            Preconditions.checkNotNull(nodeName);

            HostInfo rightHelperNode = helperNodes.get(0);

            Storage storage = new Storage(this.imageDir);
            if (roleFile.exists() && (role != storage.getRole() || !nodeName.equals(storage.getNodeName()))
                    || !roleFile.exists()) {
                storage.writeFrontendRoleAndNodeName(role, nodeName);
            }
            if (!versionFile.exists()) {
                // If the version file doesn't exist, download it from helper node
                if (!getVersionFileFromHelper(rightHelperNode)) {
                    throw new IOException("fail to download version file from "
                            + rightHelperNode.getHost() + " will exit.");
                }

                // NOTE: cluster_id will be init when Storage object is constructed,
                //       so we new one.
                storage = new Storage(this.imageDir);
                getClusterIdFromStorage(storage);
                token = storage.getToken();
                if (Strings.isNullOrEmpty(token)) {
                    token = Config.auth_token;
                }
            } else {
                // If the version file exist, read the cluster id and check the
                // id with helper node to make sure they are identical
                getClusterIdFromStorage(storage);
                token = storage.getToken();
                try {
                    String url = "http://" + NetUtils
                            .getHostPortInAccessibleFormat(rightHelperNode.getHost(), Config.http_port) + "/check";
                    HttpURLConnection conn = HttpURLUtil.getConnectionWithNodeIdent(url);
                    conn.setConnectTimeout(2 * 1000);
                    conn.setReadTimeout(2 * 1000);
                    String clusterIdString = conn.getHeaderField(MetaBaseAction.CLUSTER_ID);
                    int remoteClusterId = Integer.parseInt(clusterIdString);
                    if (remoteClusterId != clusterId) {
                        LOG.error("cluster id is not equal with helper node {}. will exit.",
                                rightHelperNode.getHost());
                        throw new IOException(
                                "cluster id is not equal with helper node "
                                        + rightHelperNode.getHost() + ". will exit.");
                    }
                    String remoteToken = conn.getHeaderField(MetaBaseAction.TOKEN);
                    if (token == null && remoteToken != null) {
                        LOG.info("get token from helper node. token={}.", remoteToken);
                        token = remoteToken;
                        storage.writeClusterIdAndToken();
                        storage.reload();
                    }
                    if (Config.enable_token_check) {
                        Preconditions.checkNotNull(token);
                        Preconditions.checkNotNull(remoteToken);
                        if (!token.equals(remoteToken)) {
                            throw new IOException(
                                    "token is not equal with helper node "
                                            + rightHelperNode.getHost() + ". will exit.");
                        }
                    }
                } catch (Exception e) {
                    throw new IOException("fail to check cluster_id and token with helper node.", e);
                }
            }

            getNewImage(rightHelperNode);
        }

        if (Config.cluster_id != -1 && clusterId != Config.cluster_id) {
            throw new IOException("cluster id is not equal with config item cluster_id. will exit. "
                    + "If you are in recovery mode, please also modify the cluster_id in 'doris-meta/image/VERSION'");
        }

        if (role.equals(FrontendNodeType.FOLLOWER)) {
            isElectable = true;
        } else {
            isElectable = false;
        }

        Preconditions.checkState(helperNodes.size() == 1);
        LOG.info("finished to get cluster id: {}, isElectable: {}, role: {} and node name: {}",
                clusterId, isElectable, role.name(), nodeName);
    }

    /**
     * write cloud/local to MODE_FILE.
     */
    protected void checkDeployMode() throws IOException {
        File modeFile = new File(this.imageDir, Storage.DEPLOY_MODE_FILE);
        Storage storage = new Storage(this.imageDir);
        String expectedMode = getDeployMode();
        if (modeFile.exists()) {
            String actualMode = storage.getDeployMode();
            Preconditions.checkArgument(expectedMode.equals(actualMode),
                    "You can't switch deploy mode from %s to %s, maybe you need to check fe.conf",
                    actualMode, expectedMode);
            LOG.info("The current deployment mode is " + expectedMode + ".");
        } else {
            storage.setDeployMode(expectedMode);
            storage.writeClusterMode();
            LOG.info("The file DEPLOY_MODE doesn't exist, create it.");
            File versionFile = new File(this.imageDir, Storage.VERSION_FILE);
            if (versionFile.exists()) {
                LOG.warn("This may be an upgrade from old version, "
                        + "or the DEPLOY_MODE file has been manually deleted");
            }
        }
    }

    public static String genFeNodeName(String host, int port, boolean isOldStyle) {
        if (isOldStyle) {
            return host + "_" + port;
        } else {
            return "fe_" + UUID.randomUUID().toString().replace("-", "_");
        }
    }

    // Get the role info and node name from helper node.
    // return false if failed.
    protected boolean getFeNodeTypeAndNameFromHelpers() {
        // we try to get info from helper nodes, once we get the right helper node,
        // other helper nodes will be ignored and removed.
        HostInfo rightHelperNode = null;
        for (HostInfo helperNode : helperNodes) {
            try {
                // For upgrade compatibility, the host parameter name remains the same
                // and the new hostname parameter is added
                String url = "http://" + NetUtils.getHostPortInAccessibleFormat(helperNode.getHost(), Config.http_port)
                        + "/role?host=" + selfNode.getHost()
                        + "&port=" + selfNode.getPort();
                HttpURLConnection conn = HttpURLUtil.getConnectionWithNodeIdent(url);
                if (conn.getResponseCode() != 200) {
                    LOG.warn("failed to get fe node type from helper node: {}. response code: {}", helperNode,
                            conn.getResponseCode());
                    continue;
                }

                String type = conn.getHeaderField("role");
                if (type == null) {
                    LOG.warn("failed to get fe node type from helper node: {}.", helperNode);
                    continue;
                }
                role = FrontendNodeType.valueOf(type);
                nodeName = conn.getHeaderField("name");

                // get role and node name before checking them, because we want to throw any exception
                // as early as we encounter.

                if (role == FrontendNodeType.UNKNOWN) {
                    LOG.warn("frontend {} is not added to cluster yet. role UNKNOWN", selfNode);
                    return false;
                }

                if (Strings.isNullOrEmpty(nodeName)) {
                    // For forward compatibility, we use old-style name: "ip_port"
                    nodeName = genFeNodeName(selfNode.getHost(), selfNode.getPort(), true /* old style */);
                }
            } catch (Exception e) {
                LOG.warn("failed to get fe node type from helper node: {}.", helperNode, e);
                continue;
            }

            LOG.info("get fe node type {}, name {} from {}:{}:{}", role, nodeName,
                    helperNode.getHost(), helperNode.getHost(), Config.http_port);
            rightHelperNode = helperNode;
            break;
        }

        if (rightHelperNode == null) {
            return false;
        }

        helperNodes.clear();
        helperNodes.add(rightHelperNode);
        return true;
    }

    private void getSelfHostPort() {
        String host = Strings.nullToEmpty(FrontendOptions.getLocalHostAddress());
        selfNode = new HostInfo(host, Config.edit_log_port);
        LOG.info("get self node: {}", selfNode);
    }

    private void getHelperNodes(String[] args) throws Exception {
        String helpers = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-helper")) {
                if (i + 1 >= args.length) {
                    throw new AnalysisException("-helper need parameter host:port,host:port");
                }
                helpers = args[i + 1];
                break;
            }
        }

        if (!Config.enable_deploy_manager.equalsIgnoreCase("disable")) {
            if (Config.enable_deploy_manager.equalsIgnoreCase("k8s")) {
                deployManager = new K8sDeployManager(this, 5000 /* 5s interval */);
            } else if (Config.enable_deploy_manager.equalsIgnoreCase("ambari")) {
                deployManager = new AmbariDeployManager(this, 5000 /* 5s interval */);
            } else if (Config.enable_deploy_manager.equalsIgnoreCase("local")) {
                deployManager = new LocalFileDeployManager(this, 5000 /* 5s interval */);
            } else {
                throw new AnalysisException("Unknow deploy manager: " + Config.enable_deploy_manager);
            }

            getHelperNodeFromDeployManager();

        } else {
            if (helpers != null) {
                String[] splittedHelpers = helpers.split(",");
                for (String helper : splittedHelpers) {
                    HostInfo helperHostPort = SystemInfoService.getHostAndPort(helper);
                    if (helperHostPort.isSame(selfNode)) {
                        /**
                         * If user specified the helper node to this FE itself,
                         * we will stop the starting FE process and report an error.
                         * First, it is meaningless to point the helper to itself.
                         * Secondly, when some users add FE for the first time, they will mistakenly
                         * point the helper that should have pointed to the Master to themselves.
                         * In this case, some errors have caused users to be troubled.
                         * So here directly exit the program and inform the user to avoid unnecessary trouble.
                         */
                        throw new AnalysisException("Do not specify the helper node to FE itself. "
                                + "Please specify it to the existing running Master or Follower FE");
                    }
                    helperNodes.add(helperHostPort);
                }
            } else {
                // If helper node is not designated, use local node as helper node.
                helperNodes.add(new HostInfo(selfNode.getHost(), Config.edit_log_port));
            }
        }

        LOG.info("get helper nodes: {}", helperNodes);
    }

    @SuppressWarnings("unchecked")
    private void getHelperNodeFromDeployManager() throws Exception {
        Preconditions.checkNotNull(deployManager);

        // 1. check if this is the first time to start up
        File roleFile = new File(this.imageDir, Storage.ROLE_FILE);
        File versionFile = new File(this.imageDir, Storage.VERSION_FILE);
        if ((roleFile.exists() && !versionFile.exists()) || (!roleFile.exists() && versionFile.exists())) {
            throw new Exception("role file and version file must both exist or both not exist. "
                    + "please specific one helper node to recover. will exit.");
        }

        if (roleFile.exists()) {
            // This is not the first time this node start up.
            // It should already added to FE group, just set helper node as it self.
            LOG.info("role file exist. this is not the first time to start up");
            helperNodes = Lists.newArrayList(new HostInfo(selfNode.getHost(),
                    Config.edit_log_port));
            return;
        }

        // This is the first time this node start up.
        // Get helper node from deploy manager.
        helperNodes = deployManager.getHelperNodes();
        if (helperNodes == null || helperNodes.isEmpty()) {
            throw new Exception("failed to get helper node from deploy manager. exit");
        }
    }

    @SuppressWarnings({"checkstyle:WhitespaceAfter", "checkstyle:LineLength"})
    private void transferToMaster() {
        try {
            // stop replayer
            if (replayer != null) {
                replayer.exit();
                try {
                    replayer.join();
                } catch (InterruptedException e) {
                    LOG.warn("got exception when stopping the replayer thread", e);
                }
                replayer = null;
            }

            // set this after replay thread stopped. to avoid replay thread modify them.
            isReady.set(false);
            canRead.set(false);

            toMasterProgress = "open editlog";
            editLog.open();

            if (Config.edit_log_type.equalsIgnoreCase("bdb")) {
                if (!haProtocol.fencing()) {
                    LOG.error("fencing failed. will exit.");
                    System.exit(-1);
                }
            }

            toMasterProgress = "replay journal";
            long replayStartTime = System.currentTimeMillis();
            // replay journals. -1 means replay all the journals larger than current journal id.
            replayJournal(-1);
            long replayEndTime = System.currentTimeMillis();
            LOG.info("finish replay in " + (replayEndTime - replayStartTime) + " msec");

            removeDroppedFrontends(removedFrontends);

            if (Config.enable_check_compatibility_mode) {
                String msg = "check metadata compatibility successfully";
                LOG.info(msg);
                LogUtils.stdout(msg);
                System.exit(0);
            }

            checkCurrentNodeExist();

            checkBeExecVersion();

            toMasterProgress = "roll editlog";
            editLog.rollEditLog();

            if (Config.enable_advance_next_id) {
                advanceNextId();
            }

            // Log meta_version
            long journalVersion = MetaContext.get().getMetaVersion();
            if (journalVersion < FeConstants.meta_version) {
                toMasterProgress = "log meta version";
                editLog.logMetaVersion(FeConstants.meta_version);
                MetaContext.get().setMetaVersion(FeConstants.meta_version);
            }

            // Log the first frontend
            if (isFirstTimeStartUp) {
                // if isFirstTimeStartUp is true, frontends must contains this Node.
                Frontend self = frontends.get(nodeName);
                Preconditions.checkNotNull(self);
                // OP_ADD_FIRST_FRONTEND is emitted, so it can write to BDBJE even if canWrite is false
                editLog.logAddFirstFrontend(self);

                initLowerCaseTableNames();
                // Set initial root password if master FE first time launch.
                auth.setInitialRootPassword(Config.initial_root_password);
            } else {
                VariableMgr.forceUpdateVariables();
                if (journalVersion <= FeMetaVersion.VERSION_114) {
                    // if journal version is less than 114, which means it is upgraded from version before 2.0.
                    // When upgrading from 1.2 to 2.0,
                    // we need to make sure that the parallelism of query remain unchanged
                    // when switch to pipeline engine, otherwise it may impact the load of entire cluster
                    // because the default parallelism of pipeline engine is higher than previous version.
                    // so set parallel_pipeline_task_num to parallel_fragment_exec_instance_num
                    int newVal = VariableMgr.newSessionVariable().parallelExecInstanceNum;
                    VariableMgr.refreshDefaultSessionVariables("1.x to 2.x",
                            SessionVariable.PARALLEL_PIPELINE_TASK_NUM,
                            String.valueOf(newVal));

                    // similar reason as above, need to upgrade broadcast scale factor during 1.2 to 2.x
                    // if the default value has been upgraded
                    double newBcFactorVal = VariableMgr.newSessionVariable().getBroadcastRightTableScaleFactor();
                    VariableMgr.refreshDefaultSessionVariables("1.x to 2.x",
                            SessionVariable.BROADCAST_RIGHT_TABLE_SCALE_FACTOR,
                            String.valueOf(newBcFactorVal));

                    // similar reason as above, need to upgrade enable_nereids_planner to true
                    VariableMgr.refreshDefaultSessionVariables("1.x to 2.x", SessionVariable.ENABLE_NEREIDS_PLANNER,
                            "true");
                }
                if (journalVersion <= FeMetaVersion.VERSION_123) {
                    VariableMgr.refreshDefaultSessionVariables("2.0 to 2.1", SessionVariable.ENABLE_NEREIDS_DML,
                            "true");
                    VariableMgr.refreshDefaultSessionVariables("2.0 to 2.1",
                            SessionVariable.FRAGMENT_TRANSMISSION_COMPRESSION_CODEC, "none");
                    if (VariableMgr.newSessionVariable().nereidsTimeoutSecond == 5) {
                        VariableMgr.refreshDefaultSessionVariables("2.0 to 2.1",
                                SessionVariable.NEREIDS_TIMEOUT_SECOND, "30");
                    }
                }
                if (journalVersion <= FeMetaVersion.VERSION_129) {
                    VariableMgr.refreshDefaultSessionVariables("2.1 to 3.0", SessionVariable.ENABLE_NEREIDS_PLANNER,
                            "true");
                    VariableMgr.refreshDefaultSessionVariables("2.1 to 3.0", SessionVariable.ENABLE_NEREIDS_DML,
                            "true");
                    VariableMgr.refreshDefaultSessionVariables("2.1 to 3.0",
                            SessionVariable.ENABLE_FALLBACK_TO_ORIGINAL_PLANNER,
                            "false");
                    VariableMgr.refreshDefaultSessionVariables("2.1 to 3.0",
                            SessionVariable.ENABLE_MATERIALIZED_VIEW_REWRITE,
                            "true");
                }
            }

            getPolicyMgr().createDefaultStoragePolicy();

            // MUST set master ip before starting checkpoint thread.
            // because checkpoint thread need this info to select non-master FE to push image

            toMasterProgress = "log master info";
            this.masterInfo = new MasterInfo(Env.getCurrentEnv().getSelfNode().getHost(),
                    Config.http_port,
                    Config.rpc_port);
            editLog.logMasterInfo(masterInfo);
            LOG.info("logMasterInfo:{}", masterInfo);

            // for master, the 'isReady' is set behind.
            // but we are sure that all metadata is replayed if we get here.
            // so no need to check 'isReady' flag in this method
            postProcessAfterMetadataReplayed(false);

            insertOverwriteManager.allTaskFail();

            toMasterProgress = "start daemon threads";

            // coz current fe was not master fe and didn't get all fes' alive session report before, which cause
            // sessionReportTimeMap is not up-to-date.
            // reset all session's last heartbeat time. must run before init of TemporaryTableMgr
            refreshAllAliveSession();

            // start all daemon threads that only running on MASTER FE
            startMasterOnlyDaemonThreads();
            // start other daemon threads that should running on all FE
            startNonMasterDaemonThreads();

            MetricRepo.init();

            toMasterProgress = "finished";
            canRead.set(true);
            isReady.set(true);
            checkLowerCaseTableNames();

            String msg = "master finished to replay journal, can write now.";
            LogUtils.stdout(msg);
            LOG.info(msg);
            // for master, there are some new thread pools need to register metric
            ThreadPoolManager.registerAllThreadPoolMetric();
            if (analysisManager != null) {
                analysisManager.getStatisticsCache().preHeat();
            }
        } catch (Throwable e) {
            // When failed to transfer to master, we need to exit the process.
            // Otherwise, the process will be in an unknown state.
            LOG.error("failed to transfer to master. progress: {}", toMasterProgress, e);
            System.exit(-1);
        }
    }

    /*
     * Advance the id generator, ensuring it doesn't roll back.
     *
     * If we need to support time travel, the next id cannot be rolled back to avoid
     * errors in the corresponding relationship of the metadata recorded in BE/MS.
     */
    void advanceNextId() {
        long currentId = idGenerator.getBatchEndId();
        long currentMill = System.currentTimeMillis();
        long nextId = currentId + 1;
        // Reserve ~1 trillion for use in case of bugs or frequent reboots (~2 billion reboots)
        if ((1L << 63) - nextId < (1L << 40)) {
            LOG.warn("nextId is too large: {}, it may be a bug and consider backup and migration", nextId);
        } else {
            // Keep compatible with previous impl, the previous impl may result in extreme large nextId,
            // and guess there are no more than 1L<<32 (~4e9) ids used since last reboot
            nextId = (currentId + 1) < currentMill ? currentMill : currentId + (1L << 32);
        }

        // ATTN: Because MetaIdGenerator has guaranteed that each id it returns must have
        // been persisted, there is no need to perform persistence again here.
        idGenerator.setId(nextId);

        LOG.info("advance the next id from {} to {}", currentId, nextId);
    }

    /*
     * There are something need to do after metadata is replayed, such as
     * 1. bug fix for metadata
     * 2. register some hook.
     * If there is, add them here.
     */
    public boolean postProcessAfterMetadataReplayed(boolean waitCatalogReady) {
        if (waitCatalogReady) {
            while (!isReady()) {
                // Avoid endless waiting if the state has changed.
                //
                // Consider the following situation:
                // 1. The follower replay journals and is not set to ready because the synchronization internval
                //    exceeds meta delay toleration seconds.
                // 2. The underlying BEBJE node of this follower is selected as the master, but the state listener
                //    thread is waiting for catalog ready.
                if (typeTransferQueue.peek() != null) {
                    return false;
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOG.warn("", e);
                }
            }
        }

        auth.rectifyPrivs();
        catalogMgr.registerCatalogRefreshListener(this);
        // MTMV needs to be compatible with old metadata, and during the compatibility process,
        // it needs to wait for all catalog data to be ready, so it cannot be processed through gsonPostProcess()
        // We catch all possible exceptions to avoid FE startup failure
        try {
            MTMVUtil.compatibleMTMV(catalogMgr);
        } catch (Throwable t) {
            LOG.warn("compatibleMTMV failed", t);
        }
        return true;
    }

    // start all daemon threads only running on Master
    protected void startMasterOnlyDaemonThreads() {
        // start checkpoint thread
        checkpointer = new Checkpoint(editLog);
        checkpointer.setMetaContext(metaContext);
        // set "checkpointThreadId" before the checkpoint thread start, because the thread
        // need to check the "checkpointThreadId" when running.
        checkpointThreadId = checkpointer.getId();

        checkpointer.start();
        LOG.info("checkpointer thread started. thread id is {}", checkpointThreadId);

        // heartbeat mgr
        heartbeatMgr.setMaster(clusterId, token, epoch);
        heartbeatMgr.start();

        // alive session of all fes' mgr
        feSessionMgr.setClusterId(clusterId);
        feSessionMgr.setToken(token);
        feSessionMgr.start();

        temporaryTableMgr.start();

        // New load scheduler
        pendingLoadTaskScheduler.start();
        loadingLoadTaskScheduler.start();
        loadManager.prepareJobs();
        loadJobScheduler.start();
        loadEtlChecker.start();
        loadLoadingChecker.start();
        if (Config.isNotCloudMode()) {
            // Tablet checker and scheduler
            tabletChecker.start();
            tabletScheduler.start();
            // Colocate tables checker and balancer
            ColocateTableCheckerAndBalancer.getInstance().start();
            // Publish Version Daemon
            publishVersionDaemon.start();
            // Start txn cleaner
            txnCleaner.start();
            // Consistency checker
            getConsistencyChecker().start();
            // start daemon thread to update global partition version and in memory information periodically
            partitionInfoCollector.start();
        }
        jobManager.start();
        // transient task manager
        transientTaskManager.start();
        // Alter
        getAlterInstance().start();
        // Backup handler
        getBackupHandler().start();
        // catalog recycle bin
        getRecycleBin().start();
        // time printer
        createTimePrinter();
        timePrinter.start();
        // deploy manager
        if (!Config.enable_deploy_manager.equalsIgnoreCase("disable")) {
            LOG.info("deploy manager {} start", deployManager.getName());
            deployManager.start();
            deployManager.startListener();
        }
        // start routine load scheduler
        routineLoadScheduler.start();
        routineLoadTaskScheduler.start();
        // start dynamic partition task
        dynamicPartitionScheduler.start();
        // start daemon thread to update db used data quota for db txn manager periodically
        dbUsedDataQuotaInfoCollector.start();
        if (Config.enable_storage_policy) {
            cooldownConfHandler.start();
        }
        streamLoadRecordMgr.start();
        tabletLoadIndexRecorderMgr.start();
        new InternalSchemaInitializer().start();
        getRefreshManager().start();

        // binlog gcer
        binlogGcer.start();
        columnIdFlusher.start();
        insertOverwriteManager.start();
        dictionaryManager.start();

        TopicPublisher wgPublisher = new WorkloadGroupPublisher(this);
        topicPublisherThread.addToTopicPublisherList(wgPublisher);
        WorkloadSchedPolicyPublisher wpPublisher = new WorkloadSchedPolicyPublisher(this);
        topicPublisherThread.addToTopicPublisherList(wpPublisher);
        topicPublisherThread.start();

        workloadGroupCheckerThread.start();

        // auto analyze related threads.
        statisticsCleaner.start();
        statisticsAutoCollector.start();
        statisticsJobAppender.start();
    }

    // start threads that should run on all FE
    protected void startNonMasterDaemonThreads() {
        // start load manager thread
        tokenManager.start();
        loadManager.start();
        tabletStatMgr.start();

        // load and export job label cleaner thread
        labelCleaner.start();
        // es repository
        getInternalCatalog().getEsRepository().start();
        // domain resolver
        domainResolver.start();
        // fe disk updater
        feDiskUpdater.start();

        metastoreEventsProcessor.start();

        dnsCache.start();

        workloadSchedPolicyMgr.start();
        workloadRuntimeStatusMgr.start();
        admissionControl.start();
        splitSourceManager.start();
    }

    private void transferToNonMaster(FrontendNodeType newType) {
        isReady.set(false);

        try {
            if (feType == FrontendNodeType.OBSERVER || feType == FrontendNodeType.FOLLOWER) {
                Preconditions.checkState(newType == FrontendNodeType.UNKNOWN);
                LOG.warn("{} to UNKNOWN, still offer read service", feType.name());
                // not set canRead here, leave canRead as what is was.
                // if meta out of date, canRead will be set to false in replayer thread.
                metaReplayState.setTransferToUnknown();
                return;
            }

            // transfer from INIT/UNKNOWN to OBSERVER/FOLLOWER

            if (replayer == null) {
                createReplayer();
                replayer.start();
            }

            // 'isReady' will be set to true in 'setCanRead()' method
            if (!postProcessAfterMetadataReplayed(true)) {
                // the state has changed, exit early.
                return;
            }

            checkLowerCaseTableNames();

            startNonMasterDaemonThreads();

            MetricRepo.init();

            if (analysisManager != null) {
                analysisManager.getStatisticsCache().preHeat();
            }

            if (followerColumnSender == null) {
                followerColumnSender = new FollowerColumnSender();
                followerColumnSender.start();
            }
        } catch (Throwable e) {
            // When failed to transfer to non-master, we need to exit the process.
            // Otherwise, the process will be in an unknown state.
            LOG.error("failed to transfer to non-master.", e);
            System.exit(-1);
        }
    }

    // Set global variable 'lower_case_table_names' only when the cluster is initialized.
    private void initLowerCaseTableNames() {
        if (Config.lower_case_table_names > 2 || Config.lower_case_table_names < 0) {
            LOG.error("Unsupported configuration value of lower_case_table_names: " + Config.lower_case_table_names);
            System.exit(-1);
        }
        try {
            VariableMgr.setLowerCaseTableNames(Config.lower_case_table_names);
        } catch (Exception e) {
            LOG.error("Initialization of lower_case_table_names failed.", e);
            System.exit(-1);
        }
        LOG.info("Finish initializing lower_case_table_names, value is {}", GlobalVariable.lowerCaseTableNames);
    }

    // After the cluster initialization is complete, 'lower_case_table_names' can not be modified during the cluster
    // restart or upgrade.
    private void checkLowerCaseTableNames() {
        while (!isReady()) {
            // Waiting for lower_case_table_names to initialize value from image or editlog.
            try {
                LOG.info("Waiting for \'lower_case_table_names\' initialization.");
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                LOG.error("Sleep got exception while waiting for lower_case_table_names initialization. ", e);
            }
        }
        if (Config.lower_case_table_names != GlobalVariable.lowerCaseTableNames) {
            LOG.error("The configuration of \'lower_case_table_names\' does not support modification, "
                            + "the expected value is {}, but the actual value is {}",
                    GlobalVariable.lowerCaseTableNames,
                    Config.lower_case_table_names);
            System.exit(-1);
        }
        LOG.info("lower_case_table_names is {}", GlobalVariable.lowerCaseTableNames);
    }

    /*
     * If the current node is not in the frontend list, then exit. This may
     * happen when this node is removed from frontend list, and the drop
     * frontend log is deleted because of checkpoint.
     */
    private void checkCurrentNodeExist() {
        boolean metadataFailureRecovery = null != System.getProperty(FeConstants.METADATA_FAILURE_RECOVERY_KEY);
        if (metadataFailureRecovery) {
            return;
        }

        Frontend fe = checkFeExist(selfNode.getHost(), selfNode.getPort());
        if (fe == null) {
            LOG.error("current node {}:{} is not added to the cluster, will exit."
                            + " Your FE IP maybe changed, please set 'priority_networks' config in fe.conf properly.",
                    selfNode.getHost(), selfNode.getPort());
            System.exit(-1);
        } else if (fe.getRole() != role) {
            LOG.error("current node role is {} not match with frontend recorded role {}. will exit", role,
                    fe.getRole());
            System.exit(-1);
        }
    }

    private void checkBeExecVersion() {
        if (Config.be_exec_version < Config.min_be_exec_version
                || Config.be_exec_version > Config.max_be_exec_version) {
            LOG.error("be_exec_version={} is not supported, please set be_exec_version in interval [{}, {}]",
                    Config.be_exec_version, Config.min_be_exec_version, Config.max_be_exec_version);
            System.exit(-1);
        }
    }

    protected boolean getVersionFileFromHelper(HostInfo helperNode) throws IOException {
        try {
            String url = "http://" + NetUtils.getHostPortInAccessibleFormat(helperNode.getHost(), Config.http_port)
                    + "/version";
            File dir = new File(this.imageDir);
            MetaHelper.getRemoteFile(url, HTTP_TIMEOUT_SECOND * 1000,
                    MetaHelper.getFile(Storage.VERSION_FILE, dir));
            MetaHelper.complete(Storage.VERSION_FILE, dir);
            return true;
        } catch (Exception e) {
            LOG.warn(e);
        }

        return false;
    }

    protected void getNewImage(HostInfo helperNode) throws IOException {
        long localImageVersion = 0;
        Storage storage = new Storage(this.imageDir);
        localImageVersion = storage.getLatestImageSeq();

        try {
            String hostPort = NetUtils.getHostPortInAccessibleFormat(helperNode.getHost(), Config.http_port);
            String infoUrl = "http://" + hostPort + "/info";
            ResponseBody<StorageInfo> responseBody = MetaHelper
                    .doGet(infoUrl, HTTP_TIMEOUT_SECOND * 1000, StorageInfo.class);
            if (responseBody.getCode() != RestApiStatusCode.OK.code) {
                LOG.warn("get image failed,responseBody:{}", responseBody);
                throw new IOException(responseBody.toString());
            }
            StorageInfo info = responseBody.getData();
            long version = info.getImageSeq();
            if (version > localImageVersion) {
                String url = "http://" + hostPort + "/image?version=" + version;
                String filename = Storage.IMAGE + "." + version;
                File dir = new File(this.imageDir);
                MetaHelper.getRemoteFile(url, Config.sync_image_timeout_second * 1000,
                        MetaHelper.getFile(filename, dir));
                MetaHelper.complete(filename, dir);
            } else {
                LOG.warn("get an image with a lower version, localImageVersion: {}, got version: {}",
                        localImageVersion, version);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    protected boolean isMyself() {
        Preconditions.checkNotNull(selfNode);
        Preconditions.checkNotNull(helperNodes);
        if (LOG.isDebugEnabled()) {
            LOG.debug("self: {}. helpers: {}", selfNode, helperNodes);
        }
        // if helper nodes contain itself, remove other helpers
        boolean containSelf = false;
        for (HostInfo helperNode : helperNodes) {
            // WARN: cannot use equals() here, because the hostname may not equal to helperNode.getHostName()
            if (selfNode.isSame(helperNode)) {
                containSelf = true;
                break;
            }
        }
        if (containSelf) {
            helperNodes.clear();
            helperNodes.add(selfNode);
        }

        return containSelf;
    }

    public StatisticsCache getStatisticsCache() {
        return analysisManager.getStatisticsCache();
    }

    public boolean hasReplayer() {
        return replayer != null;
    }

    public void loadImage(String imageDir) throws IOException, DdlException {
        Storage storage = new Storage(imageDir);
        getClusterIdFromStorage(storage);
        File curFile = storage.getCurrentImageFile();
        if (!curFile.exists()) {
            // image.0 may not exist
            LOG.info("image does not exist: {}", curFile.getAbsolutePath());
            return;
        }
        replayedJournalId.set(storage.getLatestImageSeq());
        MetaReader.read(curFile, this);
    }

    public long loadHeader(DataInputStream dis, MetaHeader metaHeader, long checksum) throws IOException, DdlException {
        switch (metaHeader.getMetaFormat()) {
            case COR1:
                return loadHeaderCOR1(dis, checksum);
            default:
                throw new DdlException("unsupported image format.");
        }
    }

    public long loadHeaderCOR1(DataInputStream dis, long checksum) throws IOException {
        int journalVersion = dis.readInt();
        if (journalVersion > FeMetaVersion.VERSION_CURRENT) {
            throw new IOException("The meta version of image is " + journalVersion
                    + ", which is higher than FE current version " + FeMetaVersion.VERSION_CURRENT
                    + ". Please upgrade your cluster to the latest version first.");
        }

        long newChecksum = checksum ^ journalVersion;
        MetaContext.get().setMetaVersion(journalVersion);

        long replayedJournalId = dis.readLong();
        newChecksum ^= replayedJournalId;

        long catalogId = dis.readLong();
        newChecksum ^= catalogId;
        idGenerator.setId(catalogId);

        // compatible with isDefaultClusterCreated, now is deprecated.
        // just read and skip it.
        dis.readBoolean();

        LOG.info("finished replay header from image");
        return newChecksum;
    }

    public long loadMasterInfo(DataInputStream dis, long checksum) throws IOException {
        masterInfo = MasterInfo.read(dis);
        long newChecksum = checksum ^ masterInfo.getRpcPort();
        newChecksum ^= masterInfo.getHttpPort();

        LOG.info("finished replay masterInfo from image");
        return newChecksum;
    }

    public long loadFrontends(DataInputStream dis, long checksum) throws IOException {
        int size = dis.readInt();
        long newChecksum = checksum ^ size;
        for (int i = 0; i < size; i++) {
            Frontend fe = Frontend.read(dis);
            replayAddFrontend(fe);
        }

        size = dis.readInt();
        newChecksum ^= size;
        for (int i = 0; i < size; i++) {
            removedFrontends.add(Text.readString(dis));
        }
        LOG.info("finished replay frontends from image");
        return newChecksum;
    }

    public long loadBackends(DataInputStream dis, long checksum) throws IOException {
        LOG.info("start loading backends from image");
        return systemInfo.loadBackends(dis, checksum);
    }

    public long loadDb(DataInputStream dis, long checksum) throws IOException, DdlException {
        LOG.info("start loading db from image");
        return getInternalCatalog().loadDb(dis, checksum);
    }

    public long loadExportJob(DataInputStream dis, long checksum) throws IOException, DdlException {
        long curTime = System.currentTimeMillis();
        long newChecksum = checksum;
        int size = dis.readInt();
        newChecksum = checksum ^ size;
        for (int i = 0; i < size; ++i) {
            long jobId = dis.readLong();
            newChecksum ^= jobId;
            ExportJob job = ExportJob.read(dis);
            job.cancelReplayedExportJob();
            if (!job.isExpired(curTime)) {
                exportMgr.unprotectAddJob(job);
            }
        }
        LOG.info("finished replay exportJob from image");
        return newChecksum;
    }

    public long loadAlterJob(DataInputStream dis, long checksum)
            throws IOException, AnalysisException {
        long newChecksum = checksum;
        for (JobType type : JobType.values()) {
            newChecksum = loadAlterJob(dis, newChecksum, type);
        }
        LOG.info("finished replay alterJob from image");
        return newChecksum;
    }

    public long loadAlterJob(DataInputStream dis, long checksum, JobType type)
            throws IOException, AnalysisException {
        // alter jobs
        int size = dis.readInt();
        long newChecksum = checksum ^ size;
        if (size > 0) {
            // There should be no old alter jobs, if exist throw exception, should not use this FE version
            throw new IOException("There are [" + size + "] old alter jobs."
                    + " Please downgrade FE to an older version and handle residual jobs");
        }

        // finished or cancelled jobs
        size = dis.readInt();
        newChecksum ^= size;
        if (size > 0) {
            throw new IOException("There are [" + size + "] old finished or cancelled alter jobs."
                    + " Please downgrade FE to an older version and handle residual jobs");
        }

        // alter job v2
        size = dis.readInt();
        newChecksum ^= size;
        for (int i = 0; i < size; i++) {
            AlterJobV2 alterJobV2 = AlterJobV2.read(dis);
            if (alterJobV2.isExpire()) {
                LOG.info("alter job {} is expired, type: {}, ignore it", alterJobV2.getJobId(), alterJobV2.getType());
                continue;
            }
            if (type == JobType.ROLLUP || type == JobType.SCHEMA_CHANGE) {
                if (type == JobType.ROLLUP) {
                    this.getMaterializedViewHandler().addAlterJobV2(alterJobV2);
                } else {
                    this.getSchemaChangeHandler().addAlterJobV2(alterJobV2);
                }
                // ATTN : we just want to add tablet into TabletInvertedIndex when only PendingJob is checkpointed
                // to prevent TabletInvertedIndex data loss,
                // So just use AlterJob.replay() instead of AlterHandler.replay().
                if (alterJobV2.getJobState() == AlterJobV2.JobState.PENDING) {
                    alterJobV2.replay(alterJobV2);
                    LOG.info("replay pending alter job when load alter job {} ", alterJobV2.getJobId());
                }
            } else {
                throw new IOException("Invalid alter job type: " + type.name());
            }
        }

        return newChecksum;
    }

    public long loadBackupHandler(DataInputStream dis, long checksum) throws IOException {
        getBackupHandler().readFields(dis);
        getBackupHandler().setEnv(this);
        LOG.info("finished replay backupHandler from image");
        return checksum;
    }

    public long loadDeleteHandler(DataInputStream dis, long checksum) throws IOException {
        this.deleteHandler = DeleteHandler.read(dis);
        LOG.info("finished replay deleteHandler from image");
        return checksum;
    }

    public long loadAuth(DataInputStream dis, long checksum) throws IOException {
        // CAN NOT use Auth.read(), cause this auth instance is already passed to DomainResolver
        auth.readFields(dis);
        LOG.info("finished replay auth from image");
        return checksum;
    }

    public long loadTransactionState(DataInputStream dis, long checksum) throws IOException {
        int size = dis.readInt();
        long newChecksum = checksum ^ size;
        globalTransactionMgr.readFields(dis);
        LOG.info("finished replay transactionState from image");
        return newChecksum;
    }

    public long loadRecycleBin(DataInputStream dis, long checksum) throws IOException {
        recycleBin = CatalogRecycleBin.read(dis);
        // add tablet in Recycle bin to TabletInvertedIndex
        recycleBin.addTabletToInvertedIndex();
        // create DatabaseTransactionMgr for db in recycle bin.
        // these dbs do not exist in `idToDb` of the catalog.
        for (Long dbId : recycleBin.getAllDbIds()) {
            globalTransactionMgr.addDatabaseTransactionMgr(dbId);
        }
        LOG.info("finished replay recycleBin from image");
        return checksum;
    }

    // global variable persistence
    public long loadGlobalVariable(DataInputStream in, long checksum) throws IOException, DdlException {
        VariableMgr.read(in);
        LOG.info("finished replay globalVariable from image");
        return checksum;
    }

    // load binlogs
    public long loadBinlogs(DataInputStream dis, long checksum) throws IOException {
        binlogManager.read(dis, checksum);
        LOG.info("finished replay binlogMgr from image");
        return checksum;
    }

    public long loadColocateTableIndex(DataInputStream dis, long checksum) throws IOException {
        Env.getCurrentColocateIndex().readFields(dis);
        LOG.info("finished replay colocateTableIndex from image");
        return checksum;
    }

    public long loadRoutineLoadJobs(DataInputStream dis, long checksum) throws IOException {
        Env.getCurrentEnv().getRoutineLoadManager().readFields(dis);
        LOG.info("finished replay routineLoadJobs from image");
        return checksum;
    }

    public long loadLoadJobsV2(DataInputStream in, long checksum) throws IOException {
        loadManager.readFields(in);
        LOG.info("finished replay loadJobsV2 from image");
        return checksum;
    }

    public long loadAsyncJobManager(DataInputStream in, long checksum) throws IOException {
        jobManager.readFields(in);
        LOG.info("finished replay asyncJobMgr from image");
        return checksum;
    }

    public long saveAsyncJobManager(CountingDataOutputStream out, long checksum) throws IOException {
        jobManager.write(out);
        LOG.info("finished save analysisMgr to image");
        return checksum;
    }

    public long loadResources(DataInputStream in, long checksum) throws IOException {
        resourceMgr = ResourceMgr.read(in);
        LOG.info("finished replay resources from image");
        return checksum;
    }

    public long loadWorkloadGroups(DataInputStream in, long checksum) throws IOException {
        workloadGroupMgr = WorkloadGroupMgr.read(in);
        LOG.info("finished replay workload groups from image");
        return checksum;
    }

    public long loadWorkloadSchedPolicy(DataInputStream in, long checksum) throws IOException {
        workloadSchedPolicyMgr = WorkloadSchedPolicyMgr.read(in);
        LOG.info("finished replay workload sched policy from image");
        return checksum;
    }

    public long loadPlsqlProcedure(DataInputStream in, long checksum) throws IOException {
        plsqlManager = PlsqlManager.read(in);
        LOG.info("finished replay plsql procedure from image");
        return checksum;
    }

    public long loadSmallFiles(DataInputStream in, long checksum) throws IOException {
        smallFileMgr.readFields(in);
        LOG.info("finished replay smallFiles from image");
        return checksum;
    }

    public long loadSqlBlockRule(DataInputStream in, long checksum) throws IOException {
        sqlBlockRuleMgr = SqlBlockRuleMgr.read(in);
        LOG.info("finished replay sqlBlockRule from image");
        return checksum;
    }

    /**
     * Load policy through file.
     **/
    public long loadPolicy(DataInputStream in, long checksum) throws IOException {
        policyMgr = PolicyMgr.read(in);
        LOG.info("finished replay policy from image");
        return checksum;
    }

    public long loadIndexPolicy(DataInputStream in, long checksum) throws IOException {
        indexPolicyMgr = IndexPolicyMgr.read(in);
        LOG.info("finished replay index policy from image");
        return checksum;
    }

    /**
     * Load catalogs through file.
     **/
    public long loadCatalog(DataInputStream in, long checksum) throws IOException {
        LOG.info("start loading catalog from image");
        CatalogMgr mgr = CatalogMgr.read(in);
        // When enable the multi catalog in the first time, the "mgr" will be a null value.
        // So ignore it to use default catalog manager.
        if (mgr != null) {
            this.catalogMgr = mgr;
        }
        LOG.info("finished replay catalog from image");
        return checksum;
    }

    /**
     * Load global function.
     **/
    public long loadGlobalFunction(DataInputStream in, long checksum) throws IOException {
        this.globalFunctionMgr = GlobalFunctionMgr.read(in);
        LOG.info("finished replay global function from image");
        return checksum;
    }

    public long loadAnalysisManager(DataInputStream in, long checksum) throws IOException {
        this.analysisManager = AnalysisManager.readFields(in);
        LOG.info("finished replay AnalysisMgr from image");
        return checksum;
    }

    public long loadInsertOverwrite(DataInputStream in, long checksum) throws IOException {
        this.insertOverwriteManager = InsertOverwriteManager.read(in);
        LOG.info("finished replay iot from image");
        return checksum;
    }

    public long loadKeyManager(DataInputStream in, long checksum) throws IOException {
        this.keyManager = KeyManager.read(in);
        LOG.info("finished replay KeyManager from image");
        return checksum;
    }

    public long saveInsertOverwrite(CountingDataOutputStream out, long checksum) throws IOException {
        this.insertOverwriteManager.write(out);
        LOG.info("finished save iot to image");
        return checksum;
    }

    public long loadDictionaryManager(DataInputStream in, long checksum) throws IOException {
        this.dictionaryManager = DictionaryManager.read(in);
        LOG.info("finished replay dictMgr from image");
        return checksum;
    }

    public long saveDictionaryManager(CountingDataOutputStream out, long checksum) throws IOException {
        this.dictionaryManager.write(out);
        LOG.info("finished save dictMgr to image");
        return checksum;
    }

    // Only called by checkpoint thread
    // return the latest image file's absolute path
    public String saveImage() throws IOException {
        // Write image.ckpt
        Storage storage = new Storage(this.imageDir);
        File curFile = storage.getImageFile(replayedJournalId.get());
        File ckpt = new File(this.imageDir, Storage.IMAGE_NEW);
        saveImage(ckpt, replayedJournalId.get());

        // Move image.ckpt to image.dataVersion
        LOG.info("Move " + ckpt.getAbsolutePath() + " to " + curFile.getAbsolutePath());
        if (!ckpt.renameTo(curFile)) {
            curFile.delete();
            throw new IOException();
        }
        return curFile.getAbsolutePath();
    }

    public void saveImage(File curFile, long replayedJournalId) throws IOException {
        if (curFile.exists()) {
            if (!curFile.delete()) {
                throw new IOException(curFile.getName() + " can not be deleted.");
            }
        }
        if (!curFile.createNewFile()) {
            throw new IOException(curFile.getName() + " can not be created.");
        }
        MetaWriter.write(curFile, this);
    }

    public long saveHeader(CountingDataOutputStream dos, long replayedJournalId, long checksum) throws IOException {
        // Write meta version
        checksum ^= FeConstants.meta_version;
        dos.writeInt(FeConstants.meta_version);

        // Write replayed journal id
        checksum ^= replayedJournalId;
        dos.writeLong(replayedJournalId);

        // Write id
        long id = idGenerator.getBatchEndId();
        checksum ^= id;
        dos.writeLong(id);

        // compatible with isDefaultClusterCreated value, now is deprecated,
        // so just write a true value.
        dos.writeBoolean(true);

        return checksum;
    }

    public long saveMasterInfo(CountingDataOutputStream dos, long checksum) throws IOException {
        masterInfo.write(dos);
        checksum ^= masterInfo.getRpcPort();
        checksum ^= masterInfo.getHttpPort();
        return checksum;
    }

    public long saveFrontends(CountingDataOutputStream dos, long checksum) throws IOException {
        int size = frontends.size();
        checksum ^= size;

        dos.writeInt(size);
        for (Frontend fe : frontends.values()) {
            fe.write(dos);
        }

        size = removedFrontends.size();
        checksum ^= size;

        dos.writeInt(size);
        for (String feName : removedFrontends) {
            Text.writeString(dos, feName);
        }

        return checksum;
    }

    public long saveBackends(CountingDataOutputStream dos, long checksum) throws IOException {
        return systemInfo.saveBackends(dos, checksum);
    }

    public long saveDb(CountingDataOutputStream dos, long checksum) throws IOException {
        return getInternalCatalog().saveDb(dos, checksum);
    }

    public long saveExportJob(CountingDataOutputStream dos, long checksum) throws IOException {
        long curTime = System.currentTimeMillis();
        List<ExportJob> jobs = exportMgr.getJobs().stream().filter(t -> !t.isExpired(curTime))
                .collect(Collectors.toList());
        if (jobs.size() > Config.max_export_history_job_num) {
            jobs.sort(Comparator.comparingLong(ExportJob::getCreateTimeMs));
            Iterator<ExportJob> iterator = jobs.iterator();
            while (jobs.size() > Config.max_export_history_job_num && iterator.hasNext()) {
                ExportJob job = iterator.next();
                if (job.getState() == ExportJobState.FINISHED || job.getState() == ExportJobState.CANCELLED) {
                    iterator.remove();
                }
            }
        }
        int size = jobs.size();
        checksum ^= size;
        dos.writeInt(size);
        for (ExportJob job : jobs) {
            long jobId = job.getId();
            checksum ^= jobId;
            dos.writeLong(jobId);
            job.write(dos);
        }
        return checksum;
    }

    public long saveAlterJob(CountingDataOutputStream dos, long checksum) throws IOException {
        for (JobType type : JobType.values()) {
            checksum = saveAlterJob(dos, checksum, type);
        }
        return checksum;
    }

    public long saveAlterJob(CountingDataOutputStream dos, long checksum, JobType type) throws IOException {
        Map<Long, AlterJobV2> alterJobsV2;
        if (type == JobType.ROLLUP) {
            alterJobsV2 = this.getMaterializedViewHandler().getAlterJobsV2();
        } else if (type == JobType.SCHEMA_CHANGE) {
            alterJobsV2 = this.getSchemaChangeHandler().getAlterJobsV2();
        } else if (type == JobType.DECOMMISSION_BACKEND) {
            // Load alter job need decommission backend type to load image
            alterJobsV2 = Maps.newHashMap();
        } else {
            throw new IOException("Invalid alter job type: " + type.name());
        }

        // alter jobs == 0
        // If the FE version upgrade from old version, if it have alter jobs, the FE will failed during start process
        // the number of old version alter jobs has to be 0
        int size = 0;
        checksum ^= size;
        dos.writeInt(size);

        // finished or cancelled jobs
        size = 0;
        checksum ^= size;
        dos.writeInt(size);

        // alter job v2
        size = alterJobsV2.size();
        checksum ^= size;
        dos.writeInt(size);
        for (AlterJobV2 alterJobV2 : alterJobsV2.values()) {
            alterJobV2.write(dos);
        }

        return checksum;
    }

    public long saveBackupHandler(CountingDataOutputStream dos, long checksum) throws IOException {
        getBackupHandler().write(dos);
        return checksum;
    }

    public long saveDeleteHandler(CountingDataOutputStream dos, long checksum) throws IOException {
        getDeleteHandler().write(dos);
        return checksum;
    }

    public long saveAuth(CountingDataOutputStream dos, long checksum) throws IOException {
        auth.write(dos);
        return checksum;
    }

    public long saveTransactionState(CountingDataOutputStream dos, long checksum) throws IOException {
        int size = globalTransactionMgr.getTransactionNum();
        checksum ^= size;
        dos.writeInt(size);
        globalTransactionMgr.write(dos);
        return checksum;
    }

    public long saveRecycleBin(CountingDataOutputStream dos, long checksum) throws IOException {
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();
        recycleBin.write(dos);
        return checksum;
    }

    public long saveColocateTableIndex(CountingDataOutputStream dos, long checksum) throws IOException {
        Env.getCurrentColocateIndex().write(dos);
        return checksum;
    }

    public long saveRoutineLoadJobs(CountingDataOutputStream dos, long checksum) throws IOException {
        Env.getCurrentEnv().getRoutineLoadManager().write(dos);
        return checksum;
    }

    public long saveGlobalVariable(CountingDataOutputStream dos, long checksum) throws IOException {
        VariableMgr.write(dos);
        return checksum;
    }

    public void replayGlobalVariableV2(GlobalVarPersistInfo info) throws IOException, DdlException {
        VariableMgr.replayGlobalVariableV2(info, false);
    }

    public long saveLoadJobsV2(CountingDataOutputStream dos, long checksum) throws IOException {
        Env.getCurrentEnv().getLoadManager().write(dos);
        return checksum;
    }

    public long saveResources(CountingDataOutputStream dos, long checksum) throws IOException {
        Env.getCurrentEnv().getResourceMgr().write(dos);
        return checksum;
    }

    public long saveWorkloadGroups(CountingDataOutputStream dos, long checksum) throws IOException {
        Env.getCurrentEnv().getWorkloadGroupMgr().write(dos);
        return checksum;
    }

    public long saveWorkloadSchedPolicy(CountingDataOutputStream dos, long checksum) throws IOException {
        Env.getCurrentEnv().getWorkloadSchedPolicyMgr().write(dos);
        return checksum;
    }

    public long savePlsqlProcedure(CountingDataOutputStream dos, long checksum) throws IOException {
        Env.getCurrentEnv().getPlsqlManager().write(dos);
        return checksum;
    }

    public long saveSmallFiles(CountingDataOutputStream dos, long checksum) throws IOException {
        smallFileMgr.write(dos);
        return checksum;
    }

    public long saveSqlBlockRule(CountingDataOutputStream out, long checksum) throws IOException {
        Env.getCurrentEnv().getSqlBlockRuleMgr().write(out);
        return checksum;
    }

    public long savePolicy(CountingDataOutputStream out, long checksum) throws IOException {
        Env.getCurrentEnv().getPolicyMgr().write(out);
        return checksum;
    }

    public long saveIndexPolicy(CountingDataOutputStream out, long checksum) throws IOException {
        Env.getCurrentEnv().getIndexPolicyMgr().write(out);
        return checksum;
    }

    /**
     * Save catalog image.
     */
    public long saveCatalog(CountingDataOutputStream out, long checksum) throws IOException {
        Env.getCurrentEnv().getCatalogMgr().write(out);
        return checksum;
    }

    public long saveGlobalFunction(CountingDataOutputStream out, long checksum) throws IOException {
        this.globalFunctionMgr.write(out);
        LOG.info("Save global function to image");
        return checksum;
    }

    public long saveBinlogs(CountingDataOutputStream out, long checksum) throws IOException {
        if (!Config.enable_feature_binlog) {
            return checksum;
        }

        this.binlogManager.write(out, checksum);
        LOG.info("Save binlogs to image");
        return checksum;
    }

    public long saveAnalysisMgr(CountingDataOutputStream dos, long checksum) throws IOException {
        analysisManager.write(dos);
        return checksum;
    }

    public long saveKeyManager(CountingDataOutputStream out, long checksum) throws IOException {
        this.keyManager.write(out);
        LOG.info("finished save KeyManager to image");
        return checksum;
    }

    public void createLabelCleaner() {
        labelCleaner = new MasterDaemon("LoadLabelCleaner", Config.label_clean_interval_second * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                loadManager.removeOldLoadJob();
                exportMgr.removeOldExportJobs();
                deleteHandler.removeOldDeleteInfos();
                loadManager.removeOverLimitLoadJob();
            }
        };
    }

    public void createTxnCleaner() {
        txnCleaner = new MasterDaemon("txnCleaner", Config.transaction_clean_interval_second * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                globalTransactionMgr.removeExpiredAndTimeoutTxns();
            }
        };
    }

    public void createFeDiskUpdater() {
        feDiskUpdater = new Daemon("feDiskUpdater", Config.heartbeat_interval_second * 1000L) {
            @Override
            protected void runOneCycle() {
                ExecuteEnv.getInstance().refreshAndGetDiskInfo(true);
            }
        };
    }

    public void createReplayer() {
        replayer = new Daemon("replayer", REPLAY_INTERVAL_MS) {
            // Avoid numerous 'meta out of date' log
            private long lastLogMetaOutOfDateTime = System.currentTimeMillis();

            @Override
            protected void runOneCycle() {
                boolean err = false;
                boolean hasLog = false;
                try {
                    hasLog = replayJournal(-1);
                    metaReplayState.setOk();
                } catch (InsufficientLogException insufficientLogEx) {
                    // Copy the missing log files from a member of the
                    // replication group who owns the files
                    LOG.error("catch insufficient log exception. please restart.", insufficientLogEx);
                    NetworkRestore restore = new NetworkRestore();
                    NetworkRestoreConfig config = new NetworkRestoreConfig();
                    config.setRetainLogFiles(false);
                    restore.execute(insufficientLogEx, config);
                    System.exit(-1);
                } catch (Throwable e) {
                    LOG.error("replayer thread catch an exception when replay journal.", e);
                    metaReplayState.setException(e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        LOG.error("sleep got exception. ", e);
                    }
                    err = true;
                }
                setCanRead(hasLog, err);
            }

            private void setCanRead(boolean hasLog, boolean err) {
                if (err) {
                    canRead.set(false);
                    isReady.set(false);
                    return;
                }

                if (Config.ignore_meta_check) {
                    // can still offer read, but is not ready
                    canRead.set(true);
                    isReady.set(false);
                    return;
                }

                long currentTimeMs = System.currentTimeMillis();
                if (currentTimeMs - synchronizedTimeMs > Config.meta_delay_toleration_second * 1000) {
                    if (currentTimeMs - lastLogMetaOutOfDateTime > 5000L) {
                        // we still need this log to observe this situation
                        // but service may be continued when there is no log being replayed.
                        LOG.warn("meta out of date. currentTime:{}, syncTime:{}, delta:{}ms, hasLog:{}, feType:{}",
                                currentTimeMs, synchronizedTimeMs, (currentTimeMs - synchronizedTimeMs),
                                hasLog, feType);
                        lastLogMetaOutOfDateTime = currentTimeMs;
                    }
                    if (hasLog || feType == FrontendNodeType.UNKNOWN) {
                        // 1. if we read log from BDB, which means master is still alive.
                        // So we need to set meta out of date.
                        // 2. if we didn't read any log from BDB and feType is UNKNOWN,
                        // which means this non-master node is disconnected with master.
                        // So we need to set meta out of date either.
                        metaReplayState.setOutOfDate(currentTimeMs, synchronizedTimeMs);
                        canRead.set(false);
                        isReady.set(false);

                        if (editLog != null) {
                            String reason = editLog.getNotReadyReason();
                            if (!Strings.isNullOrEmpty(reason)) {
                                LOG.warn("Not ready reason:{}", reason);
                            }
                        }
                    }
                } else {
                    canRead.set(true);
                    isReady.set(true);
                }
            }
        };
        replayer.setMetaContext(metaContext);
    }

    public void notifyNewFETypeTransfer(FrontendNodeType newType) {
        try {
            String msg = "notify new FE type transfer: " + newType;
            LOG.warn(msg);
            LogUtils.stdout(msg);
            this.typeTransferQueue.put(newType);
        } catch (InterruptedException e) {
            LOG.error("failed to put new FE type: {}", newType, e);
        }
    }

    public void startStateListener() {
        listener = new Daemon("stateListener", STATE_CHANGE_CHECK_INTERVAL_MS) {
            @Override
            protected synchronized void runOneCycle() {

                while (true) {
                    FrontendNodeType newType = null;
                    try {
                        newType = typeTransferQueue.take();
                    } catch (InterruptedException e) {
                        LOG.error("got exception when take FE type from queue", e);
                        LogUtils.stdout("got exception when take FE type from queue. " + e.getMessage());
                        System.exit(-1);
                    }
                    Preconditions.checkNotNull(newType);
                    LOG.info("begin to transfer FE type from {} to {}", feType, newType);
                    if (feType == newType) {
                        return;
                    }

                    /*
                     * INIT -> MASTER: transferToMaster
                     * INIT -> FOLLOWER/OBSERVER: transferToNonMaster
                     * UNKNOWN -> MASTER: transferToMaster
                     * UNKNOWN -> FOLLOWER/OBSERVER: transferToNonMaster
                     * FOLLOWER -> MASTER: transferToMaster
                     * FOLLOWER/OBSERVER -> INIT/UNKNOWN: set isReady to false
                     */
                    switch (feType) {
                        case INIT: {
                            switch (newType) {
                                case MASTER: {
                                    transferToMaster();
                                    break;
                                }
                                case FOLLOWER:
                                case OBSERVER: {
                                    transferToNonMaster(newType);
                                    break;
                                }
                                case UNKNOWN:
                                    break;
                                default:
                                    break;
                            }
                            break;
                        }
                        case UNKNOWN: {
                            switch (newType) {
                                case MASTER: {
                                    transferToMaster();
                                    break;
                                }
                                case FOLLOWER:
                                case OBSERVER: {
                                    transferToNonMaster(newType);
                                    break;
                                }
                                default:
                                    break;
                            }
                            break;
                        }
                        case FOLLOWER: {
                            switch (newType) {
                                case MASTER: {
                                    transferToMaster();
                                    break;
                                }
                                case UNKNOWN: {
                                    transferToNonMaster(newType);
                                    break;
                                }
                                default:
                                    break;
                            }
                            break;
                        }
                        case OBSERVER: {
                            switch (newType) {
                                case UNKNOWN: {
                                    transferToNonMaster(newType);
                                    break;
                                }
                                default:
                                    break;
                            }
                            break;
                        }
                        case MASTER: {
                            // exit if master changed to any other type
                            String msg = "transfer FE type from MASTER to " + newType.name() + ". exit";
                            LOG.error(msg);
                            LogUtils.stdout(msg);
                            System.exit(-1);
                            break;
                        }
                        default:
                            break;
                    } // end switch formerFeType

                    feType = newType;
                    LOG.info("finished to transfer FE type to {}", feType);
                }
            } // end runOneCycle
        };

        listener.setMetaContext(metaContext);
        listener.start();
    }

    public synchronized boolean replayJournal(long toJournalId) {
        long newToJournalId = toJournalId;
        if (newToJournalId == -1) {
            newToJournalId = getMaxJournalId();
        }
        if (newToJournalId <= replayedJournalId.get()) {
            return false;
        }

        LOG.info("replayed journal id is {}, replay to journal id is {}", replayedJournalId, newToJournalId);
        JournalCursor cursor = editLog.read(replayedJournalId.get() + 1, newToJournalId);
        if (cursor == null) {
            LOG.warn("failed to get cursor from {} to {}", replayedJournalId.get() + 1, newToJournalId);
            return false;
        }

        long startTime = System.currentTimeMillis();
        boolean hasLog = false;
        while (true) {
            long entityStartTime = System.currentTimeMillis();
            Pair<Long, JournalEntity> kv = cursor.next();
            if (kv == null) {
                break;
            }
            Long logId = kv.first;
            JournalEntity entity = kv.second;
            if (entity == null) {
                if (logId != null && forceSkipJournalIds.contains(String.valueOf(logId))) {
                    replayedJournalId.incrementAndGet();
                    String msg = "journal " + replayedJournalId + " has skipped by config force_skip_journal_id";
                    LOG.info(msg);
                    LogUtils.stdout(msg);
                    if (MetricRepo.isInit) {
                        // Metric repo may not init after this replay thread start
                        MetricRepo.COUNTER_EDIT_LOG_READ.increase(1L);
                    }
                    continue;
                } else {
                    break;
                }
            }
            hasLog = true;
            EditLog.loadJournal(this, logId, entity);
            long loadJournalEndTime = System.currentTimeMillis();
            replayedJournalId.incrementAndGet();
            if (LOG.isDebugEnabled()) {
                LOG.debug("journal {} replayed.", replayedJournalId);
            }
            if (feType != FrontendNodeType.MASTER) {
                journalObservable.notifyObservers(replayedJournalId.get());
            }
            if (MetricRepo.isInit) {
                // Metric repo may not init after this replay thread start
                MetricRepo.COUNTER_EDIT_LOG_READ.increase(1L);
            }

            long entityCost = System.currentTimeMillis() - entityStartTime;
            if (entityCost >= 1000) {
                long loadJournalCost = loadJournalEndTime - entityStartTime;
                LOG.warn("entityCost:{} loadJournalCost:{} logId:{} replayedJournalId:{} code:{} size:{}",
                        entityCost, loadJournalCost, logId, replayedJournalId, entity.getOpCode(),
                        entity.getDataSize());
            }
        }
        long cost = System.currentTimeMillis() - startTime;
        if (LOG.isDebugEnabled() && cost >= 1000) {
            LOG.debug("replay journal cost too much time: {} replayedJournalId: {}", cost, replayedJournalId);
        }

        return hasLog;
    }

    public void createTimePrinter() {
        // time printer will write timestamp edit log every 10 seconds
        timePrinter = new MasterDaemon("timePrinter", 10 * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                Timestamp stamp = new Timestamp();
                editLog.logTimestamp(stamp);
            }
        };
    }

    public void addFrontend(FrontendNodeType role, String host, int editLogPort) throws DdlException {
        addFrontend(role, host, editLogPort, "");
    }

    public void addFrontend(FrontendNodeType role, String host, int editLogPort, String nodeName) throws DdlException {
        addFrontend(role, host, editLogPort, nodeName, "");
    }

    public void addFrontend(FrontendNodeType role, String host, int editLogPort, String nodeName, String cloudUniqueId)
            throws DdlException {
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire env lock. Try again");
        }
        try {
            if (Strings.isNullOrEmpty(nodeName)) {
                nodeName = genFeNodeName(host, editLogPort, false /* new name style */);
            }

            Frontend fe = checkFeExist(host, editLogPort);
            if (fe != null) {
                throw new DdlException("frontend already exists " + fe);
            }
            if (Config.enable_fqdn_mode && StringUtils.isEmpty(host)) {
                throw new DdlException("frontend's hostName should not be empty while enable_fqdn_mode is true");
            }

            if (removedFrontends.contains(nodeName)) {
                throw new DdlException("frontend name already exists " + nodeName + ". Try again");
            }

            BDBHA bdbha = (BDBHA) haProtocol;
            bdbha.removeConflictNodeIfExist(host, editLogPort);
            int targetFollowerCount = getFollowerCount() + 1;
            if (role == FrontendNodeType.FOLLOWER || role == FrontendNodeType.REPLICA) {
                helperNodes.add(new HostInfo(host, editLogPort));
                bdbha.addUnReadyElectableNode(nodeName, targetFollowerCount);
            }

            // Only add frontend after removing the conflict nodes, to ensure the exception safety.
            fe = new Frontend(role, nodeName, host, editLogPort);
            fe.setCloudUniqueId(cloudUniqueId);
            frontends.put(nodeName, fe);

            LOG.info("add frontend: {}", fe);

            editLog.logAddFrontend(fe);
        } finally {
            unlock();
        }
    }

    public void modifyFrontendHostName(String srcHost, int srcPort, String destHost) throws DdlException {
        Frontend fe = checkFeExist(srcHost, srcPort);
        if (fe == null) {
            throw new DdlException("frontend does not exist, host:" + srcHost);
        }
        modifyFrontendHost(fe.getNodeName(), destHost);
    }

    private void modifyFrontendHost(String nodeName, String destHost) throws DdlException {
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire env lock. Try again");
        }
        try {
            Frontend fe = getFeByName(nodeName);
            if (fe == null) {
                throw new DdlException("frontend does not exist, nodeName:" + nodeName);
            }

            LOG.info("modify frontend with new host: {}, exist frontend: {}", fe.getHost(), fe);
            boolean needLog = false;
            // we use hostname as address of bdbha, so we don't need to update node address when ip changed
            if (!Strings.isNullOrEmpty(destHost) && !destHost.equals(fe.getHost())) {
                fe.setHost(destHost);
                BDBHA bdbha = (BDBHA) haProtocol;
                bdbha.updateNodeAddress(fe.getNodeName(), destHost, fe.getEditLogPort());
                needLog = true;
            }
            if (needLog) {
                Env.getCurrentEnv().getEditLog().logModifyFrontend(fe);
            }
        } finally {
            unlock();
        }
    }

    public void dropFrontend(FrontendNodeType role, String host, int port) throws DdlException {
        dropFrontendFromBDBJE(role, host, port);
    }

    public void dropFrontendFromBDBJE(FrontendNodeType role, String host, int port) throws DdlException {
        if (port == selfNode.getPort() && feType == FrontendNodeType.MASTER
                && selfNode.getHost().equals(host)) {
            throw new DdlException("can not drop current master node.");
        }
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire env lock. Try again");
        }
        try {
            Frontend fe = checkFeExist(host, port);
            if (fe == null) {
                throw new DdlException("frontend does not exist[" + NetUtils
                        .getHostPortInAccessibleFormat(host, port) + "]");
            }
            if (fe.getRole() != role) {
                throw new DdlException(role.toString() + " does not exist[" + NetUtils
                        .getHostPortInAccessibleFormat(host, port) + "]");
            }
            if (role == FrontendNodeType.FOLLOWER && fe.isAlive()) {
                // Try drop an alive follower, check the quorum safety.
                ensureSafeToDropAliveFollower();
            }

            editLog.logRemoveFrontend(fe);
            LOG.info("remove frontend: {}", fe);

            int targetFollowerCount = getFollowerCount() - 1;
            if (fe.getRole() == FrontendNodeType.FOLLOWER || fe.getRole() == FrontendNodeType.REPLICA) {
                haProtocol.removeElectableNode(fe.getNodeName());
                removeHelperNode(host, port);
                BDBHA ha = (BDBHA) haProtocol;
                ha.removeUnReadyElectableNode(fe.getNodeName(), targetFollowerCount);
            }

            // Only remove frontend after removing the electable node success, to ensure the
            // exception safety.
            frontends.remove(fe.getNodeName());
            removedFrontends.add(fe.getNodeName());

        } finally {
            unlock();
        }
    }

    private void removeDroppedFrontends(ConcurrentLinkedQueue<String> removedFrontends) {
        if (removedFrontends.size() == 0) {
            return;
        }
        if (!Strings.isNullOrEmpty(System.getProperty(FeConstants.METADATA_FAILURE_RECOVERY_KEY))) {
            // metadata recovery mode
            LOG.info("Metadata failure recovery({}), ignore removing dropped frontends",
                    System.getProperty(FeConstants.METADATA_FAILURE_RECOVERY_KEY));
            return;
        }

        if (haProtocol != null && haProtocol instanceof BDBHA) {
            BDBHA bdbha = (BDBHA) haProtocol;
            LOG.info("remove frontends, num {} frontends {}", removedFrontends.size(), removedFrontends);
            bdbha.removeDroppedMember(removedFrontends);
        }
    }

    private void removeHelperNode(String host, int port) {
        // ip may be changed, so we need use both ip and hostname to check.
        // use node.getIdent() for simplicity here.
        helperNodes.removeIf(node -> node.getHost().equals(host) && node.getPort() == port);
    }

    private void ensureSafeToDropAliveFollower() throws DdlException {
        int numFollower = 0;
        int numAliveFollower = 0;
        for (Frontend fe : frontends.values()) {
            if (fe.getRole() == FrontendNodeType.FOLLOWER) {
                numFollower += 1;
                if (fe.isAlive()) {
                    numAliveFollower += 1;
                }
            }
        }

        int nextMajority = ((numFollower - 1) / 2) + 1;
        if (nextMajority + 1 <= numAliveFollower) {
            return;
        }

        LOG.warn("Drop an alive follower is not safety. Current alive followers {}, followers {}, next majority: {}",
                numAliveFollower, numFollower, nextMajority);
        throw new DdlException("Unable to drop this alive follower, because the quorum requirements "
                + "are not met after this command execution. Current num alive followers "
                + numAliveFollower + ", num followers " + numFollower + ", majority after execution " + nextMajority);
    }

    public Frontend checkFeExist(String host, int port) {
        for (Frontend fe : frontends.values()) {
            if (fe.getEditLogPort() != port) {
                continue;
            }
            if (fe.getHost().equals(host)) {
                return fe;
            }
        }
        return null;
    }

    public Frontend getFeByName(String name) {
        for (Frontend fe : frontends.values()) {
            if (fe.getNodeName().equals(name)) {
                return fe;
            }
        }
        return null;
    }

    // The interface which DdlExecutor needs.
    public void createDb(CreateDatabaseCommand command) throws DdlException {
        CatalogIf<?> catalogIf;
        if (StringUtils.isEmpty(command.getCtlName())) {
            catalogIf = getCurrentCatalog();
        } else {
            catalogIf = catalogMgr.getCatalog(command.getCtlName());
        }
        catalogIf.createDb(command.getDbName(), command.isIfNotExists(), command.getProperties());
    }

    // For replay edit log, need't lock metadata
    public void unprotectCreateDb(Database db) {
        getInternalCatalog().unprotectCreateDb(db);
    }

    public void replayCreateDb(CreateDbInfo dbInfo) {
        if (dbInfo.getInternalDb() != null) {
            getInternalCatalog().replayCreateDb(dbInfo.getInternalDb(), "");
        } else {
            ExternalCatalog externalCatalog = (ExternalCatalog) catalogMgr.getCatalog(dbInfo.getCtlName());
            if (externalCatalog != null) {
                externalCatalog.replayCreateDb(dbInfo.getDbName());
            }
        }
    }

    public void dropDb(String catalogName, String dbName, boolean ifExists, boolean force) throws DdlException {
        CatalogIf<?> catalogIf;
        if (StringUtils.isEmpty(catalogName)) {
            catalogIf = getCurrentCatalog();
        } else {
            catalogIf = catalogMgr.getCatalog(catalogName);
        }
        catalogIf.dropDb(dbName, ifExists, force);
    }

    public void replayDropDb(DropDbInfo info) throws DdlException {
        if (Strings.isNullOrEmpty(info.getCtlName()) || info.getCtlName()
                .equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            getInternalCatalog().replayDropDb(info.getDbName(), info.isForceDrop(), info.getRecycleTime());
        } else {
            ExternalCatalog externalCatalog = (ExternalCatalog) catalogMgr.getCatalog(info.getCtlName());
            if (externalCatalog != null) {
                externalCatalog.replayDropDb(info.getDbName());
            }
        }
    }

    public void recoverDatabase(RecoverDbStmt recoverStmt) throws DdlException {
        getInternalCatalog().recoverDatabase(recoverStmt);
    }

    public void recoverDatabase(String dbName, long dbId, String newDbName) throws DdlException {
        getInternalCatalog().recoverDatabase(dbName, dbId, newDbName);
    }

    public void recoverTable(RecoverTableStmt recoverStmt) throws DdlException {
        getInternalCatalog().recoverTable(recoverStmt);
    }

    public void recoverTable(String dbName, String tableName, String newTableName, long tableId) throws DdlException {
        getInternalCatalog().recoverTable(dbName, tableName, newTableName, tableId);
    }

    public void recoverPartition(RecoverPartitionStmt recoverStmt) throws DdlException {
        getInternalCatalog().recoverPartition(recoverStmt);
    }

    public void recoverPartition(String dbName, String tableName, String partitionName,
                                    String newPartitionName, long partitionId) throws DdlException {
        getInternalCatalog().recoverPartition(dbName, tableName, partitionName, newPartitionName, partitionId);
    }

    public void dropCatalogRecycleBin(IdType idType, long id) throws DdlException {
        getInternalCatalog().dropCatalogRecycleBin(idType, id);
    }

    public void replayEraseDatabase(long dbId) throws DdlException {
        Env.getCurrentRecycleBin().replayEraseDatabase(dbId);
    }

    public void replayRecoverDatabase(RecoverInfo info) {
        getInternalCatalog().replayRecoverDatabase(info);
    }

    public void replayAlterDatabaseQuota(String dbName, long quota, QuotaType quotaType) throws MetaNotFoundException {
        getInternalCatalog().replayAlterDatabaseQuota(dbName, quota, quotaType);
    }

    public void alterDatabaseProperty(String dbName, Map<String, String> properties) throws DdlException {
        getInternalCatalog().alterDatabaseProperty(dbName, properties);
    }

    public void replayAlterDatabaseProperty(String dbName, Map<String, String> properties)
            throws MetaNotFoundException {
        getInternalCatalog().replayAlterDatabaseProperty(dbName, properties);
    }

    public void replayRenameDatabase(String dbName, String newDbName) {
        getInternalCatalog().replayRenameDatabase(dbName, newDbName);
    }

    /**
     * Following is the step to create an olap table:
     * 1. create columns
     * 2. create partition info
     * 3. create distribution info
     * 4. set table id and base index id
     * 5. set bloom filter columns
     * 6. set and build TableProperty includes:
     * 6.1. dynamicProperty
     * 6.2. replicationNum
     * 6.3. inMemory
     * 6.4. storageFormat
     * 6.5. compressionType
     * 7. set index meta
     * 8. check colocation properties
     * 9. create tablet in BE
     * 10. add this table to FE's meta
     * 11. add this table to ColocateGroup if necessary
     * @return if CreateTableStmt.isIfNotExists is true, return true if table already exists
     * otherwise return false
     */
    public boolean createTable(CreateTableCommand command) throws UserException {
        org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo createTableInfo =
                command.getCreateTableInfo();
        CatalogIf<?> catalogIf = catalogMgr.getCatalogOrException(createTableInfo.getCtlName(),
                catalog -> new DdlException(("Unknown catalog " + catalog)));
        return catalogIf.createTable(command);
    }

    /**
     * Following is the step to create an olap table:
     * 1. create columns
     * 2. create partition info
     * 3. create distribution info
     * 4. set table id and base index id
     * 5. set bloom filter columns
     * 6. set and build TableProperty includes:
     * 6.1. dynamicProperty
     * 6.2. replicationNum
     * 6.3. inMemory
     * 6.4. storageFormat
     * 6.5. compressionType
     * 7. set index meta
     * 8. check colocation properties
     * 9. create tablet in BE
     * 10. add this table to FE's meta
     * 11. add this table to ColocateGroup if necessary
     * @return if CreateTableStmt.isIfNotExists is true, return true if table already exists
     * otherwise return false
     */
    public boolean createTable(CreateTableStmt stmt) throws UserException {
        CatalogIf<?> catalogIf = catalogMgr.getCatalogOrException(stmt.getCatalogName(),
                catalog -> new DdlException(("Unknown catalog " + catalog)));
        return catalogIf.createTable(stmt);
    }

    /**
     * Adds a partition to a table
     *
     * @param db
     * @param tableName
     * @param addPartitionClause clause in the CreateTableStmt
     * @param isCreateTable this call is for creating table
     * @param generatedPartitionId the preset partition id for the partition to add
     * @param writeEditLog whether to write an edit log for this addition
     * @return PartitionPersistInfo to be written to editlog. It may be null if no partitions added.
     * @throws DdlException
     */
    public PartitionPersistInfo addPartition(Database db, String tableName, AddPartitionClause addPartitionClause,
                                             boolean isCreateTable, long generatedPartitionId,
                                             boolean writeEditLog) throws DdlException {
        return getInternalCatalog().addPartition(db, tableName, addPartitionClause,
            isCreateTable, generatedPartitionId, writeEditLog);
    }

    public void addMultiPartitions(Database db, String tableName, AlterMultiPartitionClause multiPartitionClause)
            throws DdlException {
        getInternalCatalog().addMultiPartitions(db, tableName, multiPartitionClause);
    }

    public void addPartitionLike(Database db, String tableName, AddPartitionLikeClause addPartitionLikeClause)
            throws DdlException {
        getInternalCatalog().addPartitionLike(db, tableName, addPartitionLikeClause);
    }

    public void replayAddPartition(PartitionPersistInfo info) throws MetaNotFoundException {
        getInternalCatalog().replayAddPartition(info);
    }

    public void dropPartition(Database db, OlapTable olapTable, DropPartitionClause clause) throws DdlException {
        getInternalCatalog().dropPartition(db, olapTable, clause);
    }

    public void replayDropPartition(DropPartitionInfo info) throws MetaNotFoundException {
        getInternalCatalog().replayDropPartition(info);
    }

    public void replayErasePartition(long partitionId) {
        getInternalCatalog().replayErasePartition(partitionId);
    }

    public void replayRecoverPartition(RecoverInfo info) throws MetaNotFoundException, DdlException {
        getInternalCatalog().replayRecoverPartition(info);
    }

    public void replayGcBinlog(BinlogGcInfo binlogGcInfo) {
        binlogManager.replayGc(binlogGcInfo);
    }

    public static void getDdlStmt(TableIf table, List<String> createTableStmt, List<String> addPartitionStmt,
                                  List<String> createRollupStmt, boolean separatePartition, boolean hidePassword,
                                  long specificVersion) {
        getDdlStmt(null, null, table, createTableStmt, addPartitionStmt, createRollupStmt, separatePartition,
                hidePassword, false, specificVersion, false, false);
    }

    public static void getSyncedDdlStmt(TableIf table, List<String> createTableStmt, List<String> addPartitionStmt,
                                  List<String> createRollupStmt, boolean separatePartition, boolean hidePassword,
                                  long specificVersion) {
        getDdlStmt(null, null, table, createTableStmt, addPartitionStmt, createRollupStmt, separatePartition,
                hidePassword, false, specificVersion, false, true);
    }

    public static String getMTMVDdl(MTMV mtmv) throws AnalysisException {
        if (!mtmv.tryReadLock(1, TimeUnit.MINUTES)) {
            throw new AnalysisException(
                    "get table read lock timeout, database=" + mtmv.getDBName() + ",table=" + mtmv.getName());
        }
        try {
            StringBuilder sb = new StringBuilder("CREATE MATERIALIZED VIEW ");
            sb.append(mtmv.getName());
            addMTMVCols(mtmv, sb);
            sb.append("\n");
            sb.append(mtmv.getRefreshInfo());
            addMTMVKeyInfo(mtmv, sb);
            addTableComment(mtmv, sb);
            addMTMVPartitionInfo(mtmv, sb);
            DistributionInfo distributionInfo = mtmv.getDefaultDistributionInfo();
            sb.append("\n").append(distributionInfo.toSql());
            // properties
            sb.append("\nPROPERTIES (\n");
            addOlapTablePropertyInfo(mtmv, sb, false, false, null);
            addMTMVPropertyInfo(mtmv, sb);
            sb.append("\n)");
            sb.append("\nAS ");
            sb.append(mtmv.getQuerySql());
            return sb.toString();
        } finally {
            mtmv.readUnlock();
        }
    }

    private static void addMTMVKeyInfo(MTMV mtmv, StringBuilder sb) {
        if (!mtmv.isDuplicateWithoutKey()) {
            String keySql = mtmv.getKeysType().toSql();
            sb.append("\n").append(keySql).append("(");
            List<String> keysColumnNames = Lists.newArrayList();
            for (Column column : mtmv.getBaseSchema()) {
                if (column.isKey()) {
                    keysColumnNames.add("`" + column.getName() + "`");
                }
            }
            sb.append(Joiner.on(", ").join(keysColumnNames)).append(")");
        }
    }

    private static void addMTMVPartitionInfo(MTMV mtmv, StringBuilder sb) throws AnalysisException {
        MTMVPartitionInfo mvPartitionInfo = mtmv.getMvPartitionInfo();
        if (mvPartitionInfo.getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return;
        }
        sb.append("\n").append("PARTITION BY (");
        if (mvPartitionInfo.getPartitionType() == MTMVPartitionType.FOLLOW_BASE_TABLE) {
            sb.append("`" + mvPartitionInfo.getPartitionCol() + "`");
        } else {
            sb.append(MTMVPartitionExprFactory.getExprService(mvPartitionInfo.getExpr()).toSql(mvPartitionInfo));
        }
        sb.append(")");
    }

    private static void addMTMVCols(MTMV mtmv, StringBuilder sb) {
        sb.append("\n(");
        List<Column> columns = mtmv.getBaseSchema();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            Column column = columns.get(i);
            sb.append(column.getName());
            if (!StringUtils.isEmpty(column.getComment())) {
                sb.append(" comment '");
                sb.append(column.getComment());
                sb.append("'");
            }
        }
        sb.append(")");
    }

    private static void addMTMVPropertyInfo(MTMV mtmv, StringBuilder sb) {
        Map<String, String> mvProperties = mtmv.getMvProperties();
        for (Entry<String, String> entry : mvProperties.entrySet()) {
            sb.append(",\n\"").append(entry.getKey()).append("\" = \"");
            sb.append(entry.getValue()).append("\"");
        }
    }

    private static void addOlapTablePropertyInfo(OlapTable olapTable, StringBuilder sb, boolean separatePartition,
            boolean getDdlForSync, List<Long> partitionId) {
        // replicationNum
        ReplicaAllocation replicaAlloc = olapTable.getDefaultReplicaAllocation();
        if (Config.isCloudMode()) {
            sb.append("\"").append(PropertyAnalyzer.PROPERTIES_FILE_CACHE_TTL_SECONDS).append("\" = \"");
            sb.append(olapTable.getTTLSeconds()).append("\"");
        } else {
            sb.append("\"").append(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION).append("\" = \"");
            sb.append(replicaAlloc.toCreateStmt()).append("\"");

            // min load replica num
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_MIN_LOAD_REPLICA_NUM).append("\" = \"");
            sb.append(olapTable.getMinLoadReplicaNum()).append("\"");
        }

        // bloom filter
        Set<String> bfColumnNames = olapTable.getCopiedBfColumns();
        if (bfColumnNames != null) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_BF_COLUMNS).append("\" = \"");
            sb.append(Joiner.on(", ").join(olapTable.getCopiedBfColumns())).append("\"");
        }

        if (separatePartition) {
            // version info
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_VERSION_INFO).append("\" = \"");
            Partition partition = null;
            if (olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
                partition = olapTable.getPartition(olapTable.getName());
            } else {
                Preconditions.checkState(partitionId.size() == 1);
                partition = olapTable.getPartition(partitionId.get(0));
            }
            sb.append(partition.getVisibleVersion()).append("\"");
        }

        // mark this table is being synced
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED).append("\" = \"");
        sb.append(String.valueOf(olapTable.isBeingSynced() || getDdlForSync)).append("\"");
        // mark this table if it is a auto bucket table
        if (getDdlForSync && olapTable.isAutoBucket()) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_AUTO_BUCKET).append("\" = \"");
            sb.append("true").append("\"");
        }

        // colocateTable
        String colocateTable = olapTable.getColocateGroup();
        if (colocateTable != null) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH).append("\" = \"");
            sb.append(colocateTable).append("\"");
        }

        // dynamic partition
        if (olapTable.dynamicPartitionExists()) {
            sb.append(olapTable.getTableProperty().getDynamicPartitionProperty().getProperties(replicaAlloc));
        }

        // only display z-order sort info
        if (olapTable.isZOrderSort()) {
            sb.append(olapTable.getDataSortInfo().toSql());
        }

        // in memory
        if (olapTable.isInMemory()) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_INMEMORY).append("\" = \"");
            sb.append(olapTable.isInMemory()).append("\"");
        }

        // storage medium
        if (olapTable.getStorageMedium() != null) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM).append("\" = \"");
            sb.append(olapTable.getStorageMedium().name().toLowerCase());
            sb.append("\"");
        }

        // storage type
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT).append("\" = \"");
        sb.append(olapTable.getStorageFormat()).append("\"");

        // inverted index storage type
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_INVERTED_INDEX_STORAGE_FORMAT).append("\" = \"");
        sb.append(olapTable.getInvertedIndexFileStorageFormat()).append("\"");

        // compression type
        if (olapTable.getCompressionType() != TCompressionType.LZ4F) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_COMPRESSION).append("\" = \"");
            sb.append(olapTable.getCompressionType()).append("\"");
        }

        // estimate_partition_size
        if (!olapTable.getEstimatePartitionSize().equals("")) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_ESTIMATE_PARTITION_SIZE).append("\" = \"");
            sb.append(olapTable.getEstimatePartitionSize()).append("\"");
        }

        // unique key table with merge on write, always print this property for unique table
        if (olapTable.getKeysType() == KeysType.UNIQUE_KEYS) {
            sb.append(",\n\"").append(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE).append("\" = \"");
            sb.append(olapTable.getEnableUniqueKeyMergeOnWrite()).append("\"");
        }

        // enable_unique_key_skip_bitmap, always print this property for merge-on-write unique table
        if (olapTable.getKeysType() == KeysType.UNIQUE_KEYS && olapTable.getEnableUniqueKeyMergeOnWrite()
                && olapTable.getEnableUniqueKeySkipBitmap()) {
            sb.append(",\n\"").append(PropertyAnalyzer.ENABLE_UNIQUE_KEY_SKIP_BITMAP_COLUMN).append("\" = \"");
            sb.append(olapTable.getEnableUniqueKeySkipBitmap()).append("\"");
        }

        // show lightSchemaChange only when it is set true
        if (olapTable.getEnableLightSchemaChange()) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_ENABLE_LIGHT_SCHEMA_CHANGE).append("\" = \"");
            sb.append(olapTable.getEnableLightSchemaChange()).append("\"");

        }

        // storage policy
        if (olapTable.getStoragePolicy() != null && !olapTable.getStoragePolicy().equals("")) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY).append("\" = \"");
            sb.append(olapTable.getStoragePolicy()).append("\"");
        }

        // sequence type
        if (olapTable.hasSequenceCol()) {
            if (olapTable.getSequenceMapCol() != null) {
                sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_FUNCTION_COLUMN + "."
                        + PropertyAnalyzer.PROPERTIES_SEQUENCE_COL).append("\" = \"");
                sb.append(olapTable.getSequenceMapCol()).append("\"");
            } else {
                sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_FUNCTION_COLUMN + "."
                        + PropertyAnalyzer.PROPERTIES_SEQUENCE_TYPE).append("\" = \"");
                sb.append(olapTable.getSequenceType().toString()).append("\"");
            }
        }

        // store row column
        if (olapTable.storeRowColumn()) {
            List<String> rsColumnNames = olapTable.getTableProperty().getCopiedRowStoreColumns();
            if (rsColumnNames != null && !rsColumnNames.isEmpty()) {
                sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_ROW_STORE_COLUMNS).append("\" = \"");
                sb.append(Joiner.on(",").join(rsColumnNames)).append("\"");
            } else {
                sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_STORE_ROW_COLUMN).append("\" = \"");
                sb.append(olapTable.storeRowColumn()).append("\"");
            }

            // row store page size
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_ROW_STORE_PAGE_SIZE).append("\" = \"");
            sb.append(olapTable.rowStorePageSize()).append("\"");
        }

        // storage page size
        if (olapTable.storagePageSize() != PropertyAnalyzer.STORAGE_PAGE_SIZE_DEFAULT_VALUE) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_STORAGE_PAGE_SIZE).append("\" = \"");
            sb.append(olapTable.storagePageSize()).append("\"");
        }

        // storage dict page size
        if (olapTable.storageDictPageSize() != PropertyAnalyzer.STORAGE_DICT_PAGE_SIZE_DEFAULT_VALUE) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_STORAGE_DICT_PAGE_SIZE).append("\" = \"");
            sb.append(olapTable.storageDictPageSize()).append("\"");
        }

        // skip inverted index on load
        if (olapTable.skipWriteIndexOnLoad()) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_SKIP_WRITE_INDEX_ON_LOAD).append("\" = \"");
            sb.append(olapTable.skipWriteIndexOnLoad()).append("\"");
        }

        // compaction policy
        if (olapTable.getCompactionPolicy() != null && !olapTable.getCompactionPolicy().equals("")
                && !olapTable.getCompactionPolicy().equals(PropertyAnalyzer.SIZE_BASED_COMPACTION_POLICY)) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_COMPACTION_POLICY).append("\" = \"");
            sb.append(olapTable.getCompactionPolicy()).append("\"");
        }

        // time series compaction goal size
        if (olapTable.getCompactionPolicy() != null && olapTable.getCompactionPolicy()
                .equals(PropertyAnalyzer.TIME_SERIES_COMPACTION_POLICY)) {
            sb.append(",\n\"").append(PropertyAnalyzer
                    .PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES).append("\" = \"");
            sb.append(olapTable.getTimeSeriesCompactionGoalSizeMbytes()).append("\"");
        }

        // time series compaction file count threshold
        if (olapTable.getCompactionPolicy() != null && olapTable.getCompactionPolicy()
                .equals(PropertyAnalyzer.TIME_SERIES_COMPACTION_POLICY)) {
            sb.append(",\n\"").append(PropertyAnalyzer
                    .PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD).append("\" = \"");
            sb.append(olapTable.getTimeSeriesCompactionFileCountThreshold()).append("\"");
        }

        // time series compaction time threshold
        if (olapTable.getCompactionPolicy() != null && olapTable.getCompactionPolicy()
                .equals(PropertyAnalyzer.TIME_SERIES_COMPACTION_POLICY)) {
            sb.append(",\n\"").append(PropertyAnalyzer
                    .PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS).append("\" = \"");
            sb.append(olapTable.getTimeSeriesCompactionTimeThresholdSeconds()).append("\"");
        }

        // time series compaction empty rowsets threshold
        if (olapTable.getCompactionPolicy() != null && olapTable.getCompactionPolicy()
                .equals(PropertyAnalyzer.TIME_SERIES_COMPACTION_POLICY)) {
            sb.append(",\n\"").append(PropertyAnalyzer
                    .PROPERTIES_TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD).append("\" = \"");
            sb.append(olapTable.getTimeSeriesCompactionEmptyRowsetsThreshold()).append("\"");
        }

        // time series compaction level threshold
        if (olapTable.getCompactionPolicy() != null && olapTable.getCompactionPolicy()
                .equals(PropertyAnalyzer.TIME_SERIES_COMPACTION_POLICY)) {
            sb.append(",\n\"").append(PropertyAnalyzer
                    .PROPERTIES_TIME_SERIES_COMPACTION_LEVEL_THRESHOLD).append("\" = \"");
            sb.append(olapTable.getTimeSeriesCompactionLevelThreshold()).append("\"");
        }

        // Storage Vault
        if (!Strings.isNullOrEmpty(olapTable.getStorageVaultId())) {
            sb.append(",\n\"").append(PropertyAnalyzer
                    .PROPERTIES_STORAGE_VAULT_ID).append("\" = \"");
            sb.append(olapTable.getStorageVaultId()).append("\"");
            sb.append(",\n\"").append(PropertyAnalyzer
                    .PROPERTIES_STORAGE_VAULT_NAME).append("\" = \"");
            sb.append(olapTable.getStorageVaultName()).append("\"");
        }

        // disable auto compaction
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_DISABLE_AUTO_COMPACTION).append("\" = \"");
        sb.append(olapTable.disableAutoCompaction()).append("\"");

        if (olapTable.variantEnableFlattenNested()) {
            // enable flatten nested type in variant
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_VARIANT_ENABLE_FLATTEN_NESTED).append("\" = \"");
            sb.append(olapTable.variantEnableFlattenNested()).append("\"");
        }

        // binlog
        if (Config.enable_feature_binlog) {
            BinlogConfig binlogConfig = olapTable.getBinlogConfig();
            binlogConfig.appendToShowCreateTable(sb);
        }

        // enable single replica compaction
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_ENABLE_SINGLE_REPLICA_COMPACTION).append("\" = \"");
        sb.append(olapTable.enableSingleReplicaCompaction()).append("\"");

        // group commit interval ms
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_INTERVAL_MS).append("\" = \"");
        sb.append(olapTable.getGroupCommitIntervalMs()).append("\"");

        // group commit data bytes
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_DATA_BYTES).append("\" = \"");
        sb.append(olapTable.getGroupCommitDataBytes()).append("\"");

        // enable delete on delete predicate
        if (olapTable.getKeysType() == KeysType.UNIQUE_KEYS && olapTable.getEnableUniqueKeyMergeOnWrite()) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_ENABLE_MOW_LIGHT_DELETE)
                    .append("\" = \"");
            sb.append(olapTable.getEnableMowLightDelete()).append("\"");
        }

        // enable duplicate without keys by default
        if (olapTable.isDuplicateWithoutKey()) {
            sb.append(",\n\"")
                    .append(PropertyAnalyzer.PROPERTIES_ENABLE_DUPLICATE_WITHOUT_KEYS_BY_DEFAULT)
                    .append("\" = \"");
            sb.append(olapTable.isDuplicateWithoutKey()).append("\"");
        }

        if (olapTable.isInAtomicRestore()) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_IN_ATOMIC_RESTORE).append("\" = \"true\"");
        }
    }

    /**.
     *
     * @param getDdlForLike Get schema for 'create table like' or not. when true, without hidden columns.
     */
    public static void getCreateTableLikeStmt(CreateTableLikeInfo createTableLikeInfo, String dbName,
                                              TableIf table, List<String> createTableStmt,
                                              List<String> addPartitionStmt, List<String> createRollupStmt,
                                              boolean separatePartition,
                                              boolean hidePassword, boolean getDdlForLike, long specificVersion,
                                              boolean getBriefDdl, boolean getDdlForSync) {
        StringBuilder sb = new StringBuilder();

        // 1. create table
        // 1.1 view
        if (table.getType() == TableType.VIEW) {
            View view = (View) table;

            sb.append("CREATE VIEW `").append(table.getName()).append("`");
            if (StringUtils.isNotBlank(table.getComment())) {
                sb.append(" COMMENT '").append(table.getComment()).append("'");
            }
            sb.append(" AS ").append(view.getInlineViewDef());
            createTableStmt.add(sb + ";");
            return;
        }

        // 1.2 other table type
        sb.append("CREATE ");
        if (table.getType() == TableType.ODBC || table.getType() == TableType.MYSQL
                || table.getType() == TableType.ELASTICSEARCH || table.getType() == TableType.BROKER
                || table.getType() == TableType.HIVE || table.getType() == TableType.JDBC) {
            sb.append("EXTERNAL ");
        }

        // create table like a temporary table
        if (createTableLikeInfo.isTemp()) {
            sb.append("TEMPORARY ");
        }

        sb.append(table.getType() != TableType.MATERIALIZED_VIEW ? "TABLE " : "MATERIALIZED VIEW ");

        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("`").append(dbName).append("`.");
        }
        if (table.isTemporary()) {
            sb.append("`").append(Util.getTempTableDisplayName(table.getName())).append("`");
        } else {
            sb.append("`").append(table.getName()).append("`");
        }


        sb.append(" (\n");
        int idx = 0;
        List<Column> columns = table.getBaseSchema(false);
        for (Column column : columns) {
            if (idx++ != 0) {
                sb.append(",\n");
            }
            // There MUST BE 2 space in front of each column description line
            // sqlalchemy requires this to parse SHOW CREATE TABLE stmt.
            if (table.isManagedTable()) {
                sb.append("  ").append(
                        column.toSql(((OlapTable) table).getKeysType() == KeysType.UNIQUE_KEYS, true));
            } else {
                sb.append("  ").append(column.toSql());
            }
        }
        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            if (CollectionUtils.isNotEmpty(olapTable.getIndexes())) {
                for (Index index : olapTable.getIndexes()) {
                    sb.append(",\n");
                    sb.append("  ").append(index.toSql());
                }
            }
        }
        sb.append("\n) ENGINE=");
        sb.append(table.getType().name());

        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            // keys
            String keySql = olapTable.getKeysType().toSql();
            if (olapTable.isDuplicateWithoutKey()) {
                // after #18621, use can create a DUP_KEYS olap table without key columns
                // and get a ddl schema without key type and key columns
            } else {
                sb.append("\n").append(keySql).append("(");
                List<String> keysColumnNames = Lists.newArrayList();
                Map<Integer, String> clusterKeysColumnNamesToId = new TreeMap<>();
                for (Column column : olapTable.getBaseSchema()) {
                    if (column.isKey()) {
                        keysColumnNames.add("`" + column.getName() + "`");
                    }
                    if (column.isClusterKey()) {
                        clusterKeysColumnNamesToId.put(column.getClusterKeyId(), column.getName());
                    }
                }
                sb.append(Joiner.on(", ").join(keysColumnNames)).append(")");
                // show cluster keys
                if (!clusterKeysColumnNamesToId.isEmpty()) {
                    sb.append("\n").append("CLUSTER BY (`");
                    sb.append(Joiner.on("`, `").join(clusterKeysColumnNamesToId.values())).append("`)");
                }
            }

            if (specificVersion != -1) {
                // for copy tablet operation
                sb.append("\nDISTRIBUTED BY HASH(").append(olapTable.getBaseSchema().get(0).getName())
                    .append(") BUCKETS 1");
                sb.append("\nPROPERTIES (\n" + "\"replication_num\" = \"1\",\n" + "\"version_info\" = \""
                        + specificVersion + "\"\n" + ")");
                createTableStmt.add(sb + ";");
                return;
            }

            addTableComment(olapTable, sb);

            // partition
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            List<Long> partitionId = null;
            if (separatePartition) {
                partitionId = Lists.newArrayList();
            }
            if (!getBriefDdl && (partitionInfo.getType() == PartitionType.RANGE
                    || partitionInfo.getType() == PartitionType.LIST)) {
                sb.append("\n").append(partitionInfo.toSql(olapTable, partitionId));
            }

            // distribution
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            sb.append("\n").append(distributionInfo.toSql());

            // rollup index
            ArrayList<String> rollupNames = createTableLikeInfo.getRollupNames();
            boolean withAllRollup = createTableLikeInfo.isWithAllRollup();
            List<Long> addIndexIdList = Lists.newArrayList();

            if (!CollectionUtils.isEmpty(rollupNames)) {
                for (String rollupName : rollupNames) {
                    addIndexIdList.add(olapTable.getIndexIdByName(rollupName));
                }
            } else if (withAllRollup) {
                addIndexIdList = olapTable.getIndexIdListExceptBaseIndex();
            }

            if (!addIndexIdList.isEmpty()) {
                sb.append("\n").append("rollup (");
            }

            int size = addIndexIdList.size();
            int index = 1;
            for (long indexId : addIndexIdList) {
                String indexName = olapTable.getIndexNameById(indexId);
                sb.append("\n").append(indexName).append("(");
                List<Column> indexSchema = olapTable.getSchemaByIndexId(indexId, false);
                for (int i = 0; i < indexSchema.size(); i++) {
                    Column column = indexSchema.get(i);
                    sb.append(column.getName());
                    if (i != indexSchema.size() - 1) {
                        sb.append(", ");
                    }
                }
                if (index != size) {
                    sb.append("),");
                } else {
                    sb.append(")");
                    sb.append("\n)");
                }
                index++;
            }

            // with all rollup
            do {
                if (!getDdlForSync) {
                    break;
                }
                List<Long> indexIds = new ArrayList<>(olapTable.getIndexIdToMeta().keySet());
                if (indexIds.size() == 1 && indexIds.get(0) == olapTable.getBaseIndexId()) {
                    break;
                }
                indexIds = indexIds.stream().filter(item -> item != olapTable.getBaseIndexId())
                    .collect(Collectors.toList());
                sb.append("\nROLLUP (\n");
                for (int i = 0; i < indexIds.size(); i++) {
                    Long indexId = indexIds.get(i);

                    MaterializedIndexMeta materializedIndexMeta = olapTable.getIndexIdToMeta().get(indexId);
                    String indexName = olapTable.getIndexNameById(indexId);
                    sb.append(indexName).append(" (");

                    List<Column> indexSchema = materializedIndexMeta.getSchema();
                    for (int j = 0; j < indexSchema.size(); j++) {
                        Column column = indexSchema.get(j);
                        sb.append(column.getName());
                        if (j != indexSchema.size() - 1) {
                            sb.append(", ");
                        }
                    }
                    sb.append(")");
                    if (i != indexIds.size() - 1) {
                        sb.append(",\n");
                    }
                }
                sb.append("\n)");
            } while (false);

            // properties
            sb.append("\nPROPERTIES (\n");
            addOlapTablePropertyInfo(olapTable, sb, separatePartition, getDdlForSync, partitionId);
            sb.append("\n)");
        } else if (table.getType() == TableType.MYSQL) {
            MysqlTable mysqlTable = (MysqlTable) table;

            addTableComment(mysqlTable, sb);

            // properties
            sb.append("\nPROPERTIES (\n");
            if (mysqlTable.getOdbcCatalogResourceName() == null) {
                sb.append("\"host\" = \"").append(mysqlTable.getHost()).append("\",\n");
                sb.append("\"port\" = \"").append(mysqlTable.getPort()).append("\",\n");
                sb.append("\"user\" = \"").append(mysqlTable.getUserName()).append("\",\n");
                sb.append("\"password\" = \"").append(hidePassword ? "" : mysqlTable.getPasswd()).append("\",\n");
                sb.append("\"charset\" = \"").append(mysqlTable.getCharset()).append("\",\n");
            } else {
                sb.append("\"odbc_catalog_resource\" = \"").append(mysqlTable.getOdbcCatalogResourceName())
                    .append("\",\n");
            }
            sb.append("\"database\" = \"").append(mysqlTable.getMysqlDatabaseName()).append("\",\n");
            sb.append("\"table\" = \"").append(mysqlTable.getMysqlTableName()).append("\"\n");
            sb.append(")");
        } else if (table.getType() == TableType.ODBC) {
            OdbcTable odbcTable = (OdbcTable) table;

            addTableComment(odbcTable, sb);

            // properties
            sb.append("\nPROPERTIES (\n");
            if (odbcTable.getOdbcCatalogResourceName() == null) {
                sb.append("\"host\" = \"").append(odbcTable.getHost()).append("\",\n");
                sb.append("\"port\" = \"").append(odbcTable.getPort()).append("\",\n");
                sb.append("\"user\" = \"").append(odbcTable.getUserName()).append("\",\n");
                sb.append("\"password\" = \"").append(hidePassword ? "" : odbcTable.getPasswd()).append("\",\n");
                sb.append("\"driver\" = \"").append(odbcTable.getOdbcDriver()).append("\",\n");
                sb.append("\"odbc_type\" = \"").append(odbcTable.getOdbcTableTypeName()).append("\",\n");
                sb.append("\"charest\" = \"").append(odbcTable.getCharset()).append("\",\n");
            } else {
                sb.append("\"odbc_catalog_resource\" = \"").append(odbcTable.getOdbcCatalogResourceName())
                    .append("\",\n");
            }
            sb.append("\"database\" = \"").append(odbcTable.getOdbcDatabaseName()).append("\",\n");
            sb.append("\"table\" = \"").append(odbcTable.getOdbcTableName()).append("\"\n");
            sb.append(")");
        } else if (table.getType() == TableType.BROKER) {
            BrokerTable brokerTable = (BrokerTable) table;

            addTableComment(brokerTable, sb);

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"broker_name\" = \"").append(brokerTable.getBrokerName()).append("\",\n");
            sb.append("\"path\" = \"").append(Joiner.on(",").join(brokerTable.getEncodedPaths())).append("\",\n");
            sb.append("\"column_separator\" = \"").append(brokerTable.getReadableColumnSeparator()).append("\",\n");
            sb.append("\"line_delimiter\" = \"").append(brokerTable.getReadableLineDelimiter()).append("\",\n");
            sb.append(")");
            if (!brokerTable.getBrokerProperties().isEmpty()) {
                sb.append("\nBROKER PROPERTIES (\n");
                sb.append(new PrintableMap<>(brokerTable.getBrokerProperties(), " = ", true, true,
                        hidePassword).toString());
                sb.append("\n)");
            }
        } else if (table.getType() == TableType.ELASTICSEARCH) {
            EsTable esTable = (EsTable) table;

            addTableComment(esTable, sb);

            // partition
            PartitionInfo partitionInfo = esTable.getPartitionInfo();
            if (partitionInfo.getType() == PartitionType.RANGE) {
                sb.append("\n");
                sb.append("PARTITION BY RANGE(");
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                for (Column column : rangePartitionInfo.getPartitionColumns()) {
                    sb.append("`").append(column.getName()).append("`");
                }
                sb.append(")\n()");
            }

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"hosts\" = \"").append(esTable.getHosts()).append("\",\n");
            sb.append("\"user\" = \"").append(esTable.getUserName()).append("\",\n");
            sb.append("\"password\" = \"").append(hidePassword ? "" : esTable.getPasswd()).append("\",\n");
            sb.append("\"index\" = \"").append(esTable.getIndexName()).append("\",\n");
            if (esTable.getMappingType() != null) {
                sb.append("\"type\" = \"").append(esTable.getMappingType()).append("\",\n");
            }
            sb.append("\"enable_docvalue_scan\" = \"").append(esTable.isEnableDocValueScan()).append("\",\n");
            sb.append("\"max_docvalue_fields\" = \"").append(esTable.getMaxDocValueFields()).append("\",\n");
            sb.append("\"enable_keyword_sniff\" = \"").append(esTable.isEnableKeywordSniff()).append("\",\n");
            sb.append("\"nodes_discovery\" = \"").append(esTable.isNodesDiscovery()).append("\",\n");
            sb.append("\"http_ssl_enabled\" = \"").append(esTable.isHttpSslEnabled()).append("\",\n");
            sb.append("\"like_push_down\" = \"").append(esTable.isLikePushDown()).append("\"\n");
            sb.append(")");
        } else if (table.getType() == TableType.HIVE) {
            HiveTable hiveTable = (HiveTable) table;

            addTableComment(hiveTable, sb);

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"database\" = \"").append(hiveTable.getHiveDb()).append("\",\n");
            sb.append("\"table\" = \"").append(hiveTable.getHiveTable()).append("\",\n");
            sb.append(new PrintableMap<>(hiveTable.getHiveProperties(), " = ", true, true, hidePassword).toString());
            sb.append("\n)");
        } else if (table.getType() == TableType.JDBC) {
            JdbcTable jdbcTable = (JdbcTable) table;
            addTableComment(jdbcTable, sb);
            sb.append("\nPROPERTIES (\n");
            sb.append("\"resource\" = \"").append(jdbcTable.getResourceName()).append("\",\n");
            sb.append("\"table\" = \"").append(jdbcTable.getJdbcTable()).append("\",\n");
            sb.append("\"table_type\" = \"").append(jdbcTable.getJdbcTypeName()).append("\"");
            sb.append("\n)");
        } else if (table.getType() == TableType.ICEBERG_EXTERNAL_TABLE) {
            addTableComment(table, sb);
            IcebergExternalTable icebergExternalTable = (IcebergExternalTable) table;
            sb.append("\nLOCATION '").append(icebergExternalTable.location()).append("'");
            sb.append("\nPROPERTIES (");
            Iterator<Entry<String, String>> iterator = icebergExternalTable.properties().entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, String> prop = iterator.next();
                sb.append("\n  \"").append(prop.getKey()).append("\" = \"").append(prop.getValue()).append("\"");
                if (iterator.hasNext()) {
                    sb.append(",");
                }
            }
            sb.append("\n)");
        }

        createTableStmt.add(sb + ";");

        // 2. add partition
        if (separatePartition && (table instanceof OlapTable) && ((OlapTable) table).getPartitions().size() > 1) {
            if (((OlapTable) table).getPartitionInfo().getType() == PartitionType.RANGE
                    || ((OlapTable) table).getPartitionInfo().getType() == PartitionType.LIST) {
                OlapTable olapTable = (OlapTable) table;
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                boolean first = true;
                for (Map.Entry<Long, PartitionItem> entry : partitionInfo.getPartitionItemEntryList(false, true)) {
                    if (first) {
                        first = false;
                        continue;
                    }
                    sb = new StringBuilder();
                    Partition partition = olapTable.getPartition(entry.getKey());
                    sb.append("ALTER TABLE ").append(table.getName());
                    sb.append(" ADD PARTITION ").append(partition.getName()).append(" VALUES ");
                    if (partitionInfo.getType() == PartitionType.RANGE) {
                        sb.append("[");
                        sb.append(((RangePartitionItem) entry.getValue()).getItems().lowerEndpoint().toSql());
                        sb.append(", ");
                        sb.append(((RangePartitionItem) entry.getValue()).getItems().upperEndpoint().toSql());
                        sb.append(")");
                    } else if (partitionInfo.getType() == PartitionType.LIST) {
                        sb.append("IN (");
                        sb.append(((ListPartitionItem) entry.getValue()).toSql());
                        sb.append(")");
                    }
                    sb.append("(\"version_info\" = \"");
                    sb.append(partition.getVisibleVersion()).append("\"");
                    sb.append(");");
                    addPartitionStmt.add(sb + ";");
                }
            }
        }

        // 3. rollup
        if (createRollupStmt != null && (table instanceof OlapTable)) {
            OlapTable olapTable = (OlapTable) table;
            for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getIndexIdToMeta().entrySet()) {
                if (entry.getKey() == olapTable.getBaseIndexId()) {
                    continue;
                }
                MaterializedIndexMeta materializedIndexMeta = entry.getValue();
                sb = new StringBuilder();
                String indexName = olapTable.getIndexNameById(entry.getKey());
                sb.append("ALTER TABLE ").append(table.getName()).append(" ADD ROLLUP ").append(indexName);
                sb.append("(");

                List<Column> indexSchema = materializedIndexMeta.getSchema();
                for (int i = 0; i < indexSchema.size(); i++) {
                    Column column = indexSchema.get(i);
                    sb.append(column.getName());
                    if (i != indexSchema.size() - 1) {
                        sb.append(", ");
                    }
                }
                sb.append(");");
                createRollupStmt.add(sb + ";");
            }
        }
    }

    /**
     * Get table ddl stmt.
     *
     * @param getDdlForLike Get schema for 'create table like' or not. when true, without hidden columns.
     */
    public static void getDdlStmt(DdlStmt ddlStmt, String dbName, TableIf table, List<String> createTableStmt,
                                  List<String> addPartitionStmt, List<String> createRollupStmt,
                                  boolean separatePartition,
                                  boolean hidePassword, boolean getDdlForLike, long specificVersion,
                                  boolean getBriefDdl, boolean getDdlForSync) {
        StringBuilder sb = new StringBuilder();

        // 1. create table
        // 1.1 view
        if (table.getType() == TableType.VIEW) {
            View view = (View) table;

            sb.append("CREATE VIEW `").append(table.getName()).append("`");
            if (StringUtils.isNotBlank(table.getComment())) {
                sb.append(" COMMENT '").append(table.getComment()).append("'");
            }
            sb.append(" AS ").append(view.getInlineViewDef());
            createTableStmt.add(sb + ";");
            return;
        }

        // 1.2 other table type
        sb.append("CREATE ");
        if (table.getType() == TableType.ODBC || table.getType() == TableType.MYSQL
                || table.getType() == TableType.ELASTICSEARCH || table.getType() == TableType.BROKER
                || table.getType() == TableType.HIVE || table.getType() == TableType.JDBC) {
            sb.append("EXTERNAL ");
        }

        // create table like a temporary table or create temporary table like a table
        if (ddlStmt instanceof CreateTableLikeStmt) {
            if (((CreateTableLikeStmt) ddlStmt).isTemp()) {
                sb.append("TEMPORARY ");
            }
        } else if (table.isTemporary()) {
            // used for show create table
            sb.append("TEMPORARY ");
        }
        sb.append(table.getType() != TableType.MATERIALIZED_VIEW ? "TABLE " : "MATERIALIZED VIEW ");

        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("`").append(dbName).append("`.");
        }
        if (table.isTemporary()) {
            sb.append("`").append(Util.getTempTableDisplayName(table.getName())).append("`");
        } else {
            sb.append("`").append(table.getName()).append("`");
        }


        sb.append(" (\n");
        int idx = 0;
        List<Column> columns = table.getBaseSchema(false);
        for (Column column : columns) {
            if (idx++ != 0) {
                sb.append(",\n");
            }
            // There MUST BE 2 space in front of each column description line
            // sqlalchemy requires this to parse SHOW CREATE TABLE stmt.
            if (table.isManagedTable()) {
                sb.append("  ").append(
                        column.toSql(((OlapTable) table).getKeysType() == KeysType.UNIQUE_KEYS, true));
            } else {
                sb.append("  ").append(column.toSql());
            }
        }
        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            if (CollectionUtils.isNotEmpty(olapTable.getIndexes())) {
                for (Index index : olapTable.getIndexes()) {
                    sb.append(",\n");
                    sb.append("  ").append(index.toSql());
                }
            }
        }
        sb.append("\n) ENGINE=");
        sb.append(table.getType().name());

        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            // keys
            String keySql = olapTable.getKeysType().toSql();
            if (olapTable.isDuplicateWithoutKey()) {
                // after #18621, use can create a DUP_KEYS olap table without key columns
                // and get a ddl schema without key type and key columns
            } else {
                sb.append("\n").append(keySql).append("(");
                List<String> keysColumnNames = Lists.newArrayList();
                Map<Integer, String> clusterKeysColumnNamesToId = new TreeMap<>();
                for (Column column : olapTable.getBaseSchema()) {
                    if (column.isKey()) {
                        keysColumnNames.add("`" + column.getName() + "`");
                    }
                    if (column.isClusterKey()) {
                        clusterKeysColumnNamesToId.put(column.getClusterKeyId(), column.getName());
                    }
                }
                sb.append(Joiner.on(", ").join(keysColumnNames)).append(")");
                // show cluster keys
                if (!clusterKeysColumnNamesToId.isEmpty()) {
                    sb.append("\n").append("CLUSTER BY (`");
                    sb.append(Joiner.on("`, `").join(clusterKeysColumnNamesToId.values())).append("`)");
                }
            }

            if (specificVersion != -1) {
                // for copy tablet operation
                sb.append("\nDISTRIBUTED BY HASH(").append(olapTable.getBaseSchema().get(0).getName())
                        .append(") BUCKETS 1");
                sb.append("\nPROPERTIES (\n" + "\"replication_num\" = \"1\",\n" + "\"version_info\" = \""
                        + specificVersion + "\"\n" + ")");
                createTableStmt.add(sb + ";");
                return;
            }

            addTableComment(olapTable, sb);

            // partition
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            List<Long> partitionId = null;
            if (separatePartition) {
                partitionId = Lists.newArrayList();
            }
            if (!getBriefDdl && (partitionInfo.getType() == PartitionType.RANGE
                    || partitionInfo.getType() == PartitionType.LIST)) {
                sb.append("\n").append(partitionInfo.toSql(olapTable, partitionId));
            }

            // distribution
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            sb.append("\n").append(distributionInfo.toSql());

            // rollup index
            if (ddlStmt instanceof CreateTableLikeStmt) {

                CreateTableLikeStmt stmt = (CreateTableLikeStmt) ddlStmt;

                ArrayList<String> rollupNames = stmt.getRollupNames();
                boolean withAllRollup = stmt.isWithAllRollup();
                List<Long> addIndexIdList = Lists.newArrayList();

                if (!CollectionUtils.isEmpty(rollupNames)) {
                    for (String rollupName : rollupNames) {
                        addIndexIdList.add(olapTable.getIndexIdByName(rollupName));
                    }
                } else if (withAllRollup) {
                    addIndexIdList = olapTable.getIndexIdListExceptBaseIndex();
                }

                if (!addIndexIdList.isEmpty()) {
                    sb.append("\n").append("rollup (");
                }

                int size = addIndexIdList.size();
                int index = 1;
                for (long indexId : addIndexIdList) {
                    String indexName = olapTable.getIndexNameById(indexId);
                    sb.append("\n").append(indexName).append("(");
                    List<Column> indexSchema = olapTable.getSchemaByIndexId(indexId, false);
                    for (int i = 0; i < indexSchema.size(); i++) {
                        Column column = indexSchema.get(i);
                        sb.append(column.getName());
                        if (i != indexSchema.size() - 1) {
                            sb.append(", ");
                        }
                    }
                    if (index != size) {
                        sb.append("),");
                    } else {
                        sb.append(")");
                        sb.append("\n)");
                    }
                    index++;
                }
            }

            // with all rollup
            do {
                if (!getDdlForSync) {
                    break;
                }
                List<Long> indexIds = new ArrayList<>(olapTable.getIndexIdToMeta().keySet());
                if (indexIds.size() == 1 && indexIds.get(0) == olapTable.getBaseIndexId()) {
                    break;
                }
                indexIds = indexIds.stream().filter(item -> item != olapTable.getBaseIndexId())
                        .collect(Collectors.toList());
                sb.append("\nROLLUP (\n");
                for (int i = 0; i < indexIds.size(); i++) {
                    Long indexId = indexIds.get(i);

                    MaterializedIndexMeta materializedIndexMeta = olapTable.getIndexIdToMeta().get(indexId);
                    String indexName = olapTable.getIndexNameById(indexId);
                    sb.append(indexName).append(" (");

                    List<Column> indexSchema = materializedIndexMeta.getSchema();
                    for (int j = 0; j < indexSchema.size(); j++) {
                        Column column = indexSchema.get(j);
                        sb.append(column.getName());
                        if (j != indexSchema.size() - 1) {
                            sb.append(", ");
                        }
                    }
                    sb.append(")");
                    if (i != indexIds.size() - 1) {
                        sb.append(",\n");
                    }
                }
                sb.append("\n)");
            } while (false);

            // properties
            sb.append("\nPROPERTIES (\n");
            addOlapTablePropertyInfo(olapTable, sb, separatePartition, getDdlForSync, partitionId);
            sb.append("\n)");
        } else if (table.getType() == TableType.MYSQL) {
            MysqlTable mysqlTable = (MysqlTable) table;

            addTableComment(mysqlTable, sb);

            // properties
            sb.append("\nPROPERTIES (\n");
            if (mysqlTable.getOdbcCatalogResourceName() == null) {
                sb.append("\"host\" = \"").append(mysqlTable.getHost()).append("\",\n");
                sb.append("\"port\" = \"").append(mysqlTable.getPort()).append("\",\n");
                sb.append("\"user\" = \"").append(mysqlTable.getUserName()).append("\",\n");
                sb.append("\"password\" = \"").append(hidePassword ? "" : mysqlTable.getPasswd()).append("\",\n");
                sb.append("\"charset\" = \"").append(mysqlTable.getCharset()).append("\",\n");
            } else {
                sb.append("\"odbc_catalog_resource\" = \"").append(mysqlTable.getOdbcCatalogResourceName())
                        .append("\",\n");
            }
            sb.append("\"database\" = \"").append(mysqlTable.getMysqlDatabaseName()).append("\",\n");
            sb.append("\"table\" = \"").append(mysqlTable.getMysqlTableName()).append("\"\n");
            sb.append(")");
        } else if (table.getType() == TableType.ODBC) {
            OdbcTable odbcTable = (OdbcTable) table;

            addTableComment(odbcTable, sb);

            // properties
            sb.append("\nPROPERTIES (\n");
            if (odbcTable.getOdbcCatalogResourceName() == null) {
                sb.append("\"host\" = \"").append(odbcTable.getHost()).append("\",\n");
                sb.append("\"port\" = \"").append(odbcTable.getPort()).append("\",\n");
                sb.append("\"user\" = \"").append(odbcTable.getUserName()).append("\",\n");
                sb.append("\"password\" = \"").append(hidePassword ? "" : odbcTable.getPasswd()).append("\",\n");
                sb.append("\"driver\" = \"").append(odbcTable.getOdbcDriver()).append("\",\n");
                sb.append("\"odbc_type\" = \"").append(odbcTable.getOdbcTableTypeName()).append("\",\n");
                sb.append("\"charest\" = \"").append(odbcTable.getCharset()).append("\",\n");
            } else {
                sb.append("\"odbc_catalog_resource\" = \"").append(odbcTable.getOdbcCatalogResourceName())
                        .append("\",\n");
            }
            sb.append("\"database\" = \"").append(odbcTable.getOdbcDatabaseName()).append("\",\n");
            sb.append("\"table\" = \"").append(odbcTable.getOdbcTableName()).append("\"\n");
            sb.append(")");
        } else if (table.getType() == TableType.BROKER) {
            BrokerTable brokerTable = (BrokerTable) table;

            addTableComment(brokerTable, sb);

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"broker_name\" = \"").append(brokerTable.getBrokerName()).append("\",\n");
            sb.append("\"path\" = \"").append(Joiner.on(",").join(brokerTable.getEncodedPaths())).append("\",\n");
            sb.append("\"column_separator\" = \"").append(brokerTable.getReadableColumnSeparator()).append("\",\n");
            sb.append("\"line_delimiter\" = \"").append(brokerTable.getReadableLineDelimiter()).append("\",\n");
            sb.append(")");
            if (!brokerTable.getBrokerProperties().isEmpty()) {
                sb.append("\nBROKER PROPERTIES (\n");
                sb.append(new PrintableMap<>(brokerTable.getBrokerProperties(), " = ", true, true,
                        hidePassword).toString());
                sb.append("\n)");
            }
        } else if (table.getType() == TableType.ELASTICSEARCH) {
            EsTable esTable = (EsTable) table;

            addTableComment(esTable, sb);

            // partition
            PartitionInfo partitionInfo = esTable.getPartitionInfo();
            if (partitionInfo.getType() == PartitionType.RANGE) {
                sb.append("\n");
                sb.append("PARTITION BY RANGE(");
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                for (Column column : rangePartitionInfo.getPartitionColumns()) {
                    sb.append("`").append(column.getName()).append("`");
                }
                sb.append(")\n()");
            }

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"hosts\" = \"").append(esTable.getHosts()).append("\",\n");
            sb.append("\"user\" = \"").append(esTable.getUserName()).append("\",\n");
            sb.append("\"password\" = \"").append(hidePassword ? "" : esTable.getPasswd()).append("\",\n");
            sb.append("\"index\" = \"").append(esTable.getIndexName()).append("\",\n");
            if (esTable.getMappingType() != null) {
                sb.append("\"type\" = \"").append(esTable.getMappingType()).append("\",\n");
            }
            sb.append("\"enable_docvalue_scan\" = \"").append(esTable.isEnableDocValueScan()).append("\",\n");
            sb.append("\"max_docvalue_fields\" = \"").append(esTable.getMaxDocValueFields()).append("\",\n");
            sb.append("\"enable_keyword_sniff\" = \"").append(esTable.isEnableKeywordSniff()).append("\",\n");
            sb.append("\"nodes_discovery\" = \"").append(esTable.isNodesDiscovery()).append("\",\n");
            sb.append("\"http_ssl_enabled\" = \"").append(esTable.isHttpSslEnabled()).append("\",\n");
            sb.append("\"like_push_down\" = \"").append(esTable.isLikePushDown()).append("\"\n");
            sb.append(")");
        } else if (table.getType() == TableType.HIVE) {
            HiveTable hiveTable = (HiveTable) table;

            addTableComment(hiveTable, sb);

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"database\" = \"").append(hiveTable.getHiveDb()).append("\",\n");
            sb.append("\"table\" = \"").append(hiveTable.getHiveTable()).append("\",\n");
            sb.append(new PrintableMap<>(hiveTable.getHiveProperties(), " = ", true, true, hidePassword).toString());
            sb.append("\n)");
        } else if (table.getType() == TableType.JDBC) {
            JdbcTable jdbcTable = (JdbcTable) table;
            addTableComment(jdbcTable, sb);
            sb.append("\nPROPERTIES (\n");
            sb.append("\"resource\" = \"").append(jdbcTable.getResourceName()).append("\",\n");
            sb.append("\"table\" = \"").append(jdbcTable.getJdbcTable()).append("\",\n");
            sb.append("\"table_type\" = \"").append(jdbcTable.getJdbcTypeName()).append("\"");
            sb.append("\n)");
        } else if (table.getType() == TableType.ICEBERG_EXTERNAL_TABLE) {
            addTableComment(table, sb);
            IcebergExternalTable icebergExternalTable = (IcebergExternalTable) table;
            sb.append("\nLOCATION '").append(icebergExternalTable.location()).append("'");
            sb.append("\nPROPERTIES (");
            Iterator<Entry<String, String>> iterator = icebergExternalTable.properties().entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, String> prop = iterator.next();
                sb.append("\n  \"").append(prop.getKey()).append("\" = \"").append(prop.getValue()).append("\"");
                if (iterator.hasNext()) {
                    sb.append(",");
                }
            }
            sb.append("\n)");
        } else if (table.getType() == TableType.PAIMON_EXTERNAL_TABLE) {
            addTableComment(table, sb);
            PaimonExternalTable paimonExternalTable = (PaimonExternalTable) table;
            Map<String, String> properties = paimonExternalTable.getTableProperties();
            sb.append("\nLOCATION '").append(properties.getOrDefault("path", "")).append("'");
            sb.append("\nPROPERTIES (");
            Iterator<Entry<String, String>> iterator = properties.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, String> prop = iterator.next();
                sb.append("\n  \"").append(prop.getKey()).append("\" = \"").append(prop.getValue()).append("\"");
                if (iterator.hasNext()) {
                    sb.append(",");
                }
            }
            sb.append("\n)");
        }

        createTableStmt.add(sb + ";");

        // 2. add partition
        if (separatePartition && (table instanceof OlapTable) && ((OlapTable) table).getPartitions().size() > 1) {
            if (((OlapTable) table).getPartitionInfo().getType() == PartitionType.RANGE
                    || ((OlapTable) table).getPartitionInfo().getType() == PartitionType.LIST) {
                OlapTable olapTable = (OlapTable) table;
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                boolean first = true;
                for (Map.Entry<Long, PartitionItem> entry : partitionInfo.getPartitionItemEntryList(false, true)) {
                    if (first) {
                        first = false;
                        continue;
                    }
                    sb = new StringBuilder();
                    Partition partition = olapTable.getPartition(entry.getKey());
                    sb.append("ALTER TABLE ").append(table.getName());
                    sb.append(" ADD PARTITION ").append(partition.getName()).append(" VALUES ");
                    if (partitionInfo.getType() == PartitionType.RANGE) {
                        sb.append("[");
                        sb.append(((RangePartitionItem) entry.getValue()).getItems().lowerEndpoint().toSql());
                        sb.append(", ");
                        sb.append(((RangePartitionItem) entry.getValue()).getItems().upperEndpoint().toSql());
                        sb.append(")");
                    } else if (partitionInfo.getType() == PartitionType.LIST) {
                        sb.append("IN (");
                        sb.append(((ListPartitionItem) entry.getValue()).toSql());
                        sb.append(")");
                    }
                    sb.append("(\"version_info\" = \"");
                    sb.append(partition.getVisibleVersion()).append("\"");
                    sb.append(");");
                    addPartitionStmt.add(sb + ";");
                }
            }
        }

        // 3. rollup
        if (createRollupStmt != null && (table instanceof OlapTable)) {
            OlapTable olapTable = (OlapTable) table;
            for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getIndexIdToMeta().entrySet()) {
                if (entry.getKey() == olapTable.getBaseIndexId()) {
                    continue;
                }
                MaterializedIndexMeta materializedIndexMeta = entry.getValue();
                sb = new StringBuilder();
                String indexName = olapTable.getIndexNameById(entry.getKey());
                sb.append("ALTER TABLE ").append(table.getName()).append(" ADD ROLLUP ").append(indexName);
                sb.append("(");

                List<Column> indexSchema = materializedIndexMeta.getSchema();
                for (int i = 0; i < indexSchema.size(); i++) {
                    Column column = indexSchema.get(i);
                    sb.append(column.getName());
                    if (i != indexSchema.size() - 1) {
                        sb.append(", ");
                    }
                }
                sb.append(");");
                createRollupStmt.add(sb + ";");
            }
        }
    }

    public void replayCreateTable(CreateTableInfo info) throws MetaNotFoundException {
        if (Strings.isNullOrEmpty(info.getCtlName()) || info.getCtlName()
                .equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            Table table = info.getTable();
            getInternalCatalog().replayCreateTable(info.getDbName(), table);
            if (table instanceof MTMV) {
                ((MTMV) table).compatible(Env.getCurrentEnv().getCatalogMgr());
            }
        } else {
            ExternalCatalog externalCatalog = (ExternalCatalog) catalogMgr.getCatalog(info.getCtlName());
            if (externalCatalog != null) {
                externalCatalog.replayCreateTable(info.getDbName(), info.getTblName());
            }
        }
    }

    public void replayAlterExternalTableSchema(String dbName, String tableName, List<Column> newSchema)
            throws MetaNotFoundException {
        getInternalCatalog().replayAlterExternalTableSchema(dbName, tableName, newSchema);
    }

    // Drop table
    public void dropTable(DropMTMVCommand command) throws DdlException {
        if (command == null) {
            throw new DdlException("DropMTMVCommand is null");
        }
        DropMTMVInfo dropMTMVInfo = command.getDropMTMVInfo();
        dropTable(dropMTMVInfo.getCatalogName(), dropMTMVInfo.getDbName(), dropMTMVInfo.getTableName(), false,
                true, dropMTMVInfo.isIfExists(), true);
    }

    public void dropTable(DropTableStmt stmt) throws DdlException {
        if (stmt == null) {
            throw new DdlException("DropTableStmt is null");
        }
        dropTable(stmt.getCatalogName(), stmt.getDbName(), stmt.getTableName(), stmt.isView(),
                stmt.isMaterializedView(), stmt.isSetIfExists(), stmt.isForceDrop());
    }

    public void dropTable(String catalogName, String dbName, String tableName, boolean isView, boolean isMtmv,
                          boolean ifExists, boolean force) throws DdlException {
        CatalogIf<?> catalogIf = catalogMgr.getCatalogOrException(catalogName,
                catalog -> new DdlException(("Unknown catalog " + catalog)));
        catalogIf.dropTable(dbName, tableName, isView, isMtmv, ifExists, force);
    }

    public void dropView(String catalogName, String dbName, String tableName, boolean ifExists) throws DdlException {
        CatalogIf<?> catalogIf = catalogMgr.getCatalogOrException(catalogName,
                catalog -> new DdlException(("Unknown catalog " + catalog)));
        catalogIf.dropTable(dbName, tableName, true, false, ifExists, false);
    }

    public boolean unprotectDropTable(Database db, Table table, boolean isForceDrop, boolean isReplay,
                                      Long recycleTime) {
        return getInternalCatalog().unprotectDropTable(db, table, isForceDrop, isReplay, recycleTime);
    }

    public void replayDropTable(Database db, long tableId, boolean isForceDrop,
                                Long recycleTime) throws MetaNotFoundException {
        getInternalCatalog().replayDropTable(db, tableId, isForceDrop, recycleTime);
    }

    public void replayEraseTable(long tableId) {
        getInternalCatalog().replayEraseTable(tableId);
    }

    public void replayRecoverTable(RecoverInfo info) throws MetaNotFoundException, DdlException {
        getInternalCatalog().replayRecoverTable(info);
    }

    public void replayAddReplica(ReplicaPersistInfo info) throws MetaNotFoundException {
        getInternalCatalog().replayAddReplica(info);
    }

    public void replayUpdateReplica(ReplicaPersistInfo info) throws MetaNotFoundException {
        getInternalCatalog().replayUpdateReplica(info);
    }

    public void unprotectDeleteReplica(OlapTable olapTable, ReplicaPersistInfo info) {
        getInternalCatalog().unprotectDeleteReplica(olapTable, info);
    }

    public void replayDeleteReplica(ReplicaPersistInfo info) throws MetaNotFoundException {
        getInternalCatalog().replayDeleteReplica(info);
    }

    public void replayAddFrontend(Frontend fe) {
        tryLock(true);
        try {
            Frontend existFe = checkFeExist(fe.getHost(), fe.getEditLogPort());
            if (existFe != null) {
                LOG.warn("fe {} already exist.", existFe);
                if (existFe.getRole() != fe.getRole()) {
                    /*
                     * This may happen if:
                     * 1. first, add a FE as OBSERVER.
                     * 2. This OBSERVER is restarted with ROLE and VERSION file being DELETED.
                     *    In this case, this OBSERVER will be started as a FOLLOWER, and add itself to the frontends.
                     * 3. this "FOLLOWER" begin to load image or replay journal,
                     *    then find the origin OBSERVER in image or journal.
                     * This will cause UNDEFINED behavior, so it is better to exit and fix it manually.
                     */
                    System.err.println("Try to add an already exist FE with different role" + fe.getRole());
                    System.exit(-1);
                }
                return;
            }
            LOG.info("replay add frontend: {}", fe);
            frontends.put(fe.getNodeName(), fe);
            if (fe.getRole() == FrontendNodeType.FOLLOWER || fe.getRole() == FrontendNodeType.REPLICA) {
                // DO NOT add helper sockets here, cause BDBHA is not instantiated yet.
                // helper sockets will be added after start BDBHA
                // But add to helperNodes, just for show
                helperNodes.add(new HostInfo(fe.getHost(), fe.getEditLogPort()));
            }
        } finally {
            unlock();
        }
    }

    public void replayModifyFrontend(Frontend fe) {
        tryLock(true);
        try {
            Frontend existFe = getFeByName(fe.getNodeName());
            if (existFe == null) {
                // frontend may already be dropped. this may happen when
                // drop and modify operations do not guarantee the order.
                return;
            }
            // modify fe in frontends
            LOG.info("replay modify frontend with new host: {}, exist frontend: {}", fe.getHost(), existFe);
            existFe.setHost(fe.getHost());
        } finally {
            unlock();
        }
    }

    public void replayDropFrontend(Frontend frontend) {
        tryLock(true);
        try {
            Frontend removedFe = frontends.remove(frontend.getNodeName());
            if (removedFe == null) {
                LOG.error(frontend + " does not exist.");
                return;
            }
            LOG.info("replay drop frontend: {}", removedFe);
            if (removedFe.getRole() == FrontendNodeType.FOLLOWER || removedFe.getRole() == FrontendNodeType.REPLICA) {
                removeHelperNode(removedFe.getHost(), removedFe.getEditLogPort());
            }

            removedFrontends.add(removedFe.getNodeName());

        } finally {
            unlock();
        }
    }

    public int getClusterId() {
        return this.clusterId;
    }

    public String getDeployMode() {
        return Config.isCloudMode() ? Storage.CLOUD_MODE : Storage.LOCAL_MODE;
    }

    public String getToken() {
        return token;
    }

    public EditLog getEditLog() {
        return editLog;
    }

    // Get the next available, needn't lock because of nextId is atomic.
    public long getNextId() {
        return idGenerator.getNextId();
    }

    // counter for prepared statement id
    public long getNextStmtId() {
        return this.stmtIdCounter.getAndIncrement();
    }

    public IdGeneratorBuffer getIdGeneratorBuffer(long bufferSize) {
        return idGenerator.getIdGeneratorBuffer(bufferSize);
    }

    public HashMap<Long, TStorageMedium> getPartitionIdToStorageMediumMap() {
        HashMap<Long, TStorageMedium> storageMediumMap = new HashMap<Long, TStorageMedium>();

        // record partition which need to change storage medium
        // dbId -> (tableId -> partitionId)
        HashMap<Long, Multimap<Long, Long>> changedPartitionsMap = new HashMap<Long, Multimap<Long, Long>>();
        long currentTimeMs = System.currentTimeMillis();
        List<Long> dbIds = getInternalCatalog().getDbIds();

        for (long dbId : dbIds) {
            Database db = getInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                LOG.warn("db {} does not exist while doing backend report", dbId);
                continue;
            }
            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (!table.isManagedTable()) {
                    continue;
                }

                long tableId = table.getId();
                OlapTable olapTable = (OlapTable) table;
                olapTable.readLock();
                try {
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                    for (Partition partition : olapTable.getAllPartitions()) {
                        long partitionId = partition.getId();
                        DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
                        Preconditions.checkNotNull(dataProperty,
                                partition.getName() + ", pId:" + partitionId + ", db: " + dbId + ", tbl: " + tableId);
                        if (dataProperty.getStorageMedium() == TStorageMedium.SSD
                                && dataProperty.getCooldownTimeMs() < currentTimeMs) {
                            // expire. change to HDD.
                            // record and change when holding write lock
                            Multimap<Long, Long> multimap = changedPartitionsMap.get(dbId);
                            if (multimap == null) {
                                multimap = HashMultimap.create();
                                changedPartitionsMap.put(dbId, multimap);
                            }
                            multimap.put(tableId, partitionId);
                        } else {
                            storageMediumMap.put(partitionId, dataProperty.getStorageMedium());
                        }
                    } // end for partitions
                } finally {
                    olapTable.readUnlock();
                }
            } // end for tables
        } // end for dbs

        // handle data property changed
        for (Long dbId : changedPartitionsMap.keySet()) {
            Database db = getInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                LOG.warn("db {} does not exist while checking backend storage medium", dbId);
                continue;
            }
            Multimap<Long, Long> tableIdToPartitionIds = changedPartitionsMap.get(dbId);

            for (Long tableId : tableIdToPartitionIds.keySet()) {
                TableIf table = db.getTableNullable(tableId);
                if (table == null) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                // use try lock to avoid blocking a long time.
                // if block too long, backend report rpc will timeout.
                if (!olapTable.tryWriteLockIfExist(Table.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    LOG.warn("try get table {} writelock but failed" + " when checking backend storage medium",
                            table.getName());
                    continue;
                }
                Preconditions.checkState(olapTable.isWriteLockHeldByCurrentThread());
                try {
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();

                    Collection<Long> partitionIds = tableIdToPartitionIds.get(tableId);
                    for (Long partitionId : partitionIds) {
                        Partition partition = olapTable.getPartition(partitionId);
                        if (partition == null) {
                            continue;
                        }
                        DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
                        if (dataProperty.getStorageMedium() == TStorageMedium.SSD
                                && dataProperty.getCooldownTimeMs() < currentTimeMs) {
                            // expire. change to HDD.
                            DataProperty hddProperty = new DataProperty(TStorageMedium.HDD);
                            partitionInfo.setDataProperty(partition.getId(), hddProperty);
                            storageMediumMap.put(partitionId, TStorageMedium.HDD);
                            LOG.info("partition[{}-{}-{}] storage medium changed from SSD to HDD. "
                                            + "cooldown time: {}. current time: {}", dbId, tableId, partitionId,
                                    TimeUtils.longToTimeString(dataProperty.getCooldownTimeMs()),
                                    TimeUtils.longToTimeString(currentTimeMs));

                            // log
                            ModifyPartitionInfo info = new ModifyPartitionInfo(db.getId(), olapTable.getId(),
                                    partition.getId(), hddProperty, ReplicaAllocation.NOT_SET,
                                    partitionInfo.getIsInMemory(partition.getId()),
                                    partitionInfo.getStoragePolicy(partitionId), Maps.newHashMap());

                            editLog.logModifyPartition(info);
                        }
                    } // end for partitions
                } finally {
                    olapTable.writeUnlock();
                }
            } // end for tables
        } // end for dbs
        return storageMediumMap;
    }

    public ConsistencyChecker getConsistencyChecker() {
        return this.consistencyChecker;
    }

    public Alter getAlterInstance() {
        return this.alter;
    }

    public SchemaChangeHandler getSchemaChangeHandler() {
        return (SchemaChangeHandler) this.alter.getSchemaChangeHandler();
    }

    public MaterializedViewHandler getMaterializedViewHandler() {
        return (MaterializedViewHandler) this.alter.getMaterializedViewHandler();
    }

    public CooldownConfHandler getCooldownConfHandler() {
        return cooldownConfHandler;
    }

    public SystemHandler getSystemHandler() {
        return (SystemHandler) this.alter.getSystemHandler();
    }

    public BackupHandler getBackupHandler() {
        return this.backupHandler;
    }

    public DeleteHandler getDeleteHandler() {
        return this.deleteHandler;
    }

    public LoadManager getLoadManager() {
        return loadManager;
    }

    public TokenManager getTokenManager() {
        return tokenManager;
    }

    public ProgressManager getProgressManager() {
        return progressManager;
    }

    public static ProgressManager getCurrentProgressManager() {
        return getCurrentEnv().getProgressManager();
    }

    public StreamLoadRecordMgr getStreamLoadRecordMgr() {
        return streamLoadRecordMgr;
    }

    public TabletLoadIndexRecorderMgr getTabletLoadIndexRecorderMgr() {
        return tabletLoadIndexRecorderMgr;
    }

    public MasterTaskExecutor getPendingLoadTaskScheduler() {
        return pendingLoadTaskScheduler;
    }

    public MasterTaskExecutor getLoadingLoadTaskScheduler() {
        return loadingLoadTaskScheduler;
    }

    public RoutineLoadManager getRoutineLoadManager() {
        return routineLoadManager;
    }

    public GroupCommitManager getGroupCommitManager() {
        return groupCommitManager;
    }

    public SqlBlockRuleMgr getSqlBlockRuleMgr() {
        return sqlBlockRuleMgr;
    }

    public RoutineLoadTaskScheduler getRoutineLoadTaskScheduler() {
        return routineLoadTaskScheduler;
    }

    public ExportMgr getExportMgr() {
        return this.exportMgr;
    }

    public JobManager getJobManager() {
        return jobManager;
    }

    public LabelProcessor getLabelProcessor() {
        return labelProcessor;
    }

    public TransientTaskManager getTransientTaskManager() {
        return transientTaskManager;
    }

    public SmallFileMgr getSmallFileMgr() {
        return this.smallFileMgr;
    }

    public RefreshManager getRefreshManager() {
        return this.refreshManager;
    }

    public DictionaryManager getDictionaryManager() {
        return this.dictionaryManager;
    }

    public long getReplayedJournalId() {
        return this.replayedJournalId.get();
    }

    public HAProtocol getHaProtocol() {
        return this.haProtocol;
    }

    public Long getMaxJournalId() {
        return this.editLog.getMaxJournalId();
    }

    public long getEpoch() {
        return this.epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public FrontendNodeType getRole() {
        return this.role;
    }

    public HostInfo getHelperNode() {
        Preconditions.checkState(helperNodes.size() >= 1);
        return this.helperNodes.get(0);
    }

    public List<HostInfo> getHelperNodes() {
        return Lists.newArrayList(helperNodes);
    }

    public HostInfo getSelfNode() {
        return this.selfNode;
    }

    public String getNodeName() {
        return this.nodeName;
    }

    public FrontendNodeType getFeType() {
        return this.feType;
    }

    public int getMasterRpcPort() {
        if (!isReady()) {
            return 0;
        }
        return this.masterInfo.getRpcPort();
    }

    public int getMasterHttpPort() {
        if (!isReady()) {
            return 0;
        }
        return this.masterInfo.getHttpPort();
    }

    public String getMasterHost() {
        if (!isReady()) {
            return "";
        }
        return this.masterInfo.getHost();
    }

    public EsRepository getEsRepository() {
        return getInternalCatalog().getEsRepository();
    }

    public PolicyMgr getPolicyMgr() {
        return this.policyMgr;
    }

    public IndexPolicyMgr getIndexPolicyMgr() {
        return this.indexPolicyMgr;
    }

    public void setMaster(MasterInfo info) {
        this.masterInfo = info;
        LOG.info("setMaster MasterInfo:{}", info);
    }

    public boolean canRead() {
        return this.canRead.get();
    }

    public boolean isElectable() {
        return this.isElectable;
    }

    public boolean isMaster() {
        return feType == FrontendNodeType.MASTER;
    }

    public void setSynchronizedTime(long time) {
        this.synchronizedTimeMs = time;
    }

    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
    }

    public void setNextId(long id) {
        idGenerator.setId(id);
    }

    public void setHaProtocol(HAProtocol protocol) {
        this.haProtocol = protocol;
    }

    public static short calcShortKeyColumnCount(List<Column> columns, Map<String, String> properties,
                                                boolean isKeysRequired) throws DdlException {
        List<Column> indexColumns = new ArrayList<Column>();
        Map<Integer, Column> clusterColumns = new TreeMap<>();
        boolean hasValueColumn = false;
        for (Column column : columns) {
            if (column.isKey()) {
                if (hasValueColumn && isKeysRequired) {
                    throw new DdlException("The materialized view not support value column before key column");
                }
                indexColumns.add(column);
            } else {
                hasValueColumn = true;
            }
            if (column.isClusterKey()) {
                clusterColumns.put(column.getClusterKeyId(), column);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("index column size: {}, cluster column size: {}", indexColumns.size(), clusterColumns.size());
        }
        if (isKeysRequired && indexColumns.isEmpty()) {
            throw new DdlException("The materialized view need key column");
        }
        // sort by cluster keys for mow if set, otherwise by index columns
        List<Column> sortKeyColumns = clusterColumns.isEmpty() ? indexColumns
                : clusterColumns.values().stream().collect(Collectors.toList());

        // figure out shortKeyColumnCount
        short shortKeyColumnCount = (short) -1;
        try {
            shortKeyColumnCount = PropertyAnalyzer.analyzeShortKeyColumnCount(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        if (shortKeyColumnCount != (short) -1) {
            // use user specified short key column count
            if (shortKeyColumnCount <= 0) {
                throw new DdlException("Invalid short key: " + shortKeyColumnCount);
            }

            if (shortKeyColumnCount > sortKeyColumns.size()) {
                throw new DdlException("Short key is too large. should less than: " + sortKeyColumns.size());
            }

            for (int pos = 0; pos < shortKeyColumnCount; pos++) {
                if (sortKeyColumns.get(pos).getDataType() == PrimitiveType.VARCHAR && pos != shortKeyColumnCount - 1) {
                    throw new DdlException("Varchar should not in the middle of short keys.");
                }
            }
        } else {
            /*
             * Calc short key column count. NOTE: short key column count is
             * calculated as follow: 1. All index column are taking into
             * account. 2. Max short key column count is Min(Num of
             * indexColumns, META_MAX_SHORT_KEY_NUM). 3. Short key list can
             * contains at most one VARCHAR column. And if contains, it should
             * be at the last position of the short key list.
             */
            shortKeyColumnCount = 0;
            int shortKeySizeByte = 0;
            int maxShortKeyColumnCount = Math.min(sortKeyColumns.size(), FeConstants.shortkey_max_column_count);
            for (int i = 0; i < maxShortKeyColumnCount; i++) {
                Column column = sortKeyColumns.get(i);
                shortKeySizeByte += column.getOlapColumnIndexSize();
                if (shortKeySizeByte > FeConstants.shortkey_maxsize_bytes) {
                    if (column.getDataType().isCharFamily()) {
                        ++shortKeyColumnCount;
                    }
                    break;
                }
                if (!column.getType().couldBeShortKey()) {
                    break;
                }
                if (column.getDataType() == PrimitiveType.VARCHAR) {
                    ++shortKeyColumnCount;
                    break;
                }
                ++shortKeyColumnCount;
            }
            if (isKeysRequired && shortKeyColumnCount == 0) {
                throw new DdlException("The first column could not be float or double type, use decimal instead");
            }

        } // end calc shortKeyColumnCount

        if (clusterColumns.size() > 0 && shortKeyColumnCount < clusterColumns.size()) {
            boolean sameKey = true;
            for (int i = 0; i < shortKeyColumnCount && i < indexColumns.size(); i++) {
                if (!clusterColumns.get(i).getName().equals(indexColumns.get(i).getName())) {
                    sameKey = false;
                    break;
                }
            }
            if (sameKey && !Config.random_add_cluster_keys_for_mow) {
                throw new DdlException(shortKeyColumnCount + " short keys is a part of unique keys");
            }
        }
        return shortKeyColumnCount;
    }

    /*
     * used for handling AlterTableStmt (for client is the ALTER TABLE command).
     * including SchemaChangeHandler and RollupHandler
     */
    public void alterTable(AlterTableStmt stmt) throws UserException {
        this.alter.processAlterTable(stmt);
    }

    public void alterTable(AlterTableCommand command) throws UserException {
        this.alter.processAlterTable(command);
    }

    /**
     * used for handling AlterViewComand
     */
    public void alterView(AlterViewCommand command) throws UserException {
        this.alter.processAlterView(command, ConnectContext.get());
    }

    public void createMaterializedView(CreateMaterializedViewStmt stmt)
            throws AnalysisException, DdlException, MetaNotFoundException {
        this.alter.processCreateMaterializedView(stmt);
    }

    public void createMaterializedView(CreateMaterializedViewCommand command)
            throws AnalysisException, DdlException, MetaNotFoundException {
        this.alter.processCreateMaterializedView(command);
    }

    public void dropMaterializedView(DropMaterializedViewCommand command) throws DdlException, MetaNotFoundException {
        this.alter.processDropMaterializedView(command);
    }

    /*
     * used for handling CancelAlterCommand (for client is the CANCEL ALTER
     * command). including SchemaChangeHandler and RollupHandler
     */
    public void cancelAlter(CancelAlterTableCommand command) throws DdlException {
        if (command.getAlterType() == CancelAlterTableCommand.AlterType.ROLLUP
                || command.getAlterType() == CancelAlterTableCommand.AlterType.MV) {
            this.getMaterializedViewHandler().cancel(command);
        } else if (command.getAlterType() == CancelAlterTableCommand.AlterType.COLUMN) {
            this.getSchemaChangeHandler().cancel(command);
        } else {
            throw new DdlException("Cancel " + command.getAlterType() + " does not implement yet");
        }
    }

    /*
     * used for handling CancelIndexCommand
     */
    public void cancelBuildIndex(CancelBuildIndexCommand command) throws DdlException {
        this.getSchemaChangeHandler().cancelIndexJob(command);
    }

    public void cancelBackup(CancelBackupCommand command) throws DdlException {
        getBackupHandler().cancel(command);
    }

    public void renameTable(Database db, Table table, TableRenameClause tableRenameClause) throws DdlException {
        renameTable(db, table, tableRenameClause.getNewTableName());
    }

    // entry of rename table operation
    public void renameTable(Database db, Table table, String newTableName) throws DdlException {
        db.writeLockOrDdlException();
        try {
            table.writeLockOrDdlException();
            try {
                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    olapTable.checkNormalStateForAlter();
                }

                String oldTableName = table.getName();
                if (Env.isStoredTableNamesLowerCase() && !Strings.isNullOrEmpty(newTableName)) {
                    newTableName = newTableName.toLowerCase();
                }
                if (oldTableName.equals(newTableName)) {
                    throw new DdlException("Same table name");
                }

                // check if name is already used
                if (db.getTable(newTableName).isPresent()) {
                    throw new DdlException("Table name[" + newTableName + "] is already used");
                }
                if (db.getTable(RestoreJob.tableAliasWithAtomicRestore(newTableName)).isPresent()) {
                    throw new DdlException("Table name[" + newTableName + "] is already used (in restoring)");
                }

                if (table.isManagedTable()) {
                    // If not checked first, execute db.unregisterTable first,
                    // and then check the name in setName, it cannot guarantee atomicity
                    ((OlapTable) table).checkAndSetName(newTableName, true);
                }

                db.unregisterTable(oldTableName);

                if (table.isManagedTable()) {
                    // olap table should also check if any rollup has same name as "newTableName"
                    ((OlapTable) table).checkAndSetName(newTableName, false);
                } else {
                    table.setName(newTableName);
                }

                db.registerTable(table);
                TableInfo tableInfo = TableInfo.createForTableRename(db.getId(), table.getId(), oldTableName,
                        newTableName);
                editLog.logTableRename(tableInfo);
                LOG.info("rename table[{}] to {}", oldTableName, newTableName);
            } finally {
                table.writeUnlock();
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void refreshExternalTableSchema(Database db, Table table, List<Column> newSchema) {
        RefreshExternalTableInfo refreshExternalTableInfo = new RefreshExternalTableInfo(db.getFullName(),
                table.getName(), newSchema);
        editLog.logRefreshExternalTableSchema(refreshExternalTableInfo);
        LOG.info("refresh db[{}] table[{}] for schema change", db.getFullName(), table.getName());
    }

    public void replayRenameTable(TableInfo tableInfo) throws MetaNotFoundException {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        String newTableName = tableInfo.getNewTableName();

        Database db = getInternalCatalog().getDbOrMetaException(dbId);
        db.writeLock();
        try {
            Table table = db.getTableOrMetaException(tableId);
            table.writeLock();
            try {
                String tableName = table.getName();
                db.unregisterTable(tableName);
                table.setName(newTableName);
                db.registerTable(table);
                LOG.info("replay rename table[{}] to {}", tableName, newTableName);
            } finally {
                table.writeUnlock();
            }
        } finally {
            db.writeUnlock();
        }
    }

    // the invoker should keep table's write lock
    public void modifyTableColocate(Database db, OlapTable table, String assignedGroup, boolean isReplay,
                                    GroupId assignedGroupId)
            throws DdlException {

        String oldGroup = table.getColocateGroup();
        GroupId groupId = null;
        if (!Strings.isNullOrEmpty(assignedGroup)) {
            String fullAssignedGroupName = GroupId.getFullGroupName(db.getId(), assignedGroup);
            // When the new name is the same as the old name, we return it to prevent npe
            if (!Strings.isNullOrEmpty(oldGroup)) {
                String oldFullGroupName = GroupId.getFullGroupName(db.getId(), oldGroup);
                if (oldFullGroupName.equals(fullAssignedGroupName)) {
                    LOG.warn("modify table[{}] group name same as old group name,skip.", table.getName());
                    return;
                }
            }
            if (!isReplay && table.isAutoBucket()) {
                throw new DdlException("table " + table.getName() + " is auto buckets");
            }
            ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(fullAssignedGroupName);
            if (groupSchema == null) {
                // user set a new colocate group,
                // check if all partitions all this table has same buckets num and same replication number
                PartitionInfo partitionInfo = table.getPartitionInfo();
                if (partitionInfo.getType() == PartitionType.RANGE || partitionInfo.getType() == PartitionType.LIST) {
                    int bucketsNum = -1;
                    ReplicaAllocation replicaAlloc = null;
                    for (Partition partition : table.getPartitions()) {
                        if (bucketsNum == -1) {
                            bucketsNum = partition.getDistributionInfo().getBucketNum();
                        } else if (bucketsNum != partition.getDistributionInfo().getBucketNum()) {
                            throw new DdlException(
                                    "Partitions in table " + table.getName() + " have different buckets number");
                        }

                        if (replicaAlloc == null) {
                            replicaAlloc = partitionInfo.getReplicaAllocation(partition.getId());
                        } else if (!replicaAlloc.equals(partitionInfo.getReplicaAllocation(partition.getId()))) {
                            throw new DdlException(
                                    "Partitions in table " + table.getName() + " have different replica allocation.");
                        }
                    }
                }
            } else {
                // set to an already exist colocate group, check if this table can be added to this group.
                groupSchema.checkColocateSchema(table);
            }

            if (Config.isCloudMode()) {
                groupId = colocateTableIndex.changeGroup(db.getId(), table, oldGroup, assignedGroup, assignedGroupId);
            } else {
                Map<Tag, List<List<Long>>> backendsPerBucketSeq = null;
                if (groupSchema == null) {
                    // assign to a newly created group, set backends sequence.
                    // we arbitrarily choose a tablet backends sequence from this table,
                    // let the colocation balancer do the work.
                    backendsPerBucketSeq = table.getArbitraryTabletBucketsSeq();
                }
                // change group after getting backends sequence(if has), in case 'getArbitraryTabletBucketsSeq' failed
                groupId = colocateTableIndex.changeGroup(db.getId(), table, oldGroup, assignedGroup, assignedGroupId);

                if (groupSchema == null) {
                    Preconditions.checkNotNull(backendsPerBucketSeq);
                    colocateTableIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
                }

                // set this group as unstable
                colocateTableIndex.markGroupUnstable(groupId, "Colocation group modified by user",
                        false /* edit log is along with modify table log */);
            }

            table.setColocateGroup(assignedGroup);
        } else {
            // unset colocation group
            if (Strings.isNullOrEmpty(oldGroup)) {
                // this table is not a colocate table, do nothing
                return;
            }

            // when replayModifyTableColocate, we need the groupId info
            String fullGroupName = GroupId.getFullGroupName(db.getId(), oldGroup);
            groupId = colocateTableIndex.getGroupSchema(fullGroupName).getGroupId();
            colocateTableIndex.removeTable(table.getId());
            table.setColocateGroup(null);
        }

        if (!isReplay) {
            Map<String, String> properties = Maps.newHashMapWithExpectedSize(1);
            properties.put(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, assignedGroup);
            TablePropertyInfo info = new TablePropertyInfo(db.getId(), table.getId(), groupId, properties);
            editLog.logModifyTableColocate(info);
        }
        LOG.info("finished modify table's colocation property. table: {}, is replay: {}", table.getName(), isReplay);
    }

    public void replayModifyTableColocate(TablePropertyInfo info) throws MetaNotFoundException {
        long dbId = info.getGroupId().dbId;
        if (dbId == 0) {
            dbId = info.getDbId();
        }
        Preconditions.checkState(dbId != 0, "replay modify table colocate failed, table id: " + info.getTableId());
        long tableId = info.getTableId();
        Map<String, String> properties = info.getPropertyMap();

        Database db = getInternalCatalog().getDbOrMetaException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        olapTable.writeLock();
        try {
            modifyTableColocate(db, olapTable, properties.get(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH), true,
                    info.getGroupId());
        } catch (DdlException e) {
            // should not happen
            LOG.warn("failed to replay modify table colocate", e);
        } finally {
            olapTable.writeUnlock();
        }
    }

    public void renameRollup(Database db, OlapTable table, RollupRenameClause renameClause) throws DdlException {
        table.writeLockOrDdlException();
        try {
            table.checkNormalStateForAlter();
            String rollupName = renameClause.getRollupName();
            // check if it is base table name
            if (rollupName.equals(table.getName())) {
                throw new DdlException("Using ALTER TABLE RENAME to change table name");
            }

            String newRollupName = renameClause.getNewRollupName();
            if (rollupName.equals(newRollupName)) {
                throw new DdlException("Same rollup name");
            }

            Map<String, Long> indexNameToIdMap = table.getIndexNameToId();
            if (indexNameToIdMap.get(rollupName) == null) {
                throw new DdlException("Rollup index[" + rollupName + "] does not exists");
            }

            // check if name is already used
            if (indexNameToIdMap.get(newRollupName) != null) {
                throw new DdlException("Rollup name[" + newRollupName + "] is already used");
            }

            long indexId = indexNameToIdMap.remove(rollupName);
            indexNameToIdMap.put(newRollupName, indexId);

            // log
            TableInfo tableInfo = TableInfo.createForRollupRename(db.getId(), table.getId(), indexId,
                    rollupName, newRollupName);
            editLog.logRollupRename(tableInfo);
            LOG.info("rename rollup[{}] to {}", rollupName, newRollupName);
        } finally {
            table.writeUnlock();
        }
    }

    public void replayRenameRollup(TableInfo tableInfo) throws MetaNotFoundException {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        long indexId = tableInfo.getIndexId();
        String newRollupName = tableInfo.getNewRollupName();

        Database db = getInternalCatalog().getDbOrMetaException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        olapTable.writeLock();
        try {
            String rollupName = olapTable.getIndexNameById(indexId);
            Map<String, Long> indexNameToIdMap = olapTable.getIndexNameToId();
            indexNameToIdMap.remove(rollupName);
            indexNameToIdMap.put(newRollupName, indexId);

            LOG.info("replay rename rollup[{}] to {}", rollupName, newRollupName);
        } finally {
            olapTable.writeUnlock();
        }
    }

    public void renamePartition(Database db, OlapTable table, PartitionRenameClause renameClause) throws DdlException {
        table.writeLockOrDdlException();
        try {
            table.checkNormalStateForAlter();
            if (table.getPartitionInfo().getType() != PartitionType.RANGE
                    && table.getPartitionInfo().getType() != PartitionType.LIST) {
                throw new DdlException(
                        "Table[" + table.getName() + "] is single partitioned. " + "no need to rename partition name.");
            }

            String partitionName = renameClause.getPartitionName();
            String newPartitionName = renameClause.getNewPartitionName();
            if (partitionName.equalsIgnoreCase(newPartitionName)) {
                throw new DdlException("Same partition name");
            }

            Partition partition = table.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException("Partition[" + partitionName + "] does not exists");
            }

            // check if name is already used
            if (table.checkPartitionNameExist(newPartitionName)) {
                throw new DdlException("Partition name[" + newPartitionName + "] is already used");
            }

            table.renamePartition(partitionName, newPartitionName);

            // log
            TableInfo tableInfo = TableInfo.createForPartitionRename(db.getId(), table.getId(), partition.getId(),
                    partitionName, newPartitionName);
            editLog.logPartitionRename(tableInfo);
            LOG.info("rename partition[{}] to {}", partitionName, newPartitionName);
        } finally {
            table.writeUnlock();
        }
    }

    public void replayRenamePartition(TableInfo tableInfo) throws MetaNotFoundException {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        long partitionId = tableInfo.getPartitionId();
        String newPartitionName = tableInfo.getNewPartitionName();

        Database db = getInternalCatalog().getDbOrMetaException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        olapTable.writeLock();
        try {
            Partition partition = olapTable.getPartition(partitionId);
            olapTable.renamePartition(partition.getName(), newPartitionName);
            LOG.info("replay rename partition[{}] to {}", partition.getName(), newPartitionName);
        } finally {
            olapTable.writeUnlock();
        }
    }

    private void renameColumn(Database db, OlapTable table, String colName,
                              String newColName, Map<Long, Integer> indexIdToSchemaVersion,
                              boolean isReplay) throws DdlException {
        table.checkNormalStateForAlter();
        if (colName.equalsIgnoreCase(newColName)) {
            throw new DdlException("Same column name");
        }

        // @NOTE: Rename partition columns should also rename column names in partition expressions
        // but this is not implemented currently. Therefore, forbid renaming partition columns temporarily.
        PartitionInfo partitionInfo = table.getPartitionInfo();
        if (partitionInfo.getPartitionColumns().stream().anyMatch(c -> c.getName().equalsIgnoreCase(colName))) {
            throw new DdlException("Renaming partition columns has problems, forbidden in current Doris version");
        }

        Map<Long, MaterializedIndexMeta> indexIdToMeta = table.getIndexIdToMeta();
        for (Map.Entry<Long, MaterializedIndexMeta> entry : indexIdToMeta.entrySet()) {
            // rename column is not implemented for non-light-schema-change table.
            if (!table.getEnableLightSchemaChange()) {
                throw new DdlException("not implemented for table without column unique id,"
                        + " which are created with property 'light_schema_change'.");
            }
            // check if new name is already used
            if (entry.getValue().getColumnByName(newColName) != null) {
                throw new DdlException("Column name[" + newColName + "] is already used");
            }

            // check if have materialized view on rename column
            // and check whether colName is referenced by generated columns
            for (Column column : entry.getValue().getSchema()) {
                if (column.getName().equals(colName) && !column.getGeneratedColumnsThatReferToThis().isEmpty()) {
                    throw new DdlException(
                            "Cannot rename column, because column '" + colName
                                    + "' has a generated column dependency on :"
                                    + column.getGeneratedColumnsThatReferToThis());
                }
                Expr expr = column.getDefineExpr();
                if (expr == null) {
                    continue;
                }
                List<SlotRef> slots = new ArrayList<>();
                expr.collect(SlotRef.class, slots);
                for (SlotRef slot : slots) {
                    String name = MaterializedIndexMeta
                            .normalizeName(CreateMaterializedViewStmt.mvColumnBreaker(slot.toSqlWithoutTbl()));
                    if (!isReplay && name.equals(colName)) {
                        throw new DdlException("Column[" + colName + "] have materialized view index");
                    }
                }
            }
        }

        // 1. modify old MaterializedIndexMeta
        boolean hasColumn = false;
        for (Map.Entry<Long, MaterializedIndexMeta> entry : indexIdToMeta.entrySet()) {
            Column column = entry.getValue().getColumnByName(colName);
            if (column != null) {
                column.setName(newColName);
                hasColumn = true;
                Env.getCurrentEnv().getQueryStats()
                        .rename(Env.getCurrentEnv().getCurrentCatalog().getId(), db.getId(),
                                table.getId(), entry.getKey(), colName, newColName);
                if (!isReplay) {
                    indexIdToSchemaVersion.put(entry.getKey(), entry.getValue().getSchemaVersion() + 1);
                }
                if (indexIdToSchemaVersion != null) {
                    entry.getValue().setSchemaVersion(indexIdToSchemaVersion.get(entry.getKey()));
                }
            }
        }
        if (!hasColumn) {
            throw new DdlException("Column[" + colName + "] does not exists");
        }

        // @NOTE: Rename partition columns should also rename column names in partition expressions
        // but this is not implemented currently. Therefore, forbid renaming partition columns temporarily.
        //
        // 2. modify partition key
        // PartitionInfo partitionInfo = table.getPartitionInfo();
        // List<Column> partitionColumns = partitionInfo.getPartitionColumns();
        // for (Column column : partitionColumns) {
        //    if (column.getName().equalsIgnoreCase(colName)) {
        //        column.setName(newColName);
        //    }
        //}

        // 3. modify index
        List<Index> indexes = table.getIndexes();
        for (Index index : indexes) {
            List<String> columns = index.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).equalsIgnoreCase(colName)) {
                    columns.set(i, newColName);
                }
            }
        }

        // 4. modify distribution info
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        if (distributionInfo.getType() == DistributionInfoType.HASH) {
            // modify default distribution info
            List<Column> distributionColumns = ((HashDistributionInfo) distributionInfo).getDistributionColumns();
            for (Column column : distributionColumns) {
                if (column.getName().equalsIgnoreCase(colName)) {
                    column.setName(newColName);
                }
            }
            // modify distribution info inside partitions
            for (Partition p : table.getPartitions()) {
                DistributionInfo partDistInfo = p.getDistributionInfo();
                if (partDistInfo.getType() != DistributionInfoType.HASH) {
                    continue;
                }
                List<Column> partDistColumns = ((HashDistributionInfo) partDistInfo).getDistributionColumns();
                for (Column column : partDistColumns) {
                    if (column.getName().equalsIgnoreCase(colName)) {
                        column.setName(newColName);
                    }
                }
            }
        }

        // 5. modify sequence map col
        if (table.hasSequenceCol() && table.getSequenceMapCol() != null
                && table.getSequenceMapCol().equalsIgnoreCase(colName)) {
            table.setSequenceMapCol(newColName);
        }

        // 6. modify bloom filter col
        Set<String> bfCols = table.getCopiedBfColumns();
        if (bfCols != null) {
            Set<String> newBfCols = new HashSet<>();
            for (String bfCol : bfCols) {
                if (bfCol.equalsIgnoreCase(colName)) {
                    newBfCols.add(newColName);
                } else {
                    newBfCols.add(bfCol);
                }
            }
            table.setBloomFilterInfo(newBfCols, table.getBfFpp());
        }

        table.rebuildFullSchema();

        if (!isReplay) {
            // log
            TableRenameColumnInfo info = new TableRenameColumnInfo(db.getId(), table.getId(), colName, newColName,
                    indexIdToSchemaVersion);
            editLog.logColumnRename(info);
            LOG.info("rename coloumn[{}] to {}", colName, newColName);
            AnalysisManager manager = Env.getCurrentEnv().getAnalysisManager();
            manager.removeTableStats(table.getId());
            manager.dropStats(table, null);
        }
    }

    public void renameColumn(Database db, OlapTable table, ColumnRenameClause renameClause) throws DdlException {
        table.writeLockOrDdlException();
        try {
            String colName = renameClause.getColName();
            String newColName = renameClause.getNewColName();
            Map<Long, Integer> indexIdToSchemaVersion = new HashMap<Long, Integer>();
            renameColumn(db, table, colName, newColName, indexIdToSchemaVersion, false);
        } finally {
            table.writeUnlock();
        }
    }

    public void replayRenameColumn(TableRenameColumnInfo info) throws MetaNotFoundException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("info:{}", info);
        }
        long dbId = info.getDbId();
        long tableId = info.getTableId();
        String colName = info.getColName();
        String newColName = info.getNewColName();
        Map<Long, Integer> indexIdToSchemaVersion = info.getIndexIdToSchemaVersion();

        Database db = getCurrentEnv().getInternalCatalog().getDbOrMetaException(dbId);
        OlapTable table = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        Env.getCurrentEnv().getAnalysisManager().removeTableStats(tableId);
        table.writeLock();
        try {
            renameColumn(db, table, colName, newColName, indexIdToSchemaVersion, true);
        } catch (DdlException e) {
            // should not happen
            LOG.warn("failed to replay rename column", e);
        } finally {
            table.writeUnlock();
        }
    }

    public void modifyTableDynamicPartition(Database db, OlapTable table, Map<String, String> properties)
            throws UserException {
        convertDynamicPartitionReplicaNumToReplicaAllocation(properties);
        if (properties.containsKey(DynamicPartitionProperty.REPLICATION_ALLOCATION)) {
            table.checkChangeReplicaAllocation();
        }
        Map<String, String> logProperties = new HashMap<>(properties);
        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            DynamicPartitionUtil.checkAndSetDynamicPartitionProperty(table, properties, db);
        } else {
            // Merge the new properties with origin properties, and then analyze them
            Map<String, String> origDynamicProperties = tableProperty.getOriginDynamicPartitionProperty();
            origDynamicProperties.putAll(properties);
            Map<String, String> analyzedDynamicPartition = DynamicPartitionUtil.analyzeDynamicPartition(
                    origDynamicProperties, table, db, false);
            tableProperty.modifyTableProperties(analyzedDynamicPartition);
            tableProperty.buildDynamicProperty();
        }

        DynamicPartitionUtil.registerOrRemoveDynamicPartitionTable(db.getId(), table, false);
        dynamicPartitionScheduler.createOrUpdateRuntimeInfo(table.getId(), DynamicPartitionScheduler.LAST_UPDATE_TIME,
                TimeUtils.getCurrentFormatTime());
        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), table.getName(),
                        logProperties);
        editLog.logDynamicPartition(info);
    }

    private void convertDynamicPartitionReplicaNumToReplicaAllocation(Map<String, String> properties) {
        if (properties.containsKey(DynamicPartitionProperty.REPLICATION_NUM)) {
            short repNum = Short.parseShort(properties.remove(DynamicPartitionProperty.REPLICATION_NUM));
            ReplicaAllocation replicaAlloc = new ReplicaAllocation(repNum);
            properties.put(DynamicPartitionProperty.REPLICATION_ALLOCATION, replicaAlloc.toCreateStmt());
        }
    }

    /**
     * Set replication number for unpartitioned table.
     *
     * @param db
     * @param table
     * @param properties
     * @throws DdlException
     */
    // The caller need to hold the table write lock
    public void modifyTableReplicaAllocation(Database db, OlapTable table, Map<String, String> properties)
            throws UserException {
        Preconditions.checkArgument(table.isWriteLockHeldByCurrentThread());
        String defaultReplicationNumName = "default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM;
        PartitionInfo partitionInfo = table.getPartitionInfo();
        if (partitionInfo.getType() == PartitionType.RANGE || partitionInfo.getType() == PartitionType.LIST) {
            throw new DdlException(
                    "This is a partitioned table, you should specify partitions" + " with MODIFY PARTITION clause."
                            + " If you want to set default replication number, please use '" + defaultReplicationNumName
                            + "' instead of '" + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM
                            + "' to escape misleading.");
        }
        String partitionName = table.getName();
        Partition partition = table.getPartition(partitionName);
        if (partition == null) {
            throw new DdlException("Partition does not exist. name: " + partitionName);
        }

        ReplicaAllocation replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "");
        table.checkChangeReplicaAllocation();
        Env.getCurrentSystemInfo().checkReplicaAllocation(replicaAlloc);
        Preconditions.checkState(!replicaAlloc.isNotSet());
        boolean isInMemory = partitionInfo.getIsInMemory(partition.getId());
        DataProperty newDataProperty = partitionInfo.getDataProperty(partition.getId());
        partitionInfo.setReplicaAllocation(partition.getId(), replicaAlloc);

        // set table's default replication number.
        Map<String, String> tblProperties = Maps.newHashMap();
        tblProperties.put("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION, replicaAlloc.toCreateStmt());
        table.setReplicaAllocation(tblProperties);

        // log
        ModifyPartitionInfo info = new ModifyPartitionInfo(db.getId(), table.getId(), partition.getId(),
                newDataProperty, replicaAlloc, isInMemory, partitionInfo.getStoragePolicy(partition.getId()),
                tblProperties);
        editLog.logModifyPartition(info);
        if (LOG.isDebugEnabled()) {
            LOG.debug("modify partition[{}-{}-{}] replica allocation to {}",
                    db.getId(), table.getId(), partition.getName(), replicaAlloc.toCreateStmt());
        }
    }

    /**
     * Set default replication allocation for a specified table.
     * You can see the default replication allocation by executing Show Create Table stmt.
     *
     * @param db
     * @param table
     * @param properties
     */
    // The caller need to hold the table write lock
    public void modifyTableDefaultReplicaAllocation(Database db, OlapTable table,
            Map<String, String> properties) throws UserException {
        Preconditions.checkArgument(table.isWriteLockHeldByCurrentThread());
        table.checkChangeReplicaAllocation();
        table.setReplicaAllocation(properties);
        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), table.getName(),
                        properties);
        editLog.logModifyReplicationNum(info);
        if (LOG.isDebugEnabled()) {
            LOG.debug("modify table[{}] replication num to {}", table.getName(),
                    properties.get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
        }
    }

    // The caller need to hold the table write lock
    public void modifyTableProperties(Database db, OlapTable table, Map<String, String> properties) {
        Preconditions.checkArgument(table.isWriteLockHeldByCurrentThread());
        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            tableProperty = new TableProperty(properties);
        } else {
            tableProperty.modifyTableProperties(properties);
        }
        tableProperty.buildInMemory()
                .buildMinLoadReplicaNum()
                .buildStoragePolicy()
                .buildStorageMedium()
                .buildIsBeingSynced()
                .buildCompactionPolicy()
                .buildTimeSeriesCompactionGoalSizeMbytes()
                .buildTimeSeriesCompactionFileCountThreshold()
                .buildTimeSeriesCompactionTimeThresholdSeconds()
                .buildSkipWriteIndexOnLoad()
                .buildDisableAutoCompaction()
                .buildEnableSingleReplicaCompaction()
                .buildTimeSeriesCompactionEmptyRowsetsThreshold()
                .buildTimeSeriesCompactionLevelThreshold()
                .buildTTLSeconds()
                .buildAutoAnalyzeProperty();

        // need to update partition info meta
        for (Partition partition : table.getPartitions()) {
            table.getPartitionInfo().setIsInMemory(partition.getId(), tableProperty.isInMemory());
            table.getPartitionInfo().setStoragePolicy(partition.getId(), tableProperty.getStoragePolicy());
        }

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), table.getName(),
                        properties);
        editLog.logModifyTableProperties(info);
    }

    public void updateBinlogConfig(Database db, OlapTable table, BinlogConfig newBinlogConfig) {
        Preconditions.checkArgument(table.isWriteLockHeldByCurrentThread());

        table.setBinlogConfig(newBinlogConfig);

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), table.getName(),
                        newBinlogConfig.toProperties());
        editLog.logUpdateBinlogConfig(info);
    }

    public void replayModifyTableProperty(short opCode, ModifyTablePropertyOperationLog info)
            throws MetaNotFoundException {
        String ctlName = info.getCtlName();
        long dbId = info.getDbId();
        long tableId = info.getTableId();
        Map<String, String> properties = info.getProperties();

        // Handle HMSExternalTable set auto analyze policy.
        if (ctlName != null && !(InternalCatalog.INTERNAL_CATALOG_NAME.equalsIgnoreCase(ctlName))) {
            setExternalTableAutoAnalyze(properties, info);
            return;
        }

        Database db = getInternalCatalog().getDbOrMetaException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        olapTable.writeLock();
        try {
            TableProperty tableProperty = olapTable.getTableProperty();
            if (tableProperty == null) {
                tableProperty = new TableProperty(properties).buildProperty(opCode);
                olapTable.setTableProperty(tableProperty);
            } else {
                tableProperty.modifyTableProperties(properties);
                tableProperty.buildProperty(opCode);
            }

            // need to replay partition info meta
            switch (opCode) {
                case OperationType.OP_MODIFY_TABLE_PROPERTIES:
                    for (Partition partition : olapTable.getPartitions()) {
                        olapTable.getPartitionInfo().setIsInMemory(partition.getId(), tableProperty.isInMemory());
                        // storage policy re-use modify in memory
                        Optional.ofNullable(tableProperty.getStoragePolicy()).filter(p -> !p.isEmpty())
                                .ifPresent(p -> olapTable.getPartitionInfo().setStoragePolicy(partition.getId(), p));
                        Optional.ofNullable(tableProperty.getStoragePolicy()).filter(p -> !p.isEmpty())
                                .ifPresent(p -> olapTable.getPartitionInfo().getDataProperty(partition.getId())
                                .setStoragePolicy(p));
                    }
                    break;
                case OperationType.OP_UPDATE_BINLOG_CONFIG:
                    BinlogConfig newBinlogConfig = new BinlogConfig();
                    newBinlogConfig.mergeFromProperties(properties);
                    olapTable.setBinlogConfig(newBinlogConfig);
                    break;
                default:
                    break;
            }
        } finally {
            olapTable.writeUnlock();
        }
    }

    private void setExternalTableAutoAnalyze(Map<String, String> properties, ModifyTablePropertyOperationLog info) {
        if (properties.size() != 1) {
            LOG.warn("External table property should contain exactly 1 entry.");
            return;
        }
        if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY)) {
            LOG.warn("External table property should only contain auto_analyze_policy");
            return;
        }
        String value = properties.get(PropertyAnalyzer.PROPERTIES_AUTO_ANALYZE_POLICY);
        if (!PropertyAnalyzer.ENABLE_AUTO_ANALYZE_POLICY.equalsIgnoreCase(value)
                && !PropertyAnalyzer.DISABLE_AUTO_ANALYZE_POLICY.equalsIgnoreCase(value)
                && !PropertyAnalyzer.USE_CATALOG_AUTO_ANALYZE_POLICY.equalsIgnoreCase(value)) {
            LOG.warn("External table property should be 'enable', 'disable' or 'base_on_catalog'");
            return;
        }
        try {
            CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrException(info.getCtlName(),
                        ctlName -> new DdlException("Unknown catalog " + ctlName));
            value = value.equalsIgnoreCase(PropertyAnalyzer.USE_CATALOG_AUTO_ANALYZE_POLICY) ? null : value;
            ((ExternalCatalog) catalog).setAutoAnalyzePolicy(info.getDbName(), info.getTableName(), value);
        } catch (Exception e) {
            LOG.warn("Failed to replay external table set property.", e);
        }
    }

    public void modifyDefaultDistributionBucketNum(Database db, OlapTable olapTable,
                                                   ModifyDistributionClause modifyDistributionClause)
            throws DdlException {
        olapTable.writeLockOrDdlException();
        try {
            if (olapTable.isColocateTable()) {
                throw new DdlException("Cannot change default bucket number of colocate table.");
            }

            if (olapTable.getPartitionInfo().getType() != PartitionType.RANGE
                    && olapTable.getPartitionInfo().getType() != PartitionType.LIST) {
                throw new DdlException("Only support change partitioned table's distribution.");
            }

            DistributionDesc distributionDesc = modifyDistributionClause.getDistributionDesc();
            if (distributionDesc != null) {
                DistributionInfo defaultDistributionInfo = olapTable.getDefaultDistributionInfo();
                List<Column> baseSchema = olapTable.getBaseSchema();
                DistributionInfo distributionInfo = distributionDesc.toDistributionInfo(baseSchema);
                // for now. we only support modify distribution's bucket num
                if (distributionInfo.getType() != defaultDistributionInfo.getType()) {
                    throw new DdlException(
                            "Cannot change distribution type when modify" + " default distribution bucket num");
                }
                if (distributionInfo.getType() == DistributionInfoType.HASH) {
                    HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                    if (!hashDistributionInfo.sameDistributionColumns((HashDistributionInfo) defaultDistributionInfo)) {
                        throw new DdlException("Cannot assign hash distribution with different distribution cols. "
                                + "new is: " + hashDistributionInfo.getDistributionColumns() + " default is: "
                                + ((HashDistributionInfo) defaultDistributionInfo).getDistributionColumns());
                    }
                }

                int bucketNum = distributionInfo.getBucketNum();
                if (bucketNum <= 0) {
                    throw new DdlException("Cannot assign hash distribution buckets less than 1");
                }

                defaultDistributionInfo.setBucketNum(bucketNum);

                ModifyTableDefaultDistributionBucketNumOperationLog info
                        = new ModifyTableDefaultDistributionBucketNumOperationLog(db.getId(), olapTable.getId(),
                                distributionInfo.getType(), distributionInfo.getAutoBucket(), bucketNum,
                                defaultDistributionInfo.getColumnsName());
                editLog.logModifyDefaultDistributionBucketNum(info);
                LOG.info("modify table[{}] default bucket num to {}", olapTable.getName(), bucketNum);
            }
        } finally {
            olapTable.writeUnlock();
        }
    }

    public void replayModifyTableDefaultDistributionBucketNum(ModifyTableDefaultDistributionBucketNumOperationLog info)
            throws MetaNotFoundException {
        long dbId = info.getDbId();
        long tableId = info.getTableId();
        int bucketNum = info.getBucketNum();

        Database db = getInternalCatalog().getDbOrMetaException(dbId);
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableId, TableType.OLAP);
        olapTable.writeLock();
        try {
            DistributionInfo defaultDistributionInfo = olapTable.getDefaultDistributionInfo();
            defaultDistributionInfo.setBucketNum(bucketNum);
        } finally {
            olapTable.writeUnlock();
        }
    }

    /*
     * used for handling AlterSystemStmt
     * (for client is the ALTER SYSTEM command).
     */
    public void alterSystem(AlterSystemCommand command) throws UserException {
        this.alter.processAlterSystem(command);
    }

    public void analyze(AnalyzeCommand command, boolean isProxy) throws DdlException, AnalysisException {
        this.analysisManager.createAnalyze(command, isProxy);
    }

    // Switch catalog of this session
    public void changeCatalog(ConnectContext ctx, String catalogName) throws DdlException {
        CatalogIf catalogIf = catalogMgr.getCatalog(catalogName);
        if (catalogIf == null) {
            throw new DdlException(ErrorCode.ERR_UNKNOWN_CATALOG.formatErrorMsg(catalogName),
                    ErrorCode.ERR_UNKNOWN_CATALOG);
        }

        String currentDB = ctx.getDatabase();
        if (StringUtils.isNotEmpty(currentDB)) {
            // When dropped the current catalog in current context, the current catalog will be null.
            if (ctx.getCurrentCatalog() != null) {
                ctx.addLastDBOfCatalog(ctx.getCurrentCatalog().getName(), currentDB);
            }
        }
        ctx.changeDefaultCatalog(catalogName);
        String lastDb = ctx.getLastDBOfCatalog(catalogName);
        if (StringUtils.isNotEmpty(lastDb)) {
            ctx.setDatabase(lastDb);
        }
        if (catalogIf instanceof EsExternalCatalog) {
            ctx.setDatabase(EsExternalCatalog.DEFAULT_DB);
        }
    }

    // Change current database of this session.
    public void changeDb(ConnectContext ctx, String qualifiedDb) throws DdlException {
        if (!accessManager.checkDbPriv(ctx, ctx.getDefaultCatalog(), qualifiedDb, PrivPredicate.SHOW)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_DBACCESS_DENIED_ERROR, ctx.getQualifiedUser(), qualifiedDb);
        }

        ctx.getCurrentCatalog().getDbOrDdlException(qualifiedDb);
        ctx.setDatabase(qualifiedDb);
    }

    // for test only
    public void clear() {
        getInternalCatalog().clearDbs();
        System.gc();
    }

    public void createView(CreateViewCommand command) throws DdlException {
        CreateViewInfo createViewInfo = command.getCreateViewInfo();
        String dbName = createViewInfo.getViewName().getDb();
        String tableName = createViewInfo.getViewName().getTbl();

        // check if db exists
        Database db = getInternalCatalog().getDbOrDdlException(dbName);

        // check if table exists in db
        boolean replace = false;
        if (db.getTable(tableName).isPresent()) {
            if (createViewInfo.isIfNotExists()) {
                LOG.info("create view[{}] which already exists", tableName);
                return;
            } else if (createViewInfo.isOrReplace()) {
                replace = true;
                LOG.info("view[{}] already exists, need to replace it", tableName);
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        }

        if (replace) {
            String comment = createViewInfo.getComment();
            comment = comment == null || comment.isEmpty() ? null : comment;
            AlterViewInfo alterViewInfo = new AlterViewInfo(createViewInfo.getViewName(), comment);
            alterViewInfo.setInlineViewDef(createViewInfo.getInlineViewDef());
            alterViewInfo.setFinalColumns(createViewInfo.getColumns());
            AlterViewCommand alterViewCommand = new AlterViewCommand(alterViewInfo);
            try {
                alterView(alterViewCommand);
            } catch (UserException e) {
                throw new DdlException("failed to replace view[" + tableName + "], reason=" + e.getMessage());
            }
            LOG.info("successfully replace view[{}]", tableName);
        } else {
            List<Column> columns = createViewInfo.getColumns();

            long tableId = Env.getCurrentEnv().getNextId();
            View newView = new View(tableId, tableName, columns);
            newView.setComment(createViewInfo.getComment());
            newView.setInlineViewDefWithSqlMode(createViewInfo.getInlineViewDef(),
                    ConnectContext.get().getSessionVariable().getSqlMode());
            if (!((Database) db).createTableWithLock(newView, false, createViewInfo.isIfNotExists()).first) {
                throw new DdlException("Failed to create view[" + tableName + "].");
            }
            LOG.info("successfully create view[" + tableName + "-" + newView.getId() + "]");
        }
    }

    public FunctionRegistry getFunctionRegistry() {
        return functionRegistry;
    }

    /**
     * Returns the function that best matches 'desc' that is registered with the
     * catalog using 'mode' to check for matching. If desc matches multiple
     * functions in the catalog, it will return the function with the strictest
     * matching mode. If multiple functions match at the same matching mode,
     * ties are broken by comparing argument types in lexical order. Argument
     * types are ordered by argument precision (e.g. double is preferred over
     * float) and then by alphabetical order of argument type name, to guarantee
     * deterministic results.
     */
    public Function getFunction(Function desc, Function.CompareMode mode) {
        return functionSet.getFunction(desc, mode);
    }

    public List<Function> getBuiltinFunctions() {
        return functionSet.getBulitinFunctions();
    }

    public Function getTableFunction(Function desc, Function.CompareMode mode) {
        return functionSet.getFunction(desc, mode, true);
    }

    public boolean isNondeterministicFunction(String funcName) {
        return functionSet.isNondeterministicFunction(funcName);
    }

    public boolean isNullResultWithOneNullParamFunction(String funcName) {
        return functionSet.isNullResultWithOneNullParamFunctions(funcName);
    }

    public boolean isAggFunctionName(String name) {
        return functionSet.isAggFunctionName(name);
    }

    @Deprecated
    public long loadCluster(DataInputStream dis, long checksum) throws IOException, DdlException {
        return getInternalCatalog().loadCluster(dis, checksum);
    }

    public long saveCluster(CountingDataOutputStream dos, long checksum) throws IOException {
        // do nothing
        return checksum;
    }

    public long saveBrokers(CountingDataOutputStream dos, long checksum) throws IOException {
        Map<String, List<FsBroker>> addressListMap = brokerMgr.getBrokerListMap();
        int size = addressListMap.size();
        checksum ^= size;
        dos.writeInt(size);

        for (Map.Entry<String, List<FsBroker>> entry : addressListMap.entrySet()) {
            Text.writeString(dos, entry.getKey());
            final List<FsBroker> addrs = entry.getValue();
            size = addrs.size();
            checksum ^= size;
            dos.writeInt(size);
            for (FsBroker addr : addrs) {
                addr.write(dos);
            }
        }

        return checksum;
    }

    public long loadBrokers(DataInputStream dis, long checksum) throws IOException, DdlException {
        int count = dis.readInt();
        checksum ^= count;
        for (long i = 0; i < count; ++i) {
            String brokerName = Text.readString(dis);
            int size = dis.readInt();
            checksum ^= size;
            List<FsBroker> addrs = Lists.newArrayList();
            for (int j = 0; j < size; j++) {
                FsBroker addr = FsBroker.readIn(dis);
                addrs.add(addr);
            }
            brokerMgr.replayAddBrokers(brokerName, addrs);
        }
        LOG.info("finished replay brokerMgr from image");
        return checksum;
    }

    public String dumpImage() {
        LOG.info("begin to dump meta data");
        String dumpFilePath;
        List<Database> databases = Lists.newArrayList();
        List<List<Table>> tableLists = Lists.newArrayList();
        tryLock(true);
        try {
            // sort all dbs to avoid potential dead lock
            for (long dbId : getInternalCatalog().getDbIds()) {
                Database db = getInternalCatalog().getDbNullable(dbId);
                databases.add(db);
            }
            databases.sort(Comparator.comparing(DatabaseIf::getId));

            // lock all dbs
            MetaLockUtils.readLockDatabases(databases);
            LOG.info("acquired all the dbs' read lock.");
            // lock all tables
            for (Database db : databases) {
                List<Table> tableList = db.getTablesOnIdOrder();
                MetaLockUtils.readLockTables(tableList);
                tableLists.add(tableList);
            }
            LOG.info("acquired all the tables' read lock.");

            LOG.info("acquired all jobs' read lock.");
            long journalId = getMaxJournalId();
            File dumpFile = new File(Config.meta_dir, "image." + journalId);
            if (Config.enable_check_compatibility_mode) {
                dumpFile = new File(imageDir, "image." + journalId);
            }
            dumpFilePath = dumpFile.getAbsolutePath();
            try {
                LOG.info("begin to dump {}", dumpFilePath);
                saveImage(dumpFile, journalId);
            } catch (IOException e) {
                LOG.error("failed to dump image to {}", dumpFilePath, e);
                return null;
            }
        } finally {
            // unlock all
            for (int i = databases.size() - 1; i >= 0; i--) {
                MetaLockUtils.readUnlockTables(tableLists.get(i));
            }
            MetaLockUtils.readUnlockDatabases(databases);
            unlock();
        }

        LOG.info("finished dumping image to {}", dumpFilePath);
        return dumpFilePath;
    }

    /*
     * Truncate specified table or partitions.
     * The main idea is:
     *
     * 1. using the same schema to create new table(partitions)
     * 2. use the new created table(partitions) to replace the old ones.
     *
     * if no partition specified, it will truncate all partitions of this table, including all temp partitions,
     * otherwise, it will only truncate those specified partitions.
     *
     */
    public void truncateTable(TruncateTableCommand command) throws DdlException {
        CatalogIf<?> catalogIf = catalogMgr.getCatalogOrException(command.getTableNameInfo().getCtl(),
                catalog -> new DdlException(("Unknown catalog " + catalog)));
        TableNameInfo nameInfo = command.getTableNameInfo();
        PartitionNamesInfo partitionNamesInfo = command.getPartitionNamesInfo().orElse(null);
        catalogIf.truncateTable(nameInfo.getDb(), nameInfo.getTbl(),
                partitionNamesInfo == null ? null : partitionNamesInfo.translateToLegacyPartitionNames(),
                command.isForceDrop(), command.toSqlWithoutTable());
    }

    public void replayTruncateTable(TruncateTableInfo info) throws MetaNotFoundException {
        if (Strings.isNullOrEmpty(info.getCtl()) || info.getCtl().equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            // In previous versions(before 2.1.8), there is no catalog info in TruncateTableInfo,
            // So if the catalog info is empty, we assume it's internal table.
            getInternalCatalog().replayTruncateTable(info);
            if (info.isEntireTable()) {
                Env.getCurrentEnv().getAnalysisManager().removeTableStats(info.getTblId());
            } else {
                Env.getCurrentEnv().getAnalysisManager().updateUpdatedRows(info.getUpdateRecords(),
                        info.getDbId(), info.getTblId(), 0);
            }
        } else {
            ExternalCatalog ctl = (ExternalCatalog) catalogMgr.getCatalog(info.getCtl());
            if (ctl != null) {
                ctl.replayTruncateTable(info);
            }
        }
    }

    public void createFunction(CreateFunctionStmt stmt) throws UserException {
        if (SetType.GLOBAL.equals(stmt.getType())) {
            globalFunctionMgr.addFunction(stmt.getFunction(), stmt.isIfNotExists());
        } else {
            Database db = getInternalCatalog().getDbOrDdlException(stmt.getFunctionName().getDb());
            db.addFunction(stmt.getFunction(), stmt.isIfNotExists());
            if (stmt.getFunction().isUDTFunction()) {
                // all of the table function in doris will have two function
                // one is the noraml, and another is outer, the different of them is deal with
                // empty: whether need to insert NULL result value
                Function outerFunction = stmt.getFunction().clone();
                FunctionName name = outerFunction.getFunctionName();
                name.setFn(name.getFunction() + "_outer");
                db.addFunction(outerFunction, stmt.isIfNotExists());
            }
        }
    }

    public void replayCreateFunction(Function function) throws MetaNotFoundException {
        String dbName = function.getFunctionName().getDb();
        Database db = getInternalCatalog().getDbOrMetaException(dbName);
        db.replayAddFunction(function);
    }

    public void replayCreateGlobalFunction(Function function) {
        globalFunctionMgr.replayAddFunction(function);
    }

    public void dropFunction(DropFunctionStmt stmt) throws UserException {
        FunctionName name = stmt.getFunctionName();
        if (SetType.GLOBAL.equals(stmt.getType())) {
            globalFunctionMgr.dropFunction(stmt.getFunction(), stmt.isIfExists());
        } else {
            Database db = getInternalCatalog().getDbOrDdlException(name.getDb());
            db.dropFunction(stmt.getFunction(), stmt.isIfExists());
        }
        cleanUDFCacheTask(stmt); // BE will cache classload, when drop function, BE need clear cache
    }

    public void replayDropFunction(FunctionSearchDesc functionSearchDesc) throws MetaNotFoundException {
        String dbName = functionSearchDesc.getName().getDb();
        Database db = getInternalCatalog().getDbOrMetaException(dbName);
        db.replayDropFunction(functionSearchDesc);
    }

    public void replayDropGlobalFunction(FunctionSearchDesc functionSearchDesc) {
        globalFunctionMgr.replayDropFunction(functionSearchDesc);
    }

    /**
     * we can't set callback which is in fe-core to config items which are in fe-common. so wrap them here. it's not so
     * good but is best for us now.
     */
    public void setMutableConfigWithCallback(String key, String value) throws ConfigException {
        ConfigBase.setMutableConfig(key, value);
        if (configtoThreads.get(key) != null) {
            try {
                // not atomic. maybe delay to aware. but acceptable.
                configtoThreads.get(key).get().setInterval(Config.getField(key).getLong(null) * 1000L);
                // shouldn't interrupt to keep possible bdbje writing safe.
                LOG.info("set config " + key + " to " + value);
            } catch (IllegalAccessException e) {
                LOG.warn("set config " + key + " failed: " + e.getMessage());
            }
        }
    }

    public void setConfig(AdminSetFrontendConfigCommand command) throws Exception {
        Map<String, String> configs = command.getConfigs();
        Preconditions.checkState(configs.size() == 1);

        for (Map.Entry<String, String> entry : configs.entrySet()) {
            try {
                setMutableConfigWithCallback(entry.getKey(), entry.getValue());
            } catch (ConfigException e) {
                throw new DdlException(e.getMessage());
            }
        }

        if (command.isApplyToAll()) {
            for (Frontend fe : Env.getCurrentEnv().getFrontends(null /* all */)) {
                if (!fe.isAlive() || fe.getHost().equals(Env.getCurrentEnv().getSelfNode().getHost())) {
                    continue;
                }

                TNetworkAddress feAddr = new TNetworkAddress(fe.getHost(), fe.getRpcPort());
                FEOpExecutor executor = new FEOpExecutor(feAddr, command.getLocalSetStmt(),
                        ConnectContext.get(), false);
                executor.execute();
                if (executor.getStatusCode() != TStatusCode.OK.getValue()) {
                    throw new DdlException(String.format("failed to apply to fe %s:%s, error message: %s",
                        fe.getHost(), fe.getRpcPort(), executor.getErrMsg()));
                }
            }
        }
    }

    public void setPartitionVersion(AdminSetPartitionVersionCommand command) throws Exception {
        String database = command.getDatabase();
        String table = command.getTable();
        long partitionId = command.getPartitionId();
        long visibleVersion = command.getVisibleVersion();
        int setSuccess = setPartitionVersionInternal(database, table, partitionId, visibleVersion, false);
        if (setSuccess == -1) {
            throw new DdlException("Failed to set partition visible version to " + visibleVersion + ". " + "Partition "
                    + partitionId + " not exists. Database " + database + ", Table " + table + ".");
        }
    }

    public void replayBackendReplicasInfo(BackendReplicasInfo backendReplicasInfo) {
        long backendId = backendReplicasInfo.getBackendId();
        List<BackendReplicasInfo.ReplicaReportInfo> replicaInfos = backendReplicasInfo.getReplicaReportInfos();

        for (BackendReplicasInfo.ReplicaReportInfo info : replicaInfos) {
            if (tabletInvertedIndex.getTabletMeta(info.tabletId) == null) {
                // The tablet has been deleted. Because the reporting of tablet and
                // the deletion of tablet are two independent events,
                // and directly do not do mutually exclusive processing,
                // so it may appear that the tablet is deleted first, and the reporting information is processed later.
                // Here we simply ignore the deleted tablet.
                continue;
            }
            Replica replica = tabletInvertedIndex.getReplica(info.tabletId, backendId);
            if (replica == null) {
                LOG.warn("failed to find replica of tablet {} on backend {} when replaying backend report info",
                        info.tabletId, backendId);
                continue;
            }

            switch (info.type) {
                case BAD:
                    replica.setBad(true);
                    break;
                case MISSING_VERSION:
                    replica.updateLastFailedVersion(info.lastFailedVersion);
                    break;
                default:
                    break;
            }
        }
    }

    @Deprecated
    public void replayBackendTabletsInfo(BackendTabletsInfo backendTabletsInfo) {
        List<Pair<Long, Integer>> tabletsWithSchemaHash = backendTabletsInfo.getTabletSchemaHash();
        if (!tabletsWithSchemaHash.isEmpty()) {
            // In previous version, we save replica info in `tabletsWithSchemaHash`,
            // but it is wrong because we can not get replica from `tabletInvertedIndex` when doing checkpoint,
            // because when doing checkpoint, the tabletInvertedIndex is not initialized at all.
            //
            // So we can only discard this information, in this case, it is equivalent to losing the record of these
            // operations. But it doesn't matter, these records are currently only used to record whether a replica is
            // in a bad state. This state has little effect on the system, and it can be restored after the system
            // has processed the bad state replica.
            for (Pair<Long, Integer> tabletInfo : tabletsWithSchemaHash) {
                LOG.warn("find an old backendTabletsInfo for tablet {}, ignore it", tabletInfo.first);
            }
            return;
        }

        // in new version, replica info is saved here.
        // but we need to get replica from db->tbl->partition->...
        List<ReplicaPersistInfo> replicaPersistInfos = backendTabletsInfo.getReplicaPersistInfos();
        for (ReplicaPersistInfo info : replicaPersistInfos) {
            OlapTable olapTable = (OlapTable) getInternalCatalog().getDb(info.getDbId())
                    .flatMap(db -> db.getTable(info.getTableId())).filter(t -> t.isManagedTable())
                    .orElse(null);
            if (olapTable == null) {
                continue;
            }
            olapTable.writeLock();
            try {

                Partition partition = olapTable.getPartition(info.getPartitionId());
                if (partition == null) {
                    continue;
                }
                MaterializedIndex mindex = partition.getIndex(info.getIndexId());
                if (mindex == null) {
                    continue;
                }
                Tablet tablet = mindex.getTablet(info.getTabletId());
                if (tablet == null) {
                    continue;
                }
                Replica replica = tablet.getReplicaById(info.getReplicaId());
                if (replica != null) {
                    replica.setBad(true);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("get replica {} of tablet {} on backend {} to bad when replaying",
                                info.getReplicaId(), info.getTabletId(), info.getBackendId());
                    }
                }
            } finally {
                olapTable.writeUnlock();
            }
        }
    }

    // Convert table's distribution type from hash to random.
    public void convertDistributionType(Database db, OlapTable tbl) throws DdlException {
        tbl.writeLockOrDdlException();
        try {
            if (tbl.isColocateTable()) {
                throw new DdlException("Cannot change distribution type of colocate table.");
            }
            if (tbl.getKeysType() == KeysType.UNIQUE_KEYS) {
                throw new DdlException("Cannot change distribution type of unique keys table.");
            }
            if (tbl.getKeysType() == KeysType.AGG_KEYS) {
                for (Column column : tbl.getBaseSchema()) {
                    if (column.getAggregationType() == AggregateType.REPLACE
                            || column.getAggregationType() == AggregateType.REPLACE_IF_NOT_NULL) {
                        throw new DdlException("Cannot change distribution type of aggregate keys table which has value"
                                + " columns with " + column.getAggregationType() + " type.");
                    }
                }
            }
            if (!tbl.convertHashDistributionToRandomDistribution()) {
                throw new DdlException("Table " + tbl.getName() + " is not hash distributed");
            }
            TableInfo tableInfo = TableInfo.createForModifyDistribution(db.getId(), tbl.getId());
            editLog.logModifyDistributionType(tableInfo);
            LOG.info("finished to modify distribution type of table from hash to random : " + tbl.getName());
        } finally {
            tbl.writeUnlock();
        }
    }

    public void replayConvertDistributionType(TableInfo info) throws MetaNotFoundException {
        Database db = getInternalCatalog().getDbOrMetaException(info.getDbId());
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(info.getTableId(), TableType.OLAP);
        olapTable.writeLock();
        try {
            olapTable.convertHashDistributionToRandomDistribution();
            LOG.info("replay modify distribution type of table from hash to random : " + olapTable.getName());
        } finally {
            olapTable.writeUnlock();
        }
    }

    /*
     * The entry of replacing partitions with temp partitions.
     */
    public void replaceTempPartition(Database db, OlapTable olapTable, ReplacePartitionClause clause)
            throws DdlException {
        Preconditions.checkState(olapTable.isWriteLockHeldByCurrentThread());
        List<String> partitionNames = clause.getPartitionNames();
        List<String> tempPartitionNames = clause.getTempPartitionNames();
        boolean isStrictRange = clause.isStrictRange();
        boolean useTempPartitionName = clause.useTempPartitionName();
        boolean isForceDropOld = clause.isForceDropOldPartition();
        // check partition exist
        for (String partName : partitionNames) {
            if (!olapTable.checkPartitionNameExist(partName, false)) {
                throw new DdlException("Partition[" + partName + "] does not exist");
            }
        }
        for (String partName : tempPartitionNames) {
            if (!olapTable.checkPartitionNameExist(partName, true)) {
                throw new DdlException("Temp partition[" + partName + "] does not exist");
            }
        }
        List<Long> replacedPartitionIds = olapTable.replaceTempPartitions(db.getId(), partitionNames,
                tempPartitionNames, isStrictRange,
                useTempPartitionName, isForceDropOld);
        long version = 0L;
        long versionTime = System.currentTimeMillis();
        // In cloud mode, the internal partition deletion logic will update the table version,
        // so here we only need to handle non-cloud mode.
        if (Config.isNotCloudMode()) {
            version = olapTable.getNextVersion();
            olapTable.updateVisibleVersionAndTime(version, versionTime);
        }
        // Here, we only wait for the EventProcessor to finish processing the event,
        // but regardless of the success or failure of the result,
        // it does not affect the logic of replace the partition
        try {
            Env.getCurrentEnv().getEventProcessor().processEvent(
                    new ReplacePartitionEvent(db.getCatalog().getId(), db.getId(),
                            olapTable.getId()));
        } catch (Throwable t) {
            // According to normal logic, no exceptions will be thrown,
            // but in order to avoid bugs affecting the original logic, all exceptions are caught
            LOG.warn("produceEvent failed: ", t);
        }
        // write log
        ReplacePartitionOperationLog info = new ReplacePartitionOperationLog(db.getId(), db.getFullName(),
                olapTable.getId(), olapTable.getName(),
                partitionNames, tempPartitionNames, replacedPartitionIds, isStrictRange, useTempPartitionName, version,
                versionTime,
                isForceDropOld);
        editLog.logReplaceTempPartition(info);
        LOG.info("finished to replace partitions {} with temp partitions {} from table: {}", clause.getPartitionNames(),
                clause.getTempPartitionNames(), olapTable.getName());
    }

    public void replayReplaceTempPartition(ReplacePartitionOperationLog replaceTempPartitionLog)
            throws MetaNotFoundException {
        long dbId = replaceTempPartitionLog.getDbId();
        long tableId = replaceTempPartitionLog.getTblId();
        Database db = getInternalCatalog().getDbOrMetaException(dbId);
        OlapTable olapTable = (OlapTable) db
                .getTableOrMetaException(tableId, Lists.newArrayList(TableType.OLAP, TableType.MATERIALIZED_VIEW));
        olapTable.writeLock();
        try {
            olapTable.replaceTempPartitions(dbId, replaceTempPartitionLog.getPartitions(),
                    replaceTempPartitionLog.getTempPartitions(), replaceTempPartitionLog.isStrictRange(),
                    replaceTempPartitionLog.useTempPartitionName(), replaceTempPartitionLog.isForce());
            // In cloud mode, the internal partition deletion logic will update the table version,
            // so here we only need to handle non-cloud mode.
            if (Config.isNotCloudMode()) {
                olapTable.updateVisibleVersionAndTime(replaceTempPartitionLog.getVersion(),
                        replaceTempPartitionLog.getVersionTime());
            }
        } catch (DdlException e) {
            throw new MetaNotFoundException(e);
        } finally {
            olapTable.writeUnlock();
        }
    }

    public void installPlugin(InstallPluginStmt stmt) throws UserException, IOException {
        pluginMgr.installPlugin(stmt);
    }

    public long savePlugins(CountingDataOutputStream dos, long checksum) throws IOException {
        Env.getCurrentPluginMgr().write(dos);
        return checksum;
    }

    public long loadPlugins(DataInputStream dis, long checksum) throws IOException {
        Env.getCurrentPluginMgr().readFields(dis);
        LOG.info("finished replay plugins from image");
        return checksum;
    }

    public void replayInstallPlugin(PluginInfo pluginInfo) throws MetaNotFoundException {
        try {
            pluginMgr.replayLoadDynamicPlugin(pluginInfo);
        } catch (Exception e) {
            throw new MetaNotFoundException(e);
        }
    }

    public void uninstallPlugin(UninstallPluginStmt stmt) throws IOException, UserException {
        PluginInfo info = pluginMgr.uninstallPlugin(stmt.getPluginName());
        if (null != info) {
            editLog.logUninstallPlugin(info);
        }
        LOG.info("uninstall plugin = " + stmt.getPluginName());
    }

    public void uninstallPlugin(UninstallPluginCommand cmd) throws IOException, UserException {
        PluginInfo info = pluginMgr.uninstallPlugin(cmd.getPluginName());
        if (null != info) {
            editLog.logUninstallPlugin(info);
        }
        LOG.info("uninstall plugin = " + cmd.getPluginName());
    }

    public void replayUninstallPlugin(PluginInfo pluginInfo) throws MetaNotFoundException {
        try {
            pluginMgr.uninstallPlugin(pluginInfo.getName());
        } catch (Exception e) {
            throw new MetaNotFoundException(e);
        }
    }

    public void replaySetTableStatus(SetTableStatusOperationLog log) throws MetaNotFoundException {
        setTableStatusInternal(log.getDbName(), log.getTblName(), log.getState(), true);
    }

    public void setTableStatusInternal(String dbName, String tableName, OlapTableState state, boolean isReplay)
            throws MetaNotFoundException {
        Database db = getInternalCatalog().getDbOrMetaException(dbName);
        OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableName, TableType.OLAP);
        olapTable.writeLockOrMetaException();
        try {
            OlapTableState oldState = olapTable.getState();
            if (state != null && oldState != state) {
                olapTable.setState(state);
                if (!isReplay) {
                    SetTableStatusOperationLog log = new SetTableStatusOperationLog(dbName, tableName, state);
                    editLog.logSetTableStatus(log);
                }
                LOG.info("set table {} state from {} to {}. is replay: {}.",
                            tableName, oldState, state, isReplay);
            } else {
                LOG.warn("ignore set same state {} for table {}. is replay: {}.",
                            olapTable.getState(), tableName, isReplay);
            }
        } finally {
            olapTable.writeUnlock();
        }
    }

    // Set specified replica's status. If replica does not exist, just ignore it.
    public void setReplicaStatus(AdminSetReplicaStatusCommand command) throws MetaNotFoundException {
        long tabletId = command.getTabletId();
        long backendId = command.getBackendId();
        ReplicaStatus status = command.getStatus();
        long userDropTime = status == ReplicaStatus.DROP ? System.currentTimeMillis() : -1L;
        setReplicaStatusInternal(tabletId, backendId, status, userDropTime, false);
    }

    public void replaySetReplicaStatus(SetReplicaStatusOperationLog log) throws MetaNotFoundException {
        setReplicaStatusInternal(log.getTabletId(), log.getBackendId(), log.getReplicaStatus(),
                log.getUserDropTime(), true);
    }

    private void setReplicaStatusInternal(long tabletId, long backendId, ReplicaStatus status, long userDropTime,
            boolean isReplay)
            throws MetaNotFoundException {
        try {
            TabletMeta meta = tabletInvertedIndex.getTabletMeta(tabletId);
            if (meta == null) {
                throw new MetaNotFoundException("tablet does not exist");
            }
            Database db = getInternalCatalog().getDbOrMetaException(meta.getDbId());
            Table table = db.getTableOrMetaException(meta.getTableId());
            table.writeLockOrMetaException();
            try {
                Replica replica = tabletInvertedIndex.getReplica(tabletId, backendId);
                if (replica == null) {
                    throw new MetaNotFoundException("replica does not exist on backend, beId=" + backendId);
                }
                boolean updated = false;
                if (status == ReplicaStatus.BAD || status == ReplicaStatus.OK) {
                    replica.setUserDropTime(-1L);
                    if (replica.setBad(status == ReplicaStatus.BAD)) {
                        updated = true;
                        LOG.info("set replica {} of tablet {} on backend {} as {}. is replay: {}", replica.getId(),
                                tabletId, backendId, status, isReplay);
                    }
                } else if (status == ReplicaStatus.DROP) {
                    replica.setUserDropTime(userDropTime);
                    updated = true;
                    LOG.info("set replica {} of tablet {} on backend {} as {}.", replica.getId(),
                            tabletId, backendId, status);
                }
                if (updated && !isReplay) {
                    SetReplicaStatusOperationLog log = new SetReplicaStatusOperationLog(backendId, tabletId,
                            status, userDropTime);
                    getEditLog().logSetReplicaStatus(log);
                }
            } finally {
                table.writeUnlock();
            }
        } catch (MetaNotFoundException e) {
            throw new MetaNotFoundException("set replica status failed, tabletId=" + tabletId, e);
        }
    }

    // Set specified replica's version. If replica does not exist, just ignore it.
    public void setReplicaVersion(AdminSetReplicaVersionCommand command) throws MetaNotFoundException {
        long tabletId = command.getTabletId();
        long backendId = command.getBackendId();
        Long version = command.getVersion();
        Long lastSuccessVersion = command.getLastSuccessVersion();
        Long lastFailedVersion = command.getLastFailedVersion();
        long updateTime = System.currentTimeMillis();
        setReplicaVersionInternal(tabletId, backendId, version, lastSuccessVersion, lastFailedVersion,
                updateTime, false);
    }

    public void replaySetReplicaVersion(SetReplicaVersionOperationLog log) throws MetaNotFoundException {
        setReplicaVersionInternal(log.getTabletId(), log.getBackendId(), log.getVersion(),
                log.getLastSuccessVersion(), log.getLastFailedVersion(), log.getUpdateTime(), true);
    }

    private void setReplicaVersionInternal(long tabletId, long backendId, Long version, Long lastSuccessVersion,
            Long lastFailedVersion, long updateTime, boolean isReplay)
            throws MetaNotFoundException {
        try {
            if (Config.isCloudMode()) {
                throw new MetaNotFoundException("not support modify replica version in cloud mode");
            }
            TabletMeta meta = tabletInvertedIndex.getTabletMeta(tabletId);
            if (meta == null) {
                throw new MetaNotFoundException("tablet does not exist");
            }
            Database db = getInternalCatalog().getDbOrMetaException(meta.getDbId());
            Table table = db.getTableOrMetaException(meta.getTableId());
            table.writeLockOrMetaException();
            try {
                Replica replica = tabletInvertedIndex.getReplica(tabletId, backendId);
                if (replica == null) {
                    throw new MetaNotFoundException("replica does not exist on backend, beId=" + backendId);
                }
                replica.adminUpdateVersionInfo(version, lastFailedVersion, lastSuccessVersion, updateTime);
                if (!isReplay) {
                    SetReplicaVersionOperationLog log = new SetReplicaVersionOperationLog(backendId, tabletId,
                            version, lastSuccessVersion, lastFailedVersion, updateTime);
                    getEditLog().logSetReplicaVersion(log);
                }
                LOG.info("set replica {} of tablet {} on backend {} as version {}, last success version {}, "
                        + "last failed version {}, update time {}. is replay: {}", replica.getId(), tabletId,
                        backendId, version, lastSuccessVersion, lastFailedVersion, updateTime, isReplay);
            } finally {
                table.writeUnlock();
            }
        } catch (MetaNotFoundException e) {
            throw new MetaNotFoundException("set replica version failed, tabletId=" + tabletId, e);
        }
    }

    public void eraseDatabase(long dbId, boolean needEditLog) {
        // remove database transaction manager
        Env.getCurrentGlobalTransactionMgr().removeDatabaseTransactionMgr(dbId);

        if (needEditLog) {
            Env.getCurrentEnv().getEditLog().logEraseDb(dbId);
        }
    }

    public void onEraseOlapTable(OlapTable olapTable, boolean isReplay) {
        // inverted index
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        Collection<Partition> allPartitions = olapTable.getAllPartitions();
        for (Partition partition : allPartitions) {
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    invertedIndex.deleteTablet(tablet.getId());
                }
            }
        }

        // TODO: does checkpoint need update colocate index ?
        // colocation
        Env.getCurrentColocateIndex().removeTable(olapTable.getId());

        getInternalCatalog().eraseTableDropBackendReplicas(olapTable, isReplay);
    }

    public void onErasePartition(Partition partition) {
        // remove tablet in inverted index
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
            for (Tablet tablet : index.getTablets()) {
                invertedIndex.deleteTablet(tablet.getId());
            }
        }

        getInternalCatalog().erasePartitionDropBackendReplicas(Lists.newArrayList(partition));
    }

    public void cleanUDFCacheTask(DropFunctionStmt stmt) throws UserException {
        ImmutableMap<Long, Backend> backendsInfo = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        String functionSignature = stmt.signatureString();
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Backend backend : backendsInfo.values()) {
            CleanUDFCacheTask cleanUDFCacheTask = new CleanUDFCacheTask(backend.getId(), functionSignature);
            batchTask.addTask(cleanUDFCacheTask);
            LOG.info("clean udf cache in be {}, beId {}", backend.getHost(), backend.getId());
        }
        AgentTaskExecutor.submit(batchTask);
    }

    public void setPartitionVersion(AdminSetPartitionVersionStmt stmt) throws DdlException {
        String database = stmt.getDatabase();
        String table = stmt.getTable();
        long partitionId = stmt.getPartitionId();
        long visibleVersion = stmt.getVisibleVersion();
        int setSuccess = setPartitionVersionInternal(database, table, partitionId, visibleVersion, false);
        if (setSuccess == -1) {
            throw new DdlException("Failed to set partition visible version to " + visibleVersion + ". " + "Partition "
                    + partitionId + " not exists. Database " + database + ", Table " + table + ".");
        }
    }

    public void replaySetPartitionVersion(SetPartitionVersionOperationLog log) throws DdlException {
        int setSuccess = setPartitionVersionInternal(log.getDatabase(), log.getTable(),
                log.getPartitionId(), log.getVisibleVersion(), true);
        if (setSuccess == -1) {
            LOG.warn("Failed to set partition visible version to {}. "
                    + "Database {}, Table {}, Partition {} not exists.", log.getDatabase(), log.getTable(),
                    log.getVisibleVersion(), log.getPartitionId());
        }
    }

    public int setPartitionVersionInternal(String database, String table, long partitionId,
                                           long visibleVersion, boolean isReplay) throws DdlException {
        int result = -1;
        Database db = getInternalCatalog().getDbOrDdlException(database);
        OlapTable olapTable = db.getOlapTableOrDdlException(table);
        olapTable.writeLockOrDdlException();
        try {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition != null) {
                Long oldVersion = partition.getVisibleVersion();
                partition.updateVisibleVersion(visibleVersion);
                partition.setNextVersion(visibleVersion + 1);
                result = 0;
                if (!isReplay) {
                    SetPartitionVersionOperationLog log = new SetPartitionVersionOperationLog(
                            database, table, partitionId, visibleVersion);
                    getEditLog().logSetPartitionVersion(log);
                }
                LOG.info("set partition {} visible version from {} to {}. Database {}, Table {}, is replay:"
                        + " {}.", partitionId, oldVersion, visibleVersion, database, table, isReplay);
            }
        } finally {
            olapTable.writeUnlock();
        }
        return result;
    }

    public static boolean isStoredTableNamesLowerCase() {
        return GlobalVariable.lowerCaseTableNames == 1;
    }

    public static boolean isTableNamesCaseInsensitive() {
        return GlobalVariable.lowerCaseTableNames == 2;
    }

    public static boolean isTableNamesCaseSensitive() {
        return GlobalVariable.lowerCaseTableNames == 0;
    }

    private static void getTableMeta(OlapTable olapTable, TGetMetaDBMeta dbMeta) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("get table meta. table: {}", olapTable.getName());
        }

        TGetMetaTableMeta tableMeta = new TGetMetaTableMeta();
        olapTable.readLock();
        try {
            tableMeta.setId(olapTable.getId());
            tableMeta.setName(olapTable.getName());
            tableMeta.setType(olapTable.getType().name());

            PartitionInfo tblPartitionInfo = olapTable.getPartitionInfo();

            Collection<Partition> partitions = olapTable.getAllPartitions();
            for (Partition partition : partitions) {
                TGetMetaPartitionMeta partitionMeta = new TGetMetaPartitionMeta();
                long partitionId = partition.getId();
                partitionMeta.setId(partitionId);
                partitionMeta.setName(partition.getName());
                String partitionRange = tblPartitionInfo.getPartitionRangeString(partitionId);
                partitionMeta.setRange(partitionRange);
                partitionMeta.setVisibleVersion(partition.getVisibleVersion());
                // partitionMeta.setTemp（partition.isTemp());

                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    TGetMetaIndexMeta indexMeta = new TGetMetaIndexMeta();
                    indexMeta.setId(index.getId());
                    indexMeta.setName(olapTable.getIndexNameById(index.getId()));

                    for (Tablet tablet : index.getTablets()) {
                        TGetMetaTabletMeta tabletMeta = new TGetMetaTabletMeta();
                        tabletMeta.setId(tablet.getId());

                        for (Replica replica : tablet.getReplicas()) {
                            TGetMetaReplicaMeta replicaMeta = new TGetMetaReplicaMeta();
                            replicaMeta.setId(replica.getId());
                            replicaMeta.setBackendId(replica.getBackendIdWithoutException());
                            replicaMeta.setVersion(replica.getVersion());
                            tabletMeta.addToReplicas(replicaMeta);
                        }

                        indexMeta.addToTablets(tabletMeta);
                    }

                    partitionMeta.addToIndexes(indexMeta);
                }
                tableMeta.addToPartitions(partitionMeta);
            }
            dbMeta.addToTables(tableMeta);
        } finally {
            olapTable.readUnlock();
        }
    }

    public static TGetMetaResult getMeta(Database db, List<Table> tables) throws MetaNotFoundException {
        TGetMetaResult result = new TGetMetaResult();
        result.setStatus(new TStatus(TStatusCode.OK));

        TGetMetaDBMeta dbMeta = new TGetMetaDBMeta();
        dbMeta.setId(db.getId());
        dbMeta.setName(db.getFullName());

        if (tables == null) {
            db.readLock();
            tables = db.getTables();
            db.readUnlock();
        }

        for (Table table : tables) {
            if (!table.isManagedTable()) {
                continue;
            }

            OlapTable olapTable = (OlapTable) table;
            getTableMeta(olapTable, dbMeta);
        }

        if (Config.enable_feature_binlog) {
            BinlogManager binlogManager = Env.getCurrentEnv().getBinlogManager();
            // id -> commit seq
            List<Pair<Long, Long>> droppedPartitions = binlogManager.getDroppedPartitions(db.getId());
            List<Pair<Long, Long>> droppedTables = binlogManager.getDroppedTables(db.getId());
            List<Pair<Long, Long>> droppedIndexes = binlogManager.getDroppedIndexes(db.getId());
            dbMeta.setDroppedPartitionMap(droppedPartitions.stream()
                    .collect(Collectors.toMap(p -> p.first, p -> p.second)));
            dbMeta.setDroppedTableMap(droppedTables.stream()
                    .collect(Collectors.toMap(p -> p.first, p -> p.second)));
            dbMeta.setDroppedIndexMap(droppedIndexes.stream()
                    .collect(Collectors.toMap(p -> p.first, p -> p.second)));
            // Keep compatibility with old version
            dbMeta.setDroppedPartitions(droppedPartitions.stream()
                    .map(p -> p.first)
                    .collect(Collectors.toList()));
            dbMeta.setDroppedTables(droppedTables.stream()
                    .map(p -> p.first)
                    .collect(Collectors.toList()));
            dbMeta.setDroppedIndexes(droppedIndexes.stream()
                    .map(p -> p.first)
                    .collect(Collectors.toList()));
        }

        result.setDbMeta(dbMeta);
        return result;
    }

    public void compactTable(String dbName, String tableName, String type, List<String> partitionNames)
            throws DdlException {
        Database db = getInternalCatalog().getDbOrDdlException(dbName);
        OlapTable olapTable = db.getOlapTableOrDdlException(tableName);

        AgentBatchTask batchTask = new AgentBatchTask();
        olapTable.readLock();
        try {
            LOG.info("Table compaction. database: {}, table: {}, partition: {}, type: {}", dbName, tableName,
                    Joiner.on(", ").join(partitionNames), type);
            for (String parName : partitionNames) {
                Partition partition = olapTable.getPartition(parName);
                if (partition == null) {
                    throw new DdlException("partition[" + parName + "] not exist in table[" + tableName + "]");
                }

                for (MaterializedIndex idx : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    for (Tablet tablet : idx.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            CompactionTask compactionTask = new CompactionTask(replica.getBackendIdWithoutException(),
                                    db.getId(), olapTable.getId(), partition.getId(), idx.getId(), tablet.getId(),
                                    olapTable.getSchemaHashByIndexId(idx.getId()), type);
                            batchTask.addTask(compactionTask);
                        }
                    }
                } // indices
            }
        } finally {
            olapTable.readUnlock();
        }

        // send task immediately
        AgentTaskExecutor.submit(batchTask);
    }

    private static void addTableComment(TableIf table, StringBuilder sb) {
        if (StringUtils.isNotBlank(table.getComment())) {
            sb.append("\nCOMMENT '").append(table.getComment(true)).append("'");
        }
    }

    public int getFollowerCount() {
        int count = 0;
        for (Frontend fe : frontends.values()) {
            if (fe.getRole() == FrontendNodeType.FOLLOWER) {
                count++;
            }
        }
        return count;
    }

    public AnalysisManager getAnalysisManager() {
        return analysisManager;
    }

    public HboPlanStatisticsManager getHboPlanStatisticsManager() {
        return hboPlanStatisticsManager;
    }

    public GlobalFunctionMgr getGlobalFunctionMgr() {
        return globalFunctionMgr;
    }

    public StatisticsCleaner getStatisticsCleaner() {
        return statisticsCleaner;
    }

    public QueryStats getQueryStats() {
        return queryStats;
    }

    public void cleanQueryStats(CleanQueryStatsInfo info) throws DdlException {
        queryStats.clear(info);
        editLog.logCleanQueryStats(info);
    }

    public void replayAutoIncrementIdUpdateLog(AutoIncrementIdUpdateLog log) throws Exception {
        getInternalCatalog().replayAutoIncrementIdUpdateLog(log);
    }

    public ColumnIdFlushDaemon getColumnIdFlusher() {
        return columnIdFlusher;
    }

    public StatisticsAutoCollector getStatisticsAutoCollector() {
        return statisticsAutoCollector;
    }

    public MasterDaemon getTabletStatMgr() {
        return tabletStatMgr;
    }

    public NereidsSqlCacheManager getSqlCacheManager() {
        return sqlCacheManager;
    }

    public NereidsSortedPartitionsCacheManager getSortedPartitionsCacheManager() {
        return sortedPartitionsCacheManager;
    }

    public SplitSourceManager getSplitSourceManager() {
        return splitSourceManager;
    }

    public GlobalExternalTransactionInfoMgr getGlobalExternalTransactionInfoMgr() {
        return globalExternalTransactionInfoMgr;
    }

    public StatisticsJobAppender getStatisticsJobAppender() {
        return statisticsJobAppender;
    }

    public void alterMTMVRefreshInfo(AlterMTMVRefreshInfo info) {
        AlterMTMV alter = new AlterMTMV(info.getMvName(), info.getRefreshInfo(), MTMVAlterOpType.ALTER_REFRESH_INFO);
        this.alter.processAlterMTMV(alter, false);
    }

    public void alterMTMVProperty(AlterMTMVPropertyInfo info) {
        AlterMTMV alter = new AlterMTMV(info.getMvName(), MTMVAlterOpType.ALTER_PROPERTY);
        alter.setMvProperties(info.getProperties());
        this.alter.processAlterMTMV(alter, false);
    }

    public void alterMTMVStatus(TableNameInfo mvName, MTMVStatus status) {
        AlterMTMV alter = new AlterMTMV(mvName, MTMVAlterOpType.ALTER_STATUS);
        alter.setStatus(status);
        this.alter.processAlterMTMV(alter, false);
    }

    public void addMTMVTaskResult(TableNameInfo mvName, MTMVTask task, MTMVRelation relation,
            Map<String, MTMVRefreshPartitionSnapshot> partitionSnapshots) {
        AlterMTMV alter = new AlterMTMV(mvName, MTMVAlterOpType.ADD_TASK);
        alter.setTask(task);
        alter.setRelation(relation);
        alter.setPartitionSnapshots(partitionSnapshots);
        this.alter.processAlterMTMV(alter, false);
    }

    // Ensure the env is ready, otherwise throw an exception.
    public void checkReadyOrThrow() throws Exception {
        if (isReady()) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Node catalog is not ready, please wait for a while. ")
                .append("To master progress: " + toMasterProgress + ".\n")
                .append("Frontends: \n");

        for (String name : frontends.keySet()) {
            Frontend frontend = frontends.get(name);
            if (name == null) {
                continue;
            }
            sb.append(frontend.toString()).append("\n");
        }

        String reason = editLog.getNotReadyReason();
        if (!Strings.isNullOrEmpty(reason)) {
            sb.append("Reason: ").append(reason).append("%\n");
        }

        if (haProtocol instanceof BDBHA) {
            try {
                BDBHA ha = (BDBHA) haProtocol;
                List<InetSocketAddress> electableNodes = ha.getElectableNodes(true);
                if (!electableNodes.isEmpty()) {
                    sb.append("Electable nodes: \n");
                    for (InetSocketAddress addr : electableNodes) {
                        sb.append(addr.toString()).append("\n");
                    }
                }
                List<InetSocketAddress> observerNodes = ha.getObserverNodes();
                if (!observerNodes.isEmpty()) {
                    sb.append("Observer nodes: \n");
                    for (InetSocketAddress addr : electableNodes) {
                        sb.append(addr.toString()).append("\n");
                    }
                }

            } catch (Exception e) {
                LOG.warn("checkReadyOrThrow:", e);
            }
        }

        throw new Exception(sb.toString());
    }

    public void checkReadyOrThrowTException() throws TException {
        try {
            checkReadyOrThrow();
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    private void replayJournalsAndExit() {
        replayJournal(-1);
        LOG.info("check metadata compatibility successfully");
        LogUtils.stdout("check metadata compatibility successfully");

        if (Config.checkpoint_after_check_compatibility) {
            String imagePath = dumpImage();
            String msg = "the new image file path is: " + imagePath;
            LOG.info(msg);
            LogUtils.stdout(msg);
        }

        System.exit(0);
    }

    public void registerSessionInfo(String sessionId) {
        this.aliveSessionSet.add(sessionId);
    }

    public void unregisterSessionInfo(String sessionId) {
        this.aliveSessionSet.remove(sessionId);
    }

    public List<String> getAllAliveSessionIds() {
        return new ArrayList<>(aliveSessionSet);
    }
}

