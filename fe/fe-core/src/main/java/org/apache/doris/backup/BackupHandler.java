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

package org.apache.doris.backup;

import org.apache.doris.analysis.DropRepositoryStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.backup.AbstractJob.JobType;
import org.apache.doris.backup.BackupJob.BackupJobState;
import org.apache.doris.backup.BackupJobInfo.BackupOlapTableInfo;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.cloud.backup.CloudRestoreJob;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.remote.AzureFileSystem;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.fs.remote.S3FileSystem;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelBackupCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateRepositoryCommand;
import org.apache.doris.nereids.trees.plans.commands.RestoreCommand;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableRefInfo;
import org.apache.doris.persist.BarrierLog;
import org.apache.doris.task.DirMoveTask;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.task.UploadTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class BackupHandler extends MasterDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(BackupHandler.class);

    public static final int SIGNATURE_VERSION = 1;
    public static final Path BACKUP_ROOT_DIR = Paths.get(Config.tmp_dir, "backup").normalize();
    public static final Path RESTORE_ROOT_DIR = Paths.get(Config.tmp_dir, "restore").normalize();
    private RepositoryMgr repoMgr = new RepositoryMgr();

    // this lock is used for updating dbIdToBackupOrRestoreJobs
    private final ReentrantLock jobLock = new ReentrantLock();

    // db id ->  last 10(max_backup_restore_job_num_per_db) backup/restore jobs
    // Newly submitted job will replace the current job, only if current job is finished or cancelled.
    // If the last job is finished, user can get the job info from repository. If the last job is cancelled,
    // user can get the error message before submitting the next one.
    private final Map<Long, Deque<AbstractJob>> dbIdToBackupOrRestoreJobs = new HashMap<>();

    // this lock is used for handling one backup or restore request at a time.
    private ReentrantLock seqlock = new ReentrantLock();

    private boolean isInit = false;

    private Env env;

    // map to store backup info, key is label name, value is the BackupJob
    // this map not present in persist && only in fe memory
    // one table only keep one snapshot info, only keep last
    private final Map<String, BackupJob> localSnapshots = new HashMap<>();
    private ReadWriteLock localSnapshotsLock = new ReentrantReadWriteLock();

    public BackupHandler() {
        // for persist
    }

    public BackupHandler(Env env) {
        super("backupHandler", Config.backup_handler_update_interval_millis);
        this.env = env;
    }

    public void setEnv(Env env) {
        this.env = env;
    }

    @Override
    public synchronized void start() {
        Preconditions.checkNotNull(env);
        super.start();
        repoMgr.start();
    }

    public RepositoryMgr getRepoMgr() {
        return repoMgr;
    }

    private boolean init() {
        // Check and create backup dir if necessarily
        File backupDir = new File(BACKUP_ROOT_DIR.toString());
        if (!backupDir.exists()) {
            if (!backupDir.mkdirs()) {
                LOG.warn("failed to create backup dir: " + BACKUP_ROOT_DIR);
                return false;
            }
        } else {
            if (!backupDir.isDirectory()) {
                LOG.warn("backup dir is not a directory: " + BACKUP_ROOT_DIR);
                return false;
            }
        }

        // Check and create restore dir if necessarily
        File restoreDir = new File(RESTORE_ROOT_DIR.toString());
        if (!restoreDir.exists()) {
            if (!restoreDir.mkdirs()) {
                LOG.warn("failed to create restore dir: " + RESTORE_ROOT_DIR);
                return false;
            }
        } else {
            if (!restoreDir.isDirectory()) {
                LOG.warn("restore dir is not a directory: " + RESTORE_ROOT_DIR);
                return false;
            }
        }

        isInit = true;
        return true;
    }

    public AbstractJob getJob(long dbId) {
        return getCurrentJob(dbId);
    }

    public List<AbstractJob> getJobs(long dbId, Predicate<String> predicate) {
        jobLock.lock();
        try {
            return dbIdToBackupOrRestoreJobs.getOrDefault(dbId, new LinkedList<>())
                    .stream()
                    .filter(e -> predicate.test(e.getLabel()))
                    .collect(Collectors.toList());
        } finally {
            jobLock.unlock();
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!isInit) {
            if (!init()) {
                return;
            }
        }

        for (AbstractJob job : getAllCurrentJobs()) {
            job.setEnv(env);
            job.run();
        }
    }

    // handle create repository command
    public void createRepository(CreateRepositoryCommand command) throws DdlException {
        if (!env.getBrokerMgr().containsBroker(command.getBrokerName())
                && command.getStorageType() == StorageBackend.StorageType.BROKER) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "broker does not exist: " + command.getBrokerName());
        }

        RemoteFileSystem fileSystem;
        fileSystem = FileSystemFactory.get(command.getStorageProperties());
        long repoId = env.getNextId();
        Repository repo = new Repository(repoId, command.getName(), command.isReadOnly(), command.getLocation(),
                fileSystem);

        Status st = repoMgr.addAndInitRepoIfNotExist(repo, false);
        if (!st.ok()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "Failed to create repository: " + st.getErrMsg());
        }
        if (!repo.ping()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "Failed to create repository: failed to connect to the repo");
        }
    }

    /**
     * Alters an existing repository by applying the given new properties.
     *
     * @param repoName    The name of the repository to alter.
     * @param newProps    The new properties to apply to the repository.
     * @param strictCheck If true, only allows altering S3 or Azure repositories and validates properties accordingly.
     *                    TODO: Investigate why only S3 and Azure repositories are supported for alter operation
     * @throws DdlException if the repository does not exist, fails to apply properties, or cannot connect
     * to the updated repository.
     */
    public void alterRepository(String repoName, Map<String, String> newProps, boolean strictCheck)
            throws DdlException {
        tryLock();
        try {
            Repository oldRepo = repoMgr.getRepo(repoName);
            if (oldRepo == null) {
                throw new DdlException("Repository does not exist");
            }
            // Merge new properties with the existing repository's properties
            Map<String, String> mergedProps = mergeProperties(oldRepo, newProps, strictCheck);
            // Create new remote file system with merged properties
            RemoteFileSystem fileSystem = FileSystemFactory.get(StorageProperties.createPrimary(mergedProps));
            // Create new Repository instance with updated file system
            Repository newRepo = new Repository(
                    oldRepo.getId(), oldRepo.getName(), oldRepo.isReadOnly(),
                    oldRepo.getLocation(), fileSystem
            );
            // Verify the repository can be connected with new settings
            if (!newRepo.ping()) {
                LOG.warn("Failed to connect repository {}. msg: {}", repoName, newRepo.getErrorMsg());
                throw new DdlException("Repository ping failed with new properties");
            }
            // Apply the new repository metadata
            Status st = repoMgr.alterRepo(newRepo, false /* not replay */);
            if (!st.ok()) {
                throw new DdlException("Failed to alter repository: " + st.getErrMsg());
            }
            // Update all running jobs that are using this repository
            updateOngoingJobs(oldRepo.getId(), newRepo);
        } finally {
            seqlock.unlock();
        }
    }

    /**
     * Merges new user-provided properties into the existing repository's configuration.
     * In strict mode, only supports S3 or Azure repositories and applies internal S3 merge logic.
     *
     * @param repo        The existing repository.
     * @param newProps    New user-specified properties.
     * @param strictCheck Whether to enforce S3/Azure-only and validate the new properties.
     * @return A complete set of merged properties.
     * @throws DdlException if the merge fails or the repository type is unsupported.
     */
    private Map<String, String> mergeProperties(Repository repo, Map<String, String> newProps, boolean strictCheck)
            throws DdlException {
        if (strictCheck) {
            if (!(repo.getRemoteFileSystem() instanceof S3FileSystem
                    || repo.getRemoteFileSystem() instanceof AzureFileSystem)) {
                throw new DdlException("Only support altering S3 or Azure repository");
            }
            // Let the repository validate and enrich the new S3/Azure properties
            Map<String, String> propsCopy = new HashMap<>(newProps);
            Status status = repo.alterRepositoryS3Properties(propsCopy);
            if (!status.ok()) {
                throw new DdlException("Failed to merge S3 properties: " + status.getErrMsg());
            }
            return propsCopy;
        } else {
            // General case: just override old props with new ones
            Map<String, String> combined = new HashMap<>(repo.getRemoteFileSystem().getProperties());
            combined.putAll(newProps);
            return combined;
        }
    }

    /**
     * Updates all currently running jobs associated with the given repository ID.
     * Used to ensure that all jobs operate on the new repository instance after alteration.
     *
     * @param repoId  The ID of the altered repository.
     * @param newRepo The new repository instance.
     */
    private void updateOngoingJobs(long repoId, Repository newRepo) {
        for (AbstractJob job : getAllCurrentJobs()) {
            if (!job.isDone() && job.getRepoId() == repoId) {
                job.updateRepo(newRepo);
            }
        }
    }

    // handle drop repository stmt
    public void dropRepository(DropRepositoryStmt stmt) throws DdlException {
        dropRepository(stmt.getRepoName());
    }

    // handle drop repository stmt
    public void dropRepository(String repoName) throws DdlException {
        tryLock();
        try {
            Repository repo = repoMgr.getRepo(repoName);
            if (repo == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Repository does not exist");
            }

            for (AbstractJob job : getAllCurrentJobs()) {
                if (!job.isDone() && job.getRepoId() == repo.getId()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                                   "Backup or restore job is running on this repository."
                                                           + " Can not drop it");
                }
            }

            Status st = repoMgr.removeRepo(repo.getName(), false /* not replay */);
            if (!st.ok()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                               "Failed to drop repository: " + st.getErrMsg());
            }
        } finally {
            seqlock.unlock();
        }
    }

    public void process(BackupCommand command) throws DdlException {
        if (Config.isCloudMode()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "BACKUP are not supported by the cloud mode yet");
        }

        // check if repo exist
        String repoName = command.getRepoName();
        Repository repository = null;
        if (!repoName.equals(Repository.KEEP_ON_LOCAL_REPO_NAME)) {
            repository = repoMgr.getRepo(repoName);
            if (repository == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Repository " + repoName + " does not exist");
            }
        }

        // check if db exist
        String dbName = command.getDbName();
        Database db = env.getInternalCatalog().getDbOrDdlException(dbName);

        // Try to get sequence lock.
        // We expect at most one operation on a repo at same time.
        // But this operation may take a few seconds with lock held.
        // So we use tryLock() to give up this operation if we can not get lock.
        tryLock();
        try {
            // Check if there is backup or restore job running on this database
            AbstractJob currentJob = getCurrentJob(db.getId());
            if (currentJob != null && !currentJob.isDone()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Can only run one backup or restore job of a database at same time "
                        + ", current running: label = " + currentJob.getLabel() + " jobId = "
                        + currentJob.getJobId() + ", to run label = " + command.getLabel());
            }
            backup(repository, db, command);
        } finally {
            seqlock.unlock();
        }
    }

    public void process(RestoreCommand command) throws DdlException {
        if (Config.isCloudMode() && !Config.enable_cloud_restore_job) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "Restore is an experimental feature in cloud mode. Set config "
                    + "`experimental_enable_cloud_restore_job` = `true` to enable.");
        }
        // check if repo exist
        String repoName = command.getRepoName();
        Repository repository = null;
        if (!repoName.equals(Repository.KEEP_ON_LOCAL_REPO_NAME)) {
            repository = repoMgr.getRepo(repoName);
            if (repository == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Repository " + repoName + " does not exist");
            }
        }

        // check if db exist
        String dbName = command.getDbName();
        Database db = env.getInternalCatalog().getDbOrDdlException(dbName);

        // Try to get sequence lock.
        // We expect at most one operation on a repo at same time.
        // But this operation may take a few seconds with lock held.
        // So we use tryLock() to give up this operation if we can not get lock.
        tryLock();
        try {
            // Check if there is backup or restore job running on this database
            AbstractJob currentJob = getCurrentJob(db.getId());
            if (currentJob != null && !currentJob.isDone()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Can only run one backup or restore job of a database at same time "
                        + ", current running: label = " + currentJob.getLabel() + " jobId = "
                        + currentJob.getJobId() + ", to run label = " + command.getLabel());
            }
            restore(repository, db, command);
        } finally {
            seqlock.unlock();
        }
    }

    private void tryLock() throws DdlException {
        try {
            if (!seqlock.tryLock(10, TimeUnit.SECONDS)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Another backup or restore job"
                        + " is being submitted. Please wait and try again");
            }
        } catch (InterruptedException e) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Got interrupted exception when "
                    + "try locking. Try again");
        }
    }

    private void backup(Repository repository, Database db, BackupCommand command) throws DdlException {
        if (repository != null && repository.isReadOnly()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Repository " + repository.getName()
                    + " is read only");
        }

        long commitSeq = 0;
        Set<String> tableNames = Sets.newHashSet();

        List<TableRefInfo> tableRefInfos = command.getTableRefInfos();

        // Obtain the snapshot commit seq, any creating table binlog will be visible.
        db.readLock();
        try {
            BarrierLog log = new BarrierLog(db.getId(), db.getFullName());
            commitSeq = env.getEditLog().logBarrier(log);

            // Determine the tables to be backed up
            if (tableRefInfos.isEmpty()) {
                tableNames = db.getTableNames();
            } else if (command.isExclude()) {
                tableNames = db.getTableNames();
                for (TableRefInfo tableRefInfo : tableRefInfos) {
                    if (!tableNames.remove(tableRefInfo.getTableNameInfo().getTbl())) {
                        LOG.info("exclude table " + tableRefInfo.getTableNameInfo().getTbl()
                                + " of backup stmt is not exists in db " + db.getFullName());
                    }
                }
            }
        } finally {
            db.readUnlock();
        }

        List<TableRef> tblRefs = Lists.newArrayList();
        if (!tableRefInfos.isEmpty() && !command.isExclude()) {
            for (TableRefInfo tableRefInfo : tableRefInfos) {
                tblRefs.add(tableRefInfo.translateToLegacyTableRef());
            }
        } else {
            for (String tableName : tableNames) {
                TableRefInfo tableRefInfo = new TableRefInfo(new TableNameInfo(db.getFullName(), tableName),
                        null,
                        null,
                        null,
                        new ArrayList<>(),
                        null,
                        null,
                        new ArrayList<>());
                tblRefs.add(tableRefInfo.translateToLegacyTableRef());
            }
        }

        // Check if backup objects are valid
        // This is just a pre-check to avoid most of invalid backup requests.
        // Also calculate the signature for incremental backup check.
        List<TableRef> tblRefsNotSupport = Lists.newArrayList();
        for (TableRef tableRef : tblRefs) {
            String tblName = tableRef.getName().getTbl();
            Table tbl = db.getTableOrDdlException(tblName);

            // filter the table types which are not supported by local backup.
            if (repository == null && tbl.getType() != TableType.OLAP
                    && tbl.getType() != TableType.VIEW && tbl.getType() != TableType.MATERIALIZED_VIEW) {
                tblRefsNotSupport.add(tableRef);
                continue;
            }

            if (tbl.getType() == TableType.VIEW || tbl.getType() == TableType.ODBC
                    || tbl.getType() == TableType.MATERIALIZED_VIEW) {
                continue;
            }
            if (tbl.getType() != TableType.OLAP) {
                if (Config.ignore_backup_not_support_table_type) {
                    LOG.warn("Table '{}' is a {} table, can not backup and ignore it."
                            + "Only OLAP(Doris)/ODBC/VIEW table can be backed up",
                            tblName, tbl.getType().toString());
                    tblRefsNotSupport.add(tableRef);
                    continue;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_NOT_OLAP_TABLE, tblName);
                }
            }

            if (tbl.isTemporary()) {
                if (Config.ignore_backup_not_support_table_type || tblRefs.size() > 1) {
                    LOG.warn("Table '{}' is a temporary table, can not backup and ignore it."
                            + "Only OLAP(Doris)/ODBC/VIEW table can be backed up",
                            Util.getTempTableDisplayName(tblName));
                    tblRefsNotSupport.add(tableRef);
                    continue;
                } else {
                    ErrorReport.reportDdlException("Table " + Util.getTempTableDisplayName(tblName)
                            + " is a temporary table, do not support backup");
                }
            }

            OlapTable olapTbl = (OlapTable) tbl;
            tbl.readLock();
            try {
                if (!Config.ignore_backup_tmp_partitions && olapTbl.existTempPartitions()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Do not support backup table " + olapTbl.getName() + " with temp partitions");
                }

                PartitionNames partitionNames = tableRef.getPartitionNames();
                if (partitionNames != null) {
                    if (!Config.ignore_backup_tmp_partitions && partitionNames.isTemp()) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                "Do not support backup temp partitions in table " + tableRef.getName());
                    }

                    for (String partName : partitionNames.getPartitionNames()) {
                        Partition partition = olapTbl.getPartition(partName);
                        if (partition == null) {
                            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                                    "Unknown partition " + partName + " in table" + tblName);
                        }
                    }
                }
            } finally {
                tbl.readUnlock();
            }
        }

        tblRefs.removeAll(tblRefsNotSupport);

        // Check if label already be used
        long repoId = Repository.KEEP_ON_LOCAL_REPO_ID;
        if (repository != null) {
            List<String> existSnapshotNames = Lists.newArrayList();
            Status st = repository.listSnapshots(existSnapshotNames);
            if (!st.ok()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, st.getErrMsg());
            }
            if (existSnapshotNames.contains(command.getLabel())) {
                if (command.getBackupType() == BackupCommand.BackupType.FULL) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Snapshot with name '"
                            + command.getLabel() + "' already exist in repository");
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Currently does not support "
                            + "incremental backup");
                }
            }
            repoId = repository.getId();
        }

        // Create a backup job
        BackupJob backupJob = new BackupJob(command.getLabel(), db.getId(),
                ClusterNamespace.getNameFromFullName(db.getFullName()),
                tblRefs, command.getTimeoutMs(), command.getContent(), env, repoId, commitSeq);
        // write log
        env.getEditLog().logBackupJob(backupJob);

        // must put to dbIdToBackupOrRestoreJob after edit log, otherwise the state of job may be changed.
        addBackupOrRestoreJob(db.getId(), backupJob);

        LOG.info("finished to submit backup job: {}", backupJob);
    }

    public void restore(Repository repository, Database db, RestoreCommand command) throws DdlException {
        BackupJobInfo jobInfo;
        if ((command.isLocal() || command.isAtomicRestore() || command.reserveColocate() || command.isForceReplace())
                && Config.isCloudMode()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "not supported now.");
        }
        if (command.isLocal()) {
            String jobInfoString = new String(command.getJobInfo());
            jobInfo = BackupJobInfo.genFromJson(jobInfoString);

            if (jobInfo.extraInfo == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Invalid job extra info empty");
            }
            if (jobInfo.extraInfo.beNetworkMap == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Invalid job extra info be network map");
            }
            if (Strings.isNullOrEmpty(jobInfo.extraInfo.token)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Invalid job extra info token");
            }
        } else {
            // Check if snapshot exist in repository
            List<BackupJobInfo> infos = Lists.newArrayList();
            Status status = repository.getSnapshotInfoFile(command.getLabel(), command.getBackupTimestamp(), infos);
            if (!status.ok()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Failed to get info of snapshot '" + command.getLabel() + "' because: "
                        + status.getErrMsg() + ". Maybe specified wrong backup timestamp");
            }

            // Check if all restore objects are exist in this snapshot.
            // Also remove all unrelated objs
            Preconditions.checkState(infos.size() == 1);
            jobInfo = infos.get(0);
        }

        checkAndFilterRestoreObjsExistInSnapshot(jobInfo, command);

        // Create a restore job
        RestoreJob restoreJob;
        if (command.isLocal()) {
            int metaVersion = command.getMetaVersion();
            if (metaVersion == -1) {
                metaVersion = jobInfo.metaVersion;
            }

            BackupMeta backupMeta;
            try {
                backupMeta = BackupMeta.fromBytes(command.getMeta(), metaVersion);
            } catch (IOException e) {
                LOG.warn("read backup meta failed, current meta version {}", Env.getCurrentEnvJournalVersion(), e);
                throw new DdlException("read backup meta failed", e);
            }
            String backupTimestamp = TimeUtils.longToTimeString(
                    jobInfo.getBackupTime(), TimeUtils.getDatetimeFormatWithHyphenWithTimeZone());
            restoreJob = new RestoreJob(command.getLabel(), backupTimestamp,
                db.getId(), db.getFullName(), jobInfo, command.allowLoad(), command.getReplicaAlloc(),
                command.getTimeoutMs(), command.getMetaVersion(), command.reserveReplica(), command.reserveColocate(),
                command.reserveDynamicPartitionEnable(), command.isBeingSynced(), command.isCleanTables(),
                command.isCleanPartitions(), command.isAtomicRestore(), command.isForceReplace(),
                env, Repository.KEEP_ON_LOCAL_REPO_ID, backupMeta);
        } else {
            if (Config.isCloudMode()) {
                restoreJob = new CloudRestoreJob(command.getLabel(), command.getBackupTimestamp(),
                    db.getId(), db.getFullName(), jobInfo, command.allowLoad(), command.getReplicaAlloc(),
                    command.getTimeoutMs(), command.getMetaVersion(), command.reserveReplica(),
                    command.reserveDynamicPartitionEnable(), command.isBeingSynced(), command.isCleanTables(),
                    command.isCleanPartitions(), command.isAtomicRestore(), command.isForceReplace(),
                    env, repository.getId(), command.getStorageVaultName());
            } else {
                restoreJob = new RestoreJob(command.getLabel(), command.getBackupTimestamp(),
                    db.getId(), db.getFullName(), jobInfo, command.allowLoad(), command.getReplicaAlloc(),
                    command.getTimeoutMs(), command.getMetaVersion(), command.reserveReplica(),
                    command.reserveColocate(), command.reserveDynamicPartitionEnable(), command.isBeingSynced(),
                    command.isCleanTables(), command.isCleanPartitions(), command.isAtomicRestore(),
                    command.isForceReplace(), env, repository.getId());
            }
        }

        env.getEditLog().logRestoreJob(restoreJob);

        // must put to dbIdToBackupOrRestoreJob after edit log, otherwise the state of job may be changed.
        addBackupOrRestoreJob(db.getId(), restoreJob);
        LOG.info("finished to submit restore job: {}", restoreJob);
    }

    private void addBackupOrRestoreJob(long dbId, AbstractJob job) {
        // If there are too many backup/restore jobs, it may cause OOM.  If the job num option is set to 0,
        // skip all backup/restore jobs.
        if (Config.max_backup_restore_job_num_per_db <= 0) {
            return;
        }

        List<String> removedLabels = Lists.newArrayList();
        jobLock.lock();
        try {
            Deque<AbstractJob> jobs = dbIdToBackupOrRestoreJobs.computeIfAbsent(dbId, k -> Lists.newLinkedList());
            while (jobs.size() >= Config.max_backup_restore_job_num_per_db) {
                AbstractJob removedJob = jobs.removeFirst();
                if (removedJob instanceof BackupJob && ((BackupJob) removedJob).isLocalSnapshot()) {
                    removedLabels.add(removedJob.getLabel());
                }
            }
            AbstractJob lastJob = jobs.peekLast();

            // Remove duplicate jobs and keep only the latest status
            // Otherwise, the tasks that have been successfully executed will be repeated when replaying edit log.
            if (lastJob != null && (lastJob.isPending() || lastJob.getJobId() == job.getJobId())) {
                jobs.removeLast();
            }
            jobs.addLast(job);
        } finally {
            jobLock.unlock();
        }

        if (job.isFinished() && job instanceof BackupJob) {
            // Save snapshot to local repo, when reload backupHandler from image.
            BackupJob backupJob = (BackupJob) job;
            if (backupJob.isLocalSnapshot()) {
                addSnapshot(backupJob.getLabel(), backupJob);
            }
        }
        for (String label : removedLabels) {
            removeSnapshot(label);
        }
    }

    private List<AbstractJob> getAllCurrentJobs() {
        jobLock.lock();
        try {
            return dbIdToBackupOrRestoreJobs.values().stream().filter(CollectionUtils::isNotEmpty)
                    .map(Deque::getLast).collect(Collectors.toList());
        } finally {
            jobLock.unlock();
        }
    }

    private AbstractJob getCurrentJob(long dbId) {
        jobLock.lock();
        try {
            Deque<AbstractJob> jobs = dbIdToBackupOrRestoreJobs.getOrDefault(dbId, Lists.newLinkedList());
            return jobs.isEmpty() ? null : jobs.getLast();
        } finally {
            jobLock.unlock();
        }
    }

    private void checkAndFilterRestoreObjsExistInSnapshot(BackupJobInfo jobInfo,
                                                          RestoreCommand command)
            throws DdlException {

        // case1: all table in job info
        if (command.getTableRefInfos().isEmpty()) {
            return;
        }

        // case2: exclude table ref
        if (command.isExclude()) {
            for (TableRefInfo tableRefInfo : command.getTableRefInfos()) {
                String tblName = tableRefInfo.getTableNameInfo().getTbl();
                TableType tableType = jobInfo.getTypeByTblName(tblName);
                if (tableType == null) {
                    LOG.info("Ignore error : exclude table " + tblName + " does not exist in snapshot "
                            + jobInfo.name);
                    continue;
                }
                if (tableRefInfo.hasAlias()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "The table alias in exclude clause does not make sense");
                }
                jobInfo.removeTable(tableRefInfo, tableType);
            }
            return;
        }

        // case3: include table ref
        Set<String> olapTableNames = Sets.newHashSet();
        Set<String> viewNames = Sets.newHashSet();
        Set<String> odbcTableNames = Sets.newHashSet();
        for (TableRefInfo tableRefInfo : command.getTableRefInfos()) {
            String tblName = tableRefInfo.getTableNameInfo().getTbl();
            TableType tableType = jobInfo.getTypeByTblName(tblName);
            if (tableType == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Table " + tblName + " does not exist in snapshot " + jobInfo.name);
            }
            switch (tableType) {
                case OLAP:
                    checkAndFilterRestoreOlapTableExistInSnapshot(jobInfo.backupOlapTableObjects, tableRefInfo);
                    olapTableNames.add(tblName);
                    break;
                case VIEW:
                    viewNames.add(tblName);
                    break;
                case ODBC:
                    odbcTableNames.add(tblName);
                    break;
                default:
                    break;
            }

            // set alias
            if (tableRefInfo.hasAlias()) {
                jobInfo.setAlias(tblName, tableRefInfo.getTableAlias());
            }
        }
        jobInfo.retainOlapTables(olapTableNames);
        jobInfo.retainView(viewNames);
        jobInfo.retainOdbcTables(odbcTableNames);
    }

    public void checkAndFilterRestoreOlapTableExistInSnapshot(Map<String, BackupOlapTableInfo> backupOlapTableInfoMap,
                                                              TableRefInfo tableRefInfo) throws DdlException {
        String tblName = tableRefInfo.getTableNameInfo().getTbl();
        BackupOlapTableInfo tblInfo = backupOlapTableInfoMap.get(tblName);
        PartitionNamesInfo partitionNamesInfo = tableRefInfo.getPartitionNamesInfo();
        if (partitionNamesInfo != null) {
            if (partitionNamesInfo.isTemp()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Do not support restoring temporary partitions in table " + tblName);
            }
            // check the selected partitions
            for (String partName : partitionNamesInfo.getPartitionNames()) {
                if (!tblInfo.containsPart(partName)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Partition " + partName + " of table " + tblName
                            + " does not exist in snapshot");
                }
            }
        }
        // only retain restore partitions
        tblInfo.retainPartitions(partitionNamesInfo == null ? null : partitionNamesInfo.getPartitionNames());
    }

    public void checkAndFilterRestoreOlapTableExistInSnapshot(Map<String, BackupOlapTableInfo> backupOlapTableInfoMap,
                                                              TableRef tableRef) throws DdlException {
        String tblName = tableRef.getName().getTbl();
        BackupOlapTableInfo tblInfo = backupOlapTableInfoMap.get(tblName);
        PartitionNames partitionNames = tableRef.getPartitionNames();
        if (partitionNames != null) {
            if (partitionNames.isTemp()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "Do not support restoring temporary partitions in table " + tblName);
            }
            // check the selected partitions
            for (String partName : partitionNames.getPartitionNames()) {
                if (!tblInfo.containsPart(partName)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                            "Partition " + partName + " of table " + tblName
                                    + " does not exist in snapshot");
                }
            }
        }
        // only retain restore partitions
        tblInfo.retainPartitions(partitionNames == null ? null : partitionNames.getPartitionNames());
    }

    public void cancel(CancelBackupCommand command) throws DdlException {
        String dbName = command.getDbName();
        Database db = env.getInternalCatalog().getDbOrDdlException(dbName);
        AbstractJob job = getCurrentJob(db.getId());
        if (job == null || (job instanceof BackupJob && command.isRestore())
                || (job instanceof RestoreJob && !command.isRestore())) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "No "
                    + (command.isRestore() ? "restore" : "backup" + " job")
                    + " is currently running");
        }

        Status status = job.cancel();
        if (!status.ok()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR, "Failed to cancel job: " + status.getErrMsg());
        }

        LOG.info("finished to cancel {} job: {}", (command.isRestore() ? "restore" : "backup"), job);
    }

    public boolean handleFinishedSnapshotTask(SnapshotTask task, TFinishTaskRequest request) {
        AbstractJob job = getCurrentJob(task.getDbId());
        if (job == null) {
            LOG.warn("failed to find backup or restore job for task: {}", task);
            // return true to remove this task from AgentTaskQueue
            return true;
        }

        if (job.getJobId() != task.getJobId()) {
            LOG.warn("invalid snapshot task: {}, job id: {}, task job id: {}", task, job.getJobId(), task.getJobId());
            // return true to remove this task from AgentTaskQueue
            return true;
        }

        if (job instanceof BackupJob) {
            if (task.isRestoreTask()) {
                LOG.warn("expect finding restore job, but get backup job {} for task: {}", job, task);
                // return true to remove this task from AgentTaskQueue
                return true;
            }

            return ((BackupJob) job).finishTabletSnapshotTask(task, request);
        } else {
            if (!task.isRestoreTask()) {
                LOG.warn("expect finding backup job, but get restore job {} for task: {}", job, task);
                // return true to remove this task from AgentTaskQueue
                return true;
            }
            return ((RestoreJob) job).finishTabletSnapshotTask(task, request);
        }
    }

    public boolean handleFinishedSnapshotUploadTask(UploadTask task, TFinishTaskRequest request) {
        AbstractJob job = getCurrentJob(task.getDbId());
        if (job == null || (job instanceof RestoreJob)) {
            LOG.info("invalid upload task: {}, no backup job is found. db id: {}", task, task.getDbId());
            return false;
        }
        BackupJob backupJob = (BackupJob) job;
        if (backupJob.getJobId() != task.getJobId() || backupJob.getState() != BackupJobState.UPLOADING) {
            LOG.info("invalid upload task: {}, job id: {}, job state: {}",
                     task, backupJob.getJobId(), backupJob.getState().name());
            return false;
        }
        return backupJob.finishSnapshotUploadTask(task, request);
    }

    public boolean handleDownloadSnapshotTask(DownloadTask task, TFinishTaskRequest request) {
        AbstractJob job = getCurrentJob(task.getDbId());
        if (!(job instanceof RestoreJob)) {
            LOG.warn("failed to find restore job for task: {}", task);
            // return true to remove this task from AgentTaskQueue
            return true;
        }

        if (job.getJobId() != task.getJobId()) {
            LOG.warn("invalid download task: {}, job id: {}, task job id: {}", task, job.getJobId(), task.getJobId());
            // return true to remove this task from AgentTaskQueue
            return true;
        }

        return ((RestoreJob) job).finishTabletDownloadTask(task, request);
    }

    public boolean handleDirMoveTask(DirMoveTask task, TFinishTaskRequest request) {
        AbstractJob job = getCurrentJob(task.getDbId());
        if (!(job instanceof RestoreJob)) {
            LOG.warn("failed to find restore job for task: {}", task);
            // return true to remove this task from AgentTaskQueue
            return true;
        }

        if (job.getJobId() != task.getJobId()) {
            LOG.warn("invalid dir move task: {}, job id: {}, task job id: {}", task, job.getJobId(), task.getJobId());
            // return true to remove this task from AgentTaskQueue
            return true;
        }

        return ((RestoreJob) job).finishDirMoveTask(task, request);
    }

    public void replayAddJob(AbstractJob job) {
        LOG.info("replay backup/restore job: {}", job);

        if (job.isCancelled()) {
            AbstractJob existingJob = getCurrentJob(job.getDbId());
            if (existingJob == null || existingJob.isDone()) {
                LOG.error("invalid existing job: {}. current replay job is: {}",
                        existingJob, job);
                return;
            }
            existingJob.setEnv(env);
            existingJob.replayCancel();
        } else if (!job.isPending()) {
            AbstractJob existingJob = getCurrentJob(job.getDbId());
            if (existingJob == null || existingJob.isDone()) {
                LOG.error("invalid existing job: {}. current replay job is: {}",
                        existingJob, job);
                return;
            }
            // We use replayed job, not the existing job, to do the replayRun().
            // Because if we use the existing job to run again,
            // for example: In restore job, PENDING will transfer to SNAPSHOTING, not DOWNLOAD.
            job.setEnv(env);
            job.replayRun();
        }

        addBackupOrRestoreJob(job.getDbId(), job);
    }

    public boolean report(TTaskType type, long jobId, long taskId, int finishedNum, int totalNum) {
        for (AbstractJob job : getAllCurrentJobs()) {
            if (job.getType() == JobType.BACKUP) {
                if (!job.isDone() && job.getJobId() == jobId && type == TTaskType.UPLOAD) {
                    job.taskProgress.put(taskId, Pair.of(finishedNum, totalNum));
                    return true;
                }
            } else if (job.getType() == JobType.RESTORE) {
                if (!job.isDone() && job.getJobId() == jobId && type == TTaskType.DOWNLOAD) {
                    job.taskProgress.put(taskId, Pair.of(finishedNum, totalNum));
                    return true;
                }
            }
        }
        return false;
    }

    public void addSnapshot(String labelName, BackupJob backupJob) {
        assert backupJob.isFinished();

        LOG.info("add snapshot {} to local repo", labelName);
        localSnapshotsLock.writeLock().lock();
        try {
            localSnapshots.put(labelName, backupJob);
        } finally {
            localSnapshotsLock.writeLock().unlock();
        }
    }

    public void removeSnapshot(String labelName) {
        LOG.info("remove snapshot {} from local repo", labelName);
        localSnapshotsLock.writeLock().lock();
        try {
            localSnapshots.remove(labelName);
        } finally {
            localSnapshotsLock.writeLock().unlock();
        }
    }

    public Snapshot getSnapshot(String labelName) {
        BackupJob backupJob;
        localSnapshotsLock.readLock().lock();
        try {
            backupJob = localSnapshots.get(labelName);
        } finally {
            localSnapshotsLock.readLock().unlock();
        }

        if (backupJob == null) {
            return null;
        }

        return backupJob.getSnapshot();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        repoMgr.write(out);

        List<AbstractJob> jobs = dbIdToBackupOrRestoreJobs.values()
                .stream().flatMap(Deque::stream).collect(Collectors.toList());
        out.writeInt(jobs.size());
        for (AbstractJob job : jobs) {
            job.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        repoMgr = RepositoryMgr.read(in);

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            AbstractJob job = AbstractJob.read(in);
            addBackupOrRestoreJob(job.getDbId(), job);
        }
    }
}
