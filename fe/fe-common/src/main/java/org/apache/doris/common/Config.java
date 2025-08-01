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

package org.apache.doris.common;

import java.io.File;

public class Config extends ConfigBase {

    @ConfField(description = {"用户自定义配置文件的路径，用于存放 fe_custom.conf。该文件中的配置会覆盖 fe.conf 中的配置",
            "The path of the user-defined configuration file, used to store fe_custom.conf. "
                    + "The configuration in this file will override the configuration in fe.conf"})
    public static String custom_config_dir = EnvUtils.getDorisHome() + "/conf";

    @ConfField(description = {"fe.log 和 fe.audit.log 的最大文件大小。超过这个大小后，日志文件会被切分",
            "The maximum file size of fe.log and fe.audit.log. After exceeding this size, the log file will be split"})
    public static int log_roll_size_mb = 1024; // 1 GB

    /**
     * sys_log_dir:
     *      This specifies FE log dir. FE will produces 2 log files:
     *      fe.log:      all logs of FE process.
     *      fe.warn.log  all WARN and ERROR log of FE process.
     *
     * sys_log_level:
     *      INFO, WARN, ERROR, FATAL
     *
     * sys_log_roll_num:
     *      Maximal FE log files to be kept within an sys_log_roll_interval.
     *      default is 10, which means there will be at most 10 log files in a day
     *
     * sys_log_verbose_modules:
     *      Verbose modules. VERBOSE level is implemented by log4j DEBUG level.
     *      eg:
     *          sys_log_verbose_modules = org.apache.doris.catalog
     *      This will only print debug log of files in package org.apache.doris.catalog and all its sub packages.
     *
     * sys_log_roll_interval:
     *      DAY:  log suffix is yyyyMMdd
     *      HOUR: log suffix is yyyyMMddHH
     *
     * sys_log_delete_age:
     *      default is 7 days, if log's last modify time is 7 days ago, it will be deleted.
     *      support format:
     *          7d      7 days
     *          10h     10 hours
     *          60m     60 mins
     *          120s    120 seconds
     *
     * sys_log_enable_compress:
     *      default is false. if true, will compress fe.log & fe.warn.log by gzip
     */
    @Deprecated // use env var LOG_DIR instead
    @ConfField(description = {"FE 日志文件的存放路径，用于存放 fe.log。",
            "The path of the FE log file, used to store fe.log"})
    public static String sys_log_dir = "";

    @ConfField(description = {"FE 日志的级别", "The level of FE log"}, options = {"INFO", "WARN", "ERROR", "FATAL"})
    public static String sys_log_level = "INFO";

    @ConfField(description = {"FE 日志的输出模式，其中 NORMAL 模式是日志同步输出且包含位置信息；ASYNC 模式为默认模式，日志异步输出"
            + "且包含位置信息；BRIEF 是日志异步输出但不包含位置信息，三种日志输出模式的性能依次递增",
            "The output mode of FE log. NORMAL mode is synchronous output with location information; "
                    + "ASYNC mode is the default mode, asynchronous output with location information; "
                    + "BRIEF is asynchronous output without location information. "
                    + "The performance of the three log output modes increases in turn"},
            options = {"NORMAL", "ASYNC", "BRIEF"})
    public static String sys_log_mode = "ASYNC";

    @ConfField(description = {"FE 在 sys_log_roll_interval （日志滚动间隔）内允许保留的最大日志文件数。"
            + "默认值为 10，意味着在每个日志滚动周期内，系统最多会保留 10 个日志文件。",
            "This parameter defines the maximum number of FE log files that can be retained within the "
            + "sys_log_roll_interval (log roll interval). The default value is 10, which means the system"
            + " will keep up to 10 log files during each log roll interval."})
    public static int sys_log_roll_num = 10;

    @ConfField(description = {
            "Verbose 模块。VERBOSE 级别的日志是通过 log4j 的 DEBUG 级别实现的。"
                    + "如设置为 `org.apache.doris.catalog`，则会打印这个 package 下的类的 DEBUG 日志。",
            "Verbose module. The VERBOSE level log is implemented by the DEBUG level of log4j. "
                    + "If set to `org.apache.doris.catalog`, "
                    + "the DEBUG log of the class under this package will be printed."})
    public static String[] sys_log_verbose_modules = {};
    @ConfField(description = {"FE 日志文件的切分周期", "The split cycle of the FE log file"}, options = {"DAY", "HOUR"})
    public static String sys_log_roll_interval = "DAY";
    @ConfField(description = {
            "FE 日志文件的最大存活时间。超过这个时间后，日志文件会被删除。支持的格式包括：7d, 10h, 60m, 120s",
            "The maximum survival time of the FE log file. After exceeding this time, the log file will be deleted. "
                    + "Supported formats include: 7d, 10h, 60m, 120s"})
    public static String sys_log_delete_age = "7d";
    @ConfField(description = {"是否压缩 FE 的历史日志", "enable compression for FE log file"})
    public static boolean sys_log_enable_compress = false;

    @ConfField(description = {"FE 审计日志文件的存放路径，用于存放 fe.audit.log。",
            "The path of the FE audit log file, used to store fe.audit.log"})
    public static String audit_log_dir = System.getenv("LOG_DIR");
    @ConfField(description = {"FE 审计日志文件的最大数量。超过这个数量后，最老的日志文件会被删除",
            "The maximum number of FE audit log files. "
                    + "After exceeding this number, the oldest log file will be deleted"})
    public static int audit_log_roll_num = 90;
    @ConfField(description = {"FE 审计日志文件的种类", "The type of FE audit log file"},
            options = {"slow_query", "query", "load", "stream_load"})
    public static String[] audit_log_modules = {"slow_query", "query", "load", "stream_load"};
    @ConfField(mutable = true, description = {"慢查询的阈值，单位为毫秒。如果一个查询的响应时间超过这个阈值，"
            + "则会被记录在 audit log 中。",
            "The threshold of slow query, in milliseconds. "
                    + "If the response time of a query exceeds this threshold, it will be recorded in audit log."})
    public static long qe_slow_log_ms = 5000;
    @ConfField(description = {"FE 审计日志文件的切分周期", "The split cycle of the FE audit log file"},
            options = {"DAY", "HOUR"})
    public static String audit_log_roll_interval = "DAY";
    @ConfField(description = {
            "FE 审计日志文件的最大存活时间。超过这个时间后，日志文件会被删除。支持的格式包括：7d, 10h, 60m, 120s",
            "The maximum survival time of the FE audit log file. "
                    + "After exceeding this time, the log file will be deleted. "
                    + "Supported formats include: 7d, 10h, 60m, 120s"})
    public static String audit_log_delete_age = "30d";
    @ConfField(description = {"是否压缩 FE 的 Audit 日志", "enable compression for FE audit log file"})
    public static boolean audit_log_enable_compress = false;

    @ConfField(description = {"是否使用文件记录日志。当使用 --console 启动 FE 时，全部日志同时写入到标准输出和文件。"
            + "如果关闭这个选项，不再使用文件记录日志。",
            "Whether to use file to record log. When starting FE with --console, "
                    + "all logs will be written to both standard output and file. "
                    + "Close this option will no longer use file to record log."})
    public static boolean enable_file_logger = true;

    @ConfField(mutable = false, masterOnly = false,
            description = {"是否检查table锁泄漏", "Whether to check table lock leaky"})
    public static boolean check_table_lock_leaky = false;

    @ConfField(mutable = true, masterOnly = false,
            description = {"PreparedStatement stmtId 起始位置，仅用于测试",
                    "PreparedStatement stmtId starting position, used for testing onl"})
    public static long prepared_stmt_start_id = -1;

    @ConfField(description = {"插件的安装目录", "The installation directory of the plugin"})
    public static String plugin_dir =  EnvUtils.getDorisHome() + "/plugins";

    @ConfField(mutable = true, masterOnly = true, description = {"是否启用插件", "Whether to enable the plugin"})
    public static boolean plugin_enable = true;

    @ConfField(description = {
            "JDBC 驱动的存放路径。在创建 JDBC Catalog 时，如果指定的驱动文件路径不是绝对路径，则会在这个目录下寻找",
            "The path to save jdbc drivers. When creating JDBC Catalog,"
                    + "if the specified driver file path is not an absolute path, Doris will find jars from this path"})
    public static String jdbc_drivers_dir = EnvUtils.getDorisHome() + "/plugins/jdbc_drivers";

    @ConfField(description = {"JDBC 驱动的安全路径。在创建 JDBC Catalog 时，允许使用的文件或者网络路径，可配置多个，使用分号分隔"
            + "默认为 * 表示全部允许，如果设置为空也表示全部允许",
            "The safe path of the JDBC driver. When creating a JDBC Catalog,"
                    + "you can configure multiple files or network paths that are allowed to be used,"
                    + "separated by semicolons"
                    + "The default is * to allow all, if set to empty, also means to allow all"})
    public static String jdbc_driver_secure_path = "*";

    @ConfField(description = {"MySQL Jdbc Catalog mysql 不支持下推的函数",
            "MySQL Jdbc Catalog mysql does not support pushdown functions"})
    public static String[] jdbc_mysql_unsupported_pushdown_functions = {"date_trunc", "money_format", "negative"};

    @ConfField(mutable = true, masterOnly = true, description = {"强制 SQLServer Jdbc Catalog 加密为 false",
            "Force SQLServer Jdbc Catalog encrypt to false"})
    public static boolean force_sqlserver_jdbc_encrypt_false = false;

    @ConfField(mutable = true, masterOnly = true, description = {"broker load 时，单个节点上 load 执行计划的默认并行度",
            "The default parallelism of the load execution plan on a single node when the broker load is submitted"})
    public static int default_load_parallelism = 8;

    @ConfField(mutable = true, masterOnly = true, description = {
            "已完成或取消的导入作业信息的 label 会在这个时间后被删除。被删除的 label 可以被重用。",
            "Labels of finished or cancelled load jobs will be removed after this time"
                    + "The removed labels can be reused."})
    public static int label_keep_max_second = 3 * 24 * 3600; // 3 days

    @ConfField(mutable = true, masterOnly = true, description = {
            "针对一些高频的导入作业，比如 INSERT, STREAMING LOAD, ROUTINE_LOAD_TASK, DELETE"
                    + "如果导入作业或者任务已经完成，且超过这个时间后，会被删除。被删除的作业或者任务可以被重用。",
            "For some high frequency load jobs such as INSERT, STREAMING LOAD, ROUTINE_LOAD_TASK, DELETE"
                    + "Remove the finished job or task if expired. The removed job or task can be reused."})
    public static int streaming_label_keep_max_second = 43200; // 12 hour

    @ConfField(mutable = true, masterOnly = true, description = {
            "针对 ALTER, EXPORT 作业，如果作业已经完成，且超过这个时间后，会被删除。",
            "For ALTER, EXPORT jobs, remove the finished job if expired."})
    public static int history_job_keep_max_second = 7 * 24 * 3600; // 7 days

    @ConfField(mutable = true, masterOnly = true, description = {
            "针对 EXPORT 作业，如果系统内 EXPORT 作业数量超过这个值，则会删除最老的记录。",
            "For EXPORT jobs, If the number of EXPORT jobs in the system exceeds this value, "
                    + "the oldest records will be deleted."})
    public static int max_export_history_job_num = 1000;

    @ConfField(description = {"事务的清理周期，单位为秒。每个周期内，将会清理已经结束的并且过期的历史事务信息",
            "The clean interval of transaction, in seconds. "
                    + "In each cycle, the expired history transaction will be cleaned"})
    public static int transaction_clean_interval_second = 30;

    @ConfField(description = {"导入作业的清理周期，单位为秒。每个周期内，将会清理已经结束的并且过期的导入作业",
            "The clean interval of load job, in seconds. "
                    + "In each cycle, the expired history load job will be cleaned"})
    public static int label_clean_interval_second = 1 * 3600; // 1 hours

    @ConfField(description = {"元数据的存储目录", "The directory to save Doris meta data"})
    public static String meta_dir =  EnvUtils.getDorisHome() + "/doris-meta";

    @ConfField(description = {"临时文件的存储目录", "The directory to save Doris temp data"})
    public static String tmp_dir =  EnvUtils.getDorisHome() + "/temp_dir";

    @ConfField(description = {"元数据日志的存储类型。BDB: 日志存储在 BDBJE 中。LOCAL：日志存储在本地文件中（仅用于测试）",
            "The storage type of the metadata log. BDB: Logs are stored in BDBJE. "
                    + "LOCAL: logs are stored in a local file (for testing only)"}, options = {"BDB", "LOCAL"})
    public static String edit_log_type = "bdb";

    @ConfField(description = {"BDBJE 的端口号", "The port of BDBJE"})
    public static int edit_log_port = 9010;

    @ConfField(mutable = true, masterOnly = true, description = {
            "BDBJE 的日志滚动大小。当日志条目数超过这个值后，会触发日志滚动",
            "The log roll size of BDBJE. When the number of log entries exceeds this value, the log will be rolled"})
    public static int edit_log_roll_num = 50000;

    @ConfField(mutable = true, masterOnly = true, description = {
            "批量 BDBJE 日志包含的最大条目数", "The max number of log entries for batching BDBJE"})
    public static int batch_edit_log_max_item_num = 100;

    @ConfField(mutable = true, masterOnly = true, description = {
            "批量 BDBJE 日志包含的最大长度", "The max size for batching BDBJE"})
    public static long batch_edit_log_max_byte_size = 640 * 1024L;

    @ConfField(mutable = true, masterOnly = true, description = {
            "连续写多批 BDBJE 日志后的停顿时间", "The sleep time after writting multiple batching BDBJE continuously"})
    public static long batch_edit_log_rest_time_ms = 10;

    @ConfField(mutable = true, masterOnly = true, description = {
            "连续写多批 BDBJE 日志后需要短暂停顿。这里最大的连写次数。",
            "After writting multiple batching BDBJE continuously, need a short rest. "
                    + "Indicates the writting count before a rest"})
    public static long batch_edit_log_continuous_count_for_rest = 1000;

    @ConfField(description = {
            "攒批写 EditLog。", "Batch EditLog writing"})
    public static boolean enable_batch_editlog = true;

    @ConfField(description = {"元数据同步的容忍延迟时间，单位为秒。如果元数据的延迟超过这个值，非主 FE 会停止提供服务",
            "The toleration delay time of meta data synchronization, in seconds. "
                    + "If the delay of meta data exceeds this value, non-master FE will stop offering service"})
    public static int meta_delay_toleration_second = 300;    // 5 min

    @ConfField(description = {"元数据日志的写同步策略。如果仅部署一个 Follower FE，"
            + "则推荐设置为 `SYNC`，如果有多个 Follower FE，则可以设置为 `WRITE_NO_SYNC`。"
            + "可参阅：http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.SyncPolicy.html",
            "The sync policy of meta data log. If you only deploy one Follower FE, "
                    + "set this to `SYNC`. If you deploy more than 3 Follower FE, "
                    + "you can set this and the following `replica_sync_policy` to `WRITE_NO_SYNC`. "
                    + "See: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.SyncPolicy.html"},
            options = {"SYNC", "NO_SYNC", "WRITE_NO_SYNC"})
    public static String master_sync_policy = "SYNC"; // SYNC, NO_SYNC, WRITE_NO_SYNC

    @ConfField(description = {"同 `master_sync_policy`", "Same as `master_sync_policy`"},
            options = {"SYNC", "NO_SYNC", "WRITE_NO_SYNC"})
    public static String replica_sync_policy = "SYNC"; // SYNC, NO_SYNC, WRITE_NO_SYNC

    @ConfField(description = {"BDBJE 节点间同步策略，"
            + "可参阅：http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.ReplicaAckPolicy.html",
            "The replica ack policy of bdbje. "
                    + "See: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.ReplicaAckPolicy.html"},
            options = {"ALL", "NONE", "SIMPLE_MAJORITY"})
    public static String replica_ack_policy = "SIMPLE_MAJORITY"; // ALL, NONE, SIMPLE_MAJORITY

    @ConfField(description = {"BDBJE 主从节点间心跳超时时间，单位为秒。默认值为 30 秒，与 BDBJE 的默认值相同。"
            + "如果网络不稳定，或者 Java GC 经常导致长时间的暂停，可以适当增大这个值，减少误报超时的概率",
            "The heartbeat timeout of bdbje between master and follower, in seconds. "
                    + "The default is 30 seconds, which is same as default value in bdbje. "
                    + "If the network is experiencing transient problems, "
                    + "of some unexpected long java GC annoying you, "
                    + "you can try to increase this value to decrease the chances of false timeouts"})
    public static int bdbje_heartbeat_timeout_second = 30;

    @ConfField(description = {"BDBJE 操作的锁超时时间，单位为秒。如果 FE 的 WARN 日志中出现大量的 LockTimeoutException，"
            + "可以适当增大这个值",
            "The lock timeout of bdbje operation, in seconds. "
                    + "If there are many LockTimeoutException in FE WARN log, you can try to increase this value"})
    public static int bdbje_lock_timeout_second = 5;

    @ConfField(description = {"BDBJE 主从节点间同步的超时时间，单位为秒。如果出现大量的 ReplicaWriteException，"
            + "可以适当增大这个值",
            "The replica ack timeout of bdbje between master and follower, in seconds. "
                    + "If there are many ReplicaWriteException in FE WARN log, you can try to increase this value"})
    public static int bdbje_replica_ack_timeout_second = 10;

    @ConfField(description = {"在HA模式下，BDBJE 中保留的预留空间字节数的期望上限。非 HA 模式下无效",
            "The desired upper limit on the number of bytes of reserved space to retain "
                    + "in a replicated JE Environment. "
                    + "This parameter is ignored in a non-replicated JE Environment."})
    public static long bdbje_reserved_disk_bytes = 1 * 1024 * 1024 * 1024; // 1G

    @ConfField(description = {"BDBJE 所需的空闲磁盘空间大小。如果空闲磁盘空间小于这个值，则BDBJE将无法写入。",
            "Amount of free disk space required by BDBJE. "
                    + "If the free disk space is less than this value, BDBJE will not be able to write."})
    public static long bdbje_free_disk_bytes = 1 * 1024 * 1024 * 1024; // 1G

    @ConfField(description = {"BDBJE Cache 内存大小， 最小值为 96KB。", "Amount of memory used by by BDBJE as cache. "})
    public static long bdbje_cache_size_bytes = 10 * 1024 * 1024; // 10 MB

    @ConfField(description = {"BDBJE Message 大小限制。", "Max message size of BDBJE. "})
    public static long bdbje_max_message_size_bytes = Integer.MAX_VALUE; // 2 GB

    @ConfField(masterOnly = true, description = {"心跳线程池的线程数",
            "Num of thread to handle heartbeat events"})
    public static int heartbeat_mgr_threads_num = 8;

    @ConfField(masterOnly = true, description = {"心跳线程池的队列大小",
            "Queue size to store heartbeat task in heartbeat_mgr"})
    public static int heartbeat_mgr_blocking_queue_size = 1024;

    @ConfField(masterOnly = true, description = {"TabletStatMgr线程数",
            "Num of thread to update tablet stat"})
    public static int tablet_stat_mgr_threads_num = -1;

    @ConfField(masterOnly = true, description = {"Agent任务线程池的线程数",
            "Num of thread to handle agent task in agent task thread-pool"})
    public static int max_agent_task_threads_num = 4096;

    @ConfField(description = {"BDBJE 重加入集群时，最多回滚的事务数。如果回滚的事务数超过这个值，"
            + "则 BDBJE 将无法重加入集群，需要手动清理 BDBJE 的数据。",
            "The max txn number which bdbje can rollback when trying to rejoin the group. "
                    + "If the number of rollback txn is larger than this value, "
                    + "bdbje will not be able to rejoin the group, and you need to clean up bdbje data manually."})
    public static int txn_rollback_limit = 100;

    @ConfField(description = {"优先使用的网络地址，如果 FE 有多个网络地址，"
            + "可以通过这个配置来指定优先使用的网络地址。"
            + "这是一个分号分隔的列表，每个元素是一个 CIDR 表示的网络地址",
            "The preferred network address. If FE has multiple network addresses, "
                    + "this configuration can be used to specify the preferred network address. "
                    + "This is a semicolon-separated list, "
                    + "each element is a CIDR representation of the network address"})
    public static String priority_networks = "";

    @ConfField(mutable = true, description = {"是否忽略元数据延迟，如果 FE 的元数据延迟超过这个阈值，"
            + "则非 Master FE 仍然提供读服务。这个配置可以用于当 Master FE 因为某些原因停止了较长时间，"
            + "但是仍然希望非 Master FE 可以提供读服务。",
            "If true, non-master FE will ignore the meta data delay gap between Master FE and its self, "
                    + "even if the metadata delay gap exceeds this threshold. "
                    + "Non-master FE will still offer read service. "
                    + "This is helpful when you try to stop the Master FE for a relatively long time for some reason, "
                    + "but still wish the non-master FE can offer read service."})
    public static boolean ignore_meta_check = false;

    @ConfField(description = {"非 Master FE 与 Master FE 的最大时钟偏差，单位为毫秒。"
            + "这个配置用于在非 Master FE 与 Master FE 之间建立 BDBJE 连接时检查时钟偏差，"
            + "如果时钟偏差超过这个阈值，则 BDBJE 连接会被放弃。",
            "The maximum clock skew between non-master FE to Master FE host, in milliseconds. "
                    + "This value is checked whenever a non-master FE establishes a connection to master FE via BDBJE. "
                    + "The connection is abandoned if the clock skew is larger than this value."})
    public static long max_bdbje_clock_delta_ms = 5000; // 5s

    @ConfField(mutable = true, description = {"是否启用所有 http 接口的认证",
            "Whether to enable all http interface authentication"}, varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_all_http_auth = false;

    @ConfField(description = {"FE http 端口，目前所有 FE 的 http 端口必须相同",
            "Fe http port, currently all FE's http port must be same"})
    public static int http_port = 8030;

    @ConfField(description = {"FE https 端口，目前所有 FE 的 https 端口必须相同",
            "Fe https port, currently all FE's https port must be same"})
    public static int https_port = 8050;

    @ConfField(description = {"FE https 服务的 key store 路径",
            "The key store path of FE https service"})
    public static String key_store_path =  EnvUtils.getDorisHome()
            + "/conf/ssl/doris_ssl_certificate.keystore";

    @ConfField(description = {"FE https 服务的 key store 密码",
            "The key store password of FE https service"})
    public static String key_store_password = "";

    @ConfField(description = {"FE https 服务的 key store 类型",
            "The key store type of FE https service"})
    public static String key_store_type = "JKS";

    @ConfField(description = {"FE https 服务的 key store 别名",
            "The key store alias of FE https service"})
    public static String key_store_alias = "doris_ssl_certificate";

    @ConfField(description = {"是否启用 https，如果启用，http 端口将不可用",
            "Whether to enable https, if enabled, http port will not be available"},
            varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_https = false;

    @ConfField(description = {"Jetty 的 acceptor 线程数。Jetty的线程架构模型很简单，分为三个线程池：acceptor、selector 和 worker。"
            + "acceptor 负责接受新的连接，然后交给 selector 处理HTTP报文协议的解包，最后由 worker 处理请求。"
            + "前两个线程池采用非阻塞模型，并且一个线程可以处理很多socket的读写，所以线程池的数量少。"
            + "对于大多数项目，只需要 1-2 个 acceptor 线程，2 到 4 个就足够了。Worker 的数量取决于应用的QPS和IO事件的比例。"
            + "越高QPS，或者IO占比越高，等待的线程越多，需要的线程总数越多。",
            "The number of acceptor threads for Jetty. Jetty's thread architecture model is very simple, "
                    + "divided into three thread pools: acceptor, selector and worker. "
                    + "The acceptor is responsible for accepting new connections, "
                    + "and then handing it over to the selector to process the unpacking of the HTTP message protocol, "
                    + "and finally the worker processes the request. "
                    + "The first two thread pools adopt a non-blocking model, "
                    + "and one thread can handle many socket reads and writes, "
                    + "so the number of thread pools is small. For most projects, "
                    + "only 1-2 acceptor threads are needed, 2 to 4 should be enough. "
                    + "The number of workers depends on the ratio of QPS and IO events of the application. "
                    + "The higher the QPS, or the higher the IO ratio, the more threads are waiting, "
                    + "and the more threads are required."})
    public static int jetty_server_acceptors = 2;
    @ConfField(description = {"Jetty 的 selector 线程数。", "The number of selector threads for Jetty."})
    public static int jetty_server_selectors = 4;
    @ConfField(description = {"Jetty 的 worker 线程数。0 表示使用默认线程池。",
            "The number of worker threads for Jetty. 0 means using the default thread pool."})
    public static int jetty_server_workers = 0;

    @ConfField(description = {"Jetty 的线程池的默认最小线程数。",
            "The default minimum number of threads for jetty."})
    public static int jetty_threadPool_minThreads = 20;
    @ConfField(description = {"Jetty 的线程池的默认最大线程数。",
            "The default maximum number of threads for jetty."})
    public static int jetty_threadPool_maxThreads = 400;

    @ConfField(description = {"Jetty 的最大 HTTP POST 大小，单位是字节，默认值是 100MB。",
            "The maximum HTTP POST size of Jetty, in bytes, the default value is 100MB."})
    public static int jetty_server_max_http_post_size = 100 * 1024 * 1024;

    @ConfField(description = {"Jetty 的最大 HTTP header 大小，单位是字节，默认值是 1MB。",
            "The maximum HTTP header size of Jetty, in bytes, the default value is 1MB."})
    public static int jetty_server_max_http_header_size = 1048576;

    @ConfField(description = {"是否禁用 mini load，默认禁用",
            "Whether to disable mini load, disabled by default"})
    public static boolean disable_mini_load = true;

    @ConfField(description = {"mysql nio server 的 backlog 数量。"
            + "如果调大这个值，则需同时调整 /proc/sys/net/core/somaxconn 的值",
            "The backlog number of mysql nio server. "
                    + "If you enlarge this value, you should enlarge the value in "
                    + "`/proc/sys/net/core/somaxconn` at the same time"})
    public static int mysql_nio_backlog_num = 1024;

    @ConfField(description = {"是否启用 mysql 连接中的 TCP keep alive，默认禁用",
            "Whether to enable TCP Keep-Alive for MySQL connections, disabled by default"})
    public static boolean mysql_nio_enable_keep_alive = false;

    @ConfField(description = {"thrift client 的连接超时时间，单位是毫秒。0 表示不设置超时时间。",
            "The connection timeout of thrift client, in milliseconds. 0 means no timeout."})
    public static int thrift_client_timeout_ms = 0;

    // The default value is inherited from org.apache.thrift.TConfiguration
    @ConfField(description = {"thrift server 接收请求大小的上限",
            "The maximum size of a (received) message of the thrift server, in bytes"})
    public static int thrift_max_message_size = 100 * 1024 * 1024;

    // The default value is inherited from org.apache.thrift.TConfiguration
    @ConfField(description = {"thrift server transport 接收的每帧数据大小的上限",
            "The limits of the size of one frame of thrift server transport"})
    public static int thrift_max_frame_size = 16384000;

    @ConfField(description = {"thrift server 的 backlog 数量。"
            + "如果调大这个值，则需同时调整 /proc/sys/net/core/somaxconn 的值",
            "The backlog number of thrift server. "
                    + "If you enlarge this value, you should enlarge the value in "
                    + "`/proc/sys/net/core/somaxconn` at the same time"})
    public static int thrift_backlog_num = 1024;

    @ConfField(description = {"FE thrift server 的端口号", "The port of FE thrift server"})
    public static int rpc_port = 9020;

    @ConfField(description = {"FE MySQL server 的端口号", "The port of FE MySQL server"})
    public static int query_port = 9030;

    @ConfField(description = {"FE Arrow-Flight-SQL server 的端口号", "The port of FE Arrow-Flight-SQL server"})
    public static int arrow_flight_sql_port = 8070;

    @ConfField(description = {"MySQL 服务的 IO 线程数", "The number of IO threads in MySQL service"})
    public static int mysql_service_io_threads_num = 4;

    @ConfField(description = {"MySQL 服务的最大任务线程数", "The max number of task threads in MySQL service"})
    public static int max_mysql_service_task_threads_num = 4096;

    @ConfField(description = {"BackendServiceProxy数量, 用于池化GRPC channel",
            "BackendServiceProxy pool size for pooling GRPC channels."})
    public static int backend_proxy_num = 48;

    @ConfField(description = {
            "集群 ID，用于内部认证。通常在集群第一次启动时，会随机生成一个 cluster id. 用户也可以手动指定。",
            "Cluster id used for internal authentication. Usually a random integer generated when master FE "
                    + "start at first time. You can also specify one."})
    public static int cluster_id = -1;

    @ConfField(description = {"集群 token，用于内部认证。",
            "Cluster token used for internal authentication."})
    public static String auth_token = "";

    @ConfField(mutable = true, masterOnly = true,
            description = {"创建单个 Replica 的最大超时时间，单位是秒。如果你要创建 m 个 tablet，每个 tablet 有 n 个 replica。"
                    + "则总的超时时间为 `m * n * tablet_create_timeout_second`",
                    "Maximal waiting time for creating a single replica, in seconds. "
                            + "eg. if you create a table with #m tablets and #n replicas for each tablet, "
                            + "the create table request will run at most "
                            + "(m * n * tablet_create_timeout_second) before timeout"})
    public static int tablet_create_timeout_second = 2;

    @ConfField(mutable = true, masterOnly = true, description = {"创建表的最小超时时间，单位是秒。",
            "Minimal waiting time for creating a table, in seconds."})
    public static int min_create_table_timeout_second = 30;

    @ConfField(mutable = true, masterOnly = true, description = {"创建表的最大超时时间，单位是秒。",
            "Maximal waiting time for creating a table, in seconds."})
    public static int max_create_table_timeout_second = 3600;

    @ConfField(mutable = true, masterOnly = true, description = {"导入 Publish 阶段的最大超时时间，单位是秒。",
            "Maximal waiting time for all publish version tasks of one transaction to be finished, in seconds."})
    public static int publish_version_timeout_second = 30; // 30 seconds

    @ConfField(mutable = true, masterOnly = true, description = {"导入 Publish 阶段的等待时间，单位是秒。超过此时间，"
            + "则只需每个tablet包含一个成功副本，则导入成功。值为 -1 时，表示无限等待。",
            "Waiting time for one transaction changing to \"at least one replica success\", in seconds."
            + "If time exceeds this, and for each tablet it has at least one replica publish successful, "
            + "then the load task will be successful." })
    public static int publish_wait_time_second = 300;

    @ConfField(mutable = true, masterOnly = true, description = {"导入 Publish 阶段是否检查正在做 Schema 变更的副本。"
            + "正常情况下，不要关闭此检查。除非在极端情况下出现导入和 Schema 变更出现互相等待死锁时才临时打开。",
            "Check the replicas which are doing schema change when publish transaction. Do not turn off this check "
            + " under normal circumstances. It's only temporarily skip check if publish version and schema change have"
            + " dead lock" })
    public static boolean publish_version_check_alter_replica = true;

    @ConfField(mutable = true, masterOnly = true, description = {"单个事务 publish 失败打日志间隔",
            "print log interval for publish transaction failed interval"})
    public static long publish_fail_log_interval_second = 5 * 60;

    @ConfField(mutable = true, masterOnly = true, description = {"一个 PUBLISH_VERSION 任务打印失败日志的次数上限",
            "the upper limit of failure logs of PUBLISH_VERSION task"})
    public static long publish_version_task_failed_log_threshold = 80;

    @ConfField(masterOnly = true, description = {"Publish 线程池的数目",
            "Num of thread to handle publish task"})
    public static int publish_thread_pool_num = 128;

    @ConfField(masterOnly = true, description = {"Publish 线程池的队列大小",
            "Queue size to store publish task in publish thread pool"})
    public static int publish_queue_size = 128;

    @ConfField(mutable = true, description = {"是否启用并行发布版本",
            "Whether to enable parallel publish version"})
    public static boolean enable_parallel_publish_version = true;

    @ConfField(mutable = true, masterOnly = true, description = {"提交事务的最大超时时间，单位是秒。"
            + "该参数仅用于事务型 insert 操作中。",
            "Maximal waiting time for all data inserted before one transaction to be committed, in seconds. "
                    + "This parameter is only used for transactional insert operation"})
    public static int commit_timeout_second = 30; // 30 seconds

    @ConfField(masterOnly = true, description = {"Publish 任务触发线程的执行间隔，单位是毫秒。",
            "The interval of publish task trigger thread, in milliseconds"})
    public static int publish_version_interval_ms = 10;

    @ConfField(description = {"thrift server 的最大 worker 线程数", "The max worker threads of thrift server"})
    public static int thrift_server_max_worker_threads = 4096;

    @ConfField(mutable = true, masterOnly = true, description = {"Delete 操作的最大超时时间，单位是秒。",
            "Maximal timeout for delete job, in seconds."})
    public static int delete_job_max_timeout_second = 300;

    @ConfField(mutable = true, masterOnly = true, description = {"Load 成功所需的最小写入副本数。",
            "Minimal number of write successful replicas for load job."})
    public static short min_load_replica_num = -1;

    @ConfField(description = {"load job 调度器的执行间隔，单位是秒。",
            "The interval of load job scheduler, in seconds."})
    public static int load_checker_interval_second = 5;

    @ConfField(description = {"ingestion load job 调度器的执行间隔，单位是秒。",
            "The interval of ingestion load job scheduler, in seconds."})
    public static int ingestion_load_checker_interval_second = 60;

    @ConfField(mutable = true, masterOnly = true, description = {"Broker load 的默认超时时间，单位是秒。",
            "Default timeout for broker load job, in seconds."})
    public static int broker_load_default_timeout_second = 14400; // 4 hour

    @ConfField(description = {"和 Broker 进程交互的 RPC 的超时时间，单位是毫秒。",
            "The timeout of RPC between FE and Broker, in milliseconds"})
    public static int broker_timeout_ms = 10000; // 10s

    @ConfField(description = {"主键高并发点查短路径超时时间。",
            "The timeout of RPC for high concurrenty short circuit query"})
    public static int point_query_timeout_ms = 10000; // 10s

    @ConfField(mutable = true, masterOnly = true, description = {"Insert load 的默认超时时间，单位是秒。",
            "Default timeout for insert load job, in seconds."})
    public static int insert_load_default_timeout_second = 14400; // 4 hour

    @ConfField(mutable = true, masterOnly = true, description = {"对mow表随机设置cluster keys，用于测试",
            "random set cluster keys for mow table for test"})
    public static boolean random_add_cluster_keys_for_mow = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "等内部攒批真正写入完成才返回；insert into和stream load默认开启攒批",
            "Wait for the internal batch to be written before returning; "
                    + "insert into and stream load use group commit by default."})
    public static boolean wait_internal_group_commit_finish = false;

    @ConfField(mutable = false, masterOnly = true, description = {"攒批的默认提交时间，单位是毫秒",
            "Default commit interval in ms for group commit"})
    public static int group_commit_interval_ms_default_value = 10000;

    @ConfField(mutable = false, masterOnly = true, description = {"攒批的默认提交数据量，单位是字节，默认128M",
            "Default commit data bytes for group commit"})
    public static int group_commit_data_bytes_default_value = 134217728;

    @ConfField(mutable = true, masterOnly = true, description = {
            "内部攒批的超时时间为table的group_commit_interval_ms的倍数",
            "The internal group commit timeout is the multiple of table's group_commit_interval_ms"})
    public static int group_commit_timeout_multipler = 10;

    @ConfField(mutable = true, masterOnly = true, description = {"Stream load 的默认超时时间，单位是秒。",
            "Default timeout for stream load job, in seconds."})
    public static int stream_load_default_timeout_second = 86400 * 3; // 3days

    @ConfField(mutable = true, masterOnly = true, description = {"Stream load 的默认预提交超时时间，单位是秒。",
            "Default pre-commit timeout for stream load job, in seconds."})
    public static int stream_load_default_precommit_timeout_second = 3600; // 3600s

    @ConfField(mutable = true, masterOnly = true, description = {"Stream Load 是否默认打开 memtable 前移",
            "Whether to enable memtable on sink node by default in stream load"})
    public static boolean stream_load_default_memtable_on_sink_node = false;

    @ConfField(mutable = true, masterOnly = true, description = {"Load 的最大超时时间，单位是秒。",
            "Maximal timeout for load job, in seconds."})
    public static int max_load_timeout_second = 259200; // 3days

    @ConfField(mutable = true, masterOnly = true, description = {"Stream load 的最大超时时间，单位是秒。",
            "Maximal timeout for stream load job, in seconds."})
    public static int max_stream_load_timeout_second = 259200; // 3days

    @ConfField(mutable = true, masterOnly = true, description = {"Load 的最小超时时间，单位是秒。",
            "Minimal timeout for load job, in seconds."})
    public static int min_load_timeout_second = 1; // 1s

    @ConfField(mutable = true, masterOnly = true, description = {"Ingestion load 的默认超时时间，单位是秒。",
            "Default timeout for ingestion load job, in seconds."})
    public static int ingestion_load_default_timeout_second = 86400; // 1 day

    @ConfField(mutable = true, masterOnly = true, description = {"Broker Load 的最大等待 job 数量。"
            + "这个值是一个期望值。在某些情况下，比如切换 master，当前等待的 job 数量可能会超过这个值。",
            "Maximal number of waiting jobs for Broker Load. This is a desired number. "
                    + "In some situation, such as switch the master, "
                    + "the current number is maybe more than this value."})
    public static int desired_max_waiting_jobs = 100;

    @ConfField(mutable = true, masterOnly = true, description = {"FE 从 BE 获取 Stream Load 作业信息的间隔。",
            "The interval of FE fetch stream load record from BE."})
    public static int fetch_stream_load_record_interval_second = 120;

    @ConfField(mutable = true, masterOnly = true, description = {"Stream load 的默认最大记录数。",
            "Default max number of recent stream load record that can be stored in memory."})
    public static int max_stream_load_record_size = 5000;

    @ConfField(mutable = true, masterOnly = true, description = {
            "是否禁用 show stream load 和 clear stream load 命令，以及是否清理内存中的 stream load 记录。",
            "Whether to disable show stream load and clear stream load records in memory."})
    public static boolean disable_show_stream_load = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "是否启用 stream load 和 broker load 的单副本写入。",
            "Whether to enable to write single replica for stream load and broker load."},
            varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_single_replica_load = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "对于 tablet 数量小于该数目的 DUPLICATE KEY 表，将不会启用 shuffle",
            "Shuffle won't be enabled for DUPLICATE KEY tables if its tablet num is lower than this number"},
            varType = VariableAnnotation.EXPERIMENTAL)
    public static int min_tablets_for_dup_table_shuffle = 64;

    @ConfField(mutable = true, masterOnly = true, description = {
            "单个数据库最大并发运行的事务数，包括 prepare 和 commit 事务。",
            "Maximum concurrent running txn num including prepare, commit txns under a single db.",
            "Txn manager will reject coming txns."})
    public static int max_running_txn_num_per_db = 10000;

    @ConfField(masterOnly = true, description = {"pending load task 执行线程数。这个配置可以限制当前等待的导入作业数。"
            + "并且应小于 `max_running_txn_num_per_db`。",
            "The pending load task executor pool size. "
                    + "This pool size limits the max running pending load tasks.",
            "Currently, it only limits the pending load task of broker load and ingestion load.",
            "It should be less than `max_running_txn_num_per_db`"})
    public static int async_pending_load_task_pool_size = 10;

    @ConfField(masterOnly = true, description = {"loading load task 执行线程数。这个配置可以限制当前正在导入的作业数。",
            "The loading load task executor pool size. "
                    + "This pool size limits the max running loading load tasks.",
            "Currently, it only limits the loading load task of broker load."})
    public static int async_loading_load_task_pool_size = 10;

    @ConfField(mutable = true, masterOnly = true, description = {
            "和 `tablet_create_timeout_second` 含义相同，但是是用于 Delete 操作的。",
            "The same meaning as `tablet_create_timeout_second`, but used when delete a tablet."})
    public static int tablet_delete_timeout_second = 2;

    @ConfField(mutable = true, masterOnly = true, description = {
            "磁盘使用率的高水位线。用于计算 BE 的负载分数。",
            "The high water of disk capacity used percent. This is used for calculating load score of a backend."})
    public static double capacity_used_percent_high_water = 0.75;

    @ConfField(mutable = true, masterOnly = true, description = {
            "负载均衡时，磁盘使用率最大差值。",
            "The max diff of disk capacity used percent between BE. "
                    + "It is used for calculating load score of a backend."})
    public static double used_capacity_percent_max_diff = 0.30;

    @ConfField(mutable = true, masterOnly = true, description = {
            "设置固定的 BE 负载分数中磁盘使用率系数。BE 负载分数会综合磁盘使用率和副本数而得。有效值范围为[0, 1]，"
                    + "当超出此范围时，则使用其他方法自动计算此系数。",
            "Sets a fixed disk usage factor in the BE load fraction. The BE load score is a combination of disk usage "
                    + "and replica count. The valid value range is [0, 1]. When it is out of this range, other "
                    + "methods are used to automatically calculate this coefficient."})
    public static double backend_load_capacity_coeficient = -1.0;

    @ConfField(mutable = true, masterOnly = true, description = {
            "ALTER TABLE 请求的最大超时时间。设置的足够长以适应表的数据量。",
            "Maximal timeout of ALTER TABLE request. Set long enough to fit your table data size."})
    public static int alter_table_timeout_second = 86400 * 30; // 1month

    @ConfField(mutable = true, masterOnly = true, description = {
            "是否禁用存储介质检查。如果禁用，ReportHandler 将不会检查 tablet 的存储介质，"
                    + "并且禁用存储介质冷却功能。默认值为 false。",
            "When disable_storage_medium_check is true, ReportHandler would not check tablet's storage medium "
                    + "and disable storage cool down function."})
    public static boolean disable_storage_medium_check = false;

    @ConfField(description = {"创建表或分区时，可以指定存储介质(HDD 或 SSD)。如果未指定，"
            + "则使用此配置指定的默认介质。",
            "When create a table(or partition), you can specify its storage medium(HDD or SSD)."})
    public static String default_storage_medium = "HDD";

    @ConfField(mutable = true, masterOnly = true, description = {
            "删除数据库(表/分区)后，可以使用 RECOVER 语句恢复。此配置指定了数据的最大保留时间。"
                    + "超过此时间，数据将被永久删除。",
            "After dropping database(table/partition), you can recover it by using RECOVER stmt.",
            "And this specifies the maximal data retention time. After time, the data will be deleted permanently."})
    public static long catalog_trash_expire_second = 86400L; // 1day

    @ConfField
    public static boolean catalog_trash_ignore_min_erase_latency = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "单个 broker scanner 读取的最小字节数。Broker Load 切分文件时，"
                    + "如果切分后的文件大小小于此值，将不会切分。",
            "Minimal bytes that a single broker scanner will read. When splitting file in broker load, "
                    + "if the size of split file is less than this value, it will not be split."})
    public static long min_bytes_per_broker_scanner = 67108864L; // 64MB

    @ConfField(mutable = true, masterOnly = true, description = {
            "单个 broker scanner 的最大并发数。", "Maximal concurrency of broker scanners."})
    public static int max_broker_concurrency = 100;

    // TODO(cmy): Disable by default because current checksum logic has some bugs.
    @ConfField(mutable = true, masterOnly = true, description = {
            "一致性检查的开始时间。与 `consistency_check_end_time` 配合使用，决定一致性检查的起止时间。"
                    + "如果将两个参数设置为相同的值，则一致性检查将不会被调度。",
            "Start time of consistency check. Used with `consistency_check_end_time` "
                    + "to decide the start and end time of consistency check. "
                    + "If set to the same value, consistency check will not be scheduled."})
    public static String consistency_check_start_time = "23";
    @ConfField(mutable = true, masterOnly = true, description = {
            "一致性检查的结束时间。与 `consistency_check_start_time` 配合使用，决定一致性检查的起止时间。"
                    + "如果将两个参数设置为相同的值，则一致性检查将不会被调度。",
            "End time of consistency check. Used with `consistency_check_start_time` "
                    + "to decide the start and end time of consistency check. "
                    + "If set to the same value, consistency check will not be scheduled."})
    public static String consistency_check_end_time = "23";

    @ConfField(mutable = true, masterOnly = true, description = {
            "单个一致性检查任务的默认超时时间。设置的足够长以适应表的数据量。",
            "Default timeout of a single consistency check task. Set long enough to fit your tablet size."})
    public static long check_consistency_default_timeout_second = 600; // 10 min

    @ConfField(description = {"单个 FE 的 MySQL Server 的最大连接数。",
            "Maximal number of connections of MySQL server per FE."})
    public static int qe_max_connection = 1024;

    @ConfField(mutable = true, description = {"Colocate join 每个 instance 的内存 penalty 系数。"
            + "计算方式：`exec_mem_limit / min (query_colocate_join_memory_limit_penalty_factor, instance_num)`",
            "Colocate join PlanFragment instance memory limit penalty factor.",
            "The memory_limit for colocote join PlanFragment instance = "
                    + "`exec_mem_limit / min (query_colocate_join_memory_limit_penalty_factor, instance_num)`"})
    public static int query_colocate_join_memory_limit_penalty_factor = 1;

    /**
     * This configs can set to true to disable the automatic colocate tables's relocate and balance.
     * If 'disable_colocate_balance' is set to true,
     *   ColocateTableBalancer will not relocate and balance colocate tables.
     * Attention:
     *   Under normal circumstances, there is no need to turn off balance at all.
     *   Because once the balance is turned off, the unstable colocate table may not be restored
     *   Eventually the colocate plan cannot be used when querying.
     */
    @ConfField(mutable = true, masterOnly = true) public static boolean disable_colocate_balance = false;

    @ConfField(mutable = true, masterOnly = true, description = {"是否启用group间的均衡",
            "is allow colocate balance between all groups"})
    public static boolean disable_colocate_balance_between_groups = false;

    /**
     * The default user resource publishing timeout.
     */
    @Deprecated
    @ConfField public static int meta_publish_timeout_ms = 1000;
    @ConfField public static boolean proxy_auth_enable = false;
    @ConfField public static String proxy_auth_magic_prefix = "x@8";
    /**
     * Limit on the number of expr children of an expr tree.
     * Exceed this limit may cause long analysis time while holding database read lock.
     * Do not set this if you know what you are doing.
     */
    @ConfField(mutable = true)
    public static int expr_children_limit = 10000;
    /**
     * Limit on the depth of an expr tree.
     * Exceed this limit may cause long analysis time while holding db read lock.
     * Do not set this if you know what you are doing.
     */
    @ConfField(mutable = true)
    public static int expr_depth_limit = 3000;

    // Configurations for backup and restore
    /**
     * Plugins' path for BACKUP and RESTORE operations. Currently deprecated.
     */
    @Deprecated
    @ConfField public static String backup_plugin_path = "/tools/trans_file_tool/trans_files.sh";

    // For forward compatibility, will be removed later.
    // check token when download image file.
    @ConfField public static boolean enable_token_check = true;

    /**
     * Set to true if you deploy Palo using thirdparty deploy manager
     * Valid options are:
     *      disable:    no deploy manager
     *      k8s:        Kubernetes
     *      ambari:     Ambari
     *      local:      Local File (for test or Boxer2 BCC version)
     */
    @ConfField public static String enable_deploy_manager = "disable";

    // If use k8s deploy manager locally, set this to true and prepare the certs files
    @ConfField public static boolean with_k8s_certs = false;

    // Set runtime locale when exec some cmds
    @ConfField public static String locale = "zh_CN.UTF-8";

    // default timeout of backup job
    @ConfField(mutable = true, masterOnly = true)
    public static int backup_job_default_timeout_ms = 86400 * 1000; // 1 day

    /**
     * 'storage_high_watermark_usage_percent' limit the max capacity usage percent of a Backend storage path.
     * 'storage_min_left_capacity_bytes' limit the minimum left capacity of a Backend storage path.
     * If both limitations are reached, this storage path can not be chose as tablet balance destination.
     * But for tablet recovery, we may exceed these limit for keeping data integrity as much as possible.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int storage_high_watermark_usage_percent = 85;
    @ConfField(mutable = true, masterOnly = true)
    public static long storage_min_left_capacity_bytes = 2 * 1024 * 1024 * 1024L; // 2G

    /**
     * If capacity of disk reach the 'storage_flood_stage_usage_percent' and 'storage_flood_stage_left_capacity_bytes',
     * the following operation will be rejected:
     * 1. load job
     * 2. restore job
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int storage_flood_stage_usage_percent = 95;
    @ConfField(mutable = true, masterOnly = true)
    public static long storage_flood_stage_left_capacity_bytes = 1 * 1024 * 1024 * 1024; // 1GB

    // update interval of tablet stat
    // All frontends will get tablet stat from all backends at each interval
    @ConfField(mutable = true)
    public static int tablet_stat_update_interval_second = 60;  // 1 min

    // update interval of alive session
    // Only master FE collect this info from all frontends at each interval
    @ConfField public static int alive_session_update_interval_second = 5;

    @ConfField public static int fe_session_mgr_threads_num = 1;

    @ConfField public static int fe_session_mgr_blocking_queue_size = 1024;

    @ConfField(mutable = true, masterOnly = true)
    public static int loss_conn_fe_temp_table_keep_second = 60;

    /**
     * Max bytes a broker scanner can process in one broker load job.
     * Commonly, each Backends has one broker scanner.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long max_bytes_per_broker_scanner = 500 * 1024 * 1024 * 1024L; // 500G

    /**
     * Max number of load jobs, include PENDING、ETL、LOADING、QUORUM_FINISHED.
     * If exceed this number, load job is not allowed to be submitted.
     */
    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static long max_unfinished_load_job = 1000;

    /**
     * If set to true, Planner will try to select replica of tablet on same host as this Frontend.
     * This may reduce network transmission in following case:
     * 1. N hosts with N Backends and N Frontends deployed.
     * 2. The data has N replicas.
     * 3. High concurrency queries are sent to all Frontends evenly
     * In this case, all Frontends can only use local replicas to do the query.
     * If you want to allow fallback to nonlocal replicas when no local replicas available,
     * set enable_local_replica_selection_fallback to true.
     */
    @ConfField(mutable = true)
    public static boolean enable_local_replica_selection = false;

    /**
     * Used with enable_local_replica_selection.
     * If the local replicas is not available, fallback to the nonlocal replicas.
     * */
    @ConfField(mutable = true)
    public static boolean enable_local_replica_selection_fallback = false;

    /**
     * The number of query retries.
     * A query may retry if we encounter RPC exception and no result has been sent to user.
     * You may reduce this number to avoid Avalanche disaster.
     */
    @ConfField(mutable = true)
    public static int max_query_retry_time = 3;

    /**
     * The number of point query retries in executor.
     * A query may retry if we encounter RPC exception and no result has been sent to user.
     * You may reduce this number to avoid Avalanche disaster.
     */
    @ConfField(mutable = true)
    public static int max_point_query_retry_time = 2;

    /**
     * The tryLock timeout configuration of catalog lock.
     * Normally it does not need to change, unless you need to test something.
     */
    @ConfField(mutable = true)
    public static long catalog_try_lock_timeout_ms = 5000; // 5 sec

    /**
     * if this is set to true
     *    all pending load job will failed when call begin txn api
     *    all prepare load job will failed when call commit txn api
     *    all committed load job will waiting to be published
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean disable_load_job = false;

    /*
     * One master daemon thread will update database used data quota for db txn manager
     * every db_used_data_quota_update_interval_secs
     */
    @ConfField(mutable = false, masterOnly = true)
    public static int db_used_data_quota_update_interval_secs = 300;

    /**
     * fe will call es api to get es index shard info every es_state_sync_interval_secs
     */
    @ConfField
    public static long es_state_sync_interval_second = 10;

    /**
     * the factor of delay time before deciding to repair tablet.
     * if priority is VERY_HIGH, repair it immediately.
     * HIGH, delay tablet_repair_delay_factor_second * 1;
     * NORMAL: delay tablet_repair_delay_factor_second * 2;
     * LOW: delay tablet_repair_delay_factor_second * 3;
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long tablet_repair_delay_factor_second = 60;

    /**
     * clone a tablet, further repair timeout.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long tablet_further_repair_timeout_second = 20 * 60;

    /**
     * clone a tablet, further repair max times.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int tablet_further_repair_max_times = 5;

    /**
     * if tablet loaded txn failed recently, it will get higher priority to repair.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long tablet_recent_load_failed_second = 30 * 60;

    /**
     * base time for higher tablet scheduler task,
     * set this config value bigger if want the high priority effect last longer.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long tablet_schedule_high_priority_second = 30 * 60;

    /**
     * publish version queue's size in be, report it to fe,
     * if publish task in be exceed direct_publish_limit_number,
     * fe will direct publish task
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int publish_version_queued_limit_number = 1000;

    /**
     * the default slot number per path for hdd in tablet scheduler
     * TODO(cmy): remove this config and dynamically adjust it by clone task statistic
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int schedule_slot_num_per_hdd_path = 4;


    /**
     * the default slot number per path for ssd in tablet scheduler
     * TODO(cmy): remove this config and dynamically adjust it by clone task statistic
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int schedule_slot_num_per_ssd_path = 8;

    /**
     * the default batch size in tablet scheduler for a single schedule.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int schedule_batch_size = 50;

    /**
     * tablet health check interval. Do not modify it in production environment.
     */
    @ConfField(mutable = false, masterOnly = true)
    public static long tablet_checker_interval_ms = 20 * 1000;

    /**
     * tablet scheduled interval. Do not modify it in production environment.
     */
    @ConfField(mutable = false, masterOnly = true)
    public static long tablet_schedule_interval_ms = 1000;

    /**
     * Deprecated after 0.10
     */
    @Deprecated
    @ConfField public static boolean use_new_tablet_scheduler = true;

    /**
     * the threshold of cluster balance score, if a backend's load score is 10% lower than average score,
     * this backend will be marked as LOW load, if load score is 10% higher than average score, HIGH load
     * will be marked.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static double balance_load_score_threshold = 0.1; // 10%

    // if disk usage > balance_load_score_threshold + urgent_disk_usage_extra_threshold
    // then this disk need schedule quickly
    // this value could less than 0.
    @ConfField(mutable = true, masterOnly = true)
    public static double urgent_balance_disk_usage_extra_threshold = 0.05;

    // when run urgent disk balance, shuffle the top large tablets
    // range: [ 0 ~ 100 ]
    @ConfField(mutable = true, masterOnly = true)
    public static int urgent_balance_shuffle_large_tablet_percentage = 1;

    @ConfField(mutable = true, masterOnly = true)
    public static double urgent_balance_pick_large_tablet_num_threshold = 1000;

    // range: 0 ~ 100
    @ConfField(mutable = true, masterOnly = true)
    public static int urgent_balance_pick_large_disk_usage_percentage = 80;

    // there's a case, all backend has a high disk, by default, it will not run urgent disk balance.
    // if set this value to true, urgent disk balance will always run,
    // the backends will exchange tablets among themselves.
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_urgent_balance_no_low_backend = true;

    /**
     * if set to true, TabletScheduler will not do balance.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean disable_balance = false;

    /**
     * when be rebalancer idle, then disk balance will occurs.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int be_rebalancer_idle_seconds = 0;

    /**
     * if set to true, TabletScheduler will not do disk balance.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean disable_disk_balance = false;

    // balance order
    // ATTN: a temporary config, may delete later.
    @ConfField(mutable = true, masterOnly = true)
    public static boolean balance_be_then_disk = true;

    /**
     * if set to false, TabletScheduler will not do disk balance for replica num = 1.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_disk_balance_for_single_replica = false;

    // if the number of scheduled tablets in TabletScheduler exceed max_scheduling_tablets
    // skip checking.
    @ConfField(mutable = true, masterOnly = true)
    public static int max_scheduling_tablets = 2000;

    // if the number of balancing tablets in TabletScheduler exceed max_balancing_tablets,
    // no more balance check
    @ConfField(mutable = true, masterOnly = true)
    public static int max_balancing_tablets = 100;

    // Rebalancer type(ignore case): BeLoad, Partition. If type parse failed, use BeLoad as default.
    @ConfField(masterOnly = true)
    public static String tablet_rebalancer_type = "BeLoad";

    // Valid only if use PartitionRebalancer. If this changed, cached moves will be cleared.
    @ConfField(mutable = true, masterOnly = true)
    public static long partition_rebalance_move_expire_after_access = 600; // 600s

    // Valid only if use PartitionRebalancer
    @ConfField(mutable = true, masterOnly = true)
    public static int partition_rebalance_max_moves_num_per_selection = 10;

    // 1 slot for reduce unnecessary balance task, provided a more accurate estimate of capacity
    @ConfField(masterOnly = true, mutable = true)
    public static int balance_slot_num_per_path = 1;

    // when execute admin set replica status = 'drop', the replica will marked as user drop.
    // will try to drop this replica within time not exceeds manual_drop_replica_valid_second
    @ConfField(masterOnly = true, mutable = true)
    public static long manual_drop_replica_valid_second = 24 * 3600L;

    // This threshold is to avoid piling up too many report task in FE, which may cause OOM exception.
    // In some large Doris cluster, eg: 100 Backends with ten million replicas, a tablet report may cost
    // several seconds after some modification of metadata(drop partition, etc..).
    // And one Backend will report tablets info every 1 min, so unlimited receiving reports is unacceptable.
    // TODO(cmy): we will optimize the processing speed of tablet report in future, but now, just discard
    // the report if queue size exceeding limit.
    // Some online time cost:
    // 1. disk report: 0-1 ms
    // 2. task report: 0-1 ms
    // 3. tablet report
    //      10000 replicas: 200ms
    @ConfField(mutable = true, masterOnly = true)
    public static int report_queue_size = 100;

    // if the number of report task in FE exceed max_report_task_num_per_rpc, then split it to multiple rpc
    @ConfField(mutable = true, masterOnly = true, description = {
            "重新发送 agent task 时，单次 RPC 分配给每个be的任务最大个数，默认值为10000个。",
            "The maximum number of batched tasks per RPC assigned to each BE when resending agent tasks, "
            + "the default value is 10000."
    })
    public static int report_resend_batch_task_num_per_rpc = 10000;

    /**
     * If set to true, metric collector will be run as a daemon timer to collect metrics at fix interval
     */
    @ConfField public static boolean enable_metric_calculator = true;

    /**
     * the max routine load job num, including NEED_SCHEDULED, RUNNING, PAUSE
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_routine_load_job_num = 100;

    /**
     * the max concurrent routine load task num of a single routine load job
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_routine_load_task_concurrent_num = 256;

    /**
     * the max concurrent routine load task num per BE.
     * This is to limit the num of routine load tasks sending to a BE, and it should also less
     * than BE config 'max_routine_load_thread_pool_size'(default 1024),
     * which is the routine load task thread pool max size on BE.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_routine_load_task_num_per_be = 1024;

    /**
     * routine load timeout is equal to maxBatchIntervalS * routine_load_task_timeout_multiplier.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int routine_load_task_timeout_multiplier = 10;

    /**
     * routine load task min timeout second.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int routine_load_task_min_timeout_sec = 60;

    /**
     * the max timeout of get kafka meta.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_get_kafka_meta_timeout_second = 60;


    /**
     * the expire time of routine load blacklist.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int routine_load_blacklist_expire_time_second = 300;

    /**
     * The max number of files store in SmallFileMgr
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_small_file_number = 100;

    /**
     * The max size of a single file store in SmallFileMgr
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_small_file_size_bytes = 1024 * 1024; // 1MB

    /**
     * Save small files
     */
    @ConfField
    public static String small_file_dir =  EnvUtils.getDorisHome() + "/small_files";

    /**
     * This will limit the max recursion depth of hash distribution pruner.
     * eg: where a in (5 elements) and b in (4 elements) and c in (3 elements) and d in (2 elements).
     * a/b/c/d are distribution columns, so the recursion depth will be 5 * 4 * 3 * 2 = 120, larger than 100,
     * So that distribution pruner will no work and just return all buckets.
     *
     * Increase the depth can support distribution pruning for more elements, but may cost more CPU.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static int max_distribution_pruner_recursion_depth = 100;

    /**
     * If the jvm memory used percent(heap or old mem pool) exceed this threshold, checkpoint thread will
     * not work to avoid OOM.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long metadata_checkpoint_memory_threshold = 70;

    /**
     * If set to true, the checkpoint thread will make the checkpoint regardless of the jvm memory used percent.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean force_do_metadata_checkpoint = false;

    /**
     * If some joural is wrong, and FE can't start, we can use this to skip it.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String[] force_skip_journal_ids = {};

    /**
     * Decide how often to check dynamic partition
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long dynamic_partition_check_interval_seconds = 600;

    /**
     * When scheduling dynamic partition tables,
     * the execution interval of each table to prevent excessive consumption of FE CPU at the same time
     * default is 0
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long dynamic_partition_step_interval_ms = 0;

    /**
     * If set to true, dynamic partition feature will open
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean dynamic_partition_enable = true;

    /**
     * control rollup job concurrent limit
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_running_rollup_job_num_per_table = 1;

    /**
     * If set to true, Doris will check if the compiled and running versions of Java are compatible
     */
    @ConfField
    public static boolean check_java_version = true;

    /**
     * it can't auto-resume routine load job as long as one of the backends is down
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_tolerable_backend_down_num = 0;

    /**
     * a period for auto resume routine load
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int period_of_auto_resume_min = 10;

    /**
     * If set to true, the backend will be automatically dropped after finishing decommission.
     * If set to false, the backend will not be dropped and remaining in DECOMMISSION state.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean drop_backend_after_decommission = true;

    /**
     * When tablet size of decommissioned backend is lower than this threshold,
     * SystemHandler will start to check if all tablets of this backend are in recycled status,
     * this backend will be dropped immediately if the check result is true.
     * For performance based considerations, better not set a very high value for this.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int decommission_tablet_check_threshold = 50000;

    /**
     * When decommission a backend, need to migrate all its tablets to other backends.
     * But there maybe some leaky tablets due to forgetting to delete them from TabletInvertIndex.
     * They are not in use. Decommission can skip migrating them.
     * For safety, decommission wait for a period after founding leaky tablets.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int decommission_skip_leaky_tablet_second = 3600 * 5;

    /**
     * Decommission a tablet need to wait all the previous txns finished.
     * If wait timeout, decommission will fail.
     * Need to increase this wait time if the txn take a long time.
     *
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int decommission_tablet_wait_time_seconds = 3600;

    /**
     * Define thrift server's server model, default is TThreadPoolServer model
     */
    @ConfField
    public static String thrift_server_type = "THREAD_POOL";

    /**
     * This config will decide whether to resend agent task when create_time for agent_task is set,
     * only when current_time - create_time > agent_task_resend_wait_time_ms can ReportHandler do resend agent task
     */
    @ConfField (mutable = true, masterOnly = true)
    public static long agent_task_resend_wait_time_ms = 5000;

    /**
     * min_clone_task_timeout_sec and max_clone_task_timeout_sec is to limit the
     * min and max timeout of a clone task.
     * Under normal circumstances, the timeout of a clone task is estimated by
     * the amount of data and the minimum transmission speed(5MB/s).
     * But in special cases, you may need to manually set these two configs
     * to ensure that the clone task will not fail due to timeout.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long min_clone_task_timeout_sec = 3 * 60; // 3min
    @ConfField(mutable = true, masterOnly = true)
    public static long max_clone_task_timeout_sec = 2 * 60 * 60; // 2h

    /**
     * If set to true, fe will enable sql result cache
     * This option is suitable for offline data update scenarios
     *                              case1   case2   case3   case4
     * enable_sql_cache             false   true    true    false
     * enable_partition_cache       false   false   true    true
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean cache_enable_sql_mode = true;

    /**
     *  Minimum interval between last version when caching results,
     *  This parameter distinguishes between offline and real-time updates
     */
    @ConfField(mutable = true, masterOnly = false)
    public static int cache_last_version_interval_second = 30;

    /**
     *  Expire sql sql in frontend time
     */
    @ConfField(
            mutable = true,
            masterOnly = false,
            callbackClassString = "org.apache.doris.common.cache.NereidsSqlCacheManager$UpdateConfig",
            description = {
                    "当前默认设置为 300，用来控制控制NereidsSqlCacheManager中sql cache过期时间，超过一段时间不访问cache会被回收",
                    "The current default setting is 300, which is used to control the expiration time of SQL cache"
                            + "in NereidsSqlCacheManager. If the cache is not accessed for a period of time, "
                            + "it will be reclaimed"
            }
    )
    public static int expire_sql_cache_in_fe_second = 300;

    /**
     *  Expire hbo plan stats. cache in frontend time.
     */
    @ConfField(
            mutable = true,
            masterOnly = false,
            callbackClassString = "org.apache.doris.nereids.stats.MemoryHboPlanStatisticsProvider$UpdateConfig",
            description = {
                    "当前默认设置为 86400，用来控制控制MemoryHboPlanStatisticsProvider中stats. cache过期时间，超过不访问会被回收",
                    "The default setting is 86400, which is used to control the expiration time of plan stats. cache"
                            + "in MemoryHboPlanStatisticsProvider. If the cache is not accessed for a period of time, "
                            + "it will be reclaimed."
            }
    )
    public static int expire_hbo_plan_stats_cache_in_fe_second = 86400;

    /**
     *  Expire hbo plan info cache in frontend time.
     */
    @ConfField(
            mutable = true,
            masterOnly = false,
            callbackClassString = "org.apache.doris.nereids.stats.HboPlanInfoProvider$UpdateConfig",
            description = {
                    "当前默认设置为1000，用来控制控制HboPlanInfoProvider中plan info cache过期时间，超过一段时间不访问cache会被回收",
                    "The default setting is 100, which is used to control the expiration time of hbo plan info cache"
                            + "in HboPlanInfoProvider. If the cache is not accessed for a period of time, "
                            + "it will be reclaimed."
            }
    )
    public static int expire_hbo_plan_info_cache_in_fe_second = 1000;

    /**
     *  Expire sql sql in frontend time
     */
    @ConfField(
            mutable = true,
            masterOnly = false,
            callbackClassString = "org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager$UpdateConfig",
            description = {
                "当前默认设置为 300，用来控制控制NereidsSortedPartitionsCacheManager中分区元数据缓存过期时间，"
                    + "超过一段时间不访问cache会被回收",
                "The current default setting is 300, which is used to control the expiration time of "
                    + "the partition metadata cache in NereidsSortedPartitionsCheManager. "
                    + "If the cache is not accessed for a period of time, it will be reclaimed"
            }
    )
    public static int expire_cache_partition_meta_table_in_fe_second = 300;

    /**
     * Set the maximum number of rows that can be cached
     */
    @ConfField(mutable = true, masterOnly = false, description = {"SQL/Partition Cache可以缓存的最大行数。",
        "Maximum number of rows that can be cached in SQL/Partition Cache, is 3000 by default."})
    public static int cache_result_max_row_count = 3000;

    /**
     * Set the maximum data size that can be cached
     */
    @ConfField(mutable = true, masterOnly = false, description = {"SQL/Partition Cache可以缓存的最大数据大小。",
        "Maximum data size of rows that can be cached in SQL/Partition Cache, is 3000 by default."})
    public static int cache_result_max_data_size = 31457280; // 30M

    /**
     * Used to limit element num of InPredicate in delete statement.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_allowed_in_element_num_of_delete = 1024;

    /**
     * In some cases, some tablets may have all replicas damaged or lost.
     * At this time, the data has been lost, and the damaged tablets
     * will cause the entire query to fail, and the remaining healthy tablets cannot be queried.
     * In this case, you can set this configuration to true.
     * The system will replace damaged tablets with empty tablets to ensure that the query
     * can be executed. (but at this time the data has been lost, so the query results may be inaccurate)
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean recover_with_empty_tablet = false;

    /**
     * Whether to add a version column when create unique table
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_hidden_version_column_by_default = true;

    /**
     * Whether to add a skip bitmap column when create merge-on-write unique table
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_skip_bitmap_column_by_default = false;

    /**
     * Used to set default db data quota bytes.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long default_db_data_quota_bytes = Long.MAX_VALUE; // 8192 PB

    /**
     * Used to set default db replica quota num.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long default_db_replica_quota_size = 1024 * 1024 * 1024;

    /*
     * Maximum percentage of data that can be filtered (due to reasons such as data is irregularly)
     * The default value is 0.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static double default_max_filter_ratio = 0;

    /**
     * HTTP Server V2 is implemented by SpringBoot.
     * It uses an architecture that separates front and back ends.
     * Only enable httpv2 can user to use the new Frontend UI interface
     */
    @ConfField
    public static boolean enable_http_server_v2 = true;

    /*
     * Base path is the URL prefix for all API paths.
     * Some deployment environments need to configure additional base path to match resources.
     * This Api will return the path configured in Config.http_api_extra_base_path.
     * Default is empty, which means not set.
     */
    @ConfField
    public static String http_api_extra_base_path = "";

    /**
     * If set to true, FE will be started in BDBJE debug mode
     */
    @ConfField
    public static boolean enable_bdbje_debug_mode = false;

    @ConfField(mutable = false, masterOnly = true, description = {"是否开启debug point模式，测试使用",
            "is enable debug points, use in test."})
    public static boolean enable_debug_points = false;

    /**
     * This config is used to try skip broker when access bos or other cloud storage via broker
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_access_file_without_broker = false;

    /**
     * Whether to allow the outfile function to export the results to the local disk.
     * If set to true, there's risk to run out of FE disk capacity.
     */
    @ConfField
    public static boolean enable_outfile_to_local = false;

    /**
     * Used to set the initial flow window size of the GRPC client channel, and also used to max message size.
     * When the result set is large, you may need to increase this value.
     */
    @ConfField
    public static int grpc_max_message_size_bytes = 2147483647; // 2GB

    /**
     * num of thread to handle grpc events in grpc_threadmgr
     */
    @ConfField
    public static int grpc_threadmgr_threads_nums = 4096;

    /**
     * sets the time without read activity before sending a keepalive ping
     * the smaller the value, the sooner the channel is unavailable, but it will increase network io
     */
    @ConfField(description = { "设置grpc连接发送 keepalive ping 之前没有数据传输的时间。",
            "The time without grpc read activity before sending a keepalive ping" })
    public static int grpc_keep_alive_second = 10;

    /**
     * Used to set minimal number of replication per tablet.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static short min_replication_num_per_tablet = 1;

    /**
     * Used to set maximal number of replication per tablet.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static short max_replication_num_per_tablet = Short.MAX_VALUE;

    /**
     * Used to limit the maximum number of partitions that can be created when creating a dynamic partition table,
     * to avoid creating too many partitions at one time.
     * The number is determined by "start" and "end" in the dynamic partition parameters.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_dynamic_partition_num = 500;

    /**
     * Used to limit the maximum number of partitions that can be created when creating multi partition,
     * to avoid creating too many partitions at one time.
     * The number is determined by "start" and "end" in the multi partition parameters.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_multi_partition_num = 4096;

    /**
     * Use this parameter to set the partition name prefix for multi partition,
     * Only multi partition takes effect, not dynamic partitions.
     * The default prefix is "p_".
     */
    @ConfField(mutable = true, masterOnly = true)
    public static String multi_partition_name_prefix = "p_";

    /**
     * Control the max num of backup/restore job per db
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_backup_restore_job_num_per_db = 10;

    /**
     * A internal config, to reduce the restore job size during serialization by compress.
     *
     * WARNING: Once this option is enabled and a restore is performed, the FE version cannot be rolled back.
     */
    @ConfField(mutable = false)
    public static boolean restore_job_compressed_serialization = false;

    /**
     * A internal config, to reduce the backup job size during serialization by compress.
     *
     * WARNING: Once this option is enabled and a backup is performed, the FE version cannot be rolled back.
     */
    @ConfField(mutable = false)
    public static boolean backup_job_compressed_serialization = false;

    /**
     * A internal config, to indicate whether to enable the restore snapshot rpc compression.
     *
     * The ccr syncer will depends this config to decide whether to compress the meta and job
     * info of the restore snapshot request.
     */
    @ConfField(mutable = false)
    public static boolean enable_restore_snapshot_rpc_compression = true;

    /**
     * A internal config, to indicate whether to reset the index id when restore olap table.
     *
     * The inverted index saves the index id in the file path/header, so the index id between
     * two clusters must be the same.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean restore_reset_index_id = false;

    /**
     * Control the max num of tablets per backup job involved.
     */
    @ConfField(mutable = true, masterOnly = true, description = {
        "用于控制每次 backup job 允许备份的 tablet 上限，以避免 OOM",
        "Control the max num of tablets per backup job involved, to avoid OOM"
    })
    public static int max_backup_tablets_per_job = 300000;

    /**
     * whether to ignore table that not support type when backup, and not report exception.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean ignore_backup_not_support_table_type = false;

    /**
     * whether to ignore temp partitions when backup, and not report exception.
     */
    @ConfField(mutable = true, masterOnly = true, description = {
        "是否忽略备份临时分区，不报异常",
        "Whether to ignore temp partitions when backup, and not report exception."
    })
    public static boolean ignore_backup_tmp_partitions = false;

    /**
     * A internal config, to control the update interval of backup handler. Only used to speed up tests.
     */
    @ConfField(mutable = false)
    public static long backup_handler_update_interval_millis = 3000;


    /**
     * Whether to enable cloud restore job.
     */
    @ConfField(mutable = true, masterOnly = true, description = {"是否开启存算分离恢复功能。",
        "Whether to enable cloud restore job."}, varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_cloud_restore_job = false;

    /**
     * Control the default max num of the instance for a user.
     */
    @ConfField(mutable = true)
    public static int default_max_query_instances = -1;

    /*
     * One master daemon thread will update global partition info, include in memory and visible version
     * info every partition_info_update_interval_secs
     */
    @ConfField(mutable = false, masterOnly = true)
    public static int partition_info_update_interval_secs = 60;

    @Deprecated
    @ConfField(masterOnly = true)
    public static boolean enable_concurrent_update = false;

    /**
     * This configuration can only be configured during cluster initialization and cannot be modified during cluster
     * restart and upgrade after initialization is complete.
     *
     * 0: table names are stored as specified and comparisons are case sensitive.
     * 1: table names are stored in lowercase and comparisons are not case sensitive.
     * 2: table names are stored as given but compared in lowercase.
     */
    @ConfField(masterOnly = true)
    public static int lower_case_table_names = 0;

    @ConfField(mutable = true, masterOnly = true)
    public static int table_name_length_limit = 64;

    @ConfField(mutable = true, description = {
            "用于限制列注释长度；如果存量的列注释超长，则显示时进行截断",
            "Used to limit the length of column comment; "
                    + "If the existing column comment is too long, it will be truncated when displayed."})
    public static int column_comment_length_limit = -1;

    /*
     * The job scheduling interval of the schema change handler.
     * The user should not set this parameter.
     * This parameter is currently only used in the regression test environment to appropriately
     * reduce the running speed of the schema change job to test the correctness of the system
     * in the case of multiple tasks in parallel.
     */
    @ConfField(mutable = false, masterOnly = true)
    public static int default_schema_change_scheduler_interval_millisecond = 500;

    /*
     * If set to true, the thrift structure of query plan will be sent to BE in compact mode.
     * This will significantly reduce the size of rpc data, which can reduce the chance of rpc timeout.
     * But this may slightly decrease the concurrency of queries, because compress and decompress cost more CPU.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean use_compact_thrift_rpc = true;

    /*
     * If set to true, the tablet scheduler will not work, so that all tablet repair/balance task will not work.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean disable_tablet_scheduler = false;

    /*
     * When doing clone or repair tablet task, there may be replica is REDUNDANT state, which
     * should be dropped later. But there are be loading task on these replicas, so the default strategy
     * is to wait until the loading task finished before dropping them.
     * But the default strategy may takes very long time to handle these redundant replicas.
     * So we can set this config to true to not wait any loading task.
     * Set this config to true may cause loading task failed, but will
     * speed up the process of tablet balance and repair.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_force_drop_redundant_replica = false;

    /*
     * auto set the slowest compaction replica's status to bad
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean repair_slow_replica = false;

    /*
     * The relocation of a colocation group may involve a large number of tablets moving within the cluster.
     * Therefore, we should use a more conservative strategy to avoid relocation
     * of colocation groups as much as possible.
     * Reloaction usually occurs after a BE node goes offline or goes down.
     * This parameter is used to delay the determination of BE node unavailability.
     * The default is 30 minutes, i.e., if a BE node recovers within 30 minutes, relocation of the colocation group
     * will not be triggered.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long colocate_group_relocate_delay_second = 1800; // 30 min

    /*
     * If set to true, when creating table, Doris will allow to locate replicas of a tablet
     * on same host.
     * This is only for local test, so that we can deploy multi BE on same host and create table
     * with multi replicas.
     * DO NOT use it for production env.
     */
    @ConfField
    public static boolean allow_replica_on_same_host = false;

    /**
     *  The version count threshold used to judge whether replica compaction is too slow
     */
    @ConfField(mutable = true)
    public static int min_version_count_indicate_replica_compaction_too_slow = 200;

    /**
     * The valid ratio threshold of the difference between the version count of the slowest replica and the fastest
     * replica. If repair_slow_replica is set to true, it is used to determine whether to repair the slowest replica
     */
    @ConfField(mutable = true, masterOnly = true)
    public static double valid_version_count_delta_ratio_between_replicas = 0.5;

    /**
     * The data size threshold used to judge whether replica is too large
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long min_bytes_indicate_replica_too_large = 2 * 1024 * 1024 * 1024L;

    // statistics
    /*
     * the max unfinished statistics job number
     */
    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static int cbo_max_statistics_job_num = 20;
    /*
     * the max timeout of a statistics task
     */
    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static int max_cbo_statistics_task_timeout_sec = 300;
    /*
     * the concurrency of statistics task
     */
    @Deprecated
    @ConfField(mutable = false, masterOnly = true)
    public static int cbo_concurrency_statistics_task_num = 10;
    /*
     * default sample percentage
     * The value from 0 ~ 100. The 100 means no sampling and fetch all data.
     */
    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static int cbo_default_sample_percentage = 10;

    /*
     * the system automatically checks the time interval for statistics
     */
    @ConfField(mutable = true, masterOnly = true, description = {
            "该参数控制自动收集作业检查库表统计信息健康度并触发自动收集的时间间隔",
            "This parameter controls the time interval for automatic collection jobs to check the health of table"
                    + "statistics and trigger automatic collection"
    })
    public static int auto_check_statistics_in_minutes = 1;

    /**
     * If set to TRUE, the compaction slower replica will be skipped when select get queryable replicas
     * Default is true.
     */
    @ConfField(mutable = true)
    public static boolean skip_compaction_slower_replica = true;

    /**
     * Enable quantile_state type column
     * Default is false.
     * */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_quantile_state_type = true;

    /*---------------------- JOB CONFIG START------------------------*/
    /**
     * The number of threads used to dispatch timer job.
     * If we have a lot of timer jobs, we need more threads to dispatch them.
     * All timer job will be dispatched to a thread pool, and they will be dispatched to the thread queue of the
     * corresponding type of job
     * The value should be greater than 0, if it is 0 or <=0, set it to 5
     */
    @ConfField(masterOnly = true, description = {"用于分发定时任务的线程数",
            "The number of threads used to dispatch timer job."})
    public static int job_dispatch_timer_job_thread_num = 2;

    /**
     * The number of timer jobs that can be queued.
     * if consumer is slow, the queue will be full, and the producer will be blocked.
     * if you have a lot of timer jobs, you need to increase this value or increase the number of
     * {@code @dispatch_timer_job_thread_num}
     * The value should be greater than 0, if it is 0 or <=0, set it to 1024
     */
    @ConfField(masterOnly = true, description = {"任务堆积时用于存放定时任务的队列大小", "The number of timer jobs that can be queued."})
    public static int job_dispatch_timer_job_queue_size = 1024;
    @ConfField(masterOnly = true, description = {"一个 Job 的 task 最大的持久化数量，超过这个限制将会丢弃旧的 task 记录, 如果值 < 1, 将不会持久化。",
            "Maximum number of persistence allowed per task in a job,exceeding which old tasks will be discarded，"
                   + "If the value is less than 1, it will not be persisted." })
    public static int max_persistence_task_count = 100;

    @ConfField(masterOnly = true, description = { "MTMV task 的等待队列大小，如果是负数，则会使用 1024，如果不是 2 的幂，则会自动选择一个最接近的"
                    + " 2 的幂次方数",
            "The size of the MTMV task's waiting queue If the size is negative, 1024 will be used. If "
            + "the size is not a power of two, the nearest power of the size will be"
            + " automatically selected."})
    public static int mtmv_task_queue_size = 1024;
    @ConfField(masterOnly = true, description = {"Insert task 的等待队列大小，如果是负数，则会使用 1024，如果不是 2 的幂，则会自动选择一个最接近"
            + " 的 2 的幂次方数", "The size of the Insert task's waiting queue If the size is negative, 1024 will be used."
            + " If the size is not a power of two, the nearest power of the size will "
            + "be automatically selected."})
    public static int insert_task_queue_size = 1024;
    @ConfField(masterOnly = true, description = { "字典导入 task 的等待队列大小，如果是负数，则会使用 1024，如果不是 2 的幂，则会自动选择一个最接近"
            + " 的 2 的幂次方数",
            "The size of the Dictionary loading task's waiting queue If the size is negative, 1024 will be used."
            + " If the size is not a power of two, the nearest power of the size will "
            + "be automatically selected." })
    public static int dictionary_task_queue_size = 1024;

    @ConfField(masterOnly = true, description = {"finished 状态的 job 最长保存时间，超过这个时间将会被删除, 单位：小时",
            "The longest time to save the job in finished status, it will be deleted after this time. Unit: hour"})
    public static int finished_job_cleanup_threshold_time_hour = 24;

    @ConfField(masterOnly = true, description = {"用于执行 Insert 任务的线程数,值应该大于0，否则默认为10",
            "The number of threads used to consume Insert tasks, "
                    + "the value should be greater than 0, if it is <=0, default is 10."})
    public static int job_insert_task_consumer_thread_num = 10;

    @ConfField(masterOnly = true, description = {"用于执行 MTMV 任务的线程数,值应该大于0，否则默认为10",
            "The number of threads used to consume MTMV tasks, "
                    + "the value should be greater than 0, if it is <=0, default is 10."})
    public static int job_mtmv_task_consumer_thread_num = 10;

    @ConfField(masterOnly = true, description = { "用于执行字典导入和删除任务的线程数,值应该大于0，否则默认为3",
            "The number of threads used to perform the dictionary import and delete tasks, which should be"
                    + " greater than 0, otherwise it defaults to 3." })
    public static int job_dictionary_task_consumer_thread_num = 3;

    /* job test config */
    /**
     * If set to true, we will allow the interval unit to be set to second, when creating a recurring job.
     */
    @ConfField
    public static boolean enable_job_schedule_second_for_test = false;

    /*---------------------- JOB CONFIG END------------------------*/
    /**
     * The number of async tasks that can be queued. @See TaskDisruptor
     * if consumer is slow, the queue will be full, and the producer will be blocked.
     */
    @ConfField
    public static int async_task_queen_size = 1024;

    /**
     * The number of threads used to consume async tasks. @See TaskDisruptor
     * if we have a lot of async tasks, we need more threads to consume them. Sure, it's depends on the cpu cores.
     */
    @ConfField
    public static int async_task_consumer_thread_num = 64;

    /**
     * When job is finished, it will be saved in job manager for a while.
     * This configuration is used to control the max saved time.
     * Default is 3 days.
     */
    @Deprecated
    @ConfField
    public static int finish_job_max_saved_second = 60 * 60 * 24 * 3;

    // enable_workload_group should be immutable and temporarily set to mutable during the development test phase
    @ConfField(mutable = true, varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_workload_group = true;

    @ConfField(mutable = true)
    public static boolean enable_query_queue = true;

    @ConfField(mutable = true)
    public static long query_queue_update_interval_ms = 5000;

    @ConfField(mutable = true, description = {
            "当BE内存用量大于该值时，查询会进入排队逻辑，默认值为-1，代表该值不生效。取值范围0~1的小数",
            "When be memory usage bigger than this value, query could queue, "
                    + "default value is -1, means this value not work. Decimal value range from 0 to 1"})
    public static double query_queue_by_be_used_memory = -1;

    @ConfField(mutable = true, description = {"基于内存反压场景FE定时拉取BE内存用量的时间间隔",
            "In the scenario of memory backpressure, "
                    + "the time interval for obtaining BE memory usage at regular intervals"})
    public static long get_be_resource_usage_interval_ms = 10000;

    @ConfField(mutable = false, masterOnly = true)
    public static int backend_rpc_timeout_ms = 60000; // 1 min

    /**
     * If set to TRUE, FE will:
     * 1. divide BE into high load and low load(no mid load) to force triggering tablet scheduling;
     * 2. ignore whether the cluster can be more balanced during tablet scheduling;
     *
     * It's used to test the reliability in single replica case when tablet scheduling are frequent.
     * Default is false.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static boolean be_rebalancer_fuzzy_test = false;

    /**
     * If set to TRUE, FE will convert date/datetime to datev2/datetimev2(0) automatically.
     */
    @ConfField(mutable = true)
    public static boolean enable_date_conversion = true;

    @ConfField(mutable = false, masterOnly = true)
    public static boolean enable_multi_tags = true;

    /**
     * If set to TRUE, FE will convert DecimalV2 to DecimalV3 automatically.
     */
    @ConfField(mutable = true)
    public static boolean enable_decimal_conversion = true;

    /**
     * Support complex data type ARRAY.
     */
    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_array_type = false;

    /**
     * The timeout of executing async remote fragment.
     * In normal case, the async remote fragment will be executed in a short time. If system are under high load
     * condition，try to set this timeout longer.
     */
    @ConfField(mutable = true)
    public static long remote_fragment_exec_timeout_ms = 30000; // 30 sec

    /**
     * Max data version of backends serialize block.
     */
    @ConfField(mutable = false)
    public static int max_be_exec_version = 8;

    /**
     * Min data version of backends serialize block.
     */
    @ConfField(mutable = false)
    public static int min_be_exec_version = 0;

    /**
     * Data version of backends serialize block.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int be_exec_version = max_be_exec_version;

    /*
     * mtmv is still under dev, remove this config when it is graduate.
     */
    @ConfField(mutable = true, masterOnly = true, varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_mtmv = false;

    /* Max running task num at the same time, otherwise the submitted task will still be keep in pending poll*/
    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static int max_running_mtmv_scheduler_task_num = 100;

    /* Max pending task num keep in pending poll, otherwise it reject the task submit*/
    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static int max_pending_mtmv_scheduler_task_num = 100;

    /* Remove the completed mtmv job after this expired time. */
    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static long scheduler_mtmv_job_expired = 24 * 60 * 60L; // 1day

    /* Remove the finished mtmv task after this expired time. */
    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static long scheduler_mtmv_task_expired = 24 * 60 * 60L; // 1day

    @Deprecated
    @ConfField(mutable = true, masterOnly = true)
    public static boolean keep_scheduler_mtmv_task_when_job_deleted = false;

    /**
     * If set to true, query on external table will prefer to assign to compute node.
     * And the max number of compute node is controlled by min_backend_num_for_external_table.
     * If set to false, query on external table will assign to any node.
     */
    @ConfField(mutable = true, description = {"如果设置为true，外部表的查询将优先分配给计算节点。",
            "并且计算节点的最大数量由min_backend_num_for_external_table控制。",
            "如果设置为false，外部表的查询将分配给任何节点。"
                    + "如果集群内没有计算节点，则该参数不生效。",
            "If set to true, query on external table will prefer to assign to compute node. "
                    + "And the max number of compute node is controlled by min_backend_num_for_external_table. "
                    + "If set to false, query on external table will assign to any node. "
                    + "If there is no compute node in cluster, this config takes no effect."})
    public static boolean prefer_compute_node_for_external_table = false;

    @ConfField(mutable = true, description = {"只有当prefer_compute_node_for_external_table为true时生效，"
            + "如果计算节点数小于这个值，外部表的查询会尝试获取一些混合节点来分配，以使节点总数达到这个值。"
            + "如果计算节点数大于这个值，外部表的查询将只分配给计算节点。-1表示只是用当前数量的计算节点",
            "Only take effect when prefer_compute_node_for_external_table is true. "
                    + "If the compute node number is less than this value, "
                    + "query on external table will try to get some mix de to assign, "
                    + "to let the total number of node reach this value. "
                    + "If the compute node number is larger than this value, "
                    + "query on external table will assign to compute de only. "
                    + "-1 means only use current compute node."})
    public static int min_backend_num_for_external_table = -1;

    /**
     * Max query profile num.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static int max_query_profile_num = 500;

    /**
     * Set to true to disable backend black list, so that even if we failed to send task to a backend,
     * that backend won't be added to black list.
     * This should only be set when running tests, such as regression test.
     * Highly recommended NOT disable it in product environment.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean disable_backend_black_list = false;

    @ConfField(mutable = true, masterOnly = false, description = {
        "If a backend is tried to be added to black list do_add_backend_black_list_threshold_count times "
            + "in do_add_backend_black_list_threshold_seconds, it will be added to black list."})
    public static long do_add_backend_black_list_threshold_count = 10;

    @ConfField(mutable = true, masterOnly = false, description = {
        "If a backend is tried to be added to black list do_add_backend_black_list_threshold_count times "
            + "in do_add_backend_black_list_threshold_seconds, it will be added to black list."})
    public static long do_add_backend_black_list_threshold_seconds = 30;

    @ConfField(mutable = true, masterOnly = false, description = {
        "Backend will stay in black list for this time after it is added to black list."})
    public static long stay_in_backend_black_list_threshold_seconds = 60;

    /**
     * Maximum backend heartbeat failure tolerance count.
     * Default is 1, which means if 1 heart failed, the backend will be marked as dead.
     * A larger value can improve the tolerance of the cluster to occasional heartbeat failures.
     * For example, when running regression tests, this value can be increased.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long max_backend_heartbeat_failure_tolerance_count = 1;

    /**
     * Even if a backend is healthy, still write a heartbeat editlog to update backend's lastUpdateMs of bdb image.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int editlog_healthy_heartbeat_seconds = 300;

    /**
     * Abort transaction time after lost heartbeat.
     * The default value is 300s, which means transactions of be will be aborted after lost heartbeat 300s.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int abort_txn_after_lost_heartbeat_time_second = 300;

    /**
     * Heartbeat interval in seconds.
     * Default is 10, which means every 10 seconds, the master will send a heartbeat to all backends.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static int heartbeat_interval_second = 10;

    /**
     * After a backend is marked as unavailable, it will be added to blacklist.
     * Default is 120.
     */
    @ConfField(mutable = true, masterOnly = false)
    public static int blacklist_duration_second = 120;

    /**
     * The default connection timeout for hive metastore.
     * hive.metastore.client.socket.timeout
     */
    @ConfField(mutable = true, masterOnly = false)
    public static long hive_metastore_client_timeout_second = 10;

    /**
     * Used to determined how many statistics collection SQL could run simultaneously.
     */
    @ConfField
    public static int statistics_simultaneously_running_task_num = 3;

    /**
     * if table has too many replicas, Fe occur oom when schema change.
     * 10W replicas is a reasonable value for testing.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long max_replica_count_when_schema_change = 100000;

    /**
     * Max cache num of hive partition.
     * Decrease this value if FE's memory is small
     */
    @ConfField(description = {"Hive Metastore 表级别分区缓存的最大数量。",
            "Max cache number of partition at table level in Hive Metastore."})
    public static long max_hive_partition_cache_num = 10000;

    @ConfField(description = {"Hudi/Iceberg/Paimon 表级别缓存的最大数量。",
            "Max cache number of hudi/iceberg table."})
    public static long max_external_table_cache_num = 1000;

    @ConfField(description = {"External Catalog 中，Database 和 Table 的实例缓存的最大数量。",
            "Max cache number of database and table instance in external catalog."})
    public static long max_meta_object_cache_num = 1000;

    @ConfField(description = {"Hive分区表缓存的最大数量",
            "Max cache number of hive partition table"})
    public static long max_hive_partition_table_cache_num = 1000;

    @ConfField(mutable = false, masterOnly = false, description = {"获取Hive分区值时候的最大返回数量，-1代表没有限制。",
            "Max number of hive partition values to return while list partitions, -1 means no limitation."})
    public static short max_hive_list_partition_num = -1;

    @ConfField(mutable = false, masterOnly = false, description = {"远程文件系统缓存的最大数量",
            "Max cache number of remote file system."})
    public static long max_remote_file_system_cache_num = 100;

    @ConfField(mutable = false, masterOnly = false, description = {"外表行数缓存最大数量",
        "Max cache number of external table row count"})
    public static long max_external_table_row_count_cache_num = 100000;

    @ConfField(description = {"每个查询的外表文件元数据缓存的最大文件数量。",
            "Max cache file number of external table split file meta cache at query level."})
    public static long max_external_table_split_file_meta_cache_num = 100000;

    /**
     * Max cache loader thread-pool size.
     * Max thread pool size for loading external meta cache
     */
    @ConfField(mutable = false, masterOnly = false)
    public static int max_external_cache_loader_thread_pool_size = 64;

    /**
     * Max cache num of external catalog's file
     * Decrease this value if FE's memory is small
     */
    @ConfField(mutable = false, masterOnly = false)
    public static long max_external_file_cache_num = 10000;

    /**
     * Max cache num of external table's schema
     * Decrease this value if FE's memory is small
     */
    @ConfField(mutable = false, masterOnly = false)
    public static long max_external_schema_cache_num = 10000;

    @ConfField(description = {
            "外部表元数据缓存对象在最后访问后过期的时间。",
            "The expiration time of a cache object after last access of it. For external meta cache."
    })
    public static long external_cache_expire_time_seconds_after_access = 86400L; // 24 hours

    @ConfField(description = {
            "外部表元数据缓存对象的自动刷新时间",
            "The auto refresh time of external meta cache."
    })
    public static long external_cache_refresh_time_minutes = 10; // 10 mins

    /**
     * Github workflow test type, for setting some session variables
     * only for certain test type. E.g. only settting batch_size to small
     * value for p0.
     */
    @ConfField(mutable = true, masterOnly = false, options = {"p0", "daily", "rqg", "external"})
    public static String fuzzy_test_type = "";

    /**
     * Set session variables randomly to check more issues in github workflow
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean use_fuzzy_session_variable = false;

    /**
     * Set config variables randomly to check more issues in github workflow
     */
    @ConfField(mutable = true, masterOnly = false)
    public static boolean use_fuzzy_conf = false;

    /**
     * Max num of same name meta informatntion in catalog recycle bin.
     * Default is 3.
     * 0 means do not keep any meta obj with same name.
     * < 0 means no limit
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int max_same_name_catalog_trash_num = 3;

    /**
     * NOTE: The storage policy is still under developement.
     */
    @ConfField(mutable = false, masterOnly = true)
    public static boolean enable_storage_policy = true;

    /**
     * This config is mainly used in the k8s cluster environment.
     * When enable_fqdn_mode is true, the name of the pod where be is located will remain unchanged
     * after reconstruction, while the ip can be changed.
     */
    @ConfField(mutable = false, varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_fqdn_mode = false;

    /**
     * If set to true, doris will try to parse the ddl of a hive view and try to execute the query
     * otherwise it will throw an AnalysisException.
     */
    @ConfField(mutable = true)
    public static boolean enable_query_hive_views = true;

    @ConfField(mutable = true)
    public static boolean enable_query_iceberg_views = true;

    /**
     * If set to true, doris will automatically synchronize hms metadata to the cache in fe.
     */
    @ConfField(masterOnly = true)
    public static boolean enable_hms_events_incremental_sync = false;

    /**
     * If set to true, doris will try to parse the ddl of a hive view and try to execute the query
     * otherwise it will throw an AnalysisException.
     */
    @ConfField(mutable = true, varType = VariableAnnotation.EXPERIMENTAL, description = {
            "当前默认设置为 false，开启后支持使用新优化器的load语句导入数据，失败后会降级旧的load语句。",
            "Now default set to true, After this function is enabled, the load statement of "
                    + "the new optimizer can be used to import data. If this function fails, "
                    + "the old load statement will be degraded."})
    public static boolean enable_nereids_load = false;

    /**
     * the plan cache num which can be reused for the next query
     */
    @ConfField(
            mutable = true,
            callbackClassString = "org.apache.doris.common.cache.NereidsSqlCacheManager$UpdateConfig",
            description = {
                "当前默认设置为 100，用来控制控制NereidsSqlCacheManager管理的sql cache数量。",
                "Now default set to 100, this config is used to control the number of "
                        + "sql cache managed by NereidsSqlCacheManager"
            }
    )
    public static int sql_cache_manage_num = 100;

    @ConfField(
            mutable = true,
            callbackClassString = "org.apache.doris.common.cache.NereidsSortedPartitionsCacheManager$UpdateConfig",
            description = {
                    "当前默认设置为 100，用来控制控制NereidsSortedPartitionsCacheManager中有序分区元数据的缓存个数，"
                            + "用于加速分区裁剪",
                    "The current default setting is 100, which is used to control the number of ordered "
                            + "partition metadata caches in NereidsSortedPartitionsCacheManager, "
                            + "and to accelerate partition pruning"
            }
    )
    public static int cache_partition_meta_table_manage_num = 100;

    /**
     * HBO plan stats. cache number which can be reused for the next query.
     */
    @ConfField(
            mutable = true,
            callbackClassString = "org.apache.doris.nereids.stats.MemoryHboPlanStatisticsProvider$UpdateConfig",
            description = {
                    "当前默认设置为 100000，用来控制控制MemoryHboPlanStatisticsProvider管理的plan stats. cache数量。",
                    "Now default set to 100000, this config is used to control the number of "
                            + "hbo plan stats. cache"
            }
    )
    public static int hbo_plan_stats_cache_num = 100000;

    /**
     * HBO plan recent runs entry number.
     */
    @ConfField(
            mutable = true,
            description = {
                    "当前默认设置为 10，用来控制控制MemoryHboPlanStatisticsProvider管理的plan stats. cache recent runs数量。",
                    "Now default set to 10, this config is used to control the number of "
                            + "hbo plan stats. cache recent runs' entry number."
            }
    )
    public static int hbo_plan_stats_cache_recent_runs_entry_num = 10;

    /**
     * Plan info cache number which is used for HboPlanInfoProvider.
     */
    @ConfField(
            mutable = true,
            callbackClassString = "org.apache.doris.nereids.stats.HboPlanInfoProvider$UpdateConfig",
            description = {
                    "当前默认设置为 1000，用来控制控制HboPlanInfoProvider管理的plan info cache数量。",
                    "Now default set to 1000, this config is used to control the number of "
                            + "hbo plan info cache"
            }
    )
    public static int hbo_plan_info_cache_num = 1000;

    /**
     * Maximum number of events to poll in each RPC.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static int hms_events_batch_size_per_rpc = 500;

    /**
     * HMS polling interval in milliseconds.
     */
    @ConfField(masterOnly = true)
    public static int hms_events_polling_interval_ms = 10000;

    /**
     * Maximum number of error tablets showed in broker load
     */
    @ConfField(masterOnly = true, mutable = true)
    public static int max_error_tablet_of_broker_load = 3;

    /**
     * If set to ture, doris will establish an encrypted channel based on the SSL protocol with mysql.
     */
    @ConfField(mutable = false, masterOnly = false, varType = VariableAnnotation.EXPERIMENTAL)
    public static boolean enable_ssl = false;

    /**
     * If set to ture, ssl connection needs to authenticate client's certificate.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static boolean ssl_force_client_auth = false;

    /**
     * ssl connection needs to authenticate client's certificate store type.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String ssl_trust_store_type = "PKCS12";

    /**
     * Default CA certificate file location for mysql ssl connection.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String mysql_ssl_default_ca_certificate =  EnvUtils.getDorisHome()
            + "/mysql_ssl_default_certificate/ca_certificate.p12";

    /**
     * Default server certificate file location for mysql ssl connection.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String mysql_ssl_default_server_certificate =  EnvUtils.getDorisHome()
            + "/mysql_ssl_default_certificate/server_certificate.p12";

    /**
     * Password for default CA certificate file.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String mysql_ssl_default_ca_certificate_password = "doris";

    /**
     * Password for default CA certificate file.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String mysql_ssl_default_server_certificate_password = "doris";

    /**
     * Used to set session variables randomly to check more issues in github workflow
     */
    @ConfField(mutable = true)
    public static int pull_request_id = 0;

    /**
     * Used to set default db transaction quota num.
     */
    @ConfField(mutable = true, masterOnly = true)
    public static long default_db_max_running_txn_num = -1;

    /**
     * Used by TokenManager to control the number of tokens keep in memory.
     * One token will keep alive for {token_queue_size * token_generate_period_hour} hours.
     * By defaults, one token will keep for 3 days.
     */
    @ConfField(mutable = false, masterOnly = true)
    public static int token_queue_size = 6;

    /**
     * TokenManager will generate token every token_generate_period_hour.
     */
    @ConfField(mutable = false, masterOnly = true)
    public static int token_generate_period_hour = 12;

    /**
     * The secure local path of the FE node the place the data which will be loaded in doris.
     * The default value is empty for this config which means this feature is not allowed.
     * User who want to load fe server local file should config the value to a right local path.
     */
    @ConfField(mutable = false, masterOnly = false)
    public static String mysql_load_server_secure_path = "";

    @ConfField(mutable = false, masterOnly = false)
    public static int mysql_load_in_memory_record = 20;

    @ConfField(mutable = false, masterOnly = false)
    public static int mysql_load_thread_pool = 4;

    /**
     * BDBJE file logging level
     * OFF, SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST, ALL
     */
    @ConfField
    public static String bdbje_file_logging_level = "INFO";

    /**
     * When holding lock time exceeds the threshold, need to report it.
     */
    @ConfField
    public static long lock_reporting_threshold_ms = 500L;

    /**
     * If true, auth check will be disabled. The default value is false.
     * This is to solve the case that user forgot the password.
     */
    @ConfField(mutable = false)
    public static boolean skip_localhost_auth_check  = true;

    @ConfField(mutable = true)
    public static boolean enable_round_robin_create_tablet = true;

    @ConfField(mutable = true, masterOnly = true, description = {
            "创建分区时，总是从第一个 BE 开始创建。注意：这种方式可能造成BE不均衡",
            "When creating tablet of a partition, always start from the first BE. "
                    + "Note: This method may cause BE imbalance"})
    public static boolean create_tablet_round_robin_from_start = false;

    /**
     * To prevent different types (V1, V2, V3) of behavioral inconsistencies,
     * we may delete the DecimalV2 and DateV1 types in the future.
     * At this stage, we use ‘disable_decimalv2’ and ‘disable_datev1’
     * to determine whether these two types take effect.
     */
    @ConfField(mutable = true)
    public static boolean disable_decimalv2  = true;

    @ConfField(mutable = true)
    public static boolean disable_datev1  = true;

    /*
     * This variable indicates the number of digits by which to increase the scale
     * of the result of division operations performed with the `/` operator. The
     * default value is 4, and it is currently only used for the DECIMALV3 type.
     */
    @ConfField(mutable = true)
    public static int div_precision_increment = 4;

    /**
     * This config used for export/outfile.
     * Whether delete all files in the directory specified by export/outfile.
     * It is a very dangerous operation, should only be used in test env.
     */

    @ConfField(mutable = false)
    public static boolean enable_delete_existing_files  = false;
    /*
     * The actual memory size taken by stats cache highly depends on characteristics of data, since on the different
     * dataset and scenarios the max/min literal's average size and buckets count of histogram would be highly
     * different. Besides, JVM version etc. also has influence on it, though not much as data itself.
     * Here I would give the mem size taken by stats cache with 10_0000 items.Each item's avg length of max/min literal
     * is 32, and the avg column name length is 16, and each column has a histogram with 128 buckets
     * In this case, stats cache takes total 911.954833984MiB mem.
     * If without histogram, stats cache takes total 61.2777404785MiB mem.
     * It's strongly discourage analyzing a column with a very large STRING value in the column, since it would cause
     * FE OOM.
     */
    @ConfField
    public static long stats_cache_size = 50_0000;

    /**
     * This config used for ranger cache data mask/row policy
     */
    @ConfField
    public static long ranger_cache_size = 10000;

    /**
     * This configuration is used to enable the statistics of query information, which will record
     * the access status of databases, tables, and columns, and can be used to guide the
     * optimization of table structures
     *
     */
    @ConfField(mutable = true)
    public static boolean enable_query_hit_stats = false;

    @ConfField(mutable = true, description = {
            "设置为 true，如果查询无法选择到健康副本时，会打印出该tablet所有副本的详细信息，" + "以及不可查询的具体原因。",
            "When set to true, if a query is unable to select a healthy replica, "
                    + "the detailed information of all the replicas of the tablet,"
                    + " including the specific reason why they are unqueryable, will be printed out."})
    public static boolean show_details_for_unaccessible_tablet = true;

    @ConfField(mutable = false, masterOnly = false, varType = VariableAnnotation.EXPERIMENTAL, description = {
            "是否启用binlog特性",
            "Whether to enable binlog feature"})
    public static boolean enable_feature_binlog = false;

    @ConfField(mutable = false, description = {
            "是否默认为 Database/Table 启用binlog特性",
            "Whether to enable binlog feature for Database/Table by default"})
    public static boolean force_enable_feature_binlog = false;

    @ConfField(mutable = false, masterOnly = false, varType = VariableAnnotation.EXPERIMENTAL, description = {
        "设置 binlog 消息最字节长度",
        "Set the maximum byte length of binlog message"})
    public static int max_binlog_messsage_size = 1024 * 1024 * 1024;

    @ConfField(mutable = true, masterOnly = true, description = {
            "是否禁止使用 WITH RESOURCE 语句创建 Catalog。",
            "Whether to disable creating catalog with WITH RESOURCE statement."})
    public static boolean disallow_create_catalog_with_resource = true;

    @ConfField(mutable = true, masterOnly = false, description = {
        "Hive行数估算分区采样数",
        "Sample size for hive row count estimation."})
    public static int hive_stats_partition_sample_size = 30;

    @ConfField(mutable = true, masterOnly = true, description = {
            "启用Hive分桶表",
            "Enable external hive bucket table"})
    public static boolean enable_create_hive_bucket_table = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Hive创建外部表默认指定的文件格式",
            "Default hive file format for creating table."})
    public static String hive_default_file_format = "orc";

    @ConfField
    public static int statistics_sql_parallel_exec_instance_num = 1;

    @ConfField
    public static long statistics_sql_mem_limit_in_bytes = 2L * 1024 * 1024 * 1024;

    @ConfField(mutable = true, masterOnly = true, description = {
            "用于强制设定内表的副本数，如果该参数大于零，则用户在建表时指定的副本数将被忽略，而使用本参数设置的值。"
                    + "同时，建表语句中指定的副本标签等参数会被忽略。该参数不影响包括创建分区、修改表属性的操作。该参数建议仅用于测试环境",
            "Used to force the number of replicas of the internal table. If the config is greater than zero, "
                    + "the number of replicas specified by the user when creating the table will be ignored, "
                    + "and the value set by this parameter will be used. At the same time, the replica tags "
                    + "and other parameters specified in the create table statement will be ignored. "
                    + "This config does not effect the operations including creating partitions "
                    + "and modifying table properties. "
                    + "This config is recommended to be used only in the test environment"})
    public static int force_olap_table_replication_num = 0;

    @ConfField(mutable = true, description = {
            "用于强制设定内表的副本分布，如果该参数不为空，则用户在建表或者创建分区时指定的副本数及副本标签将被忽略，而使用本参数设置的值。"
                    + "该参数影响包括创建分区、修改表属性、动态分区等操作。该参数建议仅用于测试环境",
            "Used to force set the replica allocation of the internal table. If the config is not empty, "
                    + "the replication_num and replication_allocation specified by the user when creating the table "
                    + "or partitions will be ignored, and the value set by this parameter will be used."
                    + "This config effect the operations including create tables, create partitions and create "
                    + "dynamic partitions. This config is recommended to be used only in the test environment"})
    public static String force_olap_table_replication_allocation = "";

    @ConfField
    public static int auto_analyze_simultaneously_running_task_num = 1;

    @Deprecated
    @ConfField
    public static final int period_analyze_simultaneously_running_task_num = 1;

    @ConfField(mutable = false)
    public static boolean allow_analyze_statistics_info_polluting_file_cache = true;

    @ConfField
    public static int cpu_resource_limit_per_analyze_task = 1;

    @ConfField(mutable = true)
    public static boolean force_sample_analyze = false; // avoid full analyze for performance reason

    @ConfField(mutable = true, description = {
            "Export任务允许的最大分区数量",
            "The maximum number of partitions allowed by Export job"})
    public static int maximum_number_of_export_partitions = 2000;

    @Deprecated
    @ConfField(mutable = true, description = {
            "Export任务允许的最大并行数",
            "The maximum parallelism allowed by Export job"})
    public static int maximum_parallelism_of_export_job = 50;

    @ConfField(mutable = true, description = {
            "是否用 mysql 的 bigint 类型来返回 Doris 的 largeint 类型",
            "Whether to use mysql's bigint type to return Doris's largeint type"})
    public static boolean use_mysql_bigint_for_largeint = false;

    @ConfField
    public static boolean forbid_running_alter_job = false;

    @ConfField(description = {
            "暂时性配置项，开启后会自动将所有的olap表修改为可light schema change",
            "temporary config filed, will make all olap tables enable light schema change"
    })
    public static boolean enable_convert_light_weight_schema_change = false;

    @ConfField(mutable = true, masterOnly = false, description = {
            "查询information_schema.metadata_name_ids表时,获取一个数据库中所有表用的时间",
            "When querying the information_schema.metadata_name_ids table,"
                    + " the time used to obtain all tables in one database"
    })
    public static long query_metadata_name_ids_timeout = 3;

    @ConfField(mutable = true, masterOnly = true, description = {
            "是否禁止LocalDeployManager删除节点",
            "Whether to disable LocalDeployManager drop node"})
    public static boolean disable_local_deploy_manager_drop_node = true;

    @ConfField(mutable = true, description = {
            "开启 file cache 后，一致性哈希算法中，每个节点的虚拟节点数。"
                    + "该值越大，哈希算法的分布越均匀，但是会增加内存开销。",
            "When file cache is enabled, the number of virtual nodes of each node in the consistent hash algorithm. "
                    + "The larger the value, the more uniform the distribution of the hash algorithm, "
                    + "but it will increase the memory overhead."})
    public static int split_assigner_virtual_node_number = 256;

    @ConfField(mutable = true, description = {
            "本地节点软亲缘性优化。尽可能地优先选取本地副本节点。",
            "Local node soft affinity optimization. Prefer local replication node."})
    public static boolean split_assigner_optimized_local_scheduling = true;

    @ConfField(mutable = true, description = {
            "随机算法最小的候选数目，会选取相对最空闲的节点。",
            "The random algorithm has the smallest number of candidates and will select the most idle node."})
    public static int split_assigner_min_random_candidate_num = 2;

    @ConfField(mutable = true, description = {
            "一致性哈希算法最小的候选数目，会选取相对最空闲的节点。",
            "The consistent hash algorithm has the smallest number of candidates and will select the most idle node."})
    public static int split_assigner_min_consistent_hash_candidate_num = 2;

    @ConfField(mutable = true, description = {
            "各节点之间最大的 split 数目差异，如果超过这个数目就会重新分布 split。",
            "The maximum difference in the number of splits between nodes. "
                    + "If this number is exceeded, the splits will be redistributed."})
    public static int split_assigner_max_split_num_variance = 1;

    @ConfField(description = {
            "控制统计信息的自动触发作业执行记录的持久化行数",
            "Determine the persist number of automatic triggered analyze job execution status"
    })
    public static long analyze_record_limit = 20000;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Auto Buckets中最小的buckets数目",
            "min buckets of auto bucket"
    })
    public static int autobucket_min_buckets = 1;

    @ConfField(mutable = true, masterOnly = true, description = {
        "Auto Buckets中最大的buckets数目",
        "max buckets of auto bucket"
    })
    public static int autobucket_max_buckets = 128;

    @ConfField(description = {"单个 FE 的 Arrow Flight Server 的最大连接数。",
            "Maximal number of connections of Arrow Flight Server per FE."})
    public static int arrow_flight_max_connections = 4096;

    @ConfField(mutable = true, masterOnly = true, description = {
        "Auto Buckets中按照partition size去估算bucket数，存算一体partition size 5G估算一个bucket，"
            + "但存算分离下partition size 10G估算一个bucket。 若配置小于0，会在在代码中会自适应存算一体模式默认5G，在存算分离默认10G",
        "In Auto Buckets, the number of buckets is estimated based on the partition size. "
            + "For storage and computing integration, a partition size of 5G is estimated as one bucket."
            + " but for cloud, a partition size of 10G is estimated as one bucket. "
            + "If the configuration is less than 0, the code will have an adaptive non-cloud mode with a default of 5G,"
            + " and in cloud mode with a default of 10G."
    })
    public static int autobucket_partition_size_per_bucket_gb = -1;

    @ConfField(mutable = true, masterOnly = true, description = {"Auto bucket中计算出的新的分区bucket num超过前一个分区的"
            + "bucket num的百分比，被认为是异常case报警",
            "The new partition bucket number calculated in the auto bucket exceeds the percentage "
            + "of the previous partition's bucket number, which is considered an abnormal case alert."})
    public static double autobucket_out_of_bounds_percent_threshold = 0.5;

    @ConfField(description = {"(已弃用，被 arrow_flight_max_connection 替代) Arrow Flight Server中所有用户token的缓存上限，"
            + "超过后LRU淘汰, arrow flight sql是无状态的协议，连接通常不会主动断开，"
            + "bearer token 从 cache 淘汰的同时会 unregister Connection.",
            "(Deprecated, replaced by arrow_flight_max_connection) The cache limit of all user tokens in "
            + "Arrow Flight Server. which will be eliminated by LRU rules after exceeding the limit, "
            + "arrow flight sql is a stateless protocol, the connection is usually not actively disconnected, "
            + "bearer token is evict from the cache will unregister ConnectContext."})
    public static int arrow_flight_token_cache_size = 4096;

    @ConfField(description = {"Arrow Flight Server中用户token的存活时间，自上次写入后过期时间，单位秒，默认值为86400，即1天",
            "The alive time of the user token in Arrow Flight Server, expire after write, unit second,"
            + "the default value is 86400, which is 1 days"})
    public static int arrow_flight_token_alive_time_second = 86400;

    @ConfField(mutable = true, description = {
            "Doris 为了兼用 mysql 周边工具生态，会内置一个名为 mysql 的数据库，如果该数据库与用户自建数据库冲突，"
            + "请修改这个字段，为 doris 内置的 mysql database 更换一个名字",
            "To ensure compatibility with the MySQL ecosystem, Doris includes a built-in database called mysql. "
            + "If this database conflicts with a user's own database, please modify this field to replace "
            + "the name of the Doris built-in MySQL database with a different name."})
    public static String mysqldb_replace_name = "mysql";

    @ConfField(description = {
        "设置允许跨域访问的特定域名,默认允许任何域名跨域访问",
        "Set the specific domain name that allows cross-domain access. "
            + "By default, any domain name is allowed cross-domain access"
    })
    public static String access_control_allowed_origin_domain = "*";

    @ConfField(description = {
            "开启java_udf, 默认为true。如果该配置为false，则禁止创建和使用java_udf。在一些场景下关闭该配置可防止命令注入攻击。",
            "Used to enable java_udf, default is true. if this configuration is false, creation and use of java_udf is "
                    + "disabled. in some scenarios it may be necessary to disable this configuration to prevent "
                    + "command injection attacks."
    })
    public static boolean enable_java_udf = true;

    @ConfField(mutable = true, masterOnly = true, description = {
        "开启后，可以在导入时，利用创建的全局java_udf函数处理数据, 默认为false。",
        "When enabled, data can be processed using the globally created java_udf function during import."
                + " The default setting is false."
    })
    public static boolean enable_udf_in_load = false;

    @ConfField(description = {
            "是否忽略 Image 文件中未知的模块。如果为 true，不在 PersistMetaModules.MODULE_NAMES 中的元数据模块将被忽略并跳过。"
                    + "默认为 false，如果 Image 文件中包含未知的模块，Doris 将会抛出异常。"
                    + "该参数主要用于降级操作中，老版本可以兼容新版本的 Image 文件。",
            "Whether to ignore unknown modules in Image file. "
                    + "If true, metadata modules not in PersistMetaModules.MODULE_NAMES "
                    + "will be ignored and skipped. Default is false, if Image file contains unknown modules, "
                    + "Doris will throw exception. "
                    + "This parameter is mainly used in downgrade operation, "
                    + "old version can be compatible with new version Image file."
    })
    public static boolean ignore_unknown_metadata_module = false;

    @ConfField(mutable = true, description = {
            "从主节点同步image文件的超时时间，用户可根据${meta_dir}/image文件夹下面的image文件大小和节点间的网络环境调整，"
                    + "单位为秒，默认值300",
            "The timeout for FE Follower/Observer synchronizing an image file from the FE Master, can be adjusted by "
                    + "the user on the size of image file in the ${meta_dir}/image and the network environment between "
                    + "nodes. The default values is 300."
    })
    public static int sync_image_timeout_second = 300;

    @ConfField(mutable = true, description = {
        "FE启动时加载image文件某个模块的二进制内容到字节数组，并将字节数组反序列化为utf8编码字符串时单批次（单位：byte, 至少500MB）"
            + "的大小。等于-1的值表示一次性读取完整的字节数组后反序列化反序列化为utf8编码字符串；"
            + "不等于-1的值（至少16MB）表示分批每次读取多大的字节数组后反序列化为utf8编码字符串，最后合并成完成的字符串。默认值为-1",
        "The size of a single batch (in bytes) when loading the binary content of a module of the "
            + "image file into a byte array and deserializing the byte array into a utf8 encoded string when FE starts."
            + " A value equal to -1 means reading the entire byte array at once and "
            + "then deserializing it into a utf8 encoded string; a value not equal to -1 means reading "
            + "a certain size (at least 16MB) of byte array in batches and then deserializing it into a "
            + "utf8 encoded string, and finally merging it into a completed string. The default value is -1"
    })
    public static int metadata_text_read_max_batch_bytes = -1;

    @ConfField(mutable = true, masterOnly = true)
    public static int publish_topic_info_interval_ms = 30000; // 30s

    @ConfField(mutable = true)
    public static int workload_sched_policy_interval_ms = 10000; // 10s

    @ConfField(mutable = true, masterOnly = true)
    public static int workload_group_check_interval_ms = 2000; // 2s

    @ConfField(mutable = true, masterOnly = true)
    public static int workload_max_policy_num = 25;

    @ConfField(mutable = true, masterOnly = true)
    public static int workload_max_condition_num_in_policy = 5;

    @ConfField(mutable = true, masterOnly = true)
    public static int workload_max_action_num_in_policy = 5; // mainly used to limit set session var action

    @ConfField(mutable = true)
    public static int workload_runtime_status_thread_interval_ms = 2000;

    // NOTE: it should bigger than be config report_query_statistics_interval_ms
    @ConfField(mutable = true)
    public static int query_audit_log_timeout_ms = 5000;

    @ConfField(description = {
            "在这个列表中的用户的操作，不会被记录到审计日志中。多个用户之间用逗号分隔。",
            "The operations of the users in this list will not be recorded in the audit log. "
                    + "Multiple users are separated by commas."
    })
    public static String skip_audit_user_list = "";

    @ConfField(mutable = true)
    public static int be_report_query_statistics_timeout_ms = 60000;

    @ConfField(mutable = true, masterOnly = true)
    public static int workload_group_max_num = 15;

    @ConfField(description = {"查询be wal_queue 的超时阈值(ms)",
            "the timeout threshold of checking wal_queue on be(ms)"})
    public static int check_wal_queue_timeout_threshold = 180000;   // 3 min

    @ConfField(mutable = true, masterOnly = true, description = {
            "对于自动分区表，防止用户意外创建大量分区，每个OLAP表允许的分区数量为`max_auto_partition_num`。默认2000。",
            "For auto-partitioned tables to prevent users from accidentally creating a large number of partitions, "
                    + "the number of partitions allowed per OLAP table is `max_auto_partition_num`. Default 2000."
    })
    public static int max_auto_partition_num = 2000;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Partition rebalance 方式下各个 BE 的 tablet 数最大差值，小于该值时，会诊断为已均衡",
            "The maximum difference in the number of tablets of each BE in partition rebalance mode. "
                    + "If it is less than this value, it will be diagnosed as balanced."
    })
    public static int diagnose_balance_max_tablet_num_diff = 50;

    @ConfField(mutable = true, masterOnly = true, description = {
            "Partition rebalance 方式下各个 BE 的 tablet 数的最大比率，小于该值时，会诊断为已均衡",
            "The maximum ratio of the number of tablets in each BE in partition rebalance mode. "
                    + "If it is less than this value, it will be diagnosed as balanced."
    })
    public static double diagnose_balance_max_tablet_num_ratio = 1.1;

    @ConfField(masterOnly = true, description = {
            "设置 root 用户初始化2阶段 SHA-1 加密密码，默认为''，即不设置 root 密码。"
                    + "后续 root 用户的 `set password` 操作会将 root 初始化密码覆盖。"
                    + "示例：如要配置密码的明文是 `root@123`，可在Doris执行SQL `select password('root@123')` "
                    + "获取加密密码 `*A00C34073A26B40AB4307650BFB9309D6BFA6999`",
            "Set root user initial 2-staged SHA-1 encrypted password, default as '', means no root password. "
                    + "Subsequent `set password` operations for root user will overwrite the initial root password. "
                    + "Example: If you want to configure a plaintext password `root@123`."
                    + "You can execute Doris SQL `select password('root@123')` to generate encrypted "
                    + "password `*A00C34073A26B40AB4307650BFB9309D6BFA6999`"})
    public static String initial_root_password = "";

    @ConfField(description = {"nereids trace文件的存放路径。",
            "The path of the nereids trace file."})
    public static String nereids_trace_log_dir = System.getenv("LOG_DIR") + "/nereids_trace";

    @ConfField(mutable = true, masterOnly = true, description = {
            "备份过程中，一个 upload 任务上传的快照数量上限，默认值为10个",
            "The max number of snapshots assigned to a upload task during the backup process, the default value is 10."
    })
    public static int backup_upload_snapshot_batch_size = 10;

    @ConfField(mutable = true, masterOnly = true, description = {
            "恢复过程中，一个 download 任务下载的快照数量上限，默认值为10个",
            "The max number of snapshots assigned to a download task during the restore process, "
            + "the default value is 10."
    })
    public static int restore_download_snapshot_batch_size = 10;

    @ConfField(mutable = true, masterOnly = true, description = {
            "备份恢复过程中，单次 RPC 分配给每个be的任务最大个数，默认值为10000个。",
            "The max number of batched tasks per RPC assigned to each be during the backup/restore process, "
            + "the default value is 10000."
    })
    public static int backup_restore_batch_task_num_per_rpc = 10000;

    @ConfField(mutable = true, masterOnly = true, description = {
            "一个 BE 同时执行的恢复任务的并发数",
            "The number of concurrent restore tasks per be"})
    public static int restore_task_concurrency_per_be = 5000;

    @ConfField(mutable = true, description = {"执行 agent task 时，BE心跳超过多长时间，认为BE不可用",
            "The time after which BE is considered unavailable if the heartbeat is not received"})
    public static int agent_task_be_unavailable_heartbeat_timeout_second = 300;

    @ConfField(description = {"是否开启通过http接口获取log文件的功能",
            "Whether to enable the function of getting log files through http interface"})
    public static boolean enable_get_log_file_api = false;

    @ConfField(mutable = true)
    public static boolean enable_profile_when_analyze = false;
    @ConfField(mutable = true)
    public static boolean enable_collect_internal_query_profile = false;

    @ConfField(mutable = false, masterOnly = false, description = {
        "http请求处理/api/query中sql任务的最大线程池。",
        "The max number work threads of http sql submitter."
    })
    public static int http_sql_submitter_max_worker_threads = 2;

    @ConfField(mutable = false, masterOnly = false, description = {
        "http请求处理/api/upload任务的最大线程池。",
        "The max number work threads of http upload submitter."
    })
    public static int http_load_submitter_max_worker_threads = 2;

    @ConfField(mutable = true, masterOnly = true, description = {
            "load label个数阈值，超过该个数后，对于已经完成导入作业或者任务，"
            + "其label会被删除，被删除的 label 可以被重用。 值为 -1 时，表示此阈值不生效。",
            "The threshold of load labels' number. After this number is exceeded, "
                    + "the labels of the completed import jobs or tasks will be deleted, "
                    + "and the deleted labels can be reused. "
                    + "When the value is -1, it indicates no threshold."
    })
    public static int label_num_threshold = 2000;

    @ConfField(description = {"指定 internal catalog 的默认鉴权类",
            "Specify the default authentication class of internal catalog"},
            options = {"default", "ranger-doris"})
    public static String access_controller_type = "default";

    /* https://forums.oracle.com/ords/apexds/post/je-log-checksumexception-2812
      when meeting disk damage or other reason described in the oracle forums
      and fe cannot start due to `com.sleepycat.je.log.ChecksumException`, we
      add a param `ignore_bdbje_log_checksum_read` to ignore the exception, but
      there is no guarantee of correctness for bdbje kv data
    */
    @ConfField
    public static boolean ignore_bdbje_log_checksum_read = false;

    @ConfField(description = {"指定 mysql登录身份认证类型",
            "Specifies the authentication type"},
            options = {"default", "ldap"})
    public static String authentication_type = "default";

    @ConfField(mutable = true, masterOnly = false, description = {"指定 trino-connector catalog 的插件默认加载路径",
            "Specify the default plugins loading path for the trino-connector catalog"})
    public static String trino_connector_plugin_dir = EnvUtils.getDorisHome() + "/plugins/connectors";

    @ConfField(mutable = true)
    public static boolean fix_tablet_partition_id_eq_0 = false;

    @ConfField(mutable = true, masterOnly = true, description = {
            "倒排索引默认存储格式",
            "Default storage format of inverted index, the default value is V1."
    })
    public static String inverted_index_storage_format = "V2";

    @ConfField(mutable = true, masterOnly = true, description = {
            "是否在unique表mow上开启delete语句写delete predicate。若开启，会提升delete语句的性能，"
                    + "但delete后进行部分列更新可能会出现部分数据错误的情况。若关闭，会降低delete语句的性能来保证正确性。",
            "Enable the 'delete predicate' for DELETE statements. If enabled, it will enhance the performance of "
                    + "DELETE statements, but partial column updates after a DELETE may result in erroneous data. "
                    + "If disabled, it will reduce the performance of DELETE statements to ensure accuracy."
    })
    public static boolean enable_mow_light_delete = false;

    @ConfField(description = {
            "是否开启 Proxy Protocol 支持",
            "Whether to enable proxy protocol"
    })
    public static boolean enable_proxy_protocol = false;

    @ConfField(description = {
            "Profile 异步收集过期时间，在 query 完成后，如果在该参数指定的时长内 profile 没有收集完成，则未完成的 profile 会被放弃。",
            "Profile async collect expire time, after the query is completed, if the profile is not collected within "
                    + " the time specified by this parameter, the uncompleted profile will be abandoned."
    })
    public static int profile_async_collect_expire_time_secs = 5;

    @ConfField(description = {
            "用于控制 ProfileManager 进行 Profile 垃圾回收的间隔时间，垃圾回收期间 ProfileManager 会把多余的以及过期的 profile "
                    + "从内存和磁盘中清理掉，节省内存。",
            "Used to control the interval time of ProfileManager for profile garbage collection. "
    })
    public static int profile_manager_gc_interval_seconds = 1;
    // Used to check compatibility when upgrading.
    @ConfField
    public static boolean enable_check_compatibility_mode = false;

    // Do checkpoint after replaying edit logs.
    @ConfField
    public static boolean checkpoint_after_check_compatibility = false;

    // Advance the next id before transferring to the master.
    @ConfField(description = {
            "是否在成为 Master 后推进 ID 分配器，保证即使回滚元数据时，它也不会回滚",
            "Whether to advance the ID generator after becoming Master to ensure that the id "
                    + "generator will not be rolled back even when metadata is rolled back."
    })
    public static boolean enable_advance_next_id = true;

    // The count threshold to do manual GC when doing checkpoint but not enough memory.
    // Set zero to disable it.
    @ConfField(description = {
            "如果 checkpoint 连续多次因内存不足而无法进行时，先尝试手动触发 GC",
            "The threshold to do manual GC when doing checkpoint but not enough memory"})
    public static int checkpoint_manual_gc_threshold = 0;

    @ConfField(mutable = true, description = {
            "是否在每个请求开始之前打印一遍请求内容, 主要是query语句",
            "Should the request content be logged before each request starts, specifically the query statements"})
    public static boolean enable_print_request_before_execution = false;

    @ConfField
    public static String spilled_profile_storage_path = System.getenv("LOG_DIR") + File.separator + "profile";

    @ConfField
    public static String spilled_minidump_storage_path = System.getenv("LOG_DIR") + File.separator + "minidump";

    // The max number of profiles that can be stored to storage.
    @ConfField
    public static int max_spilled_profile_num = 500;

    // The total size of profiles that can be stored to storage.
    @ConfField
    public static long spilled_profile_storage_limit_bytes = 1 * 1024 * 1024 * 1024; // 1GB

    // Profile will be spilled to storage after query has finished for this time.
    @ConfField(mutable = true, description = {
            "Profile 在 query 完成后等待多久后才会被写入磁盘",
            "Profile will be spilled to storage after query has finished for this time"})
    public static int profile_waiting_time_for_spill_seconds = 10;

    @ConfField(mutable = true, description = {
            "是否通过检测协调者BE心跳来 abort 事务",
            "SHould abort txn by checking coorinator be heartbeat"})
    public static boolean enable_abort_txn_by_checking_coordinator_be = true;

    @ConfField(mutable = true, description = {
            "是否在 schema change 过程中, 检测冲突事物并 abort 它",
            "SHould abort txn by checking conflick txn in schema change"})
    public static boolean enable_abort_txn_by_checking_conflict_txn = true;

    @ConfField(mutable = true, description = {
            "内表自动收集时间间隔，当某一列上次收集时间距离当前时间大于该值，则会触发一次新的收集，0表示不会触发。",
            "Columns that have not been collected within the specified interval will trigger automatic analyze. "
                + "0 means not trigger."
    })
    public static long auto_analyze_interval_seconds = 86400; // 24 hours.

    // A internal config to control whether to enable the checkpoint.
    //
    // ATTN: it only used in test environment.
    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_checkpoint = true;

    @ConfField(description = {
        "存放 hadoop conf 配置文件的默认目录。",
        "The default directory for storing hadoop conf configuration files."})
    public static String hadoop_config_dir = EnvUtils.getDorisHome() + "/plugins/hadoop_conf/";

    @ConfField(mutable = true, masterOnly = true, description = {"字典相关的 RPC 的超时时间",
            "Timeout of dictionary related RPC"})
    public static int dictionary_rpc_timeout_seconds = 5;

    @ConfField(mutable = true, masterOnly = true, description = { "字典触发数据过期检查的时间间隔，单位为秒",
            "Interval at which the dictionary triggers a data expiration check, in seconds" })
    public static int dictionary_auto_refresh_interval_seconds = 5;

    //==========================================================================
    //                    begin of cloud config
    //==========================================================================
    @ConfField(description = {"是否启用FE 日志文件按照大小删除策略，当日志大小超过指定大小，删除相关的log。默认为按照时间策略删除",
        "Whether to enable the FE log file deletion policy based on size, "
            + "where logs exceeding the specified size are deleted. "
            + "It is disabled by default and follows a time-based deletion policy."},
            options = {"age", "size"})
    public static String log_rollover_strategy = "age";

    @ConfField public static int info_sys_accumulated_file_size = 4;
    @ConfField public static int warn_sys_accumulated_file_size = 2;
    @ConfField public static int audit_sys_accumulated_file_size = 4;

    @ConfField
    public static String deploy_mode = "";

    // compatibily with elder version.
    // cloud_unique_id has higher priority than cluster_id.
    @ConfField
    public static String cloud_unique_id = "";

    public static boolean isCloudMode() {
        return deploy_mode.equals("cloud") || !cloud_unique_id.isEmpty();
    }

    public static boolean isNotCloudMode() {
        return !isCloudMode();
    }

    /**
     * MetaService endpoint, ip:port, such as meta_service_endpoint = "192.0.0.10:8866"
     *
     * If you want to access a group of meta services, separated the endpoints by comma,
     * like "host-1:port,host-2:port".
     */
    @ConfField(mutable = true, callback = CommaSeparatedIntersectConfHandler.class)
    public static String meta_service_endpoint = "";

    @ConfField(mutable = true)
    public static boolean meta_service_connection_pooled = true;

    @ConfField(mutable = true)
    public static int meta_service_connection_pool_size = 20;

    @ConfField(mutable = true)
    public static int meta_service_rpc_retry_times = 200;

    public static int metaServiceRpcRetryTimes() {
        if (isCloudMode() && enable_check_compatibility_mode) {
            return 1;
        }
        return meta_service_rpc_retry_times;
    }

    // A connection will expire after a random time during [base, 2*base), so that the FE
    // has a chance to connect to a new RS. Set zero to disable it.
    @ConfField(mutable = true)
    public static int meta_service_connection_age_base_minutes = 5;

    @ConfField(mutable = false)
    public static boolean enable_sts_vpc = true;

    @ConfField(mutable = true)
    public static int sts_duration = 3600;

    @ConfField(mutable = true)
    public static int drop_rpc_retry_num = 200;

    @ConfField
    public static int default_get_version_from_ms_timeout_second = 3;

    @ConfField(mutable = true)
    public static boolean enable_cloud_multi_replica = false;

    @ConfField(mutable = true)
    public static int cloud_replica_num = 3;

    @ConfField(mutable = true)
    public static int cloud_cold_read_percent = 10; // 10%

    @ConfField(mutable = true)
    public static int get_tablet_stat_batch_size = 1000;

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_light_index_change = true;

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_create_bitmap_index_as_inverted_index = true;

    // The original meta read lock is not enough to keep a snapshot of partition versions,
    // so the execution of `createScanRangeLocations` are delayed to `Coordinator::exec`,
    // to help to acquire a snapshot of partition versions.
    @ConfField
    public static boolean enable_cloud_snapshot_version = true;

    // Interval in seconds for checking the status of compute groups (cloud clusters).
    // Compute groups and cloud clusters refer to the same concept.
    @ConfField
    public static int cloud_cluster_check_interval_second = 10;

    @ConfField
    public static String cloud_sql_server_cluster_name = "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER";

    @ConfField
    public static String cloud_sql_server_cluster_id = "RESERVED_CLUSTER_ID_FOR_SQL_SERVER";

    @ConfField
    public static int cloud_txn_tablet_batch_size = 50;

    /**
     * Default number of waiting copy jobs for the whole cluster
     */
    @ConfField(mutable = true)
    public static int cluster_max_waiting_copy_jobs = 100;

    /**
     * Default number of max file num for per copy into job
     */
    @ConfField(mutable = true)
    public static int max_file_num_per_copy_into_job = 50;

    /**
     * Default number of max meta size for per copy into job
     */
    @ConfField(mutable = true)
    public static int max_meta_size_per_copy_into_job = 51200;

    // 0 means no limit
    @ConfField(mutable = true)
    public static int cloud_max_copy_job_per_table = 10000;

    @ConfField(mutable = true)
    public static int cloud_filter_copy_file_num_limit = 100;

    @ConfField(mutable = true, masterOnly = true)
    public static boolean cloud_delete_loaded_internal_stage_files = false;

    @ConfField(mutable = false)
    public static int cloud_copy_txn_conflict_error_retry_num = 5;

    @ConfField(mutable = false)
    public static int cloud_copy_into_statement_submitter_threads_num = 64;

    @ConfField
    public static int drop_user_notify_ms_max_times = 86400;

    @ConfField(mutable = true, masterOnly = true)
    public static long cloud_tablet_rebalancer_interval_second = 20;

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_cloud_partition_balance = true;

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_cloud_table_balance = true;

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_cloud_global_balance = true;

    @ConfField(mutable = true, masterOnly = true)
    public static int cloud_pre_heating_time_limit_sec = 300;

    @ConfField(mutable = true, masterOnly = true)
    public static double cloud_rebalance_percent_threshold = 0.05;

    @ConfField(mutable = true, masterOnly = true)
    public static long cloud_rebalance_number_threshold = 2;

    @ConfField(mutable = true, masterOnly = true)
    public static double cloud_balance_tablet_percent_per_run = 0.05;

    @ConfField(mutable = true, masterOnly = true)
    public static int cloud_min_balance_tablet_num_per_run = 2;

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_cloud_warm_up_for_rebalance = true;

    @ConfField(mutable = true, masterOnly = false)
    public static String security_checker_class_name = "";

    @ConfField(mutable = true)
    public static int mow_calculate_delete_bitmap_retry_times = 10;

    @ConfField(mutable = true, description = {"指定S3 Load endpoint白名单, 举例: s3_load_endpoint_white_list=a,b,c",
            "the white list for the s3 load endpoint, if it is empty, no white list will be set,"
            + "for example: s3_load_endpoint_white_list=a,b,c"})
    public static String[] s3_load_endpoint_white_list = {};

    @ConfField(mutable = true, description = {
            "此参数控制是否强制使用 Azure global endpoint。默认值为 false，系统将使用用户指定的 endpoint。"
            + "如果设置为 true，系统将强制使用 {account}.blob.core.windows.net。",
            "This parameter controls whether to force the use of the Azure global endpoint. "
            + "The default is false, meaning the system will use the user-specified endpoint. "
            + "If set to true, the system will force the use of {account}.blob.core.windows.net."
    })
    public static boolean force_azure_blob_global_endpoint = false;

    @ConfField(mutable = true, description = {"指定Jdbc driver url白名单, 举例: jdbc_driver_url_white_list=a,b,c",
            "the white list for jdbc driver url, if it is empty, no white list will be set"
            + "for example: jdbc_driver_url_white_list=a,b,c"
    })
    public static String[] jdbc_driver_url_white_list = {};

    @ConfField(description = {"Stream_Load 导入时，label 被限制的最大长度",
            "Stream_Load When importing, the maximum length of label is limited"})
    public static int label_regex_length = 128;

    @ConfField(mutable = true, masterOnly = true)
    public static int history_cloud_warm_up_job_keep_max_second = 7 * 24 * 3600;

    @ConfField(mutable = true, masterOnly = true)
    public static int max_active_cloud_warm_up_job = 10;

    @ConfField(mutable = true, masterOnly = true)
    public static int cloud_warm_up_timeout_second = 86400 * 30; // 30 days

    @ConfField(mutable = true, masterOnly = true)
    public static int cloud_warm_up_job_scheduler_interval_millisecond = 1000; // 1 seconds

    @ConfField(mutable = true, masterOnly = true)
    public static long cloud_warm_up_job_max_bytes_per_batch = 21474836480L; // 20GB

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_fetch_cluster_cache_hotspot = true;

    @ConfField(mutable = true)
    public static long fetch_cluster_cache_hotspot_interval_ms = 3600000;
    // to control the max num of values inserted into cache hotspot internal table
    // insert into cache table when the size of batch values reaches this limit
    @ConfField(mutable = true)
    public static long batch_insert_cluster_cache_hotspot_num = 5000;

    /**
     * intervals between be status checks for CloudUpgradeMgr
     */
    @ConfField(mutable = true)
    public static int cloud_upgrade_mgr_interval_second = 15;

    @ConfField(mutable = true)
    public static boolean enable_cloud_running_txn_check = true;

    //* audit_event_log_queue_size = qps * query_audit_log_timeout_ms
    @ConfField(mutable = true)
    public static int audit_event_log_queue_size = 250000;

    @ConfField(mutable = true, description = {
            "streamload导入使用的转发策略, 可选值为public-private/public/private/direct/random-be/空",
            "streamload route policy, available options are "
            + "public-private/public/private/direct/random-be and empty string" })
    public static String streamload_redirect_policy = "";

    @ConfField(description = {"存算分离模式下建表是否检查残留recycler key, 默认true",
        "create table in cloud mode, check recycler key remained, default true"})
    public static boolean check_create_table_recycle_key_remained = true;

    @ConfField(mutable = true, description = {"存算分离模式下fe向ms请求锁的过期时间，默认60s"})
    public static int delete_bitmap_lock_expiration_seconds = 60;

    @ConfField(mutable = true, description = {"存算分离模式下calculate delete bitmap task 超时时间，默认60s"})
    public static int calculate_delete_bitmap_task_timeout_seconds = 60;

    @ConfField(mutable = true, description = {"存算分离模式下事务导入calculate delete bitmap task 超时时间，默认300s"})
    public static int calculate_delete_bitmap_task_timeout_seconds_for_transaction_load = 300;

    @ConfField(mutable = true, description = {"存算分离模式下commit阶段等锁超时时间，默认5s"})
    public static int try_commit_lock_timeout_seconds = 5;

    @ConfField(mutable = true, description = {"是否在事务提交时对所有表启用提交锁。设置为 true 时，所有表都会使用提交锁。"
            + "设置为 false 时，仅对 Merge-On-Write 表使用提交锁。默认值为 true。",
            "Whether to enable commit lock for all tables during transaction commit."
            + "If true, commit lock will be applied to all tables."
            + "If false, commit lock will only be applied to Merge-On-Write tables."
            + "Default value is true." })
    public static boolean enable_commit_lock_for_all_tables = true;

    @ConfField(mutable = true, description = {"存算分离模式下是否开启大事务提交，默认false"})
    public static boolean enable_cloud_txn_lazy_commit = false;

    @ConfField(mutable = true, masterOnly = true,
            description = {"存算分离模式下，当tablet分布的be异常，是否立即映射tablet到新的be上，默认false"})
    public static boolean enable_immediate_be_assign = false;

    @ConfField(mutable = true, masterOnly = false,
            description = { "存算分离模式下，一个BE挂掉多长时间后，它的tablet彻底转移到其他BE上" })
    public static int rehash_tablet_after_be_dead_seconds = 3600;


    @ConfField(mutable = true, description = {"存算分离模式下是否启用自动启停功能，默认true",
        "Whether to enable the automatic start-stop feature in cloud model, default is true."})
    public static boolean enable_auto_start_for_cloud_cluster = true;

    @ConfField(mutable = true, description = {"存算分离模式下自动启停等待cluster唤醒退避重试次数，默认300次大约5分钟",
        "The automatic start-stop wait time for cluster wake-up backoff retry count in the cloud "
            + "model is set to 300 times, which is approximately 5 minutes by default."})
    public static int auto_start_wait_to_resume_times = 300;

    @ConfField(description = {"Get tablet stat task的最大并发数。",
        "Maximal concurrent num of get tablet stat job."})
    public static int max_get_tablet_stat_task_threads_num = 4;

    @ConfField(mutable = true, description = {"存算分离模式下schema change失败是否重试",
            "Whether to enable retry when schema change failed in cloud model, default is true."})
    public static boolean enable_schema_change_retry_in_cloud_mode = true;

    @ConfField(mutable = true, description = {"存算分离模式下schema change重试次数",
            "Max retry times when schema change failed in cloud model, default is 3."})
    public static int schema_change_max_retry_time = 3;

    @ConfField(mutable = true, description = {"是否允许使用ShowCacheHotSpotStmt语句",
            "Whether to enable the use of ShowCacheHotSpotStmt, default is false."})
    public static boolean enable_show_file_cache_hotspot_stmt = false;

    @ConfField(mutable = true, description = {"存算分离模式下FE连接meta service的请求超时, 默认30000ms",
            "Request timeout for FE connecting to meta service in cloud mode, default is 30000ms."})
    public static int meta_service_brpc_timeout_ms = 30000;

    @ConfField(mutable = true, description = {"存算分离模式下FE连接meta service的连接超时，默认500ms",
            "Connection timeout for FE connecting to meta service in cloud mode., default is 500ms."})
    public static int meta_service_brpc_connect_timeout_ms = 500;

    @ConfField(mutable = true, description = {"存算分离模式下FE请求meta service超时的重试次数，默认1次",
            "In cloud mode, the retry number when the FE requests the meta service times out is 1 by default"})
    public static int meta_service_rpc_timeout_retry_times = 1;

    @ConfField(mutable = true, description = {"存算分离模式下自动启停功能，对于该配置中的数据库名不进行唤醒操作，"
            + "用于内部作业的数据库，例如统计信息用到的数据库，"
            + "举例: auto_start_ignore_db_names=__internal_schema, information_schema",
            "In the cloud mode, the automatic start and stop ignores the DB name of the internal job,"
            + "used for databases involved in internal jobs, such as those used for statistics, "
            + "For example: auto_start_ignore_db_names=__internal_schema, information_schema"
            })
    public static String[] auto_start_ignore_resume_db_names = {"__internal_schema", "information_schema"};

    @ConfField(mutable = true, masterOnly = true)
    public static boolean enable_mow_load_force_take_ms_lock = true;

    @ConfField(mutable = true, masterOnly = true)
    public static long mow_load_force_take_ms_lock_threshold_ms = 500;

    @ConfField(mutable = true, masterOnly = true)
    public static long mow_get_ms_lock_retry_backoff_base = 20;

    @ConfField(mutable = true, masterOnly = true)
    public static long mow_get_ms_lock_retry_backoff_interval = 80;

    // ATTN: DONOT add any config not related to cloud mode here
    // ATTN: DONOT add any config not related to cloud mode here
    // ATTN: DONOT add any config not related to cloud mode here
    //==========================================================================
    //                      end of cloud config
    //==========================================================================
    //==========================================================================
    //                      start of lock config
    @ConfField(description = {"是否开启死锁检测",
            "Whether to enable deadlock detection"})
    public static boolean enable_deadlock_detection = true;

    @ConfField(description = {"死锁检测间隔时间，单位分钟",
            "Deadlock detection interval time, unit minute"})
    public static long deadlock_detection_interval_minute = 5;

    @ConfField(mutable = true, description = {"表示最大锁持有时间，超过该时间会打印告警日志，单位秒",
            "Maximum lock hold time; logs a warning if exceeded"})
    public static long max_lock_hold_threshold_seconds = 10;

    @ConfField(mutable = true, description = {"元数据同步是否开启安全模式",
        "Is metadata synchronization enabled in safe mode"})
    public static boolean meta_helper_security_mode = false;

    @ConfField(description = {"检查资源就绪的周期，单位秒",
            "Interval checking if resource is ready"})
    public static long resource_not_ready_sleep_seconds = 5;

    @ConfField(mutable = true, description = {
            "设置为 true，如果查询无法选择到健康副本时，会打印出该tablet所有副本的详细信息，"})
    public static boolean sql_block_rule_ignore_admin = false;

    @ConfField(description = {"认证插件目录",
            "Authentication plugin directory"})
    public static String authentication_plugins_dir = EnvUtils.getDorisHome() + "/plugins/authentication";

    @ConfField(description = {"鉴权插件目录",
            "Authorization plugin directory"})
    public static String authorization_plugins_dir = EnvUtils.getDorisHome() + "/plugins/authorization";

    @ConfField(description = {
            "鉴权插件配置文件路径，需在 DORIS_HOME 下，默认为 conf/authorization.conf",
            "Authorization plugin configuration file path, need to be in DORIS_HOME,"
                    + "default is conf/authorization.conf"})
    public static String authorization_config_file_path = "/conf/authorization.conf";

    @ConfField(description = {
            "认证插件配置文件路径，需在 DORIS_HOME 下，默认为 conf/authentication.conf",
            "Authentication plugin configuration file path, need to be in DORIS_HOME,"
                    + "default is conf/authentication.conf"})
    public static String authentication_config_file_path = "/conf/authentication.conf";

    @ConfField(description = {"用于测试，强制将所有的查询forward到master以验证forward query的行为",
            "For testing purposes, all queries are forcibly forwarded to the master to verify"
                    + "the behavior of forwarding queries."})
    public static boolean force_forward_all_queries = false;

    @ConfField(description = {"用于禁用某些SQL，配置项为AST的class simple name列表(例如CreateRepositoryStmt,"
            + "CreatePolicyCommand)，用逗号间隔开",
            "For disabling certain SQL queries, the configuration item is a list of simple class names of AST"
                    + "(for example CreateRepositoryStmt, CreatePolicyCommand), separated by commas."})
    public static String block_sql_ast_names = "";

    public static long meta_service_rpc_reconnect_interval_ms = 5000;

    public static long meta_service_rpc_retry_cnt = 10;

    @ConfField(mutable = true, masterOnly = true, description = {"是否允许 variant 类型的列使用倒排索引格式 v1",
            "Whether to allow the use of inverted index v1 for variant"})
    public static boolean enable_inverted_index_v1_for_variant = false;

    @ConfField(mutable = true, description = {"Prometheus 输出表维度指标的个数限制",
            "Prometheus output table dimension metric count limit"})
    public static int prom_output_table_metrics_limit = 10000;
}
