# File cache 主动增量预热

## 背景

D 项目需要构建跨 AZ（可用区）的主备集群架构，以确保在主集群发生故障时，能够快速切换到备集群，并提供与主集群接近的查询性能。这要求备集群的缓存（cache）状态与主集群保持高度一致，涵盖查询数据缓存和导入数据缓存的同步。

此外，还需要在 Doris 存算分离架构下，实现读写集群的解耦——即一个集群专注于写入（写集群），另一个集群专注于查询（读集群），以提升整体性能。但这一架构带来了新的挑战：写入数据后，如何确保 File Cache 及时同步到读集群，以避免因本地缓存缺失导致的查询性能抖动。

## 需求

无论是主备集群切换，还是读写分离模式，都需要确保导入数据能够实时同步到目标集群，并触发缓存更新。此外，在主备集群架构下，还需额外解决查询数据缓存的同步问题，以保证备集群能够在故障切换时迅速接管负载，并提供稳定的查询性能。

1. 导入数据 Cache 同步
- 在 读写分离架构 下，保证读集群能够高效获取写集群的数据，减少查询抖动，提升访问效率。
- 需要涵盖 数据导入（Load）、Compaction 以及 Schema Change 操作所产生的数据同步，确保所有数据变更都能及时反映到对端集群。
1. 查询数据 Cache 同步
- 在 主备集群架构 下，确保备集群能够及时同步主集群的查询数据 Cache，以便在故障切换时提供稳定的查询性能。

## 总体方案

1. 导入数据 Cache 同步：每次数据导入（Load）、Compaction、Schema Change 操作完成后，触发数据同步机制，将变更数据的缓存同步到对端集群。
1. 查询数据 Cache 同步：采用周期性 Cluster-to-Cluster 数据同步机制，将查询过程中产生的缓存数据同步到目标集群。
1. 同步关系管理：统一到 WARM UP 语法，将同步关系抽象成 WARM UP JOB 来管理，支持 SHOW 和 CANCEL。

## 测试用例设计

File Cache 主动增量预热 - 测试用例设计

## 详细设计

### 同步关系管理

#### 产品规格

A->B, C->B能不能做

#### 创建同步关系

复用现有的 WARM UP 语法，并通过 properties 指定同步模式，以支持不同的同步触发方式：

1. 一次性同步（One-Time）
- 适用于手动触发的单次同步，例如新集群上线时进行全量数据预热。
1. 周期性同步（Periodic）
- 适用于定期同步缓存数据，例如按分钟或小时级别同步查询缓存，以保证热点数据在对端集群可用。
1. 事件触发同步（Event-Driven）
- 适用于导入数据、Compaction 或 Schema Change 后的自动触发，确保数据变更后及时同步缓存，减少查询性能抖动。

如何指定 CLUSTER

同步关系使用 CLUSTER NAME 而不是 CLUSTER ID 来指定 CLUSTER。CLUSTER 改名或删除不会改变同步关系中的记录。新集群使用了旧的同步关系中记录的名字，将继承这个同步关系。

理想的 WARM UP 语法扩展：

```sql
WARM UP <target_cluster> FROM <source_cluster>
PROPERTIES (
    "sync_mode" = "event_driven",  -- 可选：once（默认）/ periodic / event_driven
    "sync_interval" = "10m"        -- 仅适用于周期性同步，单位：秒/分钟/小时
);
```

实际的 WARM UP 语法扩展：

现有的，一次性的

```sql
-- cluster 级别
WARM UP COMPUTE GROUP <target_cluster> WITH COMPUTE GROUP <source_cluster>;

-- 指定库表分区
WARM UP COMPUTE GROUP <target_cluster> WITH TABLE <table_name>
        [PARTITION 'partition_name'];
```

新增的，周期性的（如果一次同步消耗了15分钟，周期是10分钟，本次同步结束后立刻开始下一次同步）

```sql
WARM UP COMPUTE GROUP <target_cluster> WITH COMPUTE GROUP <source_cluster>
        PROPERTIES (
            "sync_mode" = "periodic",   -- 周期性的 warm up job
            "sync_interval_sec" = "600" -- 单位是秒
        )；
```

新增的，事件触发的

```sql
WARM UP COMPUTE GROUP <target_cluster> WITH COMPUTE GROUP <source_cluster>
        PROPERTIES (
            "sync_mode" = "event_driven",  -- 事件触发的 warm up job
            "sync_event" = "load"          -- load 包含 compaction 和 sc，后续可能扩展 query
        )；
```

接口

```thrift
CacheHotspotManager {
    long createJob(WarmUpClusterStmt stmt);
    void cancel(long jobId);
}

// isForce = false
// properties 里面写 sync_mode、sync_interval_sec、sync_event 之类的 key
WarmUpClusterStmt(String dstClusterName, String srcClusterName, boolean isForce,
            Map<String, String> properties);
```

#### 存储同步关系

同步关系在 FE 上存储成一个 CloudWarmUpJob，需要扩展 JobType，根据 JobType 改变执行逻辑

#### 展示同步关系

```sql
SHOW WARM UP JOB;
SHOW WARM UP JOB WHERE ID = 13418;
```

| 列名 | 说明 | 备注 |
| --- | --- | --- |
| JobId | Job id |   |
| ComputeGroup | 目的ComputeGroup |   |
| SrcComputeGroup | 源ComputeGroup | 新增 |
| Status | 状态：PENDING, RUNNING, FINISHED, CANCELLED, DELETED |   |
| Type | 类型：CLUSTER，TABLE |   |
| SyncMode | ONCE / PERIODIC(100s) / EVENT_DRIVEN(LOAD) | 新增 |
| CreateTime | 创建时间 |   |
| StartTime | 上一次执行开始时间 | 新增 |
| FinishBatch | 完成的batch数据，10G数据一个batch |   |
| AllBatch | 总batch数 |   |
| FinishTime | job完成时间 |   |
| ErrMsg | 错误信息 |   |

#### 删除同步关系

复用原先取消 WARM UP JOB 的语法。如需修改原有同步关系，可以先 CANCEL 旧的再创建新的，不提供 ALTER 语法。

```sql
CANCEL WARM UP JOB WHERE id = 13418;
```

### 周期性同步

- 任务 CloudWarmUpJob.java
- 调度 CacheHotspotManager.java

#### 核心设计

- 由 FE 负责管理和执行 周期性 WARM UP JOB。
- FE 记录 上次同步的开始时间，并确保 一次同步完成后 才会执行下一次调度。
- 当 上次同步开始时间超过设定的同步周期 时，FE 触发新的同步任务，避免同步任务重叠执行。

#### 工作流程

1. 初始化 WARM UP JOB
- 用户通过 WARM UP 语法创建一个周期性同步任务，并指定同步周期（如 600s）。
- FE 记录任务的配置信息，包括 sync_mode = periodic 和 sync_interval。
1. 同步任务调度逻辑
- FE 轮询 现有的 WARM UP JOB 列表，检查每个任务的上次同步开始时间。
- 若 当前时间 - 上次同步开始时间 >= sync_interval，则触发新的同步任务。
- 若前一次任务仍在执行，则等待其完成后再启动下一次任务。
1. 执行同步
- FE 将当前同步任务的状态从 FINISHED 先转到 WAITING 再转到 PENDING。
- 执行过程与一次性的同步任务相同，通过TWarmUpTabletsRequest。
- 同步任务完成后，FE 将当前同步任务的状态 FINISHED 并等待下一个周期。

#### 解互斥

原先的 warm up job 是互斥的，一个 dst cluster 上同时只能有一个 warm up job。

周期性任务是长时间存在的，会导致无法发起到这个 dst cluster 的其他预热任务。

- 原逻辑：
- 每个 dst cluster 只允许拥有一个 pending 或 running 的 job，否则创建任务失败。
- 新逻辑：
- 每个 dst cluster 只允许拥有一个 running 的 job，但是允许多个 pending 的 job。
- pending job 的调度是 FIFO 的，一个 job 变成 running 后，会 block 其他相同 dst cluster 的 job，直到这个 job 退出 running 状态。

#### FAQ

1. 一次失败是否需要 cancel？

否，只是取消这一次执行，下一次继续调度。

1. 周期性同步是否有 timeout？

是，但是 timeout 之后不 cancel 整个任务。只是取消这一次执行。

### 事件触发的同步

- 任务 CloudWarmUpJob.java
- 调度 CacheHotspotManager.java
- 触发：CloudMetaMgr::commit_rowset()
- 发起：CloudWarmUpManager::warm_up_rowset()
- 执行：CloudInternalServiceImpl::warm_up_rowset()

#### 核心设计

- FE 负责管理事件触发同步任务，但不直接调度任务。
- FE 在任务创建时，需要将任务触发条件同步给 Source Cluster 的所有 BE，这些 BE 被称为源 BE。
- 源 BE 负责执行同步任务，并定期向 FE 汇报执行状态，以便统计和监控。

#### 工作流程

1. 任务注册（FE 侧）
- 用户通过 WARM UP 语法 在 FE 侧创建事件触发的同步任务，并指定需要监听的事件。
- FE 记录任务配置信息，并将触发条件同步到源 Cluster 上的所有 BE。
1. 触发器监听（源 BE 侧）
- 当发生指定事件（如 CloudMetaMgr::commit_rowset） 时，触发器自动启动同步任务，将相关数据同步至目标集群。
- 通过 TGetTabletReplicaInfosRequest 查询 FE，获取 tablet 分布信息，并保存在专用的 tablet -> BE 缓存（有 TTL）。

```thrift
struct TGetTabletReplicaInfosRequest { // 目前 cloud 下 server 端逻辑有点问题，思阳在改
    1: required list<i64> tablet_ids
    2: optional i64 warm_up_job_id
}

struct TGetTabletReplicaInfosResult {
    1: optional Status.TStatus status
    2: optional map<i64, list<Types.TReplicaInfo>> tablet_replica_infos
    3: optional string token
}
```

- 通过类似于TWarmUpTabletsRequest的 RPC 通知目标 BE 去预热 ，但是是 rowset 级别的。
1. 数据同步执行（目标 BE 侧）
- CloudWarmUpManager 中实现预热 rowset 的逻辑。
1. 任务状态汇报（BE → FE）
- 同步任务完成后，BE 需要将执行状态（成功/失败、同步数据量、耗时等）汇报给 FE。
- FE 负责 统计同步任务的执行情况，并可用于 日志记录、监控、故障排查等。

### 缓存的管理

沿用CloudWarmUpManager中现有逻辑

### 监控指标

#### 周期性任务指标

##### FE

TODO：完成的要删掉

| FE Metrics | 含义 |
| --- | --- |
| file_cache_warm_up_job_exec_count | 给定 clusterID，到该 cluster 的同步任务的调度次数 |
| file_cache_warm_up_job_requested_tablets | 给定 clusterID，到该 cluster 的同步任务提交的 tablet 数量 |
| file_cache_warm_up_job_finished_tablets | 给定 clusterID，到该 cluster 的同步任务完成的 tablet 数量 |
| file_cache_warm_up_job_latest_start_time | 给定 jobID，该 job 最新一次的开始时间，Epoch 时间（毫秒） |
| file_cache_warm_up_job_last_finish_time | 给定 jobID，该 job 最后一次完成的时间，Epoch 时间（毫秒） |

##### 目标 BE

| Bvar 名称 | 含义 |
| --- | --- |
| file_cache_once_or_periodic_warm_up_submitted_segment_size | BE 收到的预热请求中的 segment 大小 |
| file_cache_once_or_periodic_warm_up_submitted_segment_num | BE 收到的预热请求中的 segment 数量 |
| file_cache_once_or_periodic_warm_up_submitted_index_size | BE 收到的预热请求中的 inverted index 大小 |
| file_cache_once_or_periodic_warm_up_submitted_index_num | BE 收到的预热请求中的 inverted index 数量 |
| file_cache_once_or_periodic_warm_up_finished_segment_size | BE 预热完成的 segment 大小 |
| file_cache_once_or_periodic_warm_up_finished_segment_num | BE 预热完成的 segment 数量 |
| file_cache_once_or_periodic_warm_up_finished_index_size | BE 预热完成的 inverted index 大小 |
| file_cache_once_or_periodic_warm_up_finished_index_num | BE 预热完成的 inverted index 数量 |

#### 事件触发任务指标

TODO：更新 BE 侧时间指标，最好把 job id 传过去区分任务。

##### 源 BE

| Bvar 名称 | 含义 |
| --- | --- |
| file_cache_event_driven_warm_up_requested_segment_size | BE 发起的预热请求中的 segment 大小 |
| file_cache_event_driven_warm_up_requested_segment_num | BE 发起的预热请求中的 segment 数量 |
| file_cache_event_driven_warm_up_requested_index_size | BE 发起的预热请求中的 inverted index 大小 |
| file_cache_event_driven_warm_up_requested_index_num | BE 发起的预热请求中的 inverted index 数量 |
| file_cache_warm_up_rowset_last_call_unix_ts | BE 最后一次发起预热请求的时间戳 |

##### 目标 BE

| Bvar 名称 | 含义 |
| --- | --- |
| file_cache_event_driven_warm_up_submitted_segment_size | BE 收到的预热请求中的 segment 大小 |
| file_cache_event_driven_warm_up_submitted_segment_num | BE 收到的预热请求中的 segment 数量 |
| file_cache_event_driven_warm_up_submitted_index_size | BE 收到的预热请求中的 inverted index 大小 |
| file_cache_event_driven_warm_up_submitted_index_num | BE 收到的预热请求中的 inverted index 数量 |
| file_cache_event_driven_warm_up_finished_segment_size | BE 预热完成的 segment 大小 |
| file_cache_event_driven_warm_up_finished_segment_num | BE 预热完成的 segment 数量 |
| file_cache_event_driven_warm_up_finished_index_size | BE 预热完成的 inverted index 大小 |
| file_cache_event_driven_warm_up_finished_index_num | BE 预热完成的 inverted index 数量 |
| file_cache_warm_up_rowset_last_handle_unix_ts | BE 最后一次执行预热请求的时间戳 |

#### File Cache Downloader

| Bvar 名称 | 含义 |
| --- | --- |
| file_cache_download_submitted_size | 已经提交的 download 任务大小 |
| file_cache_download_finished_size | 已经完成的 download 任务数量 |
| file_cache_download_submitted_num | 已经提交的 download 任务大小 |
| file_cache_download_finished_num | 已经完成的 download 任务数量 |
| file_cache_download_failed_num | 失败的 download 任务数量 |
| rowset_download_finished_num | 事件触发的 rowset 同步中，完成的 segment 和 inverted index 任务数 |
| rowset_download_failed_num | 事件触发的 rowset 同步中，失败的 segment 和 inverted index 任务数 |

