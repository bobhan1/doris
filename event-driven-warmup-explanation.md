# File Cache 主动增量预热（Event-Driven Warmup）详解

## 一、背景与动机

在 Doris 存算分离架构下，数据存储在远端对象存储（如 S3），而 BE 节点本地维护 **File Cache** 来加速查询。这就引出两大核心场景的缓存一致性问题：

```
┌──────────────────────────────────────────────────────────────────┐
│  场景一：读写分离                   场景二：主备容灾              │
│                                                                  │
│  ┌─────────┐   写入   ┌─────┐    ┌─────────┐  热数据  ┌───────┐ │
│  │写集群(W) │ ──────→ │ S3  │    │主集群(P) │ ──────→ │备集群 │ │
│  └─────────┘         └──┬──┘    └─────────┘  同步    │ (S)   │ │
│                         │ 查询                        └───────┘ │
│                    ┌────┴────┐                                   │
│                    │读集群(R) │  ← Cache 缺失导致查询抖动！       │
│                    └─────────┘                                   │
└──────────────────────────────────────────────────────────────────┘
```

**核心问题**: 写集群完成 Load/Compaction/Schema Change 后，产生的新数据 rowset 不会自动进入读集群的 File Cache，导致读集群首次查询这些数据时必须从 S3 远程读取，产生**查询性能抖动**。

## 二、总体方案：三种同步模式

系统提供三种 WARM UP 同步模式，统一抽象为 `CloudWarmUpJob`：

```java
// fe/fe-core/src/main/java/org/apache/doris/cloud/CloudWarmUpJob.java:83-92
public enum SyncMode {
    ONCE,           // 一次性同步（手动触发）
    PERIODIC,       // 周期性同步（定时触发）
    EVENT_DRIVEN;   // 事件触发同步（自动触发）← 本文重点
}

public enum SyncEvent {
    LOAD,   // Load/Compaction/SchemaChange 事件
    QUERY   // 查询事件（后续扩展）
}
```

对应的 SQL 语法：

```sql
-- 一次性（默认）
WARM UP COMPUTE GROUP read_cluster WITH COMPUTE GROUP write_cluster;

-- 周期性（每 600 秒同步一次）
WARM UP COMPUTE GROUP read_cluster WITH COMPUTE GROUP write_cluster
  PROPERTIES("sync_mode" = "periodic", "sync_interval_sec" = "600");

-- 事件触发（本文重点！）
WARM UP COMPUTE GROUP read_cluster WITH COMPUTE GROUP write_cluster
  PROPERTIES("sync_mode" = "event_driven", "sync_event" = "load");
```

## 三、整体架构与数据流

事件触发预热的**端到端流程**如下图所示：

```
  用户发起 WARM UP 语句
         │
         ▼
  ┌─────────────┐
  │     FE      │  ① 创建 CloudWarmUpJob (EVENT_DRIVEN)
  │ CacheHotspot│  ② 向 Source Cluster 所有 BE 注册触发器
  │  Manager    │     (SET_JOB + event=LOAD)
  └──────┬──────┘
         │ Thrift RPC: warm_up_tablets(SET_JOB, event=LOAD)
         ▼
  ┌────────────────────── Source Cluster ──────────────────────┐
  │                                                            │
  │  ┌──────────────┐    ③ 注册 job_id 到                      │
  │  │ Source BE-1   │       _tablet_replica_cache              │
  │  │ set_event()   │                                          │
  │  └──────┬───────┘                                          │
  │         │                                                   │
  │         │  ④ commit_rowset() 触发                           │
  │         │     (Compaction/Load 完成后)                       │
  │         ▼                                                   │
  │  ┌──────────────────┐                                      │
  │  │CloudWarmUpManager│  ⑤ warm_up_rowset()                  │
  │  │                  │     → get_replica_info() 查 FE        │
  │  │                  │       获取 tablet → BE 映射            │
  │  └──────┬───────────┘                                      │
  │         │                                                   │
  └─────────┼───────────────────────────────────────────────────┘
            │ brpc: warm_up_rowset(PWarmUpRowsetRequest)
            ▼
  ┌────────────────────── Target Cluster ──────────────────────┐
  │                                                            │
  │  ┌──────────────────────┐                                  │
  │  │ Target BE             │  ⑥ 接收 RowsetMeta             │
  │  │ CloudInternalService  │     解析 segment + index 信息   │
  │  │ ::warm_up_rowset()    │                                  │
  │  └──────┬───────────────┘                                  │
  │         │                                                   │
  │         ▼                                                   │
  │  ┌──────────────────────┐                                  │
  │  │FileCacheBlockDownload│  ⑦ 从 S3 下载 segment/index     │
  │  │  er                  │     到本地 File Cache             │
  │  └──────────────────────┘                                  │
  │                                                            │
  └────────────────────────────────────────────────────────────┘
```

## 四、详细流程分步解析

### 步骤 ①：创建 Job（FE 侧）

用户执行 SQL 后，`CacheHotspotManager.createJob()` 解析属性并构建 Job：

```java
// fe/.../CacheHotspotManager.java:816-828
} else if ("event_driven".equals(properties.get("sync_mode"))) {
    String syncEventStr = properties.get("sync_event");
    CloudWarmUpJob.SyncEvent syncEvent =
            CloudWarmUpJob.SyncEvent.valueOf(syncEventStr.toUpperCase());
    builder.setSyncMode(SyncMode.EVENT_DRIVEN)
            .setSyncEvent(syncEvent);
}
warmUpJob = builder.build();
```

**去重保护**：通过 `repeatJobDetectionSet`（key = `<src, dst, syncMode>` 三元组），确保同一对集群间同一模式只有一个活跃 job：

```java
// fe/.../CacheHotspotManager.java:155-168
private void registerJobForRepeatDetection(CloudWarmUpJob job, ...) {
    if (job.isEventDriven() || job.isPeriodic()) {
        JobKey key = new JobKey(src, dst, syncMode);
        boolean added = this.repeatJobDetectionSet.add(key);
        if (!added && !replay) {
            throw new AnalysisException(key + " already has a runnable job");
        }
    }
}
```

### 步骤 ②：注册触发器到 Source BE

**关键区别**：事件触发 job 连接的是**源集群**（而非目标集群）的 BE：

```java
// fe/.../CloudWarmUpJob.java:220-221
private void fetchBeToThriftAddress() {
    // EVENT_DRIVEN → srcClusterName，其他 → dstClusterName
    String clusterName = isEventDriven() ? srcClusterName : dstClusterName;
    ...
}
```

`runEventDrivenJob()` 向每个 Source BE 发送 `SET_JOB` RPC 并携带 `event` 类型：

```java
// fe/.../CloudWarmUpJob.java:690-718
private void runEventDrivenJob() throws Exception {
    initClients();
    for (Map.Entry<Long, Client> entry : beToClient.entrySet()) {
        TWarmUpTabletsRequest request = new TWarmUpTabletsRequest();
        request.setType(TWarmUpTabletsRequestType.SET_JOB);
        request.setJobId(jobId);
        request.setEvent(getTWarmUpEventType());  // LOAD 或 QUERY
        entry.getValue().warmUpTablets(request);   // Thrift RPC
    }
}
```

### 步骤 ③：Source BE 注册事件监听

Source BE 收到 RPC 后，在 `CloudBackendService::warm_up_tablets` 中分发：

```cpp
// be/src/cloud/cloud_backend_service.cpp:101-113
case TWarmUpTabletsRequestType::SET_JOB: {
    if (request.__isset.event) {
        // 事件触发模式：注册触发器
        st = manager.set_event(request.job_id, request.event);
    } else {
        // 非事件触发：常规 tablet 级别预热
        st = manager.check_and_set_job_id(request.job_id);
    }
}
```

`set_event()` 的核心逻辑极简但关键——它在 `_tablet_replica_cache` 中为该 job_id 创建一个空的缓存槽位，**作为"事件已注册"的标志**：

```cpp
// be/src/cloud/cloud_warm_up_manager.cpp:463-482
Status CloudWarmUpManager::set_event(int64_t job_id, TWarmUpEventType::type event, bool clear) {
    std::lock_guard lock(_mtx);
    if (event == TWarmUpEventType::type::LOAD) {
        if (clear) {
            _tablet_replica_cache.erase(job_id);     // 清除注册
        } else if (!_tablet_replica_cache.contains(job_id)) {
            static_cast<void>(_tablet_replica_cache[job_id]);  // ← 注册！
        }
    }
    return Status::OK();
}
```

> **设计巧妙之处**：`_tablet_replica_cache` 既是"事件注册表"（key 存在即表示已注册），又是"tablet 副本分布缓存"（value 存 tablet→BE 映射，带 TTL），一举两用。

### 步骤 ④：触发点——commit_rowset

当 Compaction/Load 完成后，`CloudMetaMgr::commit_rowset()` 作为触发入口：

```cpp
// be/src/cloud/cloud_meta_mgr.cpp:1339-1384
Status CloudMetaMgr::commit_rowset(RowsetMeta& rs_meta, const string& job_id, ...) {
    // 1. 提交 rowset 到 MetaService
    Status st = retry_rpc("commit rowset", req, &resp, &MetaService_Stub::commit_rowset);

    // 2. 计算同步等待超时（仅 compaction 场景，job_id 非空）
    int64_t timeout_ms = -1;
    if (config::enable_compaction_delay_commit_for_warm_up && !job_id.empty()) {
        // 假设下载速度 100MB/s，2倍安全系数
        timeout_ms = min(
            max(size / (100MB/s) * 2 * 1000, min_timeout),  // 下限 500ms
            max_timeout);                                     // 上限 120s
    }

    // 3. 触发预热！
    manager.warm_up_rowset(rs_meta, timeout_ms);
    return st;
}
```

**超时设计**：对于 compaction 产生的 rowset，会**同步等待**预热完成（最多等 120s），这样可以确保 compaction 完成前，目标集群已经缓存了新数据。普通 load 的 rowset（job_id 为空）则 `timeout_ms = -1`，即**异步触发**不等待。

### 步骤 ⑤：Source BE 发起预热

`warm_up_rowset()` 提交任务到线程池，然后查询 FE 获取 tablet 副本分布：

```cpp
// be/src/cloud/cloud_warm_up_manager.cpp:584-619
void CloudWarmUpManager::warm_up_rowset(RowsetMeta& rs_meta, int64_t sync_wait_timeout_ms) {
    // 提交到线程池并同步等待完成
    _thread_pool_token->submit_func([&]() {
        _warm_up_rowset(rs_meta, sync_wait_timeout_ms);
    });
}

void CloudWarmUpManager::_warm_up_rowset(RowsetMeta& rs_meta, int64_t sync_wait_timeout_ms) {
    // 查询 tablet 在目标集群的副本分布（先查缓存，miss 则 RPC 到 FE）
    bool cache_hit = false;
    auto replicas = get_replica_info(rs_meta.tablet_id(), false, cache_hit);
    if (replicas.empty()) {
        return;  // 目标集群无此 tablet 的副本，跳过
    }
    // 发起 rowset 级别预热 RPC
    Status st = _do_warm_up_rowset(rs_meta, replicas, sync_wait_timeout_ms, !cache_hit);
    // 缓存命中但报 TABLE_NOT_FOUND → 刷新缓存重试
    if (cache_hit && !st.ok() && st.is<TABLE_NOT_FOUND>()) {
        replicas = get_replica_info(rs_meta.tablet_id(), true, cache_hit);
        _do_warm_up_rowset(rs_meta, replicas, sync_wait_timeout_ms, true);
    }
}
```

`get_replica_info()` 的**两级缓存策略**：

```
┌─────────────────────────────────────────────────────┐
│              get_replica_info(tablet_id)             │
│                                                     │
│  ① 检查本地缓存 _tablet_replica_cache[job_id]       │
│     ├─ 命中 + TTL 未过期 → 返回                      │
│     └─ 未命中 / 过期                                  │
│         ② RPC 到 FE：getTabletReplicaInfos()        │
│            └─ 返回 tablet → [host:brpc_port] 映射   │
│               写入缓存并返回                          │
└─────────────────────────────────────────────────────┘
```

```cpp
// be/src/cloud/cloud_warm_up_manager.cpp:531-539
TGetTabletReplicaInfosRequest request;
request.warm_up_job_id = job_id;
request.tablet_ids.emplace_back(tablet_id);
// Thrift RPC → FE
ThriftRpcHelper::rpc<FrontendServiceClient>(
    master_addr.hostname, master_addr.port,
    [&](FrontendServiceConnection& client) {
        client->getTabletReplicaInfos(result, request);
    });
```

### 步骤 ⑥：Source BE → Target BE 的 RPC

`_do_warm_up_rowset()` 构建 protobuf 请求并通过 brpc 发送到每个目标副本：

```cpp
// be/src/cloud/cloud_warm_up_manager.cpp:621-734
Status CloudWarmUpManager::_do_warm_up_rowset(...) {
    PWarmUpRowsetRequest request;
    request.add_rowset_metas()->CopyFrom(rs_meta.get_rowset_pb());
    request.set_unix_ts_us(now_ts);
    request.set_sync_wait_timeout_ms(sync_wait_timeout_ms);
    request.set_skip_existence_check(skip_existence_check);

    for (auto& replica : replicas) {
        // DNS 解析 + 获取 brpc stub
        auto brpc_stub = brpc_client_cache->get_new_client_no_cache(brpc_addr);
        // 设置超时 = sync_wait_timeout + 1s buffer
        if (sync_wait_timeout_ms > 0) {
            cntl.set_timeout_ms(sync_wait_timeout_ms + 1000);
        }
        // 发送 brpc 请求
        brpc_stub->warm_up_rowset(&cntl, &request, &response, nullptr);
    }
}
```

RPC 使用的 protobuf 定义：

```protobuf
// gensrc/proto/internal_service.proto:914-923
message PWarmUpRowsetRequest {
    repeated RowsetMetaPB rowset_metas = 1;
    optional int64 unix_ts_us = 2;          // 请求时间戳（用于延迟监控）
    optional int64 sync_wait_timeout_ms = 3; // 同步等待超时
    optional bool skip_existence_check = 4;  // 跳过 tablet 存在性检查
}
```

### 步骤 ⑦：Target BE 执行下载

`CloudInternalServiceImpl::warm_up_rowset()` 是 Target BE 上的 RPC 处理函数：

```cpp
// be/src/cloud/cloud_internal_service.cpp:504-667
void CloudInternalServiceImpl::warm_up_rowset(..., const PWarmUpRowsetRequest* request, ...) {
    // 如需同步等待，创建 CountdownEvent
    shared_ptr<CountdownEvent> wait = nullptr;
    if (request->sync_wait_timeout_ms() > 0) {
        wait = make_shared<CountdownEvent>(0);
        due_time = milliseconds_from_now(request->sync_wait_timeout_ms());
    }

    for (auto& rs_meta_pb : request->rowset_metas()) {
        // 解析 RowsetMeta
        RowsetMeta rs_meta;
        rs_meta.init_from_pb(rs_meta_pb);

        // 防止重复预热
        if (!tablet->add_rowset_warmup_state(rs_meta, WarmUpTriggerSource::EVENT_DRIVEN)) {
            continue;  // 已在预热中，跳过
        }

        // 遍历每个 segment 文件
        for (int64_t seg_id = 0; seg_id < rs_meta.num_segments(); seg_id++) {
            // ===== 下载 Segment 数据文件 =====
            io::DownloadFileMeta download_meta {
                .path = remote_segment_path(rs_meta, seg_id),
                .file_size = segment_size,
                .file_system = rs_meta.fs(),     // 支持 PackedFileSystem
                .ctx = { .is_warmup = true },
                .download_done = [=](Status st) {
                    handle_segment_download_done(st, tablet_id, ...);
                },
            };
            _engine.file_cache_block_downloader().submit_download_task(download_meta);
            if (wait) wait->add_count();

            // ===== 下载倒排索引文件 =====
            // V1 格式：每个 segment 每个 index 独立文件
            // V2 格式：每个 segment 一个合并索引文件
            if (schema->has_inverted_index()) {
                download_inverted_index(idx_path, idx_size);
            }
        }
    }

    // 同步等待所有下载完成（或超时）
    if (wait && wait->timed_wait(due_time)) {
        // 超时
        g_warm_up_rowset_wait_for_compaction_timeout_num << 1;
    }
}
```

下载完成后的回调更新监控指标：

```cpp
// be/src/cloud/cloud_internal_service.cpp:396-449
void handle_segment_download_done(Status st, ...) {
    if (st.ok()) {
        g_file_cache_event_driven_warm_up_finished_segment_num << 1;
        g_file_cache_event_driven_warm_up_finished_segment_size << segment_size;
        // 记录延迟: request → finish
        g_file_cache_warm_up_rowset_latency << (now_ts - request_ts);
    } else {
        g_file_cache_event_driven_warm_up_failed_segment_num << 1;
    }
    // 标记 rowset 预热进度
    tablet->complete_rowset_segment_warmup(WarmUpTriggerSource::EVENT_DRIVEN, ...);
    // 唤醒同步等待线程
    if (wait) wait->signal();
}
```

## 五、三种模式的关键差异对比

```
┌──────────────────┬─────────────────┬─────────────────┬──────────────────┐
│     维度          │     ONCE        │    PERIODIC     │   EVENT_DRIVEN   │
├──────────────────┼─────────────────┼─────────────────┼──────────────────┤
│ 触发方式          │ 手动一次        │ 定时周期        │ 自动(事件触发)    │
│ 连接目标          │ 目标集群 BE     │ 目标集群 BE     │ **源集群 BE**    │
│ Tablet 计算      │ FE计算全量batch  │ FE计算全量batch  │ **不需要**       │
│ 预热粒度          │ Tablet 级别     │ Tablet 级别     │ **Rowset 级别**  │
│ 互斥控制          │ 同一dst只能一个  │ 同一dst只能一个  │ **无互斥**       │
│ Job 生命周期      │ 执行完即结束    │ 循环执行        │ 常驻直到取消      │
│ RPC 协议         │ Thrift          │ Thrift          │ **brpc+protobuf** │
│ 同步等待          │ 否              │ 否              │ 可选(compaction)  │
│ 状态机            │ PENDING→RUNNING │ PENDING→RUNNING │ PENDING→RUNNING  │
│                  │ →FINISHED       │ →PENDING(循环)   │ (保持)           │
└──────────────────┴─────────────────┴─────────────────┴──────────────────┘
```

## 六、Job 状态机

```
                    ┌──────────────────────────────┐
                    │                              │
                    ▼                              │ (PERIODIC: 完成后重回 PENDING)
              ┌──────────┐     runPendingJob()   ┌─┴────────┐
 createJob()  │          │ ──────────────────→   │          │
 ──────────→  │ PENDING  │                       │ RUNNING  │
              │          │ ←──────────────────   │          │
              └────┬─────┘     cancel(force=F)   └────┬─────┘
                   │           + PERIODIC/ED           │
                   │                              ┌────┴─────┐
                   │cancel(force=T)               │          │
                   ├──────────────────────────→   │CANCELLED │
                   │                              │          │
                   │                              └──────────┘
                   │
                   │ (ONCE/PERIODIC完成)     ┌──────────┐
                   └─────────────────────→   │ FINISHED │
                                             └──────────┘
```

**EVENT_DRIVEN 的特殊行为**：
- Job 进入 RUNNING 后**不会自动退出**——它一直保持 RUNNING 状态
- 只有用户执行 `CANCEL WARM UP JOB` 时才会终止
- 非 force cancel 时会回到 PENDING 而非 CANCELLED（允许重新注册）

## 七、监控指标体系

整个链路上精心埋设了延迟追踪点：

```
Source BE                          Target BE
   │                                  │
   │ ① request_ts(发起时间)            │
   │ ─────────────────────────────→   │
   │                                  │ ② handle_ts(处理时间)
   │                    request→handle延迟
   │                                  │
   │                                  │ ③ finish_ts(完成时间)
   │                    handle→finish延迟
   │                                  │
   │ ←─────────────────────────────   │
   │       request→finish 总延迟       │
   │                                  │
   │  超过阈值(warm_up_rowset_slow_log_ms) → 记录慢日志
```

### 源 BE 指标

| Bvar 名称 | 含义 |
| --- | --- |
| file_cache_event_driven_warm_up_requested_segment_num/size | 发起的 segment 预热请求数量/大小 |
| file_cache_event_driven_warm_up_requested_index_num/size | 发起的 index 预热请求数量/大小 |
| file_cache_event_driven_warm_up_skipped_rowset_num | 跳过的 rowset 数（无目标副本） |
| file_cache_warm_up_rowset_last_call_unix_ts | 最后一次发起预热的时间戳 |

### 目标 BE 指标

| Bvar 名称 | 含义 |
| --- | --- |
| file_cache_event_driven_warm_up_submitted_segment_num/size | 提交的 segment 下载数量/大小 |
| file_cache_event_driven_warm_up_finished_segment_num/size | 完成的 segment 下载数量/大小 |
| file_cache_event_driven_warm_up_failed_segment_num/size | 失败的 segment 下载数量/大小 |
| file_cache_event_driven_warm_up_submitted_index_num/size | 提交的 index 下载数量/大小 |
| file_cache_event_driven_warm_up_finished_index_num/size | 完成的 index 下载数量/大小 |
| file_cache_warm_up_rowset_latency | request→finish 总延迟 |
| file_cache_warm_up_rowset_handle_to_finish_latency | handle→finish 延迟 |
| file_cache_warm_up_rowset_slow_count | 慢预热计数（超过阈值） |

## 八、关键配置项

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `enable_compaction_delay_commit_for_warm_up` | false | 是否在 compaction 后同步等待预热完成 |
| `warm_up_rowset_sync_wait_min_timeout_ms` | 500 | 同步等待最小超时 |
| `warm_up_rowset_sync_wait_max_timeout_ms` | 120000 | 同步等待最大超时 |
| `warmup_tablet_replica_info_cache_ttl_sec` | - | 副本缓存 TTL |
| `warm_up_manager_thread_pool_size` | 4 | Source BE 预热线程池大小 |
| `file_cache_enable_only_warm_up_idx` | false | 仅预热索引不预热 segment |
| `max_active_cloud_warm_up_job` | - | FE 最大并发 job 数 |

## 九、总结

核心设计思想是：**通过在源集群 BE 上注册事件触发器，在数据写入（commit_rowset）的关键路径上自动感知变更，以 rowset 粒度精准推送到目标集群，实现实时增量缓存同步。**
