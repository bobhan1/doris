# BE Table-Level MS RPC Rate Limiting Implementation Plan (Part 2)

## Overview

本文档是 design.md 第二部分的详细实现计划：当 MS 返回 MS_BUSY 错误码时，BE 侧根据表级别QPS统计进行自适应限流。

## 核心组件

### 1. StrictQpsLimiter - 严格QPS限流器

**文件**: `be/src/cloud/cloud_tablet_rpc_throttler.h`

与令牌桶不同，不允许 burst，严格按照固定间隔放行请求。

```cpp
class StrictQpsLimiter {
public:
    using Clock = std::chrono::steady_clock;

    explicit StrictQpsLimiter(double qps);

    // 返回允许执行的时间点，调用方需要 sleep 到该时间点
    Clock::time_point reserve();

    // 动态更新 QPS
    void update_qps(double new_qps);

    // 获取当前 QPS 限制
    double get_qps() const;

private:
    mutable std::mutex _mtx;
    int64_t _interval_ns;
    Clock::time_point _next_allowed_time;
};
```

### 2. TableRpcQpsCounter - 表级别RPC QPS统计器 (基于bvar)

**文件**: `be/src/cloud/cloud_tablet_rpc_throttler.h`, `be/src/cloud/cloud_tablet_rpc_throttler.cpp`

使用 bvar 统计每个表在每个导入相关RPC上的QPS。

```cpp
// 需要统计的导入相关 RPC 类型
enum class LoadRelatedRpc : size_t {
    PREPARE_ROWSET,
    COMMIT_ROWSET,
    UPDATE_TMP_ROWSET,
    UPDATE_PACKED_FILE_INFO,
    UPDATE_DELETE_BITMAP,
    COUNT
};

std::string_view load_related_rpc_name(LoadRelatedRpc rpc);

// 单个表在单个RPC上的QPS统计
class TableRpcQpsCounter {
public:
    TableRpcQpsCounter(int64_t table_id, LoadRelatedRpc rpc_type);

    // 记录一次调用
    void increment();

    // 获取当前 QPS (过去1秒的平均值)
    double get_qps() const;

    int64_t table_id() const { return _table_id; }
    LoadRelatedRpc rpc_type() const { return _rpc_type; }

private:
    int64_t _table_id;
    LoadRelatedRpc _rpc_type;

    // 使用 bvar::Adder + bvar::PerSecond 统计 QPS
    // bvar name: "ms_rpc_table_qps_{rpc_name}_{table_id}"
    std::unique_ptr<bvar::Adder<int64_t>> _counter;
    std::unique_ptr<bvar::PerSecond<bvar::Adder<int64_t>>> _qps;
};

// 管理所有表的 QPS 统计
class TableRpcQpsRegistry {
public:
    TableRpcQpsRegistry();

    // 记录一次 RPC 调用
    void record(LoadRelatedRpc rpc_type, int64_t table_id);

    // 获取指定 RPC 类型下，QPS 最高的 top-k 个表
    // 返回: [(table_id, qps), ...] 按 qps 降序
    std::vector<std::pair<int64_t, double>> get_top_k_tables(
        LoadRelatedRpc rpc_type, int k) const;

    // 获取指定表在指定RPC上的当前QPS
    double get_qps(LoadRelatedRpc rpc_type, int64_t table_id) const;

    // 清理长时间无活动的表计数器 (定期调用)
    void cleanup_inactive_tables();

private:
    // 获取或创建 counter
    TableRpcQpsCounter* get_or_create_counter(LoadRelatedRpc rpc_type, int64_t table_id);

    mutable std::shared_mutex _mutex;

    // rpc_type -> (table_id -> counter)
    std::array<std::unordered_map<int64_t, std::unique_ptr<TableRpcQpsCounter>>,
               static_cast<size_t>(LoadRelatedRpc::COUNT)> _counters;

    // 最大跟踪的表数量 (防止内存无限增长)
    static constexpr size_t MAX_TRACKED_TABLES_PER_RPC = 10000;
};
```

### 3. TableRpcThrottler - 表级别限流器

**文件**: `be/src/cloud/cloud_tablet_rpc_throttler.h`, `be/src/cloud/cloud_tablet_rpc_throttler.cpp`

管理每个 (RPC类型, 表) 对上的 StrictQpsLimiter。

```cpp
class TableRpcThrottler {
public:
    TableRpcThrottler();

    // 在执行 RPC 前调用，返回需要等待的时间点
    // 如果没有限制，返回 now
    std::chrono::steady_clock::time_point throttle(
        LoadRelatedRpc rpc_type, int64_t table_id);

    // 设置或更新表级别限流
    void set_qps_limit(LoadRelatedRpc rpc_type, int64_t table_id, double qps_limit);

    // 移除表级别限流
    void remove_qps_limit(LoadRelatedRpc rpc_type, int64_t table_id);

    // 获取当前限制 (如果没有返回 0)
    double get_qps_limit(LoadRelatedRpc rpc_type, int64_t table_id) const;

    // 检查是否有限制
    bool has_limit(LoadRelatedRpc rpc_type, int64_t table_id) const;

    // 获取当前被限流的表数量
    size_t get_throttled_table_count(LoadRelatedRpc rpc_type) const;

private:
    mutable std::shared_mutex _mutex;
    // (rpc_type, table_id) -> StrictQpsLimiter
    std::map<std::pair<LoadRelatedRpc, int64_t>, std::unique_ptr<StrictQpsLimiter>> _limiters;

    // bvar: 当前被限流的表数量
    std::array<std::unique_ptr<bvar::Status<size_t>>,
               static_cast<size_t>(LoadRelatedRpc::COUNT)> _throttled_table_counts;
};
```

### 4. MSBackpressureHandler - MS 背压处理器

**文件**: `be/src/cloud/cloud_tablet_rpc_throttler.h`, `be/src/cloud/cloud_tablet_rpc_throttler.cpp`

核心控制器，协调 QPS 统计、限流升级和限流降级。

```cpp
// 限流升级的一次操作记录，用于支持限流降级时撤销
struct ThrottleUpgradeRecord {
    int64_t timestamp_ms;
    // (rpc_type, table_id) -> (old_qps_limit, new_qps_limit)
    // old_qps_limit = 0 表示之前没有限制
    std::map<std::pair<LoadRelatedRpc, int64_t>, std::pair<double, double>> changes;
};

class MSBackpressureHandler {
public:
    MSBackpressureHandler(
        TableRpcQpsRegistry* qps_registry,
        TableRpcThrottler* throttler);

    // 当收到 MS_BUSY 响应时调用
    // 返回 true 表示触发了限流升级
    bool on_ms_busy();

    // 定期调用，检查是否需要限流降级
    void try_downgrade();

    // 在 RPC 调用前调用，执行限流等待
    // 返回需要等待到的时间点
    std::chrono::steady_clock::time_point before_rpc(
        LoadRelatedRpc rpc_type, int64_t table_id);

    // 在 RPC 调用后调用，记录 QPS 统计
    void after_rpc(LoadRelatedRpc rpc_type, int64_t table_id);

    // 获取最近一次收到 MS_BUSY 的时间
    std::chrono::steady_clock::time_point last_ms_busy_time() const;

private:
    // 执行限流升级
    void upgrade_throttle();

    // 执行限流降级 (撤销最近一次升级)
    void downgrade_throttle();

    TableRpcQpsRegistry* _qps_registry;
    TableRpcThrottler* _throttler;

    mutable std::mutex _mutex;
    std::chrono::steady_clock::time_point _last_ms_busy_time;
    std::chrono::steady_clock::time_point _last_upgrade_time;

    // 限流升级历史，用于降级时撤销
    std::vector<ThrottleUpgradeRecord> _upgrade_history;
};
```

## 配置项

**文件**: `be/src/cloud/config.h`, `be/src/cloud/config.cpp`

配置命名遵循现有风格，以 `ms_rpc_` 为前缀：

```cpp
// ============== 表级别限流配置 ==============

// 是否启用 MS 背压响应处理 (表级别自适应限流)
DECLARE_mBool(enable_ms_backpressure_handling);  // 默认 false

// ------------ 限流升级配置 ------------

// 收到 MS_BUSY 后触发限流升级的最小间隔 (秒)
// 即：两次限流升级之间至少间隔这么长时间
DECLARE_mInt32(ms_backpressure_upgrade_interval_sec);  // 默认 10

// 每次限流升级时，选择 QPS 最高的 top-k 个表进行限流
DECLARE_mInt32(ms_backpressure_upgrade_top_k);  // 默认 3

// 限流升级时的 QPS 衰减比例
// 新限制 = 当前QPS * ratio (首次) 或 当前限制 * ratio (已有限制)
DECLARE_mDouble(ms_backpressure_throttle_ratio);  // 默认 0.5

// 表级别 QPS 限制的下限值
// 限流升级不会将 QPS 限制降到此值以下
DECLARE_mDouble(ms_rpc_table_qps_limit_floor);  // 默认 1.0

// ------------ 限流降级配置 ------------

// 没有收到 MS_BUSY 多长时间后触发限流降级 (秒)
DECLARE_mInt32(ms_backpressure_downgrade_interval_sec);  // 默认 60

// ------------ 内存管理配置 ------------

// 每个 RPC 类型最多跟踪的表数量 (防止内存无限增长)
DECLARE_mInt32(ms_rpc_max_tracked_tables_per_rpc);  // 默认 10000

// 清理无活动表计数器的间隔 (秒)
DECLARE_mInt32(ms_rpc_table_counter_cleanup_interval_sec);  // 默认 300
```

## Bvar 监控指标

所有 bvar 使用统一前缀 `ms_rpc_backpressure_` 便于查找：

```cpp
// ============== QPS 统计相关 ==============

// 每个 (rpc_type, table_id) 的 QPS
// bvar name: "ms_rpc_table_qps_{rpc_name}_{table_id}"
// 由 TableRpcQpsCounter 自动创建

// ============== 限流相关 ==============

// 限流升级次数
bvar::Adder<uint64_t> g_backpressure_upgrade_count("ms_rpc_backpressure_upgrade_count");
bvar::Window<bvar::Adder<uint64_t>> g_backpressure_upgrade_qpm(
    "ms_rpc_backpressure_upgrade_qpm", &g_backpressure_upgrade_count, 60);

// 限流降级次数
bvar::Adder<uint64_t> g_backpressure_downgrade_count("ms_rpc_backpressure_downgrade_count");
bvar::Window<bvar::Adder<uint64_t>> g_backpressure_downgrade_qpm(
    "ms_rpc_backpressure_downgrade_qpm", &g_backpressure_downgrade_count, 60);

// 当前被限流的表数量 (按 RPC 类型)
// bvar name: "ms_rpc_backpressure_throttled_tables_{rpc_name}"
// 由 TableRpcThrottler 维护

// 表级别限流等待时间
bvar::LatencyRecorder g_table_throttle_wait_us("ms_rpc_backpressure_throttle_wait");

// ============== MS_BUSY 响应相关 ==============

// MS_BUSY 响应次数
bvar::Adder<uint64_t> g_ms_busy_count("ms_rpc_backpressure_ms_busy_count");
bvar::Window<bvar::Adder<uint64_t>> g_ms_busy_qpm(
    "ms_rpc_backpressure_ms_busy_qpm", &g_ms_busy_count, 60);

// 距离上次 MS_BUSY 的时间 (秒)
bvar::PassiveStatus<int64_t> g_seconds_since_last_ms_busy(
    "ms_rpc_backpressure_seconds_since_last_ms_busy",
    []() -> int64_t {
        // 由 MSBackpressureHandler 提供回调
        return ...;
    });
```

## 集成点

### 1. retry_rpc 函数修改

**文件**: `be/src/cloud/cloud_meta_mgr.cpp`

```cpp
template <typename Request, typename Response>
Status retry_rpc(MetaServiceRPC rpc, const Request& req, Response* res,
                 MetaServiceMethod<Request, Response> method,
                 HostLevelMSRpcRateLimiters* host_limiters,
                 MSBackpressureHandler* backpressure_handler = nullptr) {
    // 提取 table_id (如果请求中包含)
    int64_t table_id = extract_table_id(req);  // 需要实现

    // 转换为 LoadRelatedRpc 类型 (如果是导入相关RPC)
    LoadRelatedRpc load_rpc = to_load_related_rpc(rpc);  // 需要实现

    while (true) {
        // 1. 表级别限流 (仅对导入相关RPC且有table_id时)
        if (backpressure_handler &&
            load_rpc != LoadRelatedRpc::COUNT &&
            table_id > 0) {
            auto allow_time = backpressure_handler->before_rpc(load_rpc, table_id);
            auto wait_ns = wait_until(allow_time);  // 可能需要检查线程池
            if (wait_ns > 0) {
                g_table_throttle_wait_us << (wait_ns / 1000);
            }
        }

        // 2. Host level 限流 (已有逻辑)
        if (host_limiters) {
            host_limiters->limit(rpc);
        }

        // 3. 执行 RPC
        (stub.get()->*method)(&cntl, &req, res, nullptr);

        // 4. 记录 QPS 统计 (RPC 完成后记录)
        if (backpressure_handler &&
            load_rpc != LoadRelatedRpc::COUNT &&
            table_id > 0) {
            backpressure_handler->after_rpc(load_rpc, table_id);
        }

        // 5. 检查 MS_BUSY 响应
        if (res->status().code() == MetaServiceCode::MS_BUSY) {
            g_ms_busy_count << 1;
            if (backpressure_handler) {
                backpressure_handler->on_ms_busy();
            }
            // 继续现有的重试逻辑...
        }

        // ... 现有的重试和错误处理逻辑
    }
}
```

### 2. 从请求中提取 table_id

```cpp
template <typename Request>
int64_t extract_table_id(const Request& req) {
    if constexpr (is_any_v<Request, CreateRowsetRequest>) {
        // prepare_rowset, commit_rowset, update_tmp_rowset
        if (req.has_rowset_meta() && req.rowset_meta().has_table_id()) {
            return req.rowset_meta().table_id();
        }
    } else if constexpr (is_any_v<Request, UpdateDeleteBitmapRequest>) {
        return req.table_id();
    } else if constexpr (is_any_v<Request, UpdatePackedFileInfoRequest>) {
        // 需要从 packed_file_path 中解析 table_id，或者在请求中添加该字段
        // 暂时返回 -1，后续根据实际情况处理
    }
    return -1;  // 非导入相关RPC或无法获取table_id
}
```

### 3. MetaServiceRPC 到 LoadRelatedRpc 的转换

```cpp
LoadRelatedRpc to_load_related_rpc(MetaServiceRPC rpc) {
    switch (rpc) {
        case MetaServiceRPC::PREPARE_ROWSET:
            return LoadRelatedRpc::PREPARE_ROWSET;
        case MetaServiceRPC::COMMIT_ROWSET:
            return LoadRelatedRpc::COMMIT_ROWSET;
        case MetaServiceRPC::UPDATE_TMP_ROWSET:
            return LoadRelatedRpc::UPDATE_TMP_ROWSET;
        case MetaServiceRPC::UPDATE_PACKED_FILE_INFO:
            return LoadRelatedRpc::UPDATE_PACKED_FILE_INFO;
        case MetaServiceRPC::UPDATE_DELETE_BITMAP:
            return LoadRelatedRpc::UPDATE_DELETE_BITMAP;
        default:
            return LoadRelatedRpc::COUNT;  // 表示非导入相关RPC
    }
}
```

### 4. 线程池满时的处理

```cpp
// 等待到指定时间点，返回实际等待的纳秒数
// 如果在 pthread 线程池中且池满，可能提前返回
int64_t wait_until(std::chrono::steady_clock::time_point allow_time) {
    auto now = std::chrono::steady_clock::now();
    if (allow_time <= now) {
        return 0;  // 不需要等待
    }

    auto wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        allow_time - now).count();

    // 检查当前是否在 bthread 中
    if (bthread_self() != 0) {
        // 在 bthread 中，使用 bthread_usleep
        bthread_usleep(wait_ns / 1000);  // 转换为微秒
        return wait_ns;
    }

    // 在 pthread 中
    // TODO: 检查当前线程池是否快满，如果是则可能需要返回错误而不是等待
    // 目前先简单处理，直接等待
    std::this_thread::sleep_for(std::chrono::nanoseconds(wait_ns));
    return wait_ns;
}
```

### 5. 后台降级检查线程

在 CloudMetaMgr 或 ExecEnv 中启动后台线程定期检查降级：

```cpp
// 在 ExecEnv 或 CloudStorageEngine 中
void start_backpressure_check_thread() {
    _backpressure_check_thread = std::thread([this]() {
        while (!_stop_flag) {
            if (_backpressure_handler) {
                _backpressure_handler->try_downgrade();
            }
            // 每秒检查一次
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });
}
```

### 6. ExecEnv / CloudMetaMgr 集成

**文件**: `be/src/cloud/cloud_meta_mgr.h`, `be/src/cloud/cloud_meta_mgr.cpp`

```cpp
class CloudMetaMgr {
    // ...

    void set_backpressure_handler(MSBackpressureHandler* handler) {
        _backpressure_handler = handler;
    }

private:
    MSBackpressureHandler* _backpressure_handler {nullptr};
};
```

**文件**: `be/src/runtime/exec_env.h`, `be/src/runtime/exec_env_init.cpp`

```cpp
class ExecEnv {
    // ...
    TableRpcQpsRegistry* table_rpc_qps_registry() { return _table_rpc_qps_registry.get(); }
    TableRpcThrottler* table_rpc_throttler() { return _table_rpc_throttler.get(); }
    MSBackpressureHandler* ms_backpressure_handler() { return _ms_backpressure_handler.get(); }

private:
    std::unique_ptr<TableRpcQpsRegistry> _table_rpc_qps_registry;
    std::unique_ptr<TableRpcThrottler> _table_rpc_throttler;
    std::unique_ptr<MSBackpressureHandler> _ms_backpressure_handler;
};
```

## 实现步骤

### Step 1: 基础组件

1. 实现 `StrictQpsLimiter` 类
2. 编写 `StrictQpsLimiter` 单元测试

### Step 2: QPS 统计 (基于bvar)

1. 实现 `TableRpcQpsCounter` 类
2. 实现 `TableRpcQpsRegistry` 类
3. 编写单元测试
4. 验证 bvar 指标正确暴露

### Step 3: 表级别限流器

1. 实现 `TableRpcThrottler` 类
2. 编写单元测试

### Step 4: MS 背压处理器

1. 实现 `MSBackpressureHandler` 类
2. 实现限流升级逻辑
3. 实现限流降级逻辑
4. 编写控制器单元测试

### Step 5: 配置与集成

1. 添加配置项
2. 在 ExecEnv 中初始化组件
3. 修改 `retry_rpc` 函数集成限流逻辑
4. 实现 `extract_table_id` 辅助函数
5. 实现 `to_load_related_rpc` 转换函数
6. 启动后台降级检查线程

### Step 6: 线程池检查 (可选优化)

1. 梳理导入相关 RPC 的调用上下文 (bthread vs pthread)
2. 实现线程池满时的快速失败逻辑

### Step 7: 可观测性完善

1. 确保所有 bvar 指标正确暴露
2. 添加关键路径日志

### Step 8: 测试

1. 单元测试覆盖所有组件
2. 集成测试验证端到端流程
3. 压力测试验证限流效果

## 文件变更清单

### 新增文件

| 文件路径 | 说明 |
|---------|------|
| `be/src/cloud/cloud_tablet_rpc_throttler.h` | 表级别限流相关类的头文件 |
| `be/src/cloud/cloud_tablet_rpc_throttler.cpp` | 表级别限流相关类的实现 |
| `be/test/cloud/cloud_tablet_rpc_throttler_test.cpp` | 单元测试 |

### 修改文件

| 文件路径 | 修改内容 |
|---------|---------|
| `be/src/cloud/config.h` | 添加新配置项声明 |
| `be/src/cloud/config.cpp` | 添加新配置项定义和默认值 |
| `be/src/cloud/cloud_meta_mgr.cpp` | 集成表级别限流逻辑到 retry_rpc |
| `be/src/cloud/cloud_meta_mgr.h` | 添加 MSBackpressureHandler 指针 |
| `be/src/runtime/exec_env.h` | 添加新组件成员和访问器 |
| `be/src/runtime/exec_env_init.cpp` | 初始化新组件 |

## 测试计划

### 单元测试

1. **StrictQpsLimiter 测试**
   - 验证严格的 QPS 控制 (不允许 burst)
   - 验证动态更新 QPS
   - 并发访问正确性

2. **TableRpcQpsCounter 测试**
   - 记录调用并正确统计
   - bvar 指标正确暴露
   - QPS 计算正确性

3. **TableRpcQpsRegistry 测试**
   - 多表、多RPC类型记录
   - Top-K 查询正确性
   - 并发记录正确性
   - 内存限制生效

4. **TableRpcThrottler 测试**
   - 设置、获取、移除限制
   - throttle 返回正确的等待时间
   - 多 RPC 类型/表独立

5. **MSBackpressureHandler 测试**
   - 限流升级触发条件 (MS_BUSY + 间隔)
   - 限流升级的 top-k 表选择
   - 限流升级的衰减计算
   - 限流降级触发条件
   - 升级历史正确维护和撤销

### 集成测试

1. 模拟 MS 返回 MS_BUSY 场景
2. 验证限流升级生效
3. 验证限流降级生效
4. 验证对正常查询无影响
5. 验证 bvar 指标正确

## 类图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        MSBackpressureHandler                        │
│  - 协调 QPS 统计、限流升级、限流降级                                   │
│  - on_ms_busy() / try_downgrade() / before_rpc() / after_rpc()      │
└─────────────────────────────────────────────────────────────────────┘
                │                                    │
                │ uses                               │ uses
                ▼                                    ▼
┌─────────────────────────────┐    ┌─────────────────────────────────┐
│    TableRpcQpsRegistry      │    │       TableRpcThrottler         │
│  - 管理所有表的 QPS 统计      │    │  - 管理所有表的限流器            │
│  - record() / get_top_k()   │    │  - throttle() / set_qps_limit() │
└─────────────────────────────┘    └─────────────────────────────────┘
                │                                    │
                │ contains                           │ contains
                ▼                                    ▼
┌─────────────────────────────┐    ┌─────────────────────────────────┐
│    TableRpcQpsCounter       │    │       StrictQpsLimiter          │
│  - 单个表的 QPS 统计 (bvar)  │    │  - 严格 QPS 限流 (无burst)       │
│  - increment() / get_qps()  │    │  - reserve() / update_qps()     │
└─────────────────────────────┘    └─────────────────────────────────┘
```

## 注意事项

1. **性能**: bvar 统计是 lock-free 的，性能开销很小
2. **内存**: 通过 `ms_rpc_max_tracked_tables_per_rpc` 限制跟踪的表数量
3. **线程安全**: 所有组件需要支持并发访问
4. **向后兼容**: 默认关闭 (`enable_ms_backpressure_handling = false`)
5. **可观测性**: 所有 bvar 使用统一前缀 `ms_rpc_backpressure_`

## 风险与缓解

| 风险 | 缓解措施 |
|-----|---------|
| bvar 数量过多影响性能 | 限制跟踪的表数量，定期清理不活跃的表 |
| 误判导致正常请求被限流 | 保守的默认配置，充分测试 |
| 限流器锁争用影响性能 | 使用 shared_mutex，读多写少场景下性能好 |
| 线程池判断不准确 | 保守策略，优先保证正确性 |
