# 限流动态变化状态机设计

## 问题

当前 `MSBackpressureHandler` 存在以下可测试性问题：

1. **内部感知时间**：`on_ms_busy()` 和 `try_downgrade()` 内部调用 `steady_clock::now()`，测试升级/降级的时间间隔条件必须真正 sleep，导致测试慢且不稳定。
2. **直接读取全局配置**：方法内部直接读 `config::ms_backpressure_upgrade_interval_sec` 等全局变量，测试需要 save/restore 全局状态。
3. **QPS 数据来源耦合**：`upgrade_throttle()` 内部调用 `_qps_registry->get_top_k_tables()` 获取当前 QPS 数据，测试时无法方便地构造精确的 QPS 分布（因为 bvar::PerSecond 依赖真实时间流逝来计算 QPS）。
4. **副作用混在决策中**：状态转换逻辑和执行限流动作（调用 `_throttler->set_qps_limit()`）混在一起，无法单独验证决策逻辑的正确性。

## 设计目标

将限流的动态升级/降级行为抽象为一个**纯状态机**：

- **不依赖外部配置**：所有参数通过方法参数或构造参数显式传入。
- **不感知时间流动**：不在内部调用 `Clock::now()`，由调用方告知"发生了什么事件"。
- **不执行副作用**：状态转换只产出**动作描述**（要设置/移除哪些限流），不直接操作 throttler。
- **确定性可测试**：给定相同的事件序列，产出完全相同的状态转换和动作序列。

## 核心设计

### 事件与动作

```cpp
// 状态机接收的输入事件
struct QpsSnapshot {
    // 某个 rpc 在某个 table 上的当前 QPS
    LoadRelatedRpc rpc_type;
    int64_t table_id;
    double current_qps;
};

// 状态机产出的动作
struct ThrottleAction {
    enum class Type { SET_LIMIT, REMOVE_LIMIT };
    Type type;
    LoadRelatedRpc rpc_type;
    int64_t table_id;
    double qps_limit;  // only meaningful for SET_LIMIT
};
```

### ThrottleStateMachine

```cpp
struct ThrottleParams {
    int top_k = 3;            // 每次升级选 top-k 个表
    double ratio = 0.5;       // 衰减比例
    double floor_qps = 1.0;   // QPS 限制下限
};

class ThrottleStateMachine {
public:
    explicit ThrottleStateMachine(ThrottleParams params);

    // 运行时更新参数，立即生效于下一次 on_upgrade
    // 注意：已产生的升级历史不会因参数变化而重新计算
    void update_params(ThrottleParams params);

    // 处理一次限流升级事件
    // qps_snapshot: 每个 (rpc, table) 的当前 QPS 快照，由调用方从外部获取后传入
    // 返回: 本次升级产生的动作列表
    std::vector<ThrottleAction> on_upgrade(
        const std::vector<QpsSnapshot>& qps_snapshot);

    // 处理一次限流降级事件
    // 返回: 本次降级产生的动作列表（撤销最近一次升级）
    std::vector<ThrottleAction> on_downgrade();

    // 查询当前状态
    size_t upgrade_level() const;  // 当前升级了多少层
    // 查询某个 (rpc, table) 当前的限制值，0 表示无限制
    double get_current_limit(LoadRelatedRpc rpc_type, int64_t table_id) const;
    // 获取当前参数
    ThrottleParams get_params() const;

private:
    mutable std::mutex _mtx;  // 保护并发访问
    ThrottleParams _params;
    // 每次升级的变更记录，用于降级时撤销
    // changes: (rpc_type, table_id) -> (old_limit, new_limit)
    struct UpgradeRecord {
        std::map<std::pair<LoadRelatedRpc, int64_t>,
                 std::pair<double, double>> changes;
    };
    std::vector<UpgradeRecord> _upgrade_history;

    // 当前所有 (rpc, table) 上的生效限制
    std::map<std::pair<LoadRelatedRpc, int64_t>, double> _current_limits;
};
```

### 关键行为说明

**`on_upgrade(qps_snapshot)`**

1. 按 rpc_type 分组 `qps_snapshot`。
2. 对每种 rpc_type，按 QPS 降序取 top-k 个表。
3. 对每个选中的 (rpc, table)：
   - 若 `_current_limits` 中已有限制 `L`，则 `new_limit = max(L * ratio, floor_qps)`。
   - 若无限制，则 `new_limit = max(current_qps * ratio, floor_qps)`。
   - 仅当 `new_limit < current_qps` 或已有限制时，才产出 `SET_LIMIT` 动作。
4. 记录本次变更到 `_upgrade_history`，更新 `_current_limits`。
5. 返回所有产出的 `ThrottleAction`。

**`on_downgrade()`**

1. 若 `_upgrade_history` 为空，返回空列表。
2. 取出最后一条 `UpgradeRecord`。
3. 对其中每个 (rpc, table)：
   - 若 `old_limit > 0`，产出 `SET_LIMIT(old_limit)` 动作，并将 `_current_limits` 恢复为 `old_limit`。
   - 若 `old_limit == 0`，产出 `REMOVE_LIMIT` 动作，并从 `_current_limits` 中删除。
4. 弹出该记录。
5. 返回所有产出的 `ThrottleAction`。

### UpgradeDowngradeCoordinator

`ThrottleStateMachine` 只处理纯粹的升级/降级决策，不管"何时该升级/何时该降级"。升级/降级的触发时机由 `UpgradeDowngradeCoordinator` 负责，它同样不感知时间：

```cpp
class UpgradeDowngradeCoordinator {
public:
    struct Params {
        // 两次升级之间至少间隔多少个 tick
        int upgrade_cooldown_ticks = 10;
        // 最后一次 MS_BUSY 后经过多少个 tick 触发降级
        int downgrade_after_ticks = 60;
    };

    explicit UpgradeDowngradeCoordinator(Params params);

    // 运行时更新参数，立即生效于后续的 report_ms_busy() 和 tick()
    // 注意：已有的 tick 计数不会因参数变化而重置
    void update_params(Params params);

    // 报告收到了一次 MS_BUSY
    // 返回 true 表示应该触发升级
    bool report_ms_busy();

    // 推进一个 tick（调用方按固定间隔调用，如每秒一次）
    // 返回 true 表示应该触发降级
    bool tick();

    // 告知 coordinator 当前是否有可降级的升级记录
    // 由调用方在状态机升级/降级后调用
    void set_has_pending_upgrades(bool has);

    // 查询状态
    int ticks_since_last_ms_busy() const;
    int ticks_since_last_upgrade() const;
    Params get_params() const;

private:
    mutable std::mutex _mtx;
    Params _params;
    int _ticks_since_last_ms_busy = -1;  // -1 表示从未收到
    int _ticks_since_last_upgrade = -1;  // -1 表示从未升级
    bool _has_pending_upgrades = false;   // 是否有可降级的升级记录
};
```

**`report_ms_busy()`**

1. 重置 `_ticks_since_last_ms_busy = 0`。
2. 若 `_ticks_since_last_upgrade == -1` 或 `_ticks_since_last_upgrade >= upgrade_cooldown_ticks`：
   - 重置 `_ticks_since_last_upgrade = 0`。
   - 设置 `_has_pending_upgrades = true`。
   - 返回 `true`（应触发升级）。
3. 否则返回 `false`（冷却中）。

**`tick()`**

1. 若 `_ticks_since_last_ms_busy >= 0`，递增。
2. 若 `_ticks_since_last_upgrade >= 0`，递增。
3. 若 `_has_pending_upgrades` 且 `_ticks_since_last_ms_busy >= downgrade_after_ticks`：
   - 重置 `_ticks_since_last_ms_busy = 0`（为下一次降级重新计时）。
   - 返回 `true`（应触发降级）。
4. 否则返回 `false`。

注意：降级后需由调用方告知 coordinator 是否还有剩余升级记录（即 `ThrottleStateMachine::upgrade_level() > 0`），以更新 `_has_pending_upgrades`。

### 整体协作

```
外部调用方 (MSBackpressureHandler / 后台线程)
    │
    ├── 收到 MS_BUSY ──→ coordinator.report_ms_busy()
    │                        │ 返回 true?
    │                        └──→ 从 qps_registry 获取 qps_snapshot
    │                             └──→ state_machine.on_upgrade(snapshot)
    │                                    └──→ 返回 actions
    │                                          └──→ 逐个执行到 throttler
    │
    └── 每秒 tick ──→ coordinator.tick()
                        │ 返回 true?
                        └──→ state_machine.on_downgrade()
                               └──→ 返回 actions
                                     └──→ 逐个执行到 throttler
                                     └──→ coordinator.set_has_pending(
                                              state_machine.upgrade_level() > 0)
```

在生产环境中，`MSBackpressureHandler` 仍然是对外暴露的入口类，它内部持有 `UpgradeDowngradeCoordinator` 和 `ThrottleStateMachine`，并负责：

1. 在 `on_ms_busy()` 中调用 `coordinator.report_ms_busy()`，若返回 true 则从 `_qps_registry` 获取快照，交给 `state_machine.on_upgrade()`，再将产出的 actions 应用到 `_throttler`。
2. 在 `try_downgrade()` 中调用 `coordinator.tick()`，若返回 true 则调用 `state_machine.on_downgrade()`，再将 actions 应用到 `_throttler`。

但在**单测中**，可以直接构造 `ThrottleStateMachine` 和 `UpgradeDowngradeCoordinator`，不需要 `TableRpcQpsRegistry`、`TableRpcThrottler`、全局 config、或真实时间。

## 单测示例

### ThrottleStateMachine 单测

```cpp
TEST(ThrottleStateMachineTest, SingleUpgradeAndDowngrade) {
    ThrottleParams params {.top_k = 2, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(params);

    // 构造 QPS 快照：table 100 在 PREPARE_ROWSET 上 QPS=200，
    //               table 200 在 PREPARE_ROWSET 上 QPS=100
    std::vector<QpsSnapshot> snapshot = {
        {LoadRelatedRpc::PREPARE_ROWSET, 100, 200.0},
        {LoadRelatedRpc::PREPARE_ROWSET, 200, 100.0},
        {LoadRelatedRpc::PREPARE_ROWSET, 300, 10.0},  // 不在 top-2 中
    };

    auto actions = sm.on_upgrade(snapshot);

    // 应该产出 2 个 SET_LIMIT 动作（top-2）
    ASSERT_EQ(actions.size(), 2);

    // table 100: new_limit = 200 * 0.5 = 100
    EXPECT_EQ(actions[0].type, ThrottleAction::Type::SET_LIMIT);
    EXPECT_EQ(actions[0].table_id, 100);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 100.0);

    // table 200: new_limit = 100 * 0.5 = 50
    EXPECT_EQ(actions[1].type, ThrottleAction::Type::SET_LIMIT);
    EXPECT_EQ(actions[1].table_id, 200);
    EXPECT_DOUBLE_EQ(actions[1].qps_limit, 50.0);

    EXPECT_EQ(sm.upgrade_level(), 1);

    // 降级：撤销上面的升级
    auto downgrade_actions = sm.on_downgrade();
    ASSERT_EQ(downgrade_actions.size(), 2);

    // 两个表都应该被 REMOVE_LIMIT（因为之前没有限制）
    for (const auto& action : downgrade_actions) {
        EXPECT_EQ(action.type, ThrottleAction::Type::REMOVE_LIMIT);
    }

    EXPECT_EQ(sm.upgrade_level(), 0);
}

TEST(ThrottleStateMachineTest, MultipleUpgradesThenDowngrades) {
    ThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(params);

    // 第一次升级
    auto a1 = sm.on_upgrade({{LoadRelatedRpc::COMMIT_ROWSET, 100, 80.0}});
    ASSERT_EQ(a1.size(), 1);
    EXPECT_DOUBLE_EQ(a1[0].qps_limit, 40.0);  // 80 * 0.5

    // 第二次升级，同一个表，当前限制 40
    auto a2 = sm.on_upgrade({{LoadRelatedRpc::COMMIT_ROWSET, 100, 40.0}});
    ASSERT_EQ(a2.size(), 1);
    EXPECT_DOUBLE_EQ(a2[0].qps_limit, 20.0);  // 40 * 0.5 (基于 old limit)

    EXPECT_EQ(sm.upgrade_level(), 2);

    // 第一次降级：撤销第二次升级，恢复到 40
    auto d1 = sm.on_downgrade();
    ASSERT_EQ(d1.size(), 1);
    EXPECT_EQ(d1[0].type, ThrottleAction::Type::SET_LIMIT);
    EXPECT_DOUBLE_EQ(d1[0].qps_limit, 40.0);

    // 第二次降级：撤销第一次升级，移除限制
    auto d2 = sm.on_downgrade();
    ASSERT_EQ(d2.size(), 1);
    EXPECT_EQ(d2[0].type, ThrottleAction::Type::REMOVE_LIMIT);

    EXPECT_EQ(sm.upgrade_level(), 0);
}

TEST(ThrottleStateMachineTest, FloorQpsEnforced) {
    ThrottleParams params {.top_k = 1, .ratio = 0.1, .floor_qps = 5.0};
    ThrottleStateMachine sm(params);

    // current_qps=10, 10*0.1=1.0 < floor(5.0), 应该用 floor
    auto actions = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 10.0}});
    ASSERT_EQ(actions.size(), 1);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 5.0);
}

TEST(ThrottleStateMachineTest, DowngradeOnEmptyHistoryIsNoop) {
    ThrottleParams params {};
    ThrottleStateMachine sm(params);

    auto actions = sm.on_downgrade();
    EXPECT_TRUE(actions.empty());
    EXPECT_EQ(sm.upgrade_level(), 0);
}

TEST(ThrottleStateMachineTest, UpdateTopKAtRuntime) {
    ThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(params);

    // 第一次升级，top_k=1，只有 table 100 被限流
    auto a1 = sm.on_upgrade({
        {LoadRelatedRpc::PREPARE_ROWSET, 100, 100.0},
        {LoadRelatedRpc::PREPARE_ROWSET, 200, 80.0},
    });
    ASSERT_EQ(a1.size(), 1);
    EXPECT_EQ(a1[0].table_id, 100);

    // 运行时修改 top_k=3
    sm.update_params({.top_k = 3, .ratio = 0.5, .floor_qps = 1.0});

    // 第二次升级，现在应该同时限流两个表
    auto a2 = sm.on_upgrade({
        {LoadRelatedRpc::PREPARE_ROWSET, 100, 50.0},
        {LoadRelatedRpc::PREPARE_ROWSET, 200, 40.0},
        {LoadRelatedRpc::PREPARE_ROWSET, 300, 30.0},
    });
    ASSERT_EQ(a2.size(), 3);
    EXPECT_EQ(a2[0].table_id, 100);
    EXPECT_EQ(a2[1].table_id, 200);
    EXPECT_EQ(a2[2].table_id, 300);
}

TEST(ThrottleStateMachineTest, UpdateRatioAtRuntime) {
    ThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(params);

    // 第一次升级，ratio=0.5
    auto a1 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 100.0}});
    EXPECT_DOUBLE_EQ(a1[0].qps_limit, 50.0);

    // 运行时修改 ratio=0.1（更激进）
    sm.update_params({.top_k = 1, .ratio = 0.1, .floor_qps = 1.0});

    // 第二次升级，新的 ratio 生效
    auto a2 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 50.0}});
    EXPECT_DOUBLE_EQ(a2[0].qps_limit, 5.0);  // 50 * 0.1
}

TEST(ThrottleStateMachineTest, UpdateFloorQpsAtRuntime) {
    ThrottleParams params {.top_k = 1, .ratio = 0.01, .floor_qps = 1.0};
    ThrottleStateMachine sm(params);

    // 第一次升级，qps=10, 10*0.01=0.1 < floor(1.0)，用 floor
    auto a1 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 10.0}});
    EXPECT_DOUBLE_EQ(a1[0].qps_limit, 1.0);

    // 运行时提升 floor_qps=5.0
    sm.update_params({.top_k = 1, .ratio = 0.01, .floor_qps = 5.0});

    // 第二次升级，新的 floor 生效
    auto a2 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 1.0}});
    EXPECT_DOUBLE_EQ(a2[0].qps_limit, 5.0);  // 1*0.01=0.01 < floor(5.0)
}
```

### UpgradeDowngradeCoordinator 单测

```cpp
TEST(CoordinatorTest, UpgradeCooldown) {
    UpgradeDowngradeCoordinator::Params params {
        .upgrade_cooldown_ticks = 10,
        .downgrade_after_ticks = 60
    };
    UpgradeDowngradeCoordinator coord(params);

    // 第一次 MS_BUSY 应触发升级
    EXPECT_TRUE(coord.report_ms_busy());

    // 冷却期内的 MS_BUSY 不触发升级
    for (int i = 0; i < 9; i++) {
        coord.tick();
        EXPECT_FALSE(coord.report_ms_busy());
    }

    // 冷却期结束后可以再次触发
    coord.tick();  // 第 10 个 tick
    EXPECT_TRUE(coord.report_ms_busy());
}

TEST(CoordinatorTest, DowngradeAfterQuietPeriod) {
    UpgradeDowngradeCoordinator::Params params {
        .upgrade_cooldown_ticks = 5,
        .downgrade_after_ticks = 10
    };
    UpgradeDowngradeCoordinator coord(params);

    // 触发一次升级
    coord.report_ms_busy();
    coord.set_has_pending_upgrades(true);

    // tick 9 次，不应触发降级
    for (int i = 0; i < 9; i++) {
        EXPECT_FALSE(coord.tick());
    }

    // 第 10 个 tick，触发降级
    EXPECT_TRUE(coord.tick());
}

TEST(CoordinatorTest, MsBusyResetsDowngradeTimer) {
    UpgradeDowngradeCoordinator::Params params {
        .upgrade_cooldown_ticks = 5,
        .downgrade_after_ticks = 10
    };
    UpgradeDowngradeCoordinator coord(params);

    coord.report_ms_busy();
    coord.set_has_pending_upgrades(true);

    // tick 8 次
    for (int i = 0; i < 8; i++) {
        coord.tick();
    }

    // 再收到 MS_BUSY，降级计时器重置
    coord.report_ms_busy();  // 此时 cooldown 内，不触发升级

    // 再 tick 9 次，不应触发降级（因为计时器被重置了）
    for (int i = 0; i < 9; i++) {
        EXPECT_FALSE(coord.tick());
    }

    // 第 10 个 tick，触发降级
    EXPECT_TRUE(coord.tick());
}

TEST(CoordinatorTest, NoDowngradeWithoutPendingUpgrades) {
    UpgradeDowngradeCoordinator::Params params {
        .upgrade_cooldown_ticks = 5,
        .downgrade_after_ticks = 3
    };
    UpgradeDowngradeCoordinator coord(params);

    coord.report_ms_busy();
    // 不设置 has_pending_upgrades

    for (int i = 0; i < 100; i++) {
        EXPECT_FALSE(coord.tick());
    }
}

TEST(CoordinatorTest, UpdateUpgradeCooldownAtRuntime) {
    UpgradeDowngradeCoordinator::Params params {
        .upgrade_cooldown_ticks = 10,
        .downgrade_after_ticks = 60
    };
    UpgradeDowngradeCoordinator coord(params);

    EXPECT_TRUE(coord.report_ms_busy());

    // tick 4 次
    for (int i = 0; i < 4; i++) {
        coord.tick();
        EXPECT_FALSE(coord.report_ms_busy());  // 冷却中
    }

    // 运行时将冷却期缩短到 5 ticks
    coord.update_params({.upgrade_cooldown_ticks = 5, .downgrade_after_ticks = 60});

    // 再 tick 1 次（总共第 5 个 tick），冷却期结束
    coord.tick();
    EXPECT_TRUE(coord.report_ms_busy());  // 可以触发升级了
}

TEST(CoordinatorTest, UpdateDowngradeIntervalAtRuntime) {
    UpgradeDowngradeCoordinator::Params params {
        .upgrade_cooldown_ticks = 1,
        .downgrade_after_ticks = 20
    };
    UpgradeDowngradeCoordinator coord(params);

    coord.report_ms_busy();
    coord.set_has_pending_upgrades(true);

    // tick 9 次，不应触发降级（需要 20 ticks）
    for (int i = 0; i < 9; i++) {
        EXPECT_FALSE(coord.tick());
    }

    // 运行时将降级间隔缩短到 10 ticks
    coord.update_params({.upgrade_cooldown_ticks = 1, .downgrade_after_ticks = 10});

    // 再 tick 1 次（总共第 10 个 tick），触发降级
    EXPECT_TRUE(coord.tick());
}

TEST(CoordinatorTest, UpdateBothParamsAtRuntime) {
    UpgradeDowngradeCoordinator::Params params {
        .upgrade_cooldown_ticks = 100,
        .downgrade_after_ticks = 100
    };
    UpgradeDowngradeCoordinator coord(params);

    EXPECT_TRUE(coord.report_ms_busy());
    coord.set_has_pending_upgrades(true);

    // tick 50 次，什么都不应发生
    for (int i = 0; i < 50; i++) {
        coord.tick();
        EXPECT_FALSE(coord.report_ms_busy());  // 冷却中
        EXPECT_FALSE(coord.tick());             // 降级间隔未到
    }

    // 运行时大幅缩短两个间隔
    coord.update_params({.upgrade_cooldown_ticks = 1, .downgrade_after_ticks = 1});

    // 下一个 tick 就应该触发降级
    EXPECT_TRUE(coord.tick());

    // 下一个 report_ms_busy 也应该允许升级
    EXPECT_TRUE(coord.report_ms_busy());
}
```

### 综合场景单测

```cpp
TEST(IntegrationTest, FullUpgradeDowngradeCycle) {
    // 纯内存中的完整升降级循环，不依赖时间/配置/bvar
    ThrottleParams tp {.top_k = 2, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(tp);

    UpgradeDowngradeCoordinator::Params cp {
        .upgrade_cooldown_ticks = 3,
        .downgrade_after_ticks = 5
    };
    UpgradeDowngradeCoordinator coord(cp);

    // 模拟 3 个表的 QPS 数据
    auto make_snapshot = [](double qps1, double qps2, double qps3) {
        return std::vector<QpsSnapshot> {
            {LoadRelatedRpc::PREPARE_ROWSET, 100, qps1},
            {LoadRelatedRpc::PREPARE_ROWSET, 200, qps2},
            {LoadRelatedRpc::PREPARE_ROWSET, 300, qps3},
        };
    };

    std::vector<ThrottleAction> all_actions;

    // T=0: 收到 MS_BUSY，触发第一次升级
    ASSERT_TRUE(coord.report_ms_busy());
    auto actions = sm.on_upgrade(make_snapshot(100, 50, 10));
    // top-2: table 100 (limit=50), table 200 (limit=25)
    ASSERT_EQ(actions.size(), 2);
    coord.set_has_pending_upgrades(sm.upgrade_level() > 0);

    // T=1,2: tick，无事发生
    EXPECT_FALSE(coord.tick());
    EXPECT_FALSE(coord.tick());

    // T=3: 再收到 MS_BUSY（冷却期刚好结束），触发第二次升级
    coord.tick();
    ASSERT_TRUE(coord.report_ms_busy());
    // 这次 table 100 已有 limit=50，所以 new_limit = 50*0.5 = 25
    actions = sm.on_upgrade(make_snapshot(50, 25, 10));
    EXPECT_EQ(sm.upgrade_level(), 2);
    coord.set_has_pending_upgrades(true);

    // T=4..8: 5 个 tick 没有 MS_BUSY，触发降级
    for (int i = 0; i < 4; i++) {
        EXPECT_FALSE(coord.tick());
    }
    ASSERT_TRUE(coord.tick());  // T=8: 距离 T=3 的 MS_BUSY 已过 5 ticks

    actions = sm.on_downgrade();  // 撤销第二次升级
    EXPECT_EQ(sm.upgrade_level(), 1);
    coord.set_has_pending_upgrades(true);

    // 再等 5 个 tick，触发第二次降级
    for (int i = 0; i < 4; i++) {
        EXPECT_FALSE(coord.tick());
    }
    ASSERT_TRUE(coord.tick());

    actions = sm.on_downgrade();  // 撤销第一次升级
    EXPECT_EQ(sm.upgrade_level(), 0);
    coord.set_has_pending_upgrades(false);

    // 再等更久也不会触发降级了
    for (int i = 0; i < 20; i++) {
        EXPECT_FALSE(coord.tick());
    }
}
```

## 与现有代码的关系

### 新增

| 类 | 职责 |
|---|---|
| `ThrottleStateMachine` | 纯决策：给定 QPS 快照，产出升级/降级动作列表 |
| `UpgradeDowngradeCoordinator` | 纯时序控制：基于 tick 计数判断是否应该升级/降级 |
| `QpsSnapshot` / `ThrottleAction` | 数据类型 |

### 修改

| 类 | 变化 |
|---|---|
| `MSBackpressureHandler` | 内部组合 `ThrottleStateMachine` + `UpgradeDowngradeCoordinator`，将 `upgrade_throttle()` / `downgrade_throttle()` 改为调用状态机后将 actions 应用到 throttler |

### 不变

| 类 | 说明 |
|---|---|
| `StrictQpsLimiter` | 无变化 |
| `TableRpcQpsCounter` / `TableRpcQpsRegistry` | 无变化，仍然负责采集 QPS 数据 |
| `TableRpcThrottler` | 无变化，仍然负责执行限流 |

## 优势总结

| 维度 | 当前实现 | 状态机方案 |
|---|---|---|
| 时间依赖 | 内部调 `Clock::now()` | tick 计数，外部推进 |
| 配置依赖 | 直接读全局 config，修改需 reload | 构造时传入 params，支持 `update_params()` 运行时热更新 |
| QPS 数据 | 内部从 bvar registry 获取 | 调用方传入 snapshot |
| 副作用 | 直接操作 throttler | 只返回 action 列表 |
| 单测速度 | 需 sleep 验证时间条件 | 纯内存，微秒级 |
| 单测精度 | bvar QPS 依赖真实时间，不精确 | QPS 值直接传入，完全精确 |
| 单测覆盖 | 难以测试多次升降级交错场景 | 轻松构造任意事件序列 |

### 运行时动态配置支持

状态机方案支持在不重启进程的情况下动态调整参数：

- **`ThrottleStateMachine::update_params()`**
  - 可动态调整 `top_k`：每次升级限流的表数量
  - 可动态调整 `ratio`：限流衰减比例
  - 可动态调整 `floor_qps`：QPS 限制下限

- **`UpgradeDowngradeCoordinator::update_params()`**
  - 可动态调整 `upgrade_cooldown_ticks`：升级冷却间隔
  - 可动态调整 `downgrade_after_ticks`：降级触发间隔

这些参数修改**立即生效**于下一次 `on_upgrade()` / `report_ms_busy()` / `tick()` 调用，无需重启服务，也无需 reload 配置文件。生产环境中，`MSBackpressureHandler` 可以监听配置变化并调用这些方法。
