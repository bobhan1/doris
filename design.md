目标
- 事前最小化主动配置止损, 不需要事后限速手段才能让系统恢复
- 不出现因突发流量压力过大导致ms大面积延迟提升
- 不出现突发持续大压力导入导致其他workload失败
概要设计
主要包括2件事：
1. 为了防止BE侧因为某些程序bug导致的在某个rpc上的异常突发流量，给每个到ms到rpc加一个兜底的qps限流(每个rpc可以独立配置)
2. ms侧返回了反映ms已经压力很大的错误码时，BE侧能做对应调整减小对ms的压力
详细设计
BE每个rpc兜底限流防止突发流量
每个rpc在整个节点范围内按照1核1 qps(一个be配置，默认是1)来限流，例如be是32核机器，则每个rpc按照qps=32, burst=32限流
这样，BE集群对ms的压力和集群规模相关


using MSRpcRateLimiter = S3RateLimiterHolder

std::map<MetaServiceMethod, MSRpcRateLimiter> host_level_rale_limiters_;

int qps = CpuInfo::num_cores() * config::ms_rpc_qps_limit_per_core_per_rpc;
for (auto method : methods) {
    host_level_rale_limiters_.emplace(method, MSRpcRateLimiter(/*qps*/qps, /*burst*/qps, -1));
}

template <typename Request, typename Response>
Status retry_rpc(std::string_view op_name, const Request& req, Response* res,
                 MetaServiceMethod<Request, Response> method) {
    // ...
    while (true) {
        // ...
        if (auto it = host_level_rale_limiters_.find(method); it != rale_limiters_.end()) {
            it->second.add(1);
        }
        (stub.get()->*method)(&cntl, &req, res, nullptr);
注意get_rowset因为需要根据一些额外逻辑重试，目前没走retry_rpc

每个rpc的调用频率不同，需要可以单独配置每个rpc的兜底限流参数
BE根据ms返回的反映压力的错误码作对应调整
对于导入来说：
- 相对于查询不那么关心延迟，主要关注是否成功
- ms的rpc耗时大概率只占导入总时间的一小部分
所以让导入慢下来相对来说是可接受的

这里的主要想法是，当ms反馈压力很大时，BE通过对导入相关rpc增加限流来尝试减小ms的压力，如果在限流一段时间后ms依然反馈压力很大，BE侧继续增加限流的力度，即尝试进一步降低对ms的压力。把这个过程称为限流升级。限流升级的过程没有次数限制，即如果ms侧持续反馈压力很大，BE侧会持续对rpc加大限流
当ms的压力恢复后，BE侧不会再收到MS压力反馈，当BE持续一段时间没有收到ms的压力大的信息后，可以开始逐步缓解之前加的限制。这个过程称为限流降级。

导入会对ms产生的情况基本只有一个，即持续高频高并发的导入。由于这些用户没有事先对这些写入操作分组，doris无法知道导入之间的关系，所以限流的时候只能猜测哪些表可能会触发问题，具体做法如下：
我们优先限制导入相关的qps更高的那些表，因为当前时间高qps的导入有更高概率在接下来的一段时间内也保持更高的qps，所以限制他们更有可能降低对ms的压力。通过限制这些表上的相关rpc的qps让对应的导入慢下来，从而减小到ms的压力。同时，由于限制是在表上的，这不会影响查询以及其他低频导入的表上的相关任务。

对需要限制的导入相关的rpc以表粒度统计过去一个时间窗口(一个be配置，默认1min)内的qps。这包括：
- prepare_rowset/commit_rowset/update_tmp_rowset/update_packed_file_info/update_delete_bitmap

限流升级
触发限流升级的条件：
- 当BE在任意rpc的resp中看到MS_BUSY的错误码时，如果距离上一次看到MS_BUSY错误码的时间超过10s(一个be配置，config::ms_backpressure_upgrade_interval_sec)时，触发限流升级

每次触发限流升级时，都有一个增加限流的动作：对所有需要限制导入相关的rpc，对每个rpc上一个时间窗口内qps最高的k个(一个be配置config::ms_backpressure_upgrade_top_k，默认3)表的增加限流，对与每个选中的表，对其在该rpc上的qps限制增加一个倍率(一个配置， r=config::ms_backpressure_throttle_ratio，默认值0.5)
- 如果这个rpc在这个表上目前没有表级别的qps限制，记其当前统计时间窗口内qps为A，则设置该rpc在该表上的rpc qps limit为 L=A * r
  - 对于突发的高频qps，可以迅速降低；对于qps平稳的影响相对较小
- 如果这个rpc在这个表上已经有表级别的限制，记当前限制为L，则更新 L 为 L=L * r
- rpc在表级别的限流有一个下限值(一个be配置, config::ms_rpc_table_qps_limit_floor)，限流升级不会导致qps limit低于这个值

这里rpc在表级别的qps限制使用如下限流器，不同于令牌桶，它不允许burst，例如限制50qps就是严格每20ms放一个rpc
class StrictQpsLimiter {
public:
    using Clock = std::chrono::steady_clock;

    explicit StrictQpsLimiter(double qps)
        : _interval_ns(static_cast<int64_t>(1e9 / qps)),
          _next_allowed_time(Clock::now()) {}

    Clock::time_point reserve() {
        std::lock_guard<std::mutex> lock(_mtx);

        auto now = Clock::now();
        auto allowed_time = std::max(now, _next_allowed_time);

        _next_allowed_time = allowed_time +
            std::chrono::nanoseconds(_interval_ns);

        return allowed_time;
    }

    void update_qps(double new_qps) {
        std::lock_guard<std::mutex> lock(_mtx);
        _interval_ns = static_cast<int64_t>(1e9 / new_qps);
    }

private:
    std::mutex _mtx;
    int64_t _interval_ns;
    Clock::time_point _next_allowed_time; // 单调递增
};
template <typename Request, typename Response>
Status retry_rpc(std::string_view op_name, const Request& req, Response* res,
                 MetaServiceMethod<Request, Response> method) {
    // ...
    while (true) {
        // ...
        if (auto it = table_level_rale_limiters_.find({method, table_id}); it != rale_limiters_.end()) {
            auto allow_time = it->second.reserve();
            bthread_sleep_until(allow_time);
        }
        if (auto it = host_level_rale_limiters_.find(method); it != rale_limiters_.end()) {
            it->second.add(1);
        }
        (stub.get()->*method)(&cntl, &req, res, nullptr);
这里有一个问题是：如果被限流的控制流不是跑在bthread中的，限流会占住其所在线程池的一个pthread线程。这可能会导致某些线程池全部被等待qps token的线程占满
限流降级
当在一段时间内(一个be配置，config::ms_backpressure_downgrade_interval_sec)没有收到ms的MS_BUSY错误码时，触发限流降级，即解除最后一次限流升级施加的qps限制
预期生效的场景
1. 某个导入需要写10000个rowset，均分在10个BE上，每个BE写1000个rowset，导入在每个BE上写入进度相似，导致10个be上的prepare_rowset, commit_rowset基本在相同时间发到ms，导致ms的qps很高，有上述机制后，可能每个BE在发了200个prepare_rowset请求后都收到了来自ms的MS_BUSY错误码，触发限流升级，在这个表上导入rpc qps限制减半，会以更低的qps继续后面的rpc，ms压力降低。
2. 集群中存在一些低频导入任务，突然在某个表上来了来了一批持续的高频导入。ms返回压力标志后，这个突发的高频导入表会被限制到较低水平，而存量低频任务受的影响相对较小
例子
  配置: ratio=0.5, top_k=2, upgrade_interval=10s, downgrade_interval=60s
  场景: 表A、B正常低频导入(QPS=10)，T=0时表X突然开始高频导入(QPS=500)
时间
事件
当前所有表在prepare_rowset上的限流
T=0s
表X开始高频导入(QPS=500)
A=无, B=无, X=无
T=3s
MS过载，MS_BUSY → 升级，选top2(X,A)
A=5, B=无, X=250
T=6s
MS仍过载，MS_BUSY → 跳过(<10s)
A=5, B=无, X=250
T=15s
MS仍过载，MS_BUSY → 升级，选top2(X,B)
A=5, B=5, X=125
T=27s
MS仍过载，MS_BUSY → 升级，选top2(X,A)
A=2.5, B=5, X=62.5
T=40s
MS仍过载，MS_BUSY → 升级，选top2(X,B)
A=2.5, B=2.5, X=31.25
T=40s起
MS压力缓解，无MS_BUSY
A=2.5, B=2.5, X=31.25
T=100s
60s无MS_BUSY → 降级(撤销T=40s)
A=2.5, B=5, X=62.5
T=160s
60s无MS_BUSY → 降级(撤销T=27s)
A=5, B=5, X=125
T=220s
60s无MS_BUSY → 降级(撤销T=15s)
A=5, B=无, X=250
T=280s
60s无MS_BUSY → 降级(撤销T=3s)
A=无, B=无, X=无
  关键点: 表X始终是主要被限流对象，A/B虽偶尔被选中但限流影响小(本身QPS=10，限到5/2.5仍可正常工作)
可观测
暴露如下bvar：
- 节点当前的限流等级
- 限流升级总次数
- 限流降级总次数
- 每个rpc因为节点级限流而等待的latency
- 每个rpc因为表级别限流而等待的latency

FE系统表查询所有BE上的表级别限流
测试
限流动态变化过程抽象成不感知外部配置和时间流逝的状态机，单测覆盖



