# resilient-call-proxy-go

一个**纯进程内**的服务调用代理层。上游（发起方）与下游（被调服务）都由可控的
`ScriptedDownstream` 脚本模拟，不依赖任何外部服务或真实凭据，用于让以下行为在
并发与故障抖动下保持一致且可解释：

- 请求路由与**共享超时预算**传播
- **熔断器** closed → open → half_open 状态迁移
- 仅对幂等请求的**重试**，以及故障时的**降级**

## 包结构

| 文件 | 职责 |
| --- | --- |
| `proxy.go` | 编排核心：预算检查 → 选目标 → 熔断判定 → 下游调用 → 归因 → 重试/降级 |
| `budget.go` | 共享总超时预算（所有尝试复用同一 deadline context） |
| `breaker.go` | 单目标熔断器状态机，支持受控半开探测名额与迁移观测 |
| `router.go` | 路由表与确定性目标选择（每个目标一个独立熔断器） |
| `downstream.go` | 进程内脚本下游：成功 / 明确拒绝 / 延迟 / 阻塞取消 |
| `errors.go` | 可判别的失败原因 `Reason` 与哨兵错误 |
| `trace.go` | 每次尝试的判定记录与实时日志 |

## 1. 路由与超时预算规则

### 路由

- `Router.Register(route, targets...)` 为一条路由登记**有序**候选目标，每个目标拥有
  独立熔断器。
- 第 `n` 次尝试（从 0 开始）确定性地选择 `targets[n % len(targets)]`（round-robin）。
  因此多次尝试打到哪个目标完全可追溯，失败归因（含对应熔断器状态）前后一致。

### 共享预算（关键不变量）

`Budget` 由发起方 context 派生 `context.WithTimeout(parent, total)`：

1. **所有重试尝试与下游调用复用同一个 deadline context**，任何一次尝试都不可能越过
   总截止时间——这由 context 机制在语言层面保证，而非靠每次调用前的手工判断。
2. 每次尝试前先检查预算：预算耗尽或被取消时**绝不发起下游调用、不占用熔断名额**。
3. 可选的 `PerAttemptSafety` 只是"单次尝试上限"，实际时限取
   `min(剩余预算, PerAttemptSafety)`，永远不会大于剩余预算。
4. 预算到期后，在途下游调用随 context 立即取消并退出（`ScriptedDownstream` 对
   `<-ctx.Done()` 立即返回），不残留 goroutine 或名额占用。
5. 总时限优先级：`Request.Timeout > 0` 时覆盖 `Config.TotalTimeout`；若发起方
   context 自身截止更早，则取更早者。

### 三类可区分的终止原因

| `Reason` | 含义 | 是否计入熔断失败 | 是否可重试 |
| --- | --- | --- | --- |
| `budget_exceeded` | 共享总预算（或更早的父 deadline）耗尽 | 否（不可归因于目标） | 否，立即终止 |
| `canceled` | 发起方主动取消 | 否（不可归因于目标） | 否，立即终止 |
| `downstream_rejected` | 下游**明确拒绝**（业务失败，`ErrRejected`） | 是 | 仅幂等请求 |
| `downstream_timeout` | 下游慢，单次尝试保护到期但总预算尚有余量 | 是 | 仅幂等请求 |
| `breaker_open` | 熔断 open / half_open 探测名额占满，快速失败 | 不调下游 | 否，直接降级 |
| `non_retryable` / `attempts_exhausted` / `no_route` | 非幂等失败 / 达到尝试上限 / 无路由 | 视底层原因 | 否 |

判别方式：

```go
var ce *proxy.CallError
if errors.As(err, &ce) {
    switch ce.Reason {
    case proxy.ReasonBudgetExceeded: // 预算耗尽
    case proxy.ReasonCanceled:       // 发起方取消
    case proxy.ReasonDownstreamRejected:
    }
}
// 或直接按原因判别：
errors.Is(err, &proxy.CallError{Reason: proxy.ReasonBudgetExceeded})
```

判定顺序保证"取消"不被"超时"口径覆盖：先查发起方 context 是否 `Canceled`，
再查预算是否到期。

## 2. 熔断器状态机

```
                 连续失败达到 FailureThreshold
        ┌──────────────────────────────────────┐
        ▼                                        │
   ┌─────────┐  冷却 Cooldown 到期   ┌────────────┐
   │ closed  │ ───────────────────▶ │  half_open │
   │         │                      │  (≤N 探测)  │
   └─────────┘ ◀─────────────────── └────────────┘
        ▲      任一探测成功              │ 任一探测失败
        └───────────────────────────────┘
                         (重新进入 open 并重置冷却)
```

- **closed**：累计**连续**失败次数；任意一次成功立即清零——短暂抖动（失败-成功穿插）
  不会累积到阈值，避免误熔断。
- **open**：所有普通请求**快速失败**（`breaker_open`），不触达下游，直接走降级路径；
  距进入 open 超过 `Cooldown` 后，下一个 `Allow` 原子地迁移到 half_open。
- **half_open**：最多放 `HalfOpenMaxProbes` 个**在途**探测请求，其余快速失败，
  因此半开不会把请求量放大；探测成功 → closed，探测失败 → open（重置冷却）。
- **取消/预算耗尽不计为探测结论**：只释放探测名额、不触发状态迁移，熔断器停留
  half_open，允许后续继续探测。这保证超时抖动不会把半开"打回"open。
- 所有迁移通过 `Breaker.OnTransition(func(from, to))` 同步回调，可观测、可断言；
  回调在锁外派发，避免重入死锁。

## 3. 重试与降级策略

### 重试（绝不放大不可重试的请求）

- 仅 `Request.Idempotent == true` 的请求允许重试，最多 `Config.MaxAttempts` 次
  （含首次）。非幂等请求失败一次即终止，日志记录 `non_idempotent_no_retry`。
- open / half_open 名额占满的快速失败**不消耗尝试**，因为请求从未到达下游。
- 每次重试前重新检查剩余预算；预算不足直接终止，不会再占用目标名额。
- 下游调用次数是精确的：N 个并发请求 × 3 次尝试 = 3N 次调用，可在测试中精确断言。

### 降级

- 通过 `Proxy.SetFallback(route, func(*Request, Reason) (*Response, error))` 注册。
- 任何最终失败（预算耗尽、取消、拒绝、快速失败、无路由等）都会带 `Reason` 进入降级，
  降级函数可按原因给出不同兜底；降级成功则代理调用整体返回成功。

## 4. 可观测性：每次尝试的判定日志

每次尝试打印一行，包含**剩余预算、熔断状态、判定依据**，并在结尾给出整次请求结论：

```
[proxy] attempt=1 target=slow     remaining_budget=79.99ms   breaker=closed   decision=attempt_start          reason=-
[proxy] attempt=1 target=slow     remaining_budget=-182ns    breaker=closed   decision=budget_exhausted_before_attempt reason=budget_exceeded err=context deadline exceeded
[proxy] summary route=svc idempotent=true attempts=1 outcome=FAILED final_reason=budget_exceeded
```

判定类型（`Decision`）包括：`attempt_start / success / downstream_rejected /
downstream_timeout / budget_exhausted_before_attempt / caller_canceled_before_attempt /
breaker_fast_fail / non_idempotent_no_retry / will_retry / attempts_exhausted /
fallback_served / no_route`。`Proxy.Call` 同时返回结构化的 `[]AttemptRecord` 供断言。

## 5. 快速上手

```go
r := proxy.NewRouter(proxy.BreakerConfig{
    FailureThreshold:  3,
    Cooldown:          200 * time.Millisecond,
    HalfOpenMaxProbes: 1,
})
down := proxy.NewScriptedDownstream("a",
    proxy.ScriptStep{Action: proxy.ActionReject},
    proxy.ScriptStep{Action: proxy.ActionOK, Out: "ok"},
)
r.Register("svc", down)

p := proxy.NewProxy(r, proxy.Config{
    MaxAttempts:      3,
    TotalTimeout:     500 * time.Millisecond,
    PerAttemptSafety: 100 * time.Millisecond,
})
p.SetFallback("svc", func(req *proxy.Request, reason proxy.Reason) (*proxy.Response, error) {
    return &proxy.Response{Payload: "degraded:" + string(reason)}, nil
})

resp, records, err := p.Call(context.Background(),
    &proxy.Request{Route: "svc", Idempotent: true})
```

## 6. 验证方法

不依赖外部服务与真实凭据，全部为确定性单元测试：

```bash
go test ./... -race -count=10 -v
```

覆盖矩阵：

| 测试 | 验证点 |
| --- | --- |
| `TestBudgetExhausted_NoFurtherAttempts` | 预算耗尽后不再尝试、不再占用名额、在途调用退出、原因可判别 |
| `TestCallerCanceled_DistinctFromTimeout` | 主动取消与预算耗尽严格区分、取消不重试 |
| `TestRetryRespectsSharedBudget` | 多次尝试共享同一 deadline，剩余预算单调递减，末轮在发起前被预算拦截 |
| `TestDownstreamRejected_RetryThenSuccess` | 明确拒绝与超时区分、幂等重试后成功、调用次数精确 |
| `TestNonIdempotent_NoRetry` / `TestConcurrent_NonIdempotentNeverRetried` | 非幂等绝不重试（含 50 并发，调用数 1:1） |
| `TestBreaker_Transitions` | closed→open→half_open→closed 迁移序列与半开名额受控 |
| `TestBreaker_BriefJitterDoesNotTrip` | 成功穿插清零连续失败，抖动不误熔断 |
| `TestBreaker_HalfOpenFailureReopens` | 探测失败重开 open 并重置冷却 |
| `TestBreaker_CancelReleasesProbeSlot` | 取消只释放名额、不触发迁移 |
| `TestFallback_OnBreakerOpen` | open 期间快速失败、不触达下游、降级生效 |
| `TestConcurrent_HalfOpenProbeNotAmplified` | 20 并发下半开仅 2 个探测打到下游，其余快速失败降级 |
| `TestConcurrent_IdempotentRetriesConsistent` | 50 并发 × 3 尝试 = 150 次精确调用，目标选择可追溯 |

测试日志默认进入 `go test -v` 输出，逐条打印每次尝试的剩余预算、熔断状态与判定依据。
