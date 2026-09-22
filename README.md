# resilient-call-proxy-go

本地、进程内的**服务调用代理层**。上游（调用方）与下游（被调服务）都用
可控的进程内组件替代，不依赖任何外部服务或真实凭据，使以下行为在并发与
故障抖动下**结果一致、可解释、可稳定复现**：

- 请求路由与**共享超时预算**在多次尝试 / 多次下游调用间的传播；
- 熔断器的 CLOSED → OPEN → HALF_OPEN → CLOSED 状态迁移；
- 仅对幂等请求的安全重试、退避与请求量不放大；
- 每种失败的**可区分原因**与完整判定轨迹（trace）。

> 全部时间逻辑由确定性假时钟 `FakeClock` 驱动，单测零墙钟等待、零网络，
> 可通过 `go test -race -count=N` 重复稳定验证。

## 包结构

| 文件 | 职责 |
| --- | --- |
| `reason.go` | 可区分失败原因 `Reason` 与 `CallError` |
| `clock.go` | `Clock` 抽象、`RealClock`、确定性 `FakeClock`（代际栅栏） |
| `budget.go` | 多次尝试共享的总超时预算 `Budget`（到期 / 取消可区分） |
| `router.go` | 路由表；每请求一个基号、请求内按尝试序号顺延选目标 |
| `downstream.go` | 进程内可控下游：脚本化成功 / 拒绝 / 慢故障 / 超时，观测并发占用 |
| `breaker.go` | 单目标熔断器：连续失败阈值、冷却、半开受控探测、迁移可观测 |
| `retry.go` | 重试与退避策略（仅幂等、明确拒绝不重试、退避不越预算） |
| `proxy.go` | 编排：路由 → 熔断 → 下游 → 重试/退避 → 降级，统一归因 |
| `trace.go` | 每次尝试的剩余预算 / 熔断状态 / 判定依据的轨迹与渲染 |

## 一、路由与超时预算规则

### 路由

- 每个请求开始时由 `Router.OpenRequest(route)` 分配一个**路由内基号**
  （跨并发请求公平轮询，加锁分配）。
- 同一请求第 `round` 次尝试确定地选择
  `targets[(base+round) mod n]`，与其他并发请求无关，因此多次尝试的
  目标选择在轨迹里可逐次追溯且前后一致。

### 共享超时预算

- 一次请求创建**一个** `Budget`（总时限 `Config.TotalTimeout` 或调用参数）。
  首次尝试、每次重试、每次退避睡眠、每次下游调用都共享它；
  任何一次尝试的时限 = `min(剩余预算, PerAttemptLimit)`，
  **绝不越过剩余预算**。
- 每次发起新尝试前与调用下游前都检查预算；
  剩余预算不足一个时钟精度 `ClockEpsilon` 时不再发起新尝试（不放大请求量）。
- 预算到期 / 取消后，正在进行的下游等待被立即打断并释放占用名额，
  `Dispose` 注销定时器——**超时后不继续占用名额或资源**。

三种终止原因彼此可区分（`Reason`）：

| 原因 | 触发条件 |
| --- | --- |
| `budget_exhausted` | 共享总预算到期 |
| `downstream_reject` | 下游明确拒绝（快速、确定的失败） |
| `caller_canceled` | 发起方主动取消（优先级高于到期） |
| `all_attempts_failed` | 受熔断 / 非幂等 / 尝试上限约束，未能成功 |
| `fallback_failed` | 降级函数本身失败（原始下游原因保留在 `Trace.Cause`） |

为消除“退避自然醒来”与“预算到期”在同一时刻的竞争，任何会恰好触及
预算终点的等待都提前 `ClockEpsilon` 结束，使预算到期稳定先被裁决。

## 二、熔断状态机

每个目标一个 `Breaker`，所有判断与迁移都在锁内完成：

```text
                 连续失败次数 >= FailureThreshold
        CLOSED ───────────────────────────────► OPEN
          ▲                                         │
          │ 探测成功                                 │ 冷却 Cooldown 到期（惰性迁移）
          │                                         ▼
          └────────────────────────────────── HALF_OPEN
                   探测失败 ────────────────────────┘
                   （重新 OPEN，重置冷却）
```

- **短暂抖动不误熔断**：只统计“连续”失败；一次成功立即清零；
  次数未达阈值保持 CLOSED。
- **OPEN**：普通请求一律快速失败（`BREAKER_REJECT`），不触达下游、
  不消耗预算，直接顺延其他目标 / 走降级。
- **HALF_OPEN**：冷却到期惰性进入，仅放行 `ProbeAllowance` 个**探测请求**，
  其余普通请求仍快速失败。
  - 探测成功 → CLOSED（恢复）；
  - 探测失败 → OPEN（重置冷却，名额归零）。
- 每次迁移都记录 `Transition{From,To,At,Why}`，可通过
  `Breaker.Transitions()` 与轨迹日志观测。
- 放行凭证 `Permit` 必须且只能 `Release` 一次（成功 / 失败 / 超时皆然），
  保证半开探测名额不泄漏。

## 三、降级与重试策略

### 重试

- **仅幂等请求**（`Request.Idempotent=true`）允许重试；
  非幂等请求失败后绝不重试，即使仍有大量预算。
- `DefaultRetryable`：慢故障（抖动，语义上未被接受）可重试；
  **下游明确拒绝不重试**（结论确定）；预算耗尽 / 取消不重试。
- 总尝试数受 `MaxAttempts` 硬性上限约束（含首次）。
- **半开探测请求不再触发额外尝试**，避免半开期间放大流量。
- 指数退避 `BackoffBase` / `BackoffMax`，且退避时长被裁剪到剩余预算内；
  退避中预算终止则立即终止并归因为 `budget_exhausted`。

### 降级

- 下游侧失败（熔断快速失败、明确拒绝、尝试耗尽）进入 `Config.Fallback`。
- 降级成功：返回降级响应，最终原因为 `unknown`，但原始下游原因保留在
  `Trace.Cause`，`Trace.FromFallback=true`。
- 降级本身失败：返回 `fallback_failed`。
- 预算耗尽 / 主动取消不经过降级（它们是调用方侧的终止语义）。

## 四、可观测性

`Trace.Render()` 为每次请求输出多行日志，逐次尝试打印：

- 尝试序号与目标；
- 尝试前后熔断状态、是否半开探测；
- **尝试开始时的剩余预算**与本次尝试时限；
- 下游结果与**判定依据**（人类可读，说明为何重试 / 不重试 / 被熔断 / 被预算截断）；
- 最终原因、原始原因、是否走了降级。

示例：

```text
trace route=pay idempotent=true attempts=2 final=budget_exhausted fallback=false
  attempt#1 target=pay-0 breaker=CLOSED->CLOSED remaining=100ms limit=100ms result=SLOW_FAIL | slow failure (transient); safe to retry; retry after backoff 1ms
  attempt#2 target=pay-0 breaker=CLOSED->CLOSED remaining=39ms limit=39ms result=TIMEOUT | downstream call cut off by shared budget; stop immediately without retry: budget_exhausted
```

## 五、验证方法

```bash
# 常规
go test ./...

# 并发安全（竞态检测）+ 多次重复（稳定复现）
go test ./... -race -count=10

# 查看每个用例打印的剩余预算 / 熔断状态 / 判定依据
go test ./... -v
```

测试矩阵（均为进程内、确定性、无外部依赖）：

| 测试 | 覆盖点 |
| --- | --- |
| `TestBudget_ExpiryAndCancelAreDistinguishable` | 预算耗尽 vs 主动取消可区分、取消优先 |
| `TestFakeClock_*` | 假时钟到期触发、预算打断等待 |
| `TestProxy_SharedBudgetExhausted` | 共享预算跨重试、耗尽即止、名额不泄漏 |
| `TestProxy_DownstreamExplicitReject` | 明确拒绝不重试、走降级、原因可区分 |
| `TestProxy_CallerCanceledIsDistinguishable` | 慢调用中取消立即终止、不再占用名额 |
| `TestBreaker_*` | 抖动不误熔断、阈值打开快速失败、冷却半开受控探测、探测成败迁移 |
| `TestProxy_BreakerOpensFastFailsThenRecovers` | 代理层熔断快速失败零下游调用、半开探测恢复 |
| `TestRetry_*` | 幂等重试成功、非幂等绝不重试、退避受预算约束、重试判定规则 |
| `TestProxy_ConcurrentRetriesDoNotAmplify` | 并发下总下游调用 ≤ N×MaxAttempts、目标可追溯 |
| `TestProxy_FallbackFailureIsDistinguishable` | 降级失败可判别且保留原始原因 |
| `TestRouter_TargetSelectionTraceable` | 每请求基号、请求内顺延、跨请求轮询 |

## 六、在真实时钟下使用

测试用 `FakeClock`；生产/手工验证可换 `NewRealClock()`，API 完全一致：

```go
p := resilient.New(resilient.NewRealClock(), entries, cfg)
resp, reason, trace := p.Do(&resilient.Request{Route: "pay", Idempotent: true}, nil, 0)
```
