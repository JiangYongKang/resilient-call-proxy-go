package resilient_test

import (
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/resilient-call-proxy-go/resilient"
)

// logHealth 以统一格式打印目标健康快照，便于排障时对照轨迹。
func logHealth(t *testing.T, tag string, h resilient.HealthSnapshot) {
	t.Helper()
	t.Logf("[health %s] target=%s state=%s failStreak=%d probing=%t probes=%d/%d live=%d | ok=%d reject=%d slowfail=%d breaker_reject=%d abort_caller=%d abort_budget=%d",
		tag, h.Target, h.State, h.FailStreak, h.Probing, h.ProbesUsed, h.ProbeAllowance, h.ProbesLive,
		h.Successes, h.Rejects, h.SlowFailures, h.BreakerRejects, h.AbortedByCaller, h.AbortedByBudget)
}

// healthCounts 是从轨迹推导出的目标归类计数，用于与健康视图对账（三处对齐）。
type healthCounts struct {
	successes      uint64
	rejects        uint64
	slowFails      uint64
	breakerRejects uint64
	abortCaller    uint64
	abortBudget    uint64
}

// classifyTraces 按“每次尝试的轨迹记录 + 请求最终原因”重放归类规则：
// 与健康视图、返回给调用方的原因必须得出同一结论。
func classifyTraces(traces []resilient.Trace) map[string]*healthCounts {
	out := map[string]*healthCounts{}
	for _, tr := range traces {
		for _, a := range tr.Attempts {
			c := out[a.Target.Name]
			if c == nil {
				c = &healthCounts{}
				out[a.Target.Name] = c
			}
			switch a.Result {
			case resilient.AttemptOK:
				c.successes++
			case resilient.AttemptReject:
				c.rejects++
			case resilient.AttemptSlowFail, resilient.AttemptTimeout:
				c.slowFails++
			case resilient.AttemptBreakerReject:
				c.breakerRejects++
			case resilient.AttemptAborted:
				// 被截断的尝试会立即以预算终止原因结束请求，
				// 因此最终原因即本次终止的归因。
				if tr.FinalReason == resilient.ReasonCallerCanceled {
					c.abortCaller++
				} else {
					c.abortBudget++
				}
			}
		}
	}
	return out
}

// requireHealthMatches 断言健康视图与轨迹重放结果一致。
func requireHealthMatches(t *testing.T, p *resilient.Proxy, name string, want *healthCounts) {
	t.Helper()
	h := p.Health(resilient.Target{Route: "pay", Name: name})
	if h.Successes != want.successes || h.Rejects != want.rejects ||
		h.SlowFailures != want.slowFails || h.BreakerRejects != want.breakerRejects ||
		h.AbortedByCaller != want.abortCaller || h.AbortedByBudget != want.abortBudget {
		t.Fatalf("health of %s mismatch:\n got  ok=%d reject=%d slowfail=%d breaker_reject=%d abort_caller=%d abort_budget=%d\n want ok=%d reject=%d slowfail=%d breaker_reject=%d abort_caller=%d abort_budget=%d (derived from traces)",
			name,
			h.Successes, h.Rejects, h.SlowFailures, h.BreakerRejects, h.AbortedByCaller, h.AbortedByBudget,
			want.successes, want.rejects, want.slowFails, want.breakerRejects, want.abortCaller, want.abortBudget)
	}
}

// 非目标责任终止（取消/预算耗尽）归还名额时不计失败、不清零也不累计
// 连续失败、不驱动状态迁移；半开探测名额如数退回，可再次探测。
func TestBreaker_AbandonIsNeutralAndReturnsProbeSlot(t *testing.T) {
	br, clk, _ := newTestBreaker(t, 2, 1, 10*time.Second)

	// CLOSED 下取消：不改变连续失败计数与状态。
	p, ok := br.Allow()
	if !ok {
		t.Fatal("expected allow in CLOSED")
	}
	p.Abandon(resilient.ReasonCallerCanceled)
	if got := br.State(); got != resilient.BreakerClosed {
		t.Fatalf("after abandon state=%s, want CLOSED", got)
	}
	h := br.Snapshot()
	if h.FailStreak != 0 || h.AbortedByCaller != 1 || h.SlowFailures != 0 {
		t.Fatalf("abandon must not touch failure accounting: %+v", h)
	}

	// 两次真实失败 -> OPEN。
	failOnce(t, br)
	failOnce(t, br)
	if br.State() != resilient.BreakerOpen {
		t.Fatal("want OPEN after 2 consecutive failures")
	}

	// 冷却 -> HALF_OPEN，拿到唯一探测名额后被预算耗尽截断。
	clk.Advance(10 * time.Second)
	if br.State() != resilient.BreakerHalfOpen {
		t.Fatal("want HALF_OPEN after cooldown")
	}
	probe, ok := br.Allow()
	if !ok || !probe.IsProbe() {
		t.Fatal("want half-open probe permit")
	}
	if _, ok := br.Allow(); ok {
		t.Fatal("allowance=1 must reject a second concurrent probe")
	}
	probe.Abandon(resilient.ReasonBudgetExhausted)

	h = br.Snapshot()
	logHealth(t, "after-budget-abandon", h)
	if h.State != resilient.BreakerHalfOpen {
		t.Fatalf("budget-terminated probe must not reopen circuit, state=%s", h.State)
	}
	if h.FailStreak != 2 {
		t.Fatalf("failStreak must stay 2 (untouched), got %d", h.FailStreak)
	}
	if h.ProbesLive != 0 || h.ProbesUsed != 0 {
		t.Fatalf("probe slot must be fully returned: live=%d used=%d", h.ProbesLive, h.ProbesUsed)
	}
	if h.AbortedByBudget != 1 || h.SlowFailures != 2 {
		t.Fatalf("abort must be counted separately, not as failure: %+v", h)
	}
	if h.BreakerRejects != 1 {
		t.Fatalf("the rejected second probe must be counted, got %d", h.BreakerRejects)
	}

	// 名额退回后可以再次探测；探测成功正常恢复。
	probe2, ok := br.Allow()
	if !ok || !probe2.IsProbe() {
		t.Fatal("returned probe slot must allow a new probe")
	}
	probe2.Release(resilient.AttemptOK)
	if br.State() != resilient.BreakerClosed {
		t.Fatal("successful probe must close circuit")
	}
	// Release/Abandon 幂等：重复归还不得重复计数。
	probe2.Abandon(resilient.ReasonCallerCanceled)
	probe2.Release(resilient.AttemptSlowFail)
	h = br.Snapshot()
	if h.Successes != 1 || h.AbortedByCaller != 1 || h.SlowFailures != 2 {
		t.Fatalf("permit must be released exactly once: %+v", h)
	}
	logHealth(t, "recovered", h)
}

// 冷却结束进入半开后，探测请求被发起方取消：熔断状态保持 HALF_OPEN 不重开、
// 探测名额归还、健康视图把这次终止单独归类，且半开窗口内下游调用数有确定上界。
func TestProxy_HalfOpenProbeCallerCancelKeepsBreaker(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.BackoffBase = 0
	cfg.Breaker.FailureThreshold = 1
	cfg.Breaker.Cooldown = 30 * time.Millisecond
	cfg.Breaker.ProbeAllowance = 1
	p := resilient.New(clk, singleTarget(), cfg)
	tgt := payTarget()

	// 请求 1：真实慢故障 -> OPEN。
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{Delay: 2 * time.Millisecond, Default: resilient.AttemptSlowFail})
	res := runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil)
	if res.reason != resilient.ReasonAllAttemptsFailed {
		t.Fatalf("request 1 reason=%s, want all_attempts_failed", res.reason)
	}
	if got := p.BreakerOf(tgt).State(); got != resilient.BreakerOpen {
		t.Fatalf("state=%s, want OPEN", got)
	}
	logHealth(t, "after-open", p.Health(tgt))

	// 请求 2（OPEN 期间）：快速失败，不触达下游。
	res = runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil)
	if res.reason != resilient.ReasonAllAttemptsFailed || len(res.tr.Attempts) != 1 ||
		res.tr.Attempts[0].Result != resilient.AttemptBreakerReject {
		t.Fatalf("OPEN period must fast-fail without downstream call:\n%s", res.tr.Render())
	}
	if calls := p.Downstream().Calls(tgt); calls != 1 {
		t.Fatalf("OPEN period must not touch downstream, calls=%d", calls)
	}

	// 冷却结束 -> HALF_OPEN。请求 3：探测被发起方在 10ms 处取消。
	clk.Advance(31 * time.Millisecond)
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{Delay: 80 * time.Millisecond, Default: resilient.AttemptSlowFail})
	callerDone := make(chan struct{})
	canceled := false
	res = runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true},
		callerDone, 100*time.Millisecond,
		func(elapsed time.Duration) {
			if !canceled && elapsed >= 10*time.Millisecond {
				canceled = true
				close(callerDone)
			}
		})
	t.Logf("\n[canceled probe trace]\n%s", res.tr.Render())
	if res.reason != resilient.ReasonCallerCanceled {
		t.Fatalf("reason=%s, want caller_canceled", res.reason)
	}
	if len(res.tr.Attempts) != 1 {
		t.Fatalf("want exactly 1 probe attempt, got %d", len(res.tr.Attempts))
	}
	att := res.tr.Attempts[0]
	if !att.IsProbe || att.Result != resilient.AttemptAborted {
		t.Fatalf("probe attempt must be classified ABORTED, got probe=%t result=%s", att.IsProbe, att.Result)
	}
	if att.StateBefore != resilient.BreakerHalfOpen || att.StateAfter != resilient.BreakerHalfOpen {
		t.Fatalf("cancel must not move breaker: %s->%s", att.StateBefore, att.StateAfter)
	}
	// 半开窗口内下游调用上界：本次请求只有 1 次探测触达下游。
	if calls := p.Downstream().Calls(tgt); calls != 1 {
		t.Fatalf("half-open probe must reach downstream exactly once, calls=%d", calls)
	}

	h := p.Health(tgt)
	logHealth(t, "after-cancel", h)
	if h.State != resilient.BreakerHalfOpen {
		t.Fatalf("canceled probe must not reopen circuit, state=%s", h.State)
	}
	if h.Probing || h.ProbesLive != 0 || h.ProbesUsed != 0 {
		t.Fatalf("probe slot must be returned: probing=%t live=%d used=%d", h.Probing, h.ProbesLive, h.ProbesUsed)
	}
	if h.AbortedByCaller != 1 || h.SlowFailures != 1 || h.FailStreak != 1 {
		t.Fatalf("cancel must be counted separately, not as target failure: %+v", h)
	}

	// 名额已归还：后续请求可再次探测，探测成功即恢复。
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{Default: resilient.AttemptOK})
	res = runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil)
	if res.reason != resilient.ReasonUnknown || res.resp == nil {
		t.Fatalf("returned probe slot must allow recovery probe, reason=%s", res.reason)
	}
	h = p.Health(tgt)
	logHealth(t, "recovered", h)
	if h.State != resilient.BreakerClosed || h.Successes != 1 {
		t.Fatalf("successful probe must close circuit: %+v", h)
	}
}

// 共享预算在请求中途耗尽（检查点裁决）：终止本身不产生任何目标失败/终止计数，
// 健康视图只反映真实发生过的目标裁决，三处结论一致。
func TestProxy_BudgetExhaustionAddsNoPhantomFailure(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.BackoffBase = 0 // 退避为 0，时序完全确定
	cfg.Breaker.FailureThreshold = 5
	p := resilient.New(clk, singleTarget(), cfg)
	tgt := payTarget()
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{Delay: 30 * time.Millisecond, Default: resilient.AttemptSlowFail})

	// 预算 45ms：第 1 次尝试占 30ms，第 2 次在 44.9ms 自然醒来（ClockEpsilon
	// 纪律保证预算检查点稳定裁决），随后剩余预算不足一个时钟精度，终止。
	res := runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 45*time.Millisecond, nil)
	t.Logf("\n[budget exhausted trace]\n%s", res.tr.Render())
	if res.reason != resilient.ReasonBudgetExhausted {
		t.Fatalf("reason=%s, want budget_exhausted", res.reason)
	}
	if len(res.tr.Attempts) != 2 {
		t.Fatalf("want 2 target-attributed attempts, got %d", len(res.tr.Attempts))
	}
	for _, a := range res.tr.Attempts {
		if a.Result != resilient.AttemptSlowFail {
			t.Fatalf("both attempts are genuine target slow failures, got %s", a.Result)
		}
	}

	h := p.Health(tgt)
	logHealth(t, "budget-exhausted", h)
	// 两次慢故障是目标责任；预算终止本身不追加任何失败或终止计数。
	if h.SlowFailures != 2 || h.AbortedByBudget != 0 || h.AbortedByCaller != 0 {
		t.Fatalf("budget termination must not add phantom counts: %+v", h)
	}
	if h.FailStreak != 2 || h.State != resilient.BreakerClosed {
		t.Fatalf("breaker must reflect only genuine target results: %+v", h)
	}
	// 三处对齐：轨迹重放 == 健康视图。
	requireHealthMatches(t, p, "pay-0", classifyTraces(p.Tracer().Snapshot())["pay-0"])
}

// 预算在“即将向半开目标发起探测”前耗尽：不消耗探测名额、不触达下游、
// 不改变该目标任何计数与熔断状态。
func TestProxy_BudgetExhaustedBeforeHalfOpenProbe(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 2
	cfg.Retry.BackoffBase = 0
	cfg.Breaker.FailureThreshold = 1
	cfg.Breaker.Cooldown = 30 * time.Millisecond
	cfg.Breaker.ProbeAllowance = 1
	p := resilient.New(clk, twoTargets(), cfg)
	t0 := resilient.Target{Route: "pay", Index: 0, Name: "pay-0"}
	t1 := resilient.Target{Route: "pay", Index: 1, Name: "pay-1"}

	// 让 t1 真实失败一次进入 OPEN（请求基号 1 的第 1 次尝试落在 t1）。
	p.Downstream().SetScript(t0, &resilient.ScriptedBehavior{Default: resilient.AttemptOK})
	p.Downstream().SetScript(t1, &resilient.ScriptedBehavior{Delay: time.Millisecond, Default: resilient.AttemptSlowFail})
	res := runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil) // base0 -> t0 OK
	if res.reason != resilient.ReasonUnknown {
		t.Fatalf("warmup request on t0 failed: %s", res.reason)
	}
	res = runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil) // base1 -> t1 fail, t0 OK
	if res.reason != resilient.ReasonUnknown {
		t.Fatalf("request should recover via t0: %s", res.reason)
	}
	if got := p.BreakerOf(t1).State(); got != resilient.BreakerOpen {
		t.Fatalf("t1 state=%s, want OPEN", got)
	}

	// 冷却结束，t1 进入 HALF_OPEN。此时来一个预算几乎耗尽的请求：
	// 第 1 次尝试（t0）把预算用到不足一个时钟精度，第 2 次尝试（t1）在
	// 放行前被预算检查拦下——t1 的探测名额、下游调用、健康计数全部不变。
	clk.Advance(31 * time.Millisecond)
	p.Downstream().SetScript(t0, &resilient.ScriptedBehavior{Delay: 60 * time.Millisecond, Default: resilient.AttemptSlowFail})
	t1CallsBefore := p.Downstream().Calls(t1)
	res = runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 50*time.Millisecond, nil) // base2 -> t0, then t1
	t.Logf("\n[budget cut before probe trace]\n%s", res.tr.Render())
	if res.reason != resilient.ReasonBudgetExhausted {
		t.Fatalf("reason=%s, want budget_exhausted", res.reason)
	}
	if len(res.tr.Attempts) != 1 || res.tr.Attempts[0].Target.Name != "pay-0" {
		t.Fatalf("t1 must never be attempted: %+v", res.tr.Attempts)
	}
	if calls := p.Downstream().Calls(t1); calls != t1CallsBefore {
		t.Fatalf("t1 downstream calls must not change: before=%d after=%d", t1CallsBefore, calls)
	}
	h1 := p.Health(t1)
	logHealth(t, "t1-untouched", h1)
	if h1.State != resilient.BreakerHalfOpen || h1.ProbesUsed != 0 || h1.ProbesLive != 0 ||
		h1.AbortedByBudget != 0 || h1.AbortedByCaller != 0 {
		t.Fatalf("budget termination must not touch t1 breaker/health: %+v", h1)
	}
	if h1.SlowFailures != 1 || h1.FailStreak != 1 {
		t.Fatalf("t1 must keep only its genuine failure: %+v", h1)
	}
}

// 并发压测：冷却结束后的半开窗口里，大量请求同时涌入，探测被发起方取消。
// 验证：熔断状态不被取消推动、探测名额不泄漏、在途探测不超过名额（下游并发
// 占用峰值 <= 1）、健康视图与轨迹重放一致。RealClock + 微小真实延迟，
// -race -count=N 下结论稳定（所有探测都被取消，状态必然保持 HALF_OPEN）。
func TestProxy_ConcurrentHalfOpenProbeCancel(t *testing.T) {
	cfg := resilient.Config{
		TotalTimeout:    100 * time.Millisecond,
		ClockEpsilon:    time.Nanosecond,
		PerAttemptLimit: 0,
		Breaker: resilient.BreakerConfig{
			FailureThreshold: 1,
			Cooldown:         40 * time.Millisecond,
			ProbeAllowance:   1,
		},
		Retry: resilient.RetryPolicy{MaxAttempts: 1, Retryable: resilient.DefaultRetryable},
	}
	p := resilient.New(resilient.NewRealClock(), singleTarget(), cfg)
	tgt := payTarget()
	// 全程同一脚本：慢故障且延迟远超任何请求预算，探测不可能自然完成。
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{Delay: 200 * time.Millisecond, Default: resilient.AttemptSlowFail})

	// 真实失败一次 -> OPEN。
	_, reason, _ := p.Do(&resilient.Request{Route: "pay", Idempotent: true}, nil, 300*time.Millisecond)
	if reason != resilient.ReasonAllAttemptsFailed {
		t.Fatalf("opening request reason=%s, want all_attempts_failed", reason)
	}
	if p.BreakerOf(tgt).State() != resilient.BreakerOpen {
		t.Fatal("want OPEN")
	}
	time.Sleep(50 * time.Millisecond) // 冷却结束（惰性迁移到 HALF_OPEN）

	const N = 12
	results := make(chan resilient.Reason, N)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			callerDone := make(chan struct{})
			// 20ms 后取消：远小于下游 200ms 延迟与 100ms 预算，
			// 探测必然在途中被取消（非目标责任终止）。
			time.AfterFunc(20*time.Millisecond, func() { close(callerDone) })
			_, reason, _ := p.Do(&resilient.Request{Route: "pay", Idempotent: true}, callerDone, 100*time.Millisecond)
			results <- reason
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	reasons := map[resilient.Reason]int{}
	for r := range results {
		reasons[r]++
	}
	t.Logf("[concurrent cancel] reasons=%v", reasons)

	h := p.Health(tgt)
	logHealth(t, "concurrent-cancel", h)
	// 取消不推动熔断：没有探测能完成（200ms 延迟 >> 20ms 取消点），
	// 状态必然保持 HALF_OPEN，探测名额全部归还。
	if h.State != resilient.BreakerHalfOpen {
		t.Fatalf("canceled probes must not move breaker, state=%s", h.State)
	}
	if h.Probing || h.ProbesLive != 0 || h.ProbesUsed != 0 {
		t.Fatalf("probe slots must all be returned: probing=%t live=%d used=%d", h.Probing, h.ProbesLive, h.ProbesUsed)
	}
	if h.Successes != 0 || h.SlowFailures != 1 {
		t.Fatalf("only the opening failure may be counted: %+v", h)
	}
	// 在途探测上界 = 名额 1：下游并发占用峰值不得超过 1。
	if peak := p.Downstream().PeakActive(tgt); peak != 1 {
		t.Fatalf("in-flight probes must never exceed allowance, peak=%d", peak)
	}
	// 下游实际被调用次数 = 打开熔断的 1 次 + 被取消的探测次数。
	if calls := p.Downstream().Calls(tgt); calls != 1+int(h.AbortedByCaller) {
		t.Fatalf("downstream calls=%d, want 1+aborted(%d)", calls, h.AbortedByCaller)
	}
	// 三处对齐：轨迹重放 == 健康视图。
	requireHealthMatches(t, p, "pay-0", classifyTraces(p.Tracer().Snapshot())["pay-0"])
	for _, tr := range p.Tracer().Snapshot() {
		for _, a := range tr.Attempts {
			t.Logf("[attempt] target=%s breaker=%s->%s probe=%t remaining=%s result=%s class=%s | %s",
				a.Target, a.StateBefore, a.StateAfter, a.IsProbe,
				a.Remaining.Round(time.Millisecond), a.Result, tr.FinalReason, a.Decision)
		}
	}
	// 至少发生过一次被取消的探测（12 个并发请求撞 1 个名额，必然有探测）。
	if h.AbortedByCaller < 1 {
		t.Fatal("expected at least one canceled probe")
	}
	if h.AbortedByCaller+h.BreakerRejects == 0 {
		t.Fatal("half-open window must show probe and/or fast-fail activity")
	}
}
