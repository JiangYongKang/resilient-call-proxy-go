package resilient_test

import (
	"testing"
	"time"

	"github.com/highcumontoa/resilient-call-proxy-go/resilient"
)

// 半开探测期间共享预算耗尽：与取消同样属于非目标责任终止——
// 熔断保持 HALF_OPEN、探测名额归还、下游调用数有确定上界、健康视图单独计数。
func TestHealth_HalfOpenProbeBudgetExhaustedStaysHalfOpen(t *testing.T) {
	const allowance = 1
	clk, p, tgt := halfOpenSetup(t, allowance)

	// 探测预算只有 10ms，而下游慢故障要占用 80ms：预算必然在探测在途时耗尽。
	res := runPumped(t, clk, p,
		&resilient.Request{Route: "pay", Idempotent: true}, nil, 10*time.Millisecond, nil)

	if res.reason != resilient.ReasonBudgetExhausted {
		t.Fatalf("reason=%s want budget_exhausted", res.reason)
	}
	a := res.tr.Attempts[0]
	if a.Attribution != resilient.AttrBudgetExhausted {
		t.Fatalf("attribution=%s want budget_exhausted", a.Attribution)
	}
	if !a.IsProbe || !a.Dispatched || a.Result == resilient.AttemptAborted {
		t.Fatal("in-flight probe must be a dispatched (non-aborted) probe")
	}
	t.Logf("\n%s", res.tr.Render())

	if state := p.BreakerOf(tgt).State(); state != resilient.BreakerHalfOpen {
		t.Fatalf("state=%s want HALF_OPEN (budget exhaustion must not reopen)", state)
	}
	h, _ := p.TargetHealthOf(tgt)
	if !h.Probing || h.ProbesUsed != 0 || h.ProbesLive != 0 {
		t.Fatalf("probe slot must be returned and probing continue: %+v", h)
	}
	if h.ProbesRemaining != allowance {
		t.Fatalf("probes remaining=%d want %d", h.ProbesRemaining, allowance)
	}
	if h.BudgetExhausted != 1 {
		t.Fatalf("budget_exhausted=%d want 1", h.BudgetExhausted)
	}
	if h.CallerCanceled != 0 {
		t.Fatalf("caller_canceled=%d want 0", h.CallerCanceled)
	}
	if h.SlowFailures != 1 {
		t.Fatalf("slow_failures=%d want 1 (pre-open failure only; budget cutoff is neutral)", h.SlowFailures)
	}
	// 下游实际调用：setup 1 次 + 本轮被预算截断的探测 1 次，确定上界 2。
	if calls := p.Downstream().Calls(tgt); calls != 2 {
		t.Fatalf("downstream calls=%d want 2", calls)
	}
	if h.Dispatched != 2 {
		t.Fatalf("dispatched=%d want 2", h.Dispatched)
	}

	// 名额归还后，新请求仍能在半开窗口拿到探测名额（不重开、名额未被消耗）。
	// 给一个充足预算，下游脚本仍是慢故障——这次是真正的目标失败，应重开。
	res2 := runPumped(t, clk, p,
		&resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil)
	if res2.reason != resilient.ReasonAllAttemptsFailed {
		t.Fatalf("real failing probe reason=%s want all_attempts_failed", res2.reason)
	}
	if state := p.BreakerOf(tgt).State(); state != resilient.BreakerOpen {
		t.Fatalf("real target failure must reopen, got %s", state)
	}
}

// CLOSED 下预算耗尽也绝不能推动熔断：一次真实慢失败 streak=1 后，
// 被预算截断的调用不增加 streak；再来一次真实慢失败才恰好达到阈值 OPEN。
func TestHealth_BudgetCutoffInClosedDoesNotAdvanceStreak(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 1
	cfg.Breaker.FailureThreshold = 2
	p := resilient.New(clk, singleTarget(), cfg)
	tgt := payTarget()
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{
		Delay:   50 * time.Millisecond,
		Default: resilient.AttemptSlowFail,
	})

	// 一次真实慢失败：streak=1，仍 CLOSED。
	r1 := runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil)
	if r1.reason != resilient.ReasonAllAttemptsFailed {
		t.Fatalf("r1 reason=%s", r1.reason)
	}
	if h, _ := p.TargetHealthOf(tgt); h.FailStreak != 1 || h.State != resilient.BreakerClosed {
		t.Fatalf("after real failure streak/state=%d/%s want 1/CLOSED", h.FailStreak, h.State)
	}

	// 预算只有 5ms：探测在途时被预算截断，中性终止，streak 不前进。
	r2 := runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 5*time.Millisecond, nil)
	if r2.reason != resilient.ReasonBudgetExhausted {
		t.Fatalf("r2 reason=%s want budget_exhausted", r2.reason)
	}
	if h, _ := p.TargetHealthOf(tgt); h.FailStreak != 1 || h.State != resilient.BreakerClosed {
		t.Fatalf("budget cutoff must not advance streak: streak/state=%d/%s want 1/CLOSED", h.FailStreak, h.State)
	}

	// 再来一次真实慢失败：streak=2 -> OPEN（证明前一次预算终止没有白送一次失败）。
	r3 := runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil)
	if r3.reason != resilient.ReasonAllAttemptsFailed {
		t.Fatalf("r3 reason=%s", r3.reason)
	}
	if state := p.BreakerOf(tgt).State(); state != resilient.BreakerOpen {
		t.Fatalf("state=%s want OPEN after two real failures", state)
	}
	h, _ := p.TargetHealthOf(tgt)
	if h.BudgetExhausted != 1 || h.SlowFailures != 2 {
		t.Fatalf("counters budget=%d slow=%d want 1/2", h.BudgetExhausted, h.SlowFailures)
	}
}

// “放行后、未真正进入下游就终止”：下游调用必须返回 ABORTED 且不增加调用计数，
// 熔断器中性归还探测名额、状态不变。这是对极小竞争窗口的确定性直接验证。
func TestHealth_AbortedBeforeDispatchNotCounted(t *testing.T) {
	clk, p, tgt := halfOpenSetup(t, 1)

	// 已终止的预算进入 Downstream.Call：不得计入“被调用次数”，也不占并发名额。
	dead := resilient.NewBudget(clk, time.Millisecond, nil)
	clk.Advance(2 * time.Millisecond)
	if err := dead.Err(); err == nil || err.Reason != resilient.ReasonBudgetExhausted {
		t.Fatalf("budget should be expired, got %v", err)
	}
	callsBefore := p.Downstream().Calls(tgt)
	res, peak := p.Downstream().Call(tgt, dead, 5*time.Millisecond)
	dead.Dispose()
	if res != resilient.AttemptAborted {
		t.Fatalf("downstream result=%s want ABORTED", res)
	}
	if peak != 0 {
		t.Fatalf("aborted-before-dispatch must not occupy downstream, peak=%d", peak)
	}
	if calls := p.Downstream().Calls(tgt); calls != callsBefore {
		t.Fatalf("calls changed %d->%d; aborted call must not count", callsBefore, calls)
	}

	// 熔断器层面：半开拿到探测名额后以“未发出”中性归还，状态与名额都恢复。
	br := p.BreakerOf(tgt)
	permit, ok := br.Allow()
	if !ok || !permit.IsProbe() {
		t.Fatal("want one half-open probe")
	}
	permit.ReleaseAborted(false, resilient.ReasonCallerCanceled)
	if br.State() != resilient.BreakerHalfOpen {
		t.Fatal("aborted (not dispatched) cancel must keep HALF_OPEN")
	}
	h, _ := p.TargetHealthOf(tgt)
	if h.ProbesUsed != 0 || h.ProbesRemaining != 1 {
		t.Fatalf("probe slot must be fully returned: %+v", h)
	}
	if h.CallerCanceled != 1 {
		t.Fatalf("caller_canceled=%d want 1", h.CallerCanceled)
	}
	if h.Dispatched != 1 { // 仅 setup 那一次真实慢失败
		t.Fatalf("dispatched=%d want 1 (aborted permit not dispatched)", h.Dispatched)
	}
	// 归还后可再次拿到探测名额。
	if permit2, ok := br.Allow(); !ok || !permit2.IsProbe() {
		t.Fatal("a subsequent probe must be allowed after aborted slot return")
	} else {
		permit2.ReleaseAborted(false, resilient.ReasonBudgetExhausted)
	}
}
