package resilient_test

import (
	"testing"
	"time"

	"github.com/highcumontoa/resilient-call-proxy-go/resilient"
)

// halfOpenSetup 构造一个熔断器恰好停在 HALF_OPEN、探测窗口刚打开的代理：
// 一次慢故障把熔断打开（threshold=1），推进时钟越过冷却后进入半开。
func halfOpenSetup(t *testing.T, allowance int) (*resilient.FakeClock, *resilient.Proxy, resilient.Target) {
	t.Helper()
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 1
	cfg.Breaker.FailureThreshold = 1
	cfg.Breaker.Cooldown = 30 * time.Millisecond
	cfg.Breaker.ProbeAllowance = allowance
	p := resilient.New(clk, singleTarget(), cfg)
	tgt := payTarget()
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{
		Delay:   80 * time.Millisecond,
		Default: resilient.AttemptSlowFail,
	})

	// 第一次调用慢故障：CLOSED -> OPEN。
	res := runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil)
	if res.reason != resilient.ReasonAllAttemptsFailed {
		t.Fatalf("setup reason=%s want all_attempts_failed", res.reason)
	}
	if p.BreakerOf(tgt).State() != resilient.BreakerOpen {
		t.Fatal("setup: want OPEN")
	}
	clk.Advance(31 * time.Millisecond)
	if p.BreakerOf(tgt).State() != resilient.BreakerHalfOpen {
		t.Fatal("setup: want HALF_OPEN after cooldown")
	}
	return clk, p, tgt
}

// launchCallsShared 并发发起 n 个共享同一 callerDone 的调用（各自独立预算，
// 共享同一假时钟）。返回结果通道与统一的取消信号。
func launchCallsShared(p *resilient.Proxy, clk *resilient.FakeClock, n, budgetMs int) (<-chan callResult, chan struct{}) {
	ch := make(chan callResult, n)
	callerDone := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			budget := resilient.NewBudget(clk, time.Duration(budgetMs)*time.Millisecond, callerDone)
			r, why, tr := p.DoWithBudget(&resilient.Request{Route: "pay", Idempotent: true}, budget)
			ch <- callResult{resp: r, reason: why, tr: tr, ok: true}
		}()
	}
	return ch, callerDone
}

// 冷却结束、半开放探测期间发起方取消：
//   - 熔断状态必须保持 HALF_OPEN（取消不是目标失败，不能重开）；
//   - 探测名额必须归还（后续探测仍可被放行）；
//   - 半开窗口内下游实际被调用次数有确定上界（只放过探测，拒绝者不触达下游）；
//   - 健康视图把取消单独计数，不混进失败，dispatched 只含真正发出的调用。
func TestHealth_HalfOpenProbeCanceledStaysHalfOpen(t *testing.T) {
	const allowance = 2
	const N = 6
	clk, p, tgt := halfOpenSetup(t, allowance)

	ch, callerDone := launchCallsShared(p, clk, N, 100)

	// 先收齐被半开名额快速拒绝的调用（它们不睡眠、立即结束）。
	rejected := 0
	for i := 0; i < N-allowance; i++ {
		r := <-ch
		if r.reason != resilient.ReasonAllAttemptsFailed {
			t.Fatalf("fast-fail call reason=%s want all_attempts_failed", r.reason)
		}
		for _, a := range r.tr.Attempts {
			if a.Result != resilient.AttemptBreakerReject || a.Attribution != resilient.AttrBreakerReject {
				t.Fatalf("want breaker_reject attribution, got %s/%s", a.Result, a.Attribution)
			}
			if a.Dispatched {
				t.Fatal("breaker-rejected attempt must not be dispatched")
			}
		}
		rejected++
	}
	if rejected != N-allowance {
		t.Fatalf("rejected=%d want %d", rejected, N-allowance)
	}

	// 此时恰好 allowance 个探测在途（下游 Sleep），等它们都挂上去。
	if !clk.WaitForSleepers(allowance, nil) {
		t.Fatal("probes never entered downstream wait")
	}
	probeInFlight := p.Downstream().Calls(tgt) - 1 // 减去 setup 的 1 次
	if probeInFlight != allowance {
		t.Fatalf("half-open probes dispatched=%d want %d", probeInFlight, allowance)
	}

	// 在预算仍有大量剩余时，由发起方统一取消在途探测。
	close(callerDone)

	canceled := 0
	for i := 0; i < allowance; i++ {
		r := <-ch
		if r.reason != resilient.ReasonCallerCanceled {
			t.Fatalf("probe reason=%s want caller_canceled", r.reason)
		}
		for _, a := range r.tr.Attempts {
			if a.Attribution != resilient.AttrCallerCanceled {
				t.Fatalf("probe attribution=%s want caller_canceled", a.Attribution)
			}
			if !a.Dispatched || a.Result == resilient.AttemptAborted {
				t.Fatal("in-flight canceled probe was dispatched; want non-aborted cancel")
			}
		}
		canceled++
		t.Logf("\n%s", r.tr.Render())
	}
	if canceled != allowance {
		t.Fatalf("canceled=%d want %d", canceled, allowance)
	}

	// 熔断状态不变、名额归还。
	if state := p.BreakerOf(tgt).State(); state != resilient.BreakerHalfOpen {
		t.Fatalf("state after cancel=%s want HALF_OPEN (cancel must not reopen)", state)
	}
	h, _ := p.TargetHealthOf(tgt)
	if !h.Probing || h.State != resilient.BreakerHalfOpen {
		t.Fatalf("health must report HALF_OPEN/probing, got %s probing=%t", h.State, h.Probing)
	}
	if h.ProbesUsed != 0 || h.ProbesLive != 0 {
		t.Fatalf("probe slots must be returned: used=%d live=%d", h.ProbesUsed, h.ProbesLive)
	}
	if h.ProbesRemaining != allowance {
		t.Fatalf("probes remaining=%d want %d", h.ProbesRemaining, allowance)
	}
	if h.CallerCanceled != int64(allowance) {
		t.Fatalf("caller_canceled counter=%d want %d", h.CallerCanceled, allowance)
	}
	if h.SlowFailures != 1 {
		t.Fatalf("slow_failures=%d want 1 (only the pre-open failure)", h.SlowFailures)
	}
	if h.BudgetExhausted != 0 || h.Rejects != 0 {
		t.Fatalf("budget_exhausted=%d rejects=%d want 0/0", h.BudgetExhausted, h.Rejects)
	}

	// 半开窗口内下游实际被调用次数有确定上界：setup 的 1 次 + 本轮 allowance 次。
	wantCalls := 1 + allowance
	if calls := p.Downstream().Calls(tgt); calls != wantCalls {
		t.Fatalf("downstream calls in half-open window=%d want %d", calls, wantCalls)
	}
	if h.Dispatched != int64(wantCalls) {
		t.Fatalf("health dispatched=%d want %d", h.Dispatched, wantCalls)
	}
	if h.BreakerRejects != int64(N-allowance) {
		t.Fatalf("breaker_rejects=%d want %d", h.BreakerRejects, N-allowance)
	}

	// 名额归还后再来一次请求：它仍能拿到探测名额；这次下游给出真正的目标侧
	// 失败（慢故障），熔断器应按目标失败正常重开 OPEN（对照：取消不会重开）。
	res2 := runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil)
	if res2.reason != resilient.ReasonAllAttemptsFailed {
		t.Fatalf("fresh failing probe reason=%s want all_attempts_failed", res2.reason)
	}
	if state := p.BreakerOf(tgt).State(); state != resilient.BreakerOpen {
		t.Fatalf("a real target-failure probe should reopen, got %s", state)
	}
	t.Logf("[half-open-cancel] state stays HALF_OPEN on cancel; slots returned; calls<=%d; a later real failure reopens", wantCalls)
}
