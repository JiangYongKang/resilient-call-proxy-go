package resilient_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/resilient-call-proxy-go/resilient"
)

func twoTargets() []resilient.RouteEntry {
	return []resilient.RouteEntry{{
		Route: "pay",
		Targets: []resilient.Target{
			{Route: "pay", Index: 0, Name: "pay-0"},
			{Route: "pay", Index: 1, Name: "pay-1"},
		},
	}}
}

// 连续失败把熔断器打开后，后续请求必须被快速失败并走降级，完全不触达下游；
// 冷却后半开只放行受控探测，探测成功后恢复。
func TestProxy_BreakerOpensFastFailsThenRecovers(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 2
	cfg.Breaker.FailureThreshold = 2
	cfg.Breaker.Cooldown = 30 * time.Millisecond
	cfg.Breaker.ProbeAllowance = 1
	var fallbackOK atomic.Int64
	cfg.Fallback = func(req *resilient.Request) (*resilient.Response, error) {
		fallbackOK.Add(1)
		return &resilient.Response{OK: true, Body: "degraded"}, nil
	}
	p := resilient.New(clk, twoTargets(), cfg)
	t0 := resilient.Target{Route: "pay", Index: 0, Name: "pay-0"}
	t1 := resilient.Target{Route: "pay", Index: 1, Name: "pay-1"}
	// 两个目标都持续慢故障。
	p.Downstream().SetScript(t0, &resilient.ScriptedBehavior{Delay: 2 * time.Millisecond, Default: resilient.AttemptSlowFail})
	p.Downstream().SetScript(t1, &resilient.ScriptedBehavior{Delay: 2 * time.Millisecond, Default: resilient.AttemptSlowFail})

	// 两个请求（各顺延尝试 t0/t1 共 2 次）让每个熔断器各累积 2 次连续失败。
	for i := 0; i < 2; i++ {
		res := runPumped(t, clk, p,
			&resilient.Request{Route: "pay", Idempotent: true},
			nil, 100*time.Millisecond, nil)
		if res.reason != resilient.ReasonUnknown {
			t.Fatalf("request %d should be served by fallback, got %s", i, res.reason)
		}
	}
	if state := p.BreakerOf(t0).State(); state != resilient.BreakerOpen {
		t.Fatalf("t0 breaker=%s, want OPEN", state)
	}
	if state := p.BreakerOf(t1).State(); state != resilient.BreakerOpen {
		t.Fatalf("t1 breaker=%s, want OPEN", state)
	}

	callsBeforeOpen := p.Downstream().Calls(t0) + p.Downstream().Calls(t1)
	// OPEN 期间再来请求：两个目标都快速拒绝，零下游调用，走降级。
	res := runPumped(t, clk, p,
		&resilient.Request{Route: "pay", Idempotent: true},
		nil, 100*time.Millisecond, nil)
	if res.reason != resilient.ReasonUnknown || res.resp.Body != "degraded" {
		t.Fatalf("open-circuit request must be degraded, reason=%s", res.reason)
	}
	callsAfterOpen := p.Downstream().Calls(t0) + p.Downstream().Calls(t1)
	if callsAfterOpen != callsBeforeOpen {
		t.Fatalf("OPEN period must not touch downstream: before=%d after=%d",
			callsBeforeOpen, callsAfterOpen)
	}
	if n := len(res.tr.Attempts); n != 2 {
		t.Fatalf("want 2 breaker-fast-fail records (one per target), got %d", n)
	}
	for _, a := range res.tr.Attempts {
		if a.Result != resilient.AttemptBreakerReject {
			t.Fatalf("attempt result=%s, want BREAKER_REJECT", a.Result)
		}
	}

	// 冷却后：半开仅允许探测。两个目标都恢复成功；轮询使探测依次落到
	// t1/t0，可能需要两个请求才能让两个熔断器都见到成功探测。
	clk.Advance(31 * time.Millisecond)
	p.Downstream().SetScript(t0, &resilient.ScriptedBehavior{Default: resilient.AttemptOK})
	p.Downstream().SetScript(t1, &resilient.ScriptedBehavior{Default: resilient.AttemptOK})

	var recovered int
	for req := 0; req < 4; req++ {
		resN := runPumped(t, clk, p,
			&resilient.Request{Route: "pay", Idempotent: true},
			nil, 100*time.Millisecond, nil)
		if resN.reason != resilient.ReasonUnknown || resN.resp == nil {
			t.Fatalf("recovery request %d should succeed, reason=%s", req, resN.reason)
		}
		sawProbe := false
		for _, a := range resN.tr.Attempts {
			if a.IsProbe && a.Result == resilient.AttemptOK {
				sawProbe = true
			}
		}
		if sawProbe {
			recovered++
		}
		if p.BreakerOf(t0).State() == resilient.BreakerClosed &&
			p.BreakerOf(t1).State() == resilient.BreakerClosed {
			break
		}
		// 探测失败会重新 OPEN 并重置冷却；恢复成功后不会再这样。
		if p.BreakerOf(t0).State() == resilient.BreakerOpen ||
			p.BreakerOf(t1).State() == resilient.BreakerOpen {
			clk.Advance(31 * time.Millisecond)
		}
	}
	if p.BreakerOf(t0).State() != resilient.BreakerClosed ||
		p.BreakerOf(t1).State() != resilient.BreakerClosed {
		t.Fatalf("both breakers should recover, got t0=%s t1=%s",
			p.BreakerOf(t0).State(), p.BreakerOf(t1).State())
	}
	if recovered == 0 {
		t.Fatal("expected at least one successful half-open probe")
	}
	res2 := runPumped(t, clk, p,
		&resilient.Request{Route: "pay", Idempotent: true},
		nil, 100*time.Millisecond, nil)
	if res2.reason != resilient.ReasonUnknown || res2.resp == nil {
		t.Fatalf("post-recovery request should succeed, reason=%s", res2.reason)
	}
	for _, tr := range p.BreakerOf(t0).Transitions() {
		t.Logf("[t0 transition] %s->%s why=%q", tr.From, tr.To, tr.Why)
	}
	t.Logf("\n[open-period trace]\n%s", res.tr.Render())
}

// 并发下幂等请求的重试不得放大请求量：每个请求的尝试数 <= MaxAttempts，
// 多次尝试的目标选择可追溯，且在 -race 下无数据竞争。使用 RealClock 与
// 极小真实延迟，纯粹压测代理在并发下的安全性与请求量上界（时序确定性由
// 其余 FakeClock 用例覆盖）。
func TestProxy_ConcurrentRetriesDoNotAmplify(t *testing.T) {
	cfg := resilient.Config{
		TotalTimeout:    200 * time.Millisecond,
		ClockEpsilon:    time.Nanosecond,
		PerAttemptLimit: 50 * time.Millisecond,
		Breaker: resilient.BreakerConfig{
			FailureThreshold: 1000, // 本用例不触发熔断，隔离重试行为
			Cooldown:         time.Second,
			ProbeAllowance:   1,
		},
		Retry: resilient.RetryPolicy{
			MaxAttempts: 3,
			Retryable:   resilient.DefaultRetryable,
		},
	}
	p := resilient.New(resilient.NewRealClock(), twoTargets(), cfg)
	t0 := resilient.Target{Route: "pay", Index: 0, Name: "pay-0"}
	t1 := resilient.Target{Route: "pay", Index: 1, Name: "pay-1"}
	script := func() *resilient.ScriptedBehavior {
		return &resilient.ScriptedBehavior{
			Delay: time.Millisecond,
			Sequence: []resilient.AttemptResult{
				resilient.AttemptSlowFail, resilient.AttemptSlowFail,
				resilient.AttemptOK,
			},
			Default: resilient.AttemptOK,
		}
	}
	p.Downstream().SetScript(t0, script())
	p.Downstream().SetScript(t1, script())

	const N = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p.Do(&resilient.Request{Route: "pay", Idempotent: true}, nil, 0)
		}()
	}
	close(start)
	wg.Wait()

	snap := p.Tracer().Snapshot()
	if len(snap) != N {
		t.Fatalf("traces=%d, want %d", len(snap), N)
	}
	for i, tr := range snap {
		if len(tr.Attempts) == 0 || len(tr.Attempts) > 3 {
			t.Fatalf("request %d made %d attempts, want 1..%d", i, len(tr.Attempts), 3)
		}
		for _, a := range tr.Attempts {
			if a.Target.Name != "pay-0" && a.Target.Name != "pay-1" {
				t.Fatalf("unknown target %s", a.Target)
			}
			if a.Decision == "" {
				t.Fatalf("attempt on %s missing decision rationale", a.Target)
			}
		}
	}
	success := 0
	for _, tr := range snap {
		if tr.FinalReason == resilient.ReasonUnknown {
			success++
		}
	}
	if success != N {
		t.Fatalf("successful requests=%d, want %d", success, N)
	}
	totalCalls := p.Downstream().Calls(t0) + p.Downstream().Calls(t1)
	if totalCalls > N*3 {
		t.Fatalf("total downstream calls=%d > cap %d (retry amplification!)", totalCalls, N*3)
	}
	if totalCalls < N {
		t.Fatalf("total downstream calls=%d < requests %d", totalCalls, N)
	}
	t.Logf("[concurrency] requests=%d total_attempts=%d cap=%d", N, totalCalls, N*3)
}

// 降级函数本身失败时，原因必须可判别为 fallback_failed，且保留原始下游原因。
func TestProxy_FallbackFailureIsDistinguishable(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 1
	cfg.Fallback = func(req *resilient.Request) (*resilient.Response, error) {
		return nil, &resilient.CallError{Reason: resilient.ReasonFallbackFailed, Message: "cache also down"}
	}
	p := resilient.New(clk, singleTarget(), cfg)
	tgt := payTarget()
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{Default: resilient.AttemptReject})

	res := runPumped(t, clk, p,
		&resilient.Request{Route: "pay", Idempotent: true},
		nil, 100*time.Millisecond, nil)
	if res.reason != resilient.ReasonFallbackFailed {
		t.Fatalf("reason=%s, want fallback_failed", res.reason)
	}
	if res.tr.Cause != resilient.ReasonDownstreamReject {
		t.Fatalf("trace must retain original cause downstream_reject, got %s", res.tr.Cause)
	}
	if !res.tr.FromFallback {
		t.Fatal("trace must mark FromFallback")
	}
	t.Logf("\n%s", res.tr.Render())
}

// 多目标路由的目标选择在多次尝试间顺延、跨请求轮询，且轨迹可追溯。
func TestRouter_TargetSelectionTraceable(t *testing.T) {
	r := resilient.NewRouter(twoTargets()...)
	// 请求 A（base=0）：round0->pay-0, round1->pay-1, round2->pay-0
	baseA, ok := r.OpenRequest("pay")
	if !ok || baseA != 0 {
		t.Fatalf("baseA=%d ok=%v, want 0,true", baseA, ok)
	}
	wantA := []string{"pay-0", "pay-1", "pay-0"}
	for round, name := range wantA {
		got, ok := r.PickBase("pay", baseA, round)
		if !ok || got.Name != name {
			t.Fatalf("request A round %d = %s, want %s", round, got.Name, name)
		}
	}
	// 请求 B（base=1）：顺延起点整体偏移一位 -> pay-1, pay-0, pay-1
	baseB, ok := r.OpenRequest("pay")
	if !ok || baseB != 1 {
		t.Fatalf("baseB=%d ok=%v, want 1,true", baseB, ok)
	}
	wantB := []string{"pay-1", "pay-0", "pay-1"}
	for round, name := range wantB {
		got, _ := r.PickBase("pay", baseB, round)
		if got.Name != name {
			t.Fatalf("request B round %d = %s, want %s", round, got.Name, name)
		}
	}
	if _, ok := r.OpenRequest("missing"); ok {
		t.Fatal("unknown route must not yield a base")
	}
}
