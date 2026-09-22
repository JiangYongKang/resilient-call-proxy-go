package resilient_test

import (
	"testing"
	"time"

	"github.com/highcumontoa/resilient-call-proxy-go/resilient"
)

func testConfig() resilient.Config {
	return resilient.Config{
		TotalTimeout:    100 * time.Millisecond,
		PerAttemptLimit: 0,
		ClockEpsilon:    100 * time.Microsecond,
		Breaker: resilient.BreakerConfig{
			FailureThreshold: 5,
			Cooldown:         50 * time.Millisecond,
			ProbeAllowance:   1,
		},
		Retry: resilient.RetryPolicy{
			MaxAttempts: 3,
			Retryable:   resilient.DefaultRetryable,
			BackoffBase: 1 * time.Millisecond,
			BackoffMax:  4 * time.Millisecond,
		},
	}
}

func singleTarget() []resilient.RouteEntry {
	return []resilient.RouteEntry{{
		Route:   "pay",
		Targets: []resilient.Target{{Route: "pay", Index: 0, Name: "pay-0"}},
	}}
}

func payTarget() resilient.Target {
	return resilient.Target{Route: "pay", Index: 0, Name: "pay-0"}
}

// 所有重试共享同一总预算：慢故障反复耗时，预算耗尽即终止，绝不越过剩余预算。
func TestProxy_SharedBudgetExhausted(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	p := resilient.New(clk, singleTarget(), cfg)
	tgt := payTarget()
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{
		Delay:   60 * time.Millisecond,
		Default: resilient.AttemptSlowFail,
	})

	res := runPumped(t, clk, p,
		&resilient.Request{Route: "pay", Idempotent: true},
		nil, 100*time.Millisecond, nil)

	if res.resp != nil {
		t.Fatalf("expected no response, got %+v", res.resp)
	}
	if res.reason != resilient.ReasonBudgetExhausted {
		t.Fatalf("reason=%s, want budget_exhausted", res.reason)
	}
	// 第一次尝试占满 60ms，退避 1ms 后第二次尝试在 100ms 处被共享预算截断。
	if calls := p.Downstream().Calls(tgt); calls != 2 {
		t.Fatalf("downstream calls=%d, want 2 (2nd attempt cut off by shared budget)", calls)
	}
	// 超时后名额必须释放：下游并发占用峰值恒为 1，无泄漏。
	if peak := p.Downstream().PeakActive(tgt); peak != 1 {
		t.Fatalf("peak active=%d, want 1 (no leaked occupancy after timeout)", peak)
	}
	t.Logf("\n%s", res.tr.Render())
}

// 下游明确拒绝：快速返回确定结论，不重试，原因可区分，并走降级。
func TestProxy_DownstreamExplicitReject(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	cfg.Fallback = func(req *resilient.Request) (*resilient.Response, error) {
		return &resilient.Response{OK: true, Body: "fallback"}, nil
	}
	p := resilient.New(clk, singleTarget(), cfg)
	tgt := payTarget()
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{
		Default: resilient.AttemptReject,
	})

	res := runPumped(t, clk, p,
		&resilient.Request{Route: "pay", Idempotent: true},
		nil, 100*time.Millisecond, nil)

	if res.reason != resilient.ReasonUnknown || res.resp == nil || res.resp.Body != "fallback" {
		t.Fatalf("reject should be served by fallback, resp=%+v reason=%s", res.resp, res.reason)
	}
	if calls := p.Downstream().Calls(tgt); calls != 1 {
		t.Fatalf("downstream calls=%d, want exactly 1 (reject is never retried)", calls)
	}
	if res.tr.Cause != resilient.ReasonDownstreamReject {
		t.Fatalf("trace cause=%s, want downstream_reject", res.tr.Cause)
	}
	t.Logf("\n%s", res.tr.Render())
}

// 发起方主动取消：即使下游正在慢调用中，也必须立即以 caller_canceled 终止，
// 且与预算耗尽可区分。
func TestProxy_CallerCanceledIsDistinguishable(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	p := resilient.New(clk, singleTarget(), cfg)
	tgt := payTarget()
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{
		Delay:   80 * time.Millisecond,
		Default: resilient.AttemptSlowFail,
	})

	callerDone := make(chan struct{})
	canceled := false
	res := runPumped(t, clk, p,
		&resilient.Request{Route: "pay", Idempotent: true},
		callerDone, 100*time.Millisecond,
		func(elapsed time.Duration) {
			if !canceled && elapsed >= 10*time.Millisecond {
				canceled = true
				close(callerDone) // 在预算到期（100ms）之前主动取消
			}
		})

	if res.reason != resilient.ReasonCallerCanceled {
		t.Fatalf("reason=%s, want caller_canceled", res.reason)
	}
	// 取消在第 10ms 发生：只允许发生过 1 次下游调用；取消后不得再以重试
	// 名义占用名额或发起后续下游调用（即使总预算还有 90ms）。
	if calls := p.Downstream().Calls(tgt); calls != 1 {
		t.Fatalf("downstream calls=%d, want 1 (no further occupancy after cancel)", calls)
	}
	if peak := p.Downstream().PeakActive(tgt); peak != 1 {
		t.Fatalf("peak concurrent occupancy=%d, want 1", peak)
	}
	t.Logf("\n%s", res.tr.Render())
}
