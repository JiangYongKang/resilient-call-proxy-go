package resilient_test

import (
	"testing"
	"time"

	"github.com/highcumontoa/resilient-call-proxy-go/resilient"
)

// 目标侧结论（成功/拒绝/慢故障）进入对应健康计数；熔断拒绝单独计数，
// 且“实际发出次数”只统计真正进入下游的调用。
func TestHealthView_TargetSideCounters(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 1
	cfg.Breaker.FailureThreshold = 1000 // 本用例不触发熔断迁移
	p := resilient.New(clk, singleTarget(), cfg)
	tgt := payTarget()

	// 一次成功。
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{
		Sequence: []resilient.AttemptResult{
			resilient.AttemptOK,
			resilient.AttemptReject,
			resilient.AttemptSlowFail,
		},
		Delay: time.Millisecond,
	})

	r1 := runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil)
	if r1.reason != resilient.ReasonUnknown {
		t.Fatalf("r1 reason=%s want success", r1.reason)
	}
	r2 := runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil)
	if r2.reason != resilient.ReasonDownstreamReject {
		t.Fatalf("r2 reason=%s want downstream_reject", r2.reason)
	}
	r3 := runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: false}, nil, 100*time.Millisecond, nil)
	if r3.reason != resilient.ReasonAllAttemptsFailed {
		t.Fatalf("r3 reason=%s want all_attempts_failed", r3.reason)
	}

	h, ok := p.TargetHealthOf(tgt)
	if !ok {
		t.Fatal("target health missing")
	}
	if h.State != resilient.BreakerClosed || h.FailStreak != 2 {
		t.Fatalf("state=%s streak=%d want CLOSED/2", h.State, h.FailStreak)
	}
	if h.Successes != 1 || h.Rejects != 1 || h.SlowFailures != 1 {
		t.Fatalf("counters ok=%d reject=%d slow=%d, want 1/1/1", h.Successes, h.Rejects, h.SlowFailures)
	}
	if h.Dispatched != 3 {
		t.Fatalf("dispatched=%d want 3", h.Dispatched)
	}
	if h.BreakerRejects != 0 || h.CallerCanceled != 0 || h.BudgetExhausted != 0 {
		t.Fatalf("neutral/reject counters must stay zero: %+v", h)
	}
	// 三处结论对齐：轨迹归因 == 最终原因语义。
	if r1.tr.Attempts[0].Attribution != resilient.AttrSuccess {
		t.Fatal("r1 attribution must be success")
	}
	if r2.tr.Attempts[0].Attribution != resilient.AttrDownstreamReject {
		t.Fatal("r2 attribution must be downstream_reject")
	}
	if r3.tr.Attempts[0].Attribution != resilient.AttrSlowFailure {
		t.Fatal("r3 attribution must be slow_failure")
	}
	t.Logf("[health] %+v", h)
}
