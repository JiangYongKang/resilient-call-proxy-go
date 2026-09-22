package resilient_test

import (
	"testing"
	"time"

	"github.com/highcumontoa/resilient-call-proxy-go/resilient"
)

// 幂等请求遇到慢故障可重试，第二次成功；目标选择、失败归因在轨迹中可追溯。
func TestRetry_IdempotentSlowFailRetriedUntilSuccess(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	cfg.Retry.BackoffBase = 0 // 关闭退避，时序更直观
	p := resilient.New(clk, singleTarget(), cfg)
	tgt := payTarget()
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{
		Delay:    10 * time.Millisecond,
		Sequence: []resilient.AttemptResult{resilient.AttemptSlowFail, resilient.AttemptOK},
	})

	res := runPumped(t, clk, p,
		&resilient.Request{Route: "pay", Idempotent: true},
		nil, 100*time.Millisecond, nil)

	if res.reason != resilient.ReasonUnknown || res.resp == nil {
		t.Fatalf("expected success after retry, reason=%s", res.reason)
	}
	if calls := p.Downstream().Calls(tgt); calls != 2 {
		t.Fatalf("downstream calls=%d, want 2", calls)
	}
	if len(res.tr.Attempts) != 2 ||
		res.tr.Attempts[0].Result != resilient.AttemptSlowFail ||
		res.tr.Attempts[1].Result != resilient.AttemptOK {
		t.Fatalf("attempt sequence not traceable: %+v", res.tr.Attempts)
	}
	// 第二次尝试的剩余预算必须严格小于第一次（同一共享预算被消耗）。
	if !(res.tr.Attempts[1].Remaining < res.tr.Attempts[0].Remaining) {
		t.Fatalf("budget must shrink across attempts: %s then %s",
			res.tr.Attempts[0].Remaining, res.tr.Attempts[1].Remaining)
	}
	t.Logf("\n%s", res.tr.Render())
}

// 非幂等请求失败后绝不重试，即使策略允许慢故障重试、即使仍有大量预算。
func TestRetry_NonIdempotentNeverRetried(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	cfg.Retry.BackoffBase = 0
	p := resilient.New(clk, singleTarget(), cfg)
	tgt := payTarget()
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{
		Delay:   5 * time.Millisecond,
		Default: resilient.AttemptSlowFail,
	})

	res := runPumped(t, clk, p,
		&resilient.Request{Route: "pay", Idempotent: false},
		nil, 100*time.Millisecond, nil)

	if res.reason != resilient.ReasonAllAttemptsFailed {
		t.Fatalf("reason=%s, want all_attempts_failed", res.reason)
	}
	if calls := p.Downstream().Calls(tgt); calls != 1 {
		t.Fatalf("non-idempotent calls=%d, must be exactly 1 (no retry)", calls)
	}
	if len(res.tr.Attempts) != 1 {
		t.Fatalf("attempts=%d, want 1", len(res.tr.Attempts))
	}
	t.Logf("\n%s", res.tr.Render())
}

// 退避不得越过剩余预算：当退避恰好会耗尽剩余预算时，预算在退避期间到期，
// 最终原因为预算耗尽，且不得再发起新的下游尝试（请求量不被重试放大）。
func TestRetry_BackoffBoundedByBudget(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.BackoffBase = 80 * time.Millisecond
	cfg.Retry.BackoffMax = 0 // 不用上限，首次退避恰为 80ms
	p := resilient.New(clk, singleTarget(), cfg)
	tgt := payTarget()
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{
		Delay:   20 * time.Millisecond,
		Default: resilient.AttemptSlowFail,
	})

	res := runPumped(t, clk, p,
		&resilient.Request{Route: "pay", Idempotent: true},
		nil, 100*time.Millisecond, nil)

	// attempt1 占 20ms -> 剩余 80ms；退避 80ms 恰在预算到期点结束
	// （提前一个 epsilon，由预算稳定打断），attempt2 永远不会发起。
	if res.reason != resilient.ReasonBudgetExhausted {
		t.Logf("\n%s", res.tr.Render())
		t.Fatalf("reason=%s, want budget_exhausted during backoff", res.reason)
	}
	if calls := p.Downstream().Calls(tgt); calls != 1 {
		t.Logf("\n%s", res.tr.Render())
		t.Fatalf("downstream calls=%d, want 1 (no attempt after budget-consuming backoff)", calls)
	}
	t.Logf("\n%s", res.tr.Render())
}

func TestRetryPolicy_CanRetryRules(t *testing.T) {
	pol := resilient.RetryPolicy{MaxAttempts: 3, Retryable: resilient.DefaultRetryable}
	cases := []struct {
		idempotent bool
		res        resilient.AttemptResult
		reason     resilient.Reason
		want       bool
	}{
		{true, resilient.AttemptSlowFail, resilient.ReasonAllAttemptsFailed, true},
		{false, resilient.AttemptSlowFail, resilient.ReasonAllAttemptsFailed, false},
		{true, resilient.AttemptReject, resilient.ReasonDownstreamReject, false},
		{true, resilient.AttemptSlowFail, resilient.ReasonBudgetExhausted, false},
		{true, resilient.AttemptSlowFail, resilient.ReasonCallerCanceled, false},
	}
	for _, c := range cases {
		if got := pol.CanRetry(c.idempotent, c.res, c.reason); got != c.want {
			t.Errorf("CanRetry(idem=%v,res=%s,reason=%s)=%v want %v",
				c.idempotent, c.res, c.reason, got, c.want)
		}
	}
}
