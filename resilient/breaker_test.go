package resilient_test

import (
	"testing"
	"time"

	"github.com/highcumontoa/resilient-call-proxy-go/resilient"
)

func newTestBreaker(t *testing.T, threshold, allowance int, cooldown time.Duration) (*resilient.Breaker, *resilient.FakeClock, resilient.Target) {
	t.Helper()
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	tgt := resilient.Target{Route: "pay", Index: 0, Name: "pay-0"}
	br := resilient.NewBreaker(tgt, resilient.BreakerConfig{
		FailureThreshold: threshold,
		Cooldown:         cooldown,
		ProbeAllowance:   allowance,
	}, clk)
	return br, clk, tgt
}

func failOnce(t *testing.T, br *resilient.Breaker) {
	t.Helper()
	p, ok := br.Allow()
	if !ok {
		t.Fatal("expected allow in closed state")
	}
	if p.IsProbe() {
		t.Fatal("closed-state permit must not be a probe")
	}
	p.Release(resilient.AttemptSlowFail)
}

// 短暂抖动（未达连续失败阈值）不得误熔断；一次成功清零计数。
func TestBreaker_JitterBelowThresholdDoesNotTrip(t *testing.T) {
	br, _, _ := newTestBreaker(t, 3, 1, 10*time.Second)

	failOnce(t, br)
	failOnce(t, br)
	if got := br.State(); got != resilient.BreakerClosed {
		t.Fatalf("after 2/3 failures state=%s, want CLOSED", got)
	}

	// 一次成功，连续失败计数清零。
	p, _ := br.Allow()
	p.Release(resilient.AttemptOK)

	failOnce(t, br)
	failOnce(t, br)
	if got := br.State(); got != resilient.BreakerClosed {
		t.Fatalf("success must reset failure streak, got state=%s", got)
	}
	t.Log("[jitter] 2 failures then success then 2 failures => CLOSED; threshold=3 protects against transient flapping")
}

// 连续失败达到阈值 -> OPEN，迁移可观测；OPEN 期间普通请求快速失败。
func TestBreaker_OpensAtThresholdAndFastFails(t *testing.T) {
	br, clk, tgt := newTestBreaker(t, 3, 1, 10*time.Second)
	for i := 0; i < 3; i++ {
		failOnce(t, br)
	}
	if got := br.State(); got != resilient.BreakerOpen {
		t.Fatalf("state=%s, want OPEN", got)
	}
	if _, allowed := br.Allow(); allowed {
		t.Fatal("requests must be fast-failed while OPEN")
	}
	trs := br.Transitions()
	if len(trs) != 1 || trs[0].From != resilient.BreakerClosed || trs[0].To != resilient.BreakerOpen {
		t.Fatalf("unexpected transitions: %+v", trs)
	}
	t.Logf("[open] target=%s transition=%s->%s at=%s why=%q decision=fast-fail ordinary requests and route to fallback",
		tgt, trs[0].From, trs[0].To, trs[0].At.Format(time.RFC3339), trs[0].Why)

	// 冷却未到，仍为 OPEN。
	clk.Advance(9 * time.Second)
	if got := br.State(); got != resilient.BreakerOpen {
		t.Fatalf("within cooldown state=%s, want OPEN", got)
	}
}

// 冷却到期 -> HALF_OPEN，仅放行受控数量探测；探测失败重新 OPEN；
// 再冷却后探测成功 -> CLOSED。
func TestBreaker_HalfOpenControlledProbes(t *testing.T) {
	br, clk, _ := newTestBreaker(t, 1, 2, 10*time.Second)
	failOnce(t, br) // CLOSED -> OPEN
	if br.State() != resilient.BreakerOpen {
		t.Fatal("want OPEN")
	}

	clk.Advance(10 * time.Second)
	if br.State() != resilient.BreakerHalfOpen {
		t.Fatal("want HALF_OPEN after cooldown")
	}

	p1, ok1 := br.Allow()
	p2, ok2 := br.Allow()
	if !ok1 || !ok2 || !p1.IsProbe() || !p2.IsProbe() {
		t.Fatalf("want exactly 2 probes allowed, got %v %v", ok1, ok2)
	}
	if _, ok := br.Allow(); ok {
		t.Fatal("3rd request must be fast-failed in HALF_OPEN (probe allowance=2)")
	}

	// 第一个探测失败：重新 OPEN（其探测名额立即归还，不泄漏）。
	p1.Release(resilient.AttemptSlowFail)
	if br.State() != resilient.BreakerOpen {
		t.Fatal("failed probe must reopen circuit")
	}
	p2.Release(resilient.AttemptSlowFail) // 在 OPEN 期间归还：不得重复迁移
	if br.State() != resilient.BreakerOpen {
		t.Fatal("late release must not change state")
	}

	// 再冷却：HALF_OPEN，这次探测成功 -> CLOSED。
	clk.Advance(10 * time.Second)
	if br.State() != resilient.BreakerHalfOpen {
		t.Fatal("want HALF_OPEN after second cooldown")
	}
	probe, ok := br.Allow()
	if !ok || !probe.IsProbe() {
		t.Fatal("want one probe in new half-open window")
	}
	probe.Release(resilient.AttemptOK)
	if br.State() != resilient.BreakerClosed {
		t.Fatal("successful probe must close circuit")
	}
	for _, tr := range br.Transitions() {
		t.Logf("[transition] %s->%s why=%q", tr.From, tr.To, tr.Why)
	}
}
