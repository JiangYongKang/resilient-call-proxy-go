package resilient_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/resilient-call-proxy-go/resilient"
)

// 健康快照与正常调用并发：快照只读内存、不在调用热路径，-race 下无竞争，
// 且每次读到的每个目标内部字段自洽（剩余名额 = 总额度 - 已用）。
func TestHealthView_ConcurrentSnapshotWithCalls(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	cfg.Breaker.FailureThreshold = 1000
	p := resilient.New(clk, twoTargets(), cfg)
	t0 := resilient.Target{Route: "pay", Index: 0, Name: "pay-0"}
	t1 := resilient.Target{Route: "pay", Index: 1, Name: "pay-1"}
	p.Downstream().SetScript(t0, &resilient.ScriptedBehavior{Default: resilient.AttemptOK})
	p.Downstream().SetScript(t1, &resilient.ScriptedBehavior{Default: resilient.AttemptOK})

	const M = 40
	var stop atomic.Bool
	var wg sync.WaitGroup

	// 并发读者：持续抓取全量快照并校验内部自洽。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			v := p.HealthSnapshot()
			if len(v.Targets) != 2 {
				t.Errorf("snapshot targets=%d want 2", len(v.Targets))
				return
			}
			for _, h := range v.Targets {
				if h.ProbesRemaining != h.ProbeAllowance-h.ProbesUsed &&
					!(h.ProbesUsed >= h.ProbeAllowance && h.ProbesRemaining == 0) {
					t.Errorf("inconsistent probe slots on %s: %+v", h.Target, h)
					return
				}
				if h.Dispatched < h.Successes+h.Rejects+h.SlowFailures {
					t.Errorf("dispatched %d < verdicts on %s", h.Dispatched, h.Target)
					return
				}
			}
		}
	}()

	// 写者：确定性泵驱动一串成功调用。
	for i := 0; i < M; i++ {
		runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil)
	}
	stop.Store(true)
	wg.Wait()

	v := p.HealthSnapshot()
	var succ int64
	for _, h := range v.Targets {
		succ += h.Successes
		if h.State != resilient.BreakerClosed {
			t.Fatalf("%s state=%s want CLOSED", h.Target, h.State)
		}
		if h.CallerCanceled != 0 || h.BudgetExhausted != 0 {
			t.Fatalf("%s neutral counters must be 0: %+v", h.Target, h)
		}
	}
	if succ != M {
		t.Fatalf("total successes=%d want %d", succ, M)
	}
	// 快照按固定顺序排序。
	if v.Targets[0].Target.Name != "pay-0" || v.Targets[1].Target.Name != "pay-1" {
		t.Fatalf("snapshot not sorted: %s, %s", v.Targets[0].Target, v.Targets[1].Target)
	}
}

// 尝试开始前就取消：没有任何目标被尝试，属于无法归到目标的非目标责任终止，
// 计入孤儿计数，调用方结论与（空）轨迹一致，且不产生任何下游调用。
func TestHealth_OrphanCancelBeforeAnyAttempt(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	p := resilient.New(clk, singleTarget(), testConfig())
	tgt := payTarget()
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{Default: resilient.AttemptOK})

	budget := resilient.NewBudget(clk, 100*time.Millisecond, nil)
	budget.Cancel() // 先取消再发起
	resp, reason, tr := p.DoWithBudget(&resilient.Request{Route: "pay", Idempotent: true}, budget)

	if resp != nil || reason != resilient.ReasonCallerCanceled {
		t.Fatalf("resp=%v reason=%s want nil/caller_canceled", resp, reason)
	}
	if len(tr.Attempts) != 0 {
		t.Fatalf("attempts=%d want 0 (cancel before any attempt)", len(tr.Attempts))
	}
	if calls := p.Downstream().Calls(tgt); calls != 0 {
		t.Fatalf("downstream calls=%d want 0", calls)
	}
	v := p.HealthSnapshot()
	if v.OrphanCallerCanceled != 1 || v.OrphanBudgetExhausted != 0 {
		t.Fatalf("orphan canceled=%d budget=%d want 1/0", v.OrphanCallerCanceled, v.OrphanBudgetExhausted)
	}
	h, _ := p.TargetHealthOf(tgt)
	if h.CallerCanceled != 0 || h.Dispatched != 0 {
		t.Fatalf("target must not be charged: %+v", h)
	}
	if h.State != resilient.BreakerClosed {
		t.Fatalf("state=%s want CLOSED", h.State)
	}
}

// 三处结论对齐：调用方最终原因、轨迹每次尝试的 Attribution、健康视图分类，
// 在一次“成功 + 拒绝 + 预算耗尽”的序列上必须给出同一组结论。
func TestHealth_ThreeWayAlignment(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	cfg := testConfig()
	cfg.Retry.MaxAttempts = 1
	cfg.Breaker.FailureThreshold = 1000
	p := resilient.New(clk, singleTarget(), cfg)
	tgt := payTarget()
	p.Downstream().SetScript(tgt, &resilient.ScriptedBehavior{
		Delay: 50 * time.Millisecond,
		Sequence: []resilient.AttemptResult{
			resilient.AttemptOK,
			resilient.AttemptReject,
		},
		Default: resilient.AttemptSlowFail,
	})

	// 1) 成功。
	rOK := runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil)
	if rOK.reason != resilient.ReasonUnknown || rOK.tr.Attempts[0].Attribution != resilient.AttrSuccess {
		t.Fatal("success alignment broken")
	}
	// 2) 明确拒绝。
	rRej := runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 100*time.Millisecond, nil)
	if rRej.reason != resilient.ReasonDownstreamReject ||
		rRej.tr.Cause != resilient.ReasonDownstreamReject ||
		rRej.tr.Attempts[0].Attribution != resilient.AttrDownstreamReject {
		t.Fatal("reject alignment broken")
	}
	// 3) 预算耗尽（慢调用顶到预算终点）。
	rBud := runPumped(t, clk, p, &resilient.Request{Route: "pay", Idempotent: true}, nil, 50*time.Millisecond, nil)
	if rBud.reason != resilient.ReasonBudgetExhausted ||
		rBud.tr.Attempts[0].Attribution != resilient.AttrBudgetExhausted {
		t.Fatalf("budget alignment broken: reason=%s attr=%s",
			rBud.reason, rBud.tr.Attempts[0].Attribution)
	}

	// 健康视图分类必须与上面的结论一一对应。
	h, _ := p.TargetHealthOf(tgt)
	if h.Successes != 1 || h.Rejects != 1 || h.SlowFailures != 0 || h.BudgetExhausted != 1 {
		t.Fatalf("health classification misaligned: %+v", h)
	}
	t.Logf("[alignment] caller reasons: %s/%s/%s ; health: ok=%d reject=%d slow=%d budget=%d",
		rOK.reason, rRej.reason, rBud.reason, h.Successes, h.Rejects, h.SlowFailures, h.BudgetExhausted)
}
