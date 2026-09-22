package resilient_test

import (
	"testing"
	"time"

	"github.com/highcumontoa/resilient-call-proxy-go/resilient"
)

// 在独立协程中等待，主协程推进时钟；Sleep 由条件变量驱动，零墙钟等待。
func asyncSleep(clk *resilient.FakeClock, b *resilient.Budget, d time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		clk.Sleep(b, d)
		close(done)
	}()
	return done
}

func TestFakeClock_AdvanceTriggersTimer(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(1000, 0))
	b := resilient.NewBudget(clk, 10*time.Second, nil)
	defer b.Dispose()
	done := asyncSleep(clk, b, 3*time.Second)
	clk.WaitForSleepers(1, done)

	clk.Advance(2 * time.Second)
	select {
	case <-done:
		t.Fatal("timer should not fire before deadline")
	default:
	}
	clk.Advance(1 * time.Second)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timer should fire at deadline")
	}
}

func TestFakeClock_BudgetInterruptsSleep(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))
	b := resilient.NewBudget(clk, 5*time.Second, nil)
	defer b.Dispose()
	// 等待时长超过预算：在 5s 处被预算打断，返回 false。
	done := asyncSleep(clk, b, 10*time.Second)
	clk.WaitForSleepers(1, done)
	clk.Advance(5 * time.Second)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sleep should be interrupted when budget expires")
	}
	if got := b.Remaining(); got != 0 {
		t.Fatalf("remaining=%s, want 0", got)
	}
}

func TestBudget_ExpiryAndCancelAreDistinguishable(t *testing.T) {
	clk := resilient.NewFakeClock(time.Unix(0, 0))

	// 场景一：预算自然耗尽。
	b1 := resilient.NewBudget(clk, 5*time.Second, nil)
	if r := b1.Remaining(); r != 5*time.Second {
		t.Fatalf("initial remaining = %s, want 5s", r)
	}
	clk.Advance(5 * time.Second)
	if err := b1.Err(); err == nil || err.Reason != resilient.ReasonBudgetExhausted {
		t.Fatalf("want budget_exhausted, got %v", err)
	}
	t.Logf("[budget-expire] remaining=%s reason=%s decision=shared deadline reached; no attempt may cross it",
		b1.Remaining(), b1.Err().Reason)
	b1.Dispose()

	// 场景二：发起方主动取消（同步 Cancel，确定性）。
	b2 := resilient.NewBudget(clk, 10*time.Second, nil)
	b2.Cancel()
	if err := b2.Err(); err == nil || err.Reason != resilient.ReasonCallerCanceled {
		t.Fatalf("want caller_canceled, got %v", err)
	}
	t.Logf("[caller-cancel] remaining=%s reason=%s decision=cancellation is distinguishable from timeout",
		b2.Remaining(), b2.Err().Reason)
	b2.Dispose()

	// 场景三：取消优先于稍后的到期时刻。
	b3 := resilient.NewBudget(clk, 10*time.Second, nil)
	b3.Cancel()
	clk.Advance(10 * time.Second)
	if err := b3.Err(); err == nil || err.Reason != resilient.ReasonCallerCanceled {
		t.Fatalf("cancellation must win over expiry, got %v", err)
	}
	b3.Dispose()
}
