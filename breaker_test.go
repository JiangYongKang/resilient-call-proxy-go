package proxy

import (
	"sync"
	"testing"
	"time"
)

// TestBreaker_Transitions 观测 closed->open->half_open->closed 完整迁移。
func TestBreaker_Transitions(t *testing.T) {
	var mu sync.Mutex
	var got []string
	b := NewBreaker(BreakerConfig{FailureThreshold: 2, Cooldown: 40 * time.Millisecond, HalfOpenMaxProbes: 1})
	b.OnTransition(func(from, to BreakerState) {
		mu.Lock()
		got = append(got, string(from)+"->"+string(to))
		mu.Unlock()
	})

	rep, err := b.Allow(nil)
	if err != nil {
		t.Fatal(err)
	}
	rep(OutcomeFailure) // 1 次连续失败，仍 closed（短暂抖动不误熔断）
	if b.State() != StateClosed {
		t.Fatalf("单次失败不应打开, got %s", b.State())
	}
	rep, _ = b.Allow(nil)
	rep(OutcomeFailure) // 2 次 -> open
	if b.State() != StateOpen {
		t.Fatalf("应 open, got %s", b.State())
	}

	// open 期间快速失败，且不发放 report。
	if _, err := b.Allow(nil); err == nil {
		t.Fatal("open 期间应快速失败")
	}

	// 冷却期内一次成功插入的前提：等待冷却后进入 half_open，只放行 1 个探测。
	time.Sleep(45 * time.Millisecond)
	rep1, err := b.Allow(nil)
	if err != nil {
		t.Fatalf("冷却后第一个请求应作为探测放行: %v", err)
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("应 half_open, got %s", b.State())
	}
	// 探测在途时，第二个请求必须被快速失败（探测名额受控）。
	if _, err := b.Allow(nil); err == nil {
		t.Fatal("half_open 探测名额已满时必须快速失败")
	}
	rep1(OutcomeSuccess) // 探测成功 -> closed
	if b.State() != StateClosed {
		t.Fatalf("探测成功应 closed, got %s", b.State())
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{"closed->open", "open->half_open", "half_open->closed"}
	if len(got) != len(want) {
		t.Fatalf("迁移序列不匹配: %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("迁移 #%d want %s got %s (all=%v)", i, want[i], got[i], got)
		}
	}
}

// TestBreaker_BriefJitterDoesNotTrip 验证成功穿插时连续失败计数被清零。
func TestBreaker_BriefJitterDoesNotTrip(t *testing.T) {
	b := NewBreaker(BreakerConfig{FailureThreshold: 3, Cooldown: 50 * time.Millisecond, HalfOpenMaxProbes: 1})
	rep, _ := b.Allow(nil)
	rep(OutcomeFailure)
	rep, _ = b.Allow(nil)
	rep(OutcomeFailure)
	rep, _ = b.Allow(nil)
	rep(OutcomeSuccess) // 成功清零
	rep, _ = b.Allow(nil)
	rep(OutcomeFailure)
	rep, _ = b.Allow(nil)
	rep(OutcomeFailure) // 连续失败只有 2，不应打开
	if b.State() != StateClosed {
		t.Fatalf("短暂抖动（成功穿插）不应熔断, got %s", b.State())
	}
}

// TestBreaker_HalfOpenFailureReopens 验证半开探测失败重新打开并重置冷却。
func TestBreaker_HalfOpenFailureReopens(t *testing.T) {
	b := NewBreaker(BreakerConfig{FailureThreshold: 1, Cooldown: 30 * time.Millisecond, HalfOpenMaxProbes: 1})
	rep, _ := b.Allow(nil)
	rep(OutcomeFailure)
	if b.State() != StateOpen {
		t.Fatal("threshold=1 时一次失败即应打开")
	}
	time.Sleep(35 * time.Millisecond)
	rep, _ = b.Allow(nil)
	if b.State() != StateHalfOpen {
		t.Fatalf("应 half_open, got %s", b.State())
	}
	rep(OutcomeFailure)
	if b.State() != StateOpen {
		t.Fatalf("探测失败应重新 open, got %s", b.State())
	}
	if _, err := b.Allow(nil); err == nil {
		t.Fatal("重新 open 后必须快速失败")
	}
}

// TestBreaker_CancelReleasesProbeSlot 验证探测请求被取消/预算耗尽时：
// 只释放名额、不触发迁移，后续仍可继续探测直至成功。
func TestBreaker_CancelReleasesProbeSlot(t *testing.T) {
	b := NewBreaker(BreakerConfig{FailureThreshold: 1, Cooldown: 20 * time.Millisecond, HalfOpenMaxProbes: 1})
	rep, _ := b.Allow(nil)
	rep(OutcomeFailure)
	time.Sleep(25 * time.Millisecond)

	rep, err := b.Allow(nil)
	if err != nil {
		t.Fatalf("探测应放行: %v", err)
	}
	if _, err := b.Allow(nil); err == nil {
		t.Fatal("探测在途时第二个请求必须快速失败")
	}
	rep(OutcomeCanceled) // 预算耗尽，结论不可归因
	if b.State() != StateHalfOpen {
		t.Fatalf("取消不应触发迁移，应保持 half_open, got %s", b.State())
	}
	// 名额已释放：可再次探测并成功关闭。
	rep, err = b.Allow(nil)
	if err != nil {
		t.Fatalf("取消释放名额后应能再次探测: %v", err)
	}
	rep(OutcomeSuccess)
	if b.State() != StateClosed {
		t.Fatalf("再次探测成功应 closed, got %s", b.State())
	}
}
