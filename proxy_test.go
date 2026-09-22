package proxy

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func isReason(err error, r Reason) bool {
	var ce *CallError
	return errors.As(err, &ce) && errors.Is(err, &CallError{Reason: r})
}

// runCall 把代理日志同时输出到测试日志与捕获 buffer，便于断言与人工检查。
func runCall(t *testing.T, p *Proxy, ctx context.Context, req *Request) (*Response, []AttemptRecord, error, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	prev := p.cfg.LogOutput
	p.cfg.LogOutput = &buf
	t.Cleanup(func() { p.cfg.LogOutput = prev })
	resp, recs, err := p.Call(ctx, req)
	t.Logf("\n%s", buf.String())
	return resp, recs, err, &buf
}

// TestBudgetExhausted_NoFurtherAttempts 验证共享预算耗尽：
// 慢下游吃掉整个预算后不得继续尝试，错误原因可判别，下游不残留占用。
func TestBudgetExhausted_NoFurtherAttempts(t *testing.T) {
	r := NewRouter(BreakerConfig{FailureThreshold: 3, Cooldown: 50 * time.Millisecond, HalfOpenMaxProbes: 1})
	slow := NewScriptedDownstream("slow", ScriptStep{Action: ActionBlock})
	r.Register("svc", slow)
	p := NewProxy(r, Config{MaxAttempts: 3, TotalTimeout: 80 * time.Millisecond})

	start := time.Now()
	_, recs, err, _ := runCall(t, p, context.Background(), &Request{Route: "svc", Idempotent: true})
	elapsed := time.Since(start)

	if !isReason(err, ReasonBudgetExceeded) {
		t.Fatalf("want budget_exceeded, got %v", err)
	}
	maxIdx := 0
	for _, rc := range recs {
		if rc.Index > maxIdx {
			maxIdx = rc.Index
		}
	}
	if maxIdx != 1 {
		t.Fatalf("预算耗尽后不得发起更多尝试，期望尝试序号仅到 1, got %d: %+v", maxIdx, recs)
	}
	if slow.CallCount() != 1 {
		t.Fatalf("下游最多被调用 1 次, got %d", slow.CallCount())
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("应在预算到期附近返回, elapsed=%v", elapsed)
	}
	// 超时取消后，在途调用必须退出并释放名额。
	deadline := time.Now().Add(200 * time.Millisecond)
	for slow.Inflight() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if slow.Inflight() != 0 {
		t.Fatalf("超时后下游仍有在途调用: %d", slow.Inflight())
	}
}

// TestCallerCanceled_DistinctFromTimeout 验证发起方主动取消与预算耗尽可区分。
func TestCallerCanceled_DistinctFromTimeout(t *testing.T) {
	r := NewRouter(BreakerConfig{FailureThreshold: 5, Cooldown: 50 * time.Millisecond, HalfOpenMaxProbes: 1})
	slow := NewScriptedDownstream("slow", ScriptStep{Action: ActionBlock})
	r.Register("svc", slow)
	p := NewProxy(r, Config{MaxAttempts: 1, TotalTimeout: 5 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()
	_, recs, err, _ := runCall(t, p, ctx, &Request{Route: "svc", Idempotent: true})

	if !isReason(err, ReasonCanceled) {
		t.Fatalf("want canceled, got %v", err)
	}
	if errors.Is(err, &CallError{Reason: ReasonBudgetExceeded}) {
		t.Fatal("取消不应被归类为预算耗尽")
	}
	maxIdx := 0
	for _, rc := range recs {
		if rc.Index > maxIdx {
			maxIdx = rc.Index
		}
	}
	if maxIdx != 1 {
		t.Fatalf("取消后不得重试, attempts=%d records=%+v", maxIdx, recs)
	}
}

// TestDownstreamRejected_Distinct 验证下游明确拒绝与超时严格区分，且可安全重试后成功。
func TestDownstreamRejected_RetryThenSuccess(t *testing.T) {
	r := NewRouter(BreakerConfig{FailureThreshold: 10, Cooldown: 50 * time.Millisecond, HalfOpenMaxProbes: 1})
	flaky := NewScriptedDownstream("flaky",
		ScriptStep{Action: ActionReject},
		ScriptStep{Action: ActionReject},
		ScriptStep{Action: ActionOK, Out: "ok"},
	)
	r.Register("svc", flaky)
	p := NewProxy(r, Config{MaxAttempts: 3, TotalTimeout: time.Second})

	resp, recs, err, _ := runCall(t, p, context.Background(), &Request{Route: "svc", Idempotent: true})
	if err != nil {
		t.Fatalf("第三次应成功, got %v", err)
	}
	if resp.Payload != "ok" {
		t.Fatalf("unexpected payload %q", resp.Payload)
	}
	if flaky.CallCount() != 3 {
		t.Fatalf("应恰好调用 3 次, got %d", flaky.CallCount())
	}
	// 前两次归因必须是明确拒绝而非超时。
	for i := 0; i < 2; i++ {
		found := false
		for _, rc := range recs {
			if rc.Decision == DecisionReject && rc.Reason == ReasonDownstreamRejected {
				found = true
			}
		}
		if !found {
			t.Fatalf("缺少明确拒绝归因: %+v", recs)
		}
	}
}

// TestNonIdempotent_NoRetry 验证非幂等请求失败后绝不重试。
func TestNonIdempotent_NoRetry(t *testing.T) {
	r := NewRouter(BreakerConfig{FailureThreshold: 10, Cooldown: 50 * time.Millisecond, HalfOpenMaxProbes: 1})
	down := NewScriptedDownstream("d",
		ScriptStep{Action: ActionReject},
		ScriptStep{Action: ActionOK, Out: "should-not-reach"},
	)
	r.Register("pay", down)
	p := NewProxy(r, Config{MaxAttempts: 5, TotalTimeout: time.Second})

	_, recs, err, _ := runCall(t, p, context.Background(), &Request{Route: "pay", Idempotent: false})
	if !isReason(err, ReasonDownstreamRejected) {
		t.Fatalf("want rejected, got %v", err)
	}
	if down.CallCount() != 1 {
		t.Fatalf("非幂等请求只允许调用 1 次, got %d", down.CallCount())
	}
	hasNoRetry := false
	for _, rc := range recs {
		if rc.Decision == DecisionNoRetry {
			hasNoRetry = true
		}
	}
	if !hasNoRetry {
		t.Fatalf("必须记录 non_idempotent_no_retry 判定: %+v", recs)
	}
}

// TestRetryRespectsSharedBudget 验证多次尝试共享同一总预算：
// 前两次慢调用耗掉大部分预算后，第三次只能在剩余预算内发起并随预算耗尽而终止。
func TestRetryRespectsSharedBudget(t *testing.T) {
	r := NewRouter(BreakerConfig{FailureThreshold: 100, Cooldown: time.Second, HalfOpenMaxProbes: 1})
	// 每次延迟 60ms > 单次保护 50ms：第 1、2 次都在 50ms 处超时，共耗掉 100ms 总预算，
	// 第 3 次在循环顶部即被预算拦截，不得发起。
	down := NewScriptedDownstream("d", ScriptStep{Action: ActionDelay, Delay: 60 * time.Millisecond})
	r.Register("svc", down)
	p := NewProxy(r, Config{MaxAttempts: 5, TotalTimeout: 100 * time.Millisecond, PerAttemptSafety: 50 * time.Millisecond})

	_, recs, err, _ := runCall(t, p, context.Background(), &Request{Route: "svc", Idempotent: true})
	if !isReason(err, ReasonBudgetExceeded) {
		t.Fatalf("want budget_exceeded, got %v", err)
	}
	// 每次尝试都使用同一 deadline 派生的 context：实际调用次数 2 次（45+45≈90ms），
	// 第 3 次在循环顶部因预算耗尽被拦截。
	calls := down.CallCount()
	if calls != 2 {
		t.Fatalf("共享预算下期望实际调用 2 次, got %d, records=%+v", calls, recs)
	}
	// 最后一条记录必须是"预算耗尽、不再发起尝试"。
	last := recs[len(recs)-1]
	if last.Decision != DecisionBudgetGone {
		t.Fatalf("最后判定应为 budget_exhausted_before_attempt, got %s", last.Decision)
	}
	// 每次记录的剩余预算应单调不增。
	var prev time.Duration = 1<<63 - 1
	for _, rc := range recs {
		if rc.Decision == DecisionStart && rc.RemainingBudget > prev {
			t.Fatalf("剩余预算必须单调不增: %v -> %v", prev, rc.RemainingBudget)
		}
		if rc.Decision == DecisionStart {
			prev = rc.RemainingBudget
		}
	}
}

// TestFallback_OnBreakerOpen 验证熔断打开期间普通请求快速失败并走降级。
func TestFallback_OnBreakerOpen(t *testing.T) {
	r := NewRouter(BreakerConfig{FailureThreshold: 2, Cooldown: 200 * time.Millisecond, HalfOpenMaxProbes: 1})
	down := NewScriptedDownstream("d", ScriptStep{Action: ActionReject})
	r.Register("svc", down)
	p := NewProxy(r, Config{MaxAttempts: 1, TotalTimeout: time.Second})

	// 连续 2 次失败触发熔断打开。
	for i := 0; i < 2; i++ {
		p.Call(context.Background(), &Request{Route: "svc", Idempotent: true})
	}
	if state := r.Breaker("svc", "d").State(); state != StateOpen {
		t.Fatalf("熔断应为 open, got %s", state)
	}
	p.SetFallback("svc", func(req *Request, reason Reason) (*Response, error) {
		if reason != ReasonBreakerOpen {
			t.Errorf("降级收到的原因应为 breaker_open, got %s", reason)
		}
		return &Response{Payload: "fallback"}, nil
	})

	var buf bytes.Buffer
	p.cfg.LogOutput = &buf
	resp, recs, err := p.Call(context.Background(), &Request{Route: "svc", Idempotent: true})
	t.Logf("\n%s", buf.String())
	if err != nil {
		t.Fatalf("降级后不应返回错误, got %v", err)
	}
	if resp.Payload != "fallback" {
		t.Fatalf("want fallback, got %q", resp.Payload)
	}
	callsBefore := down.CallCount()
	// open 期间必须快速失败：不触碰下游。
	if callsBefore != 2 {
		t.Fatalf("open 期间不得调用下游, calls=%d", callsBefore)
	}
	if recs[0].Decision != DecisionFastFail {
		t.Fatalf("应记录快速失败, got %s", recs[0].Decision)
	}
}
