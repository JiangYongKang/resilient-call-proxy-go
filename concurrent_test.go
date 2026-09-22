package proxy

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

// TestConcurrent_HalfOpenProbeNotAmplified 验证半开期间并发请求只有受控数量真正打到下游，
// 其余全部快速失败并走降级，请求量不被放大；取消探测只释放名额、不触发迁移。
func TestConcurrent_HalfOpenProbeNotAmplified(t *testing.T) {
	r := NewRouter(BreakerConfig{FailureThreshold: 1, Cooldown: 80 * time.Millisecond, HalfOpenMaxProbes: 2})
	down := NewScriptedDownstream("d",
		ScriptStep{Action: ActionReject}, // 第 1 次：触发 open
		ScriptStep{Action: ActionBlock},  // 探测：阻塞至 ctx 结束
		ScriptStep{Action: ActionBlock},
	)
	r.Register("svc", down)
	p := NewProxy(r, Config{MaxAttempts: 1, TotalTimeout: 30 * time.Second, LogOutput: io.Discard})

	if _, _, err := p.Call(context.Background(), &Request{Route: "svc", Idempotent: true, LogOutput: io.Discard}); err == nil {
		t.Fatal("首次应失败")
	}
	if r.Breaker("svc", "d").State() != StateOpen {
		t.Fatal("应已打开熔断")
	}
	time.Sleep(85 * time.Millisecond) // 越过冷却 -> 下一次 Allow 迁移 half_open

	// 降级计数：普通快速失败 -> breaker_open；之后主动取消的探测 -> canceled。
	var reasonMu sync.Mutex
	reasons := map[Reason]int{}
	p.SetFallback("svc", func(req *Request, reason Reason) (*Response, error) {
		reasonMu.Lock()
		reasons[reason]++
		reasonMu.Unlock()
		return &Response{Payload: "fb"}, nil
	})

	// 先占满 2 个探测名额。
	var pwg sync.WaitGroup
	probeCtxs := make([]context.CancelFunc, 2)
	pstart := make(chan struct{})
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		probeCtxs[i] = cancel
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			<-pstart
			p.Call(ctx, &Request{Route: "svc", Idempotent: true, LogOutput: io.Discard})
		}()
	}
	close(pstart)
	deadline := time.Now().Add(2 * time.Second)
	for down.Inflight() < 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if got := down.Inflight(); got != 2 {
		t.Fatalf("半开只应放行 2 个探测, 在途=%d", got)
	}

	// 探测占满后，普通并发请求必须全部快速失败。
	const N = 18
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp, _, err := p.Call(context.Background(), &Request{Route: "svc", Idempotent: true, LogOutput: io.Discard})
			if err != nil || resp == nil || resp.Payload != "fb" {
				t.Errorf("普通请求应快速失败并获得降级, err=%v resp=%v", err, resp)
			}
		}()
	}
	close(start)
	wg.Wait()
	if calls := down.CallCount(); calls != 3 {
		t.Fatalf("半开放大了请求量: 实际下游调用=%d, 期望 3（1 触发 + 2 探测）", calls)
	}
	reasonMu.Lock()
	openFalls := reasons[ReasonBreakerOpen]
	reasonMu.Unlock()
	if openFalls != N {
		t.Fatalf("应有 %d 次 breaker_open 降级, got %d", N, openFalls)
	}

	// 取消两个探测：立即退出、释放名额；降级原因为 canceled，熔断器保持 half_open。
	for _, c := range probeCtxs {
		c()
	}
	pdone := make(chan struct{})
	go func() { pwg.Wait(); close(pdone) }()
	select {
	case <-pdone:
	case <-time.After(2 * time.Second):
		t.Fatal("取消后探测请求未结束，名额可能泄漏")
	}
	if got := down.Inflight(); got != 0 {
		t.Fatalf("取消后不应残留占用, inflight=%d", got)
	}
	if state := r.Breaker("svc", "d").State(); state != StateHalfOpen {
		t.Fatalf("取消不应触发迁移，应保持 half_open, got %s", state)
	}
	reasonMu.Lock()
	canceledFalls := reasons[ReasonCanceled]
	reasonMu.Unlock()
	if canceledFalls != 2 {
		t.Fatalf("两个被取消的探测应产生 2 次 canceled 降级, got %d", canceledFalls)
	}
}

// TestConcurrent_IdempotentRetriesConsistent 并发下每个幂等请求最多 MaxAttempts 次尝试，
// 总调用数精确可预测；每次尝试目标选择可追溯。
func TestConcurrent_IdempotentRetriesConsistent(t *testing.T) {
	r := NewRouter(BreakerConfig{FailureThreshold: 1_000_000, Cooldown: time.Hour, HalfOpenMaxProbes: 1})
	down := NewScriptedDownstream("d", ScriptStep{Action: ActionReject})
	r.Register("svc", down)
	p := NewProxy(r, Config{MaxAttempts: 3, TotalTimeout: 2 * time.Second, LogOutput: io.Discard})

	const M = 50
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < M; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var buf bytes.Buffer
			_, recs, err := p.Call(context.Background(), &Request{Route: "svc", Idempotent: true, LogOutput: &buf})
			if !isReason(err, ReasonDownstreamRejected) {
				t.Errorf("want rejected, got %v", err)
			}
			maxIdx := 0
			for _, rc := range recs {
				if rc.Index > maxIdx {
					maxIdx = rc.Index
				}
				if rc.Decision == DecisionStart && rc.Target != "d" {
					t.Errorf("目标选择不一致: %s", rc.Target)
				}
			}
			if maxIdx != 3 {
				t.Errorf("每个请求恰好 3 次尝试, got %d", maxIdx)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := down.CallCount(); got != int64(M*3) {
		t.Fatalf("总调用数应精确等于 %d, got %d（重试放大/缩减）", M*3, got)
	}
}

// TestConcurrent_NonIdempotentNeverRetried 高并发下非幂等请求总数与请求数 1:1。
func TestConcurrent_NonIdempotentNeverRetried(t *testing.T) {
	r := NewRouter(BreakerConfig{FailureThreshold: 1_000_000, Cooldown: time.Hour, HalfOpenMaxProbes: 1})
	down := NewScriptedDownstream("d", ScriptStep{Action: ActionReject})
	r.Register("pay", down)
	p := NewProxy(r, Config{MaxAttempts: 4, TotalTimeout: 2 * time.Second, LogOutput: io.Discard})

	const M = 50
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < M; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := p.Call(context.Background(), &Request{Route: "pay", Idempotent: false})
			if !isReason(err, ReasonDownstreamRejected) {
				t.Errorf("want rejected, got %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := down.CallCount(); got != int64(M) {
		t.Fatalf("非幂等请求总调用必须等于请求数 %d, got %d", M, got)
	}
}
