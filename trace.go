package proxy

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Decision 描述单次尝试的判定依据，使测试日志与最终结论可判别。
type Decision string

const (
	DecisionStart      Decision = "attempt_start"
	DecisionSuccess    Decision = "success"
	DecisionReject     Decision = "downstream_rejected"
	DecisionTimeout    Decision = "downstream_timeout"
	DecisionBudgetGone Decision = "budget_exhausted_before_attempt"
	DecisionCanceled   Decision = "caller_canceled_before_attempt"
	DecisionFastFail   Decision = "breaker_fast_fail"
	DecisionNoRetry    Decision = "non_idempotent_no_retry"
	DecisionRetry      Decision = "will_retry"
	DecisionGiveUp     Decision = "attempts_exhausted"
	DecisionFallback   Decision = "fallback_served"
	DecisionNoRoute    Decision = "no_route"
)

// AttemptRecord 记录一次尝试的完整判定上下文。
type AttemptRecord struct {
	Index           int           // 尝试序号，从 1 开始
	Target          string        // 本次尝试选择的目标（未选出时为空）
	RemainingBudget time.Duration // 发起该尝试时的剩余共享预算
	BreakerState    BreakerState  // 该目标熔断器当时状态
	Decision        Decision      // 判定依据
	Reason          Reason        // 归一化的失败原因
	Err             string        // 底层错误文本（成功为空）
}

// Tracer 并发安全地收集并实时打印尝试记录。
type Tracer struct {
	mu      sync.Mutex
	records []AttemptRecord
	w       io.Writer
}

// NewTracer 创建记录器，默认写入 stderr，单测中可直接看到判定过程。
func NewTracer() *Tracer { return &Tracer{w: os.Stderr} }

// NewTracerTo 创建写入指定 writer 的记录器（测试可传 &bytes.Buffer 断言）。
func NewTracerTo(w io.Writer) *Tracer { return &Tracer{w: w} }

// Add 记录一次尝试并立即打印一行判定日志。
func (t *Tracer) Add(rec AttemptRecord) {
	t.mu.Lock()
	t.records = append(t.records, rec)
	w := t.w
	t.mu.Unlock()
	if w == nil {
		return
	}
	fmt.Fprintf(w,
		"[proxy] attempt=%d target=%-8s remaining_budget=%-10s breaker=%-9s decision=%-28s reason=%-20s err=%s\n",
		rec.Index, dash(rec.Target), rec.RemainingBudget, rec.BreakerState,
		rec.Decision, dash(string(rec.Reason)), dash(rec.Err))
}

// Summary 打印整次请求的结论日志。
func (t *Tracer) Summary(req *Request, finalReason Reason, ok bool) {
	t.mu.Lock()
	recs := make([]AttemptRecord, len(t.records))
	copy(recs, t.records)
	w := t.w
	t.mu.Unlock()
	// 真实尝试次数取记录中出现过的最大尝试序号。
	attempts := 0
	for _, rc := range recs {
		if rc.Index > attempts {
			attempts = rc.Index
		}
	}
	if w == nil {
		return
	}
	outcome := "SUCCESS"
	if !ok {
		outcome = "FAILED"
	}
	fmt.Fprintf(w,
		"[proxy] summary route=%s idempotent=%t attempts=%d outcome=%s final_reason=%s\n",
		req.Route, req.Idempotent, attempts, outcome, dash(string(finalReason)))
}

func (t *Tracer) Records() []AttemptRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]AttemptRecord, len(t.records))
	copy(out, t.records)
	return out
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
