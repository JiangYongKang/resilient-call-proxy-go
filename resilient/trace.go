package resilient

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// AttemptRecord 记录单次尝试的完整判定轨迹。
type AttemptRecord struct {
	Index       int
	Target      Target
	StateBefore BreakerState
	IsProbe     bool
	Remaining   time.Duration // 尝试开始时的剩余预算
	Limit       time.Duration // 本次尝试时限（0 表示以剩余预算为限）
	Result      AttemptResult
	StateAfter  BreakerState
	// Attribution 是本次尝试的唯一归因结论。它同时决定：
	// 返回给调用方的最终原因、本轨迹的判定分类、健康视图的计数类别。
	Attribution Attribution
	Dispatched  bool // 下游调用是否真的发出（false=放行后未发出即终止）
	Decision    string
}

func (r AttemptResult) String() string {
	switch r {
	case AttemptOK:
		return "OK"
	case AttemptReject:
		return "REJECT"
	case AttemptSlowFail:
		return "SLOW_FAIL"
	case AttemptTimeout:
		return "TIMEOUT"
	case AttemptBreakerReject:
		return "BREAKER_REJECT"
	case AttemptAborted:
		return "ABORTED"
	default:
		return "UNKNOWN"
	}
}

// Trace 是一次请求的完整轨迹。
type Trace struct {
	Route        string
	Idempotent   bool
	FinalReason  Reason
	Cause        Reason // 触发降级/终止的原始下游原因
	FromFallback bool
	Attempts     []AttemptRecord
}

// Tracer 收集轨迹（并发安全）。
type Tracer struct {
	mu     sync.Mutex
	traces []Trace
}

func NewTracer() *Tracer { return &Tracer{} }

func (t *Tracer) add(tr Trace) {
	t.mu.Lock()
	t.traces = append(t.traces, tr)
	t.mu.Unlock()
}

// Snapshot 返回轨迹快照（副本）。
func (t *Tracer) Snapshot() []Trace {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Trace, len(t.traces))
	copy(out, t.traces)
	return out
}

// Render 返回供单测日志打印的多行文本：每次尝试的剩余预算、
// 熔断前后状态与判定依据。
func (tr Trace) Render() string {
	var b strings.Builder
	cause := ""
	if tr.Cause != ReasonUnknown {
		cause = " cause=" + tr.Cause.String()
	}
	fmt.Fprintf(&b, "trace route=%s idempotent=%t attempts=%d final=%s%s fallback=%t\n",
		tr.Route, tr.Idempotent, len(tr.Attempts), tr.FinalReason, cause, tr.FromFallback)
	for _, a := range tr.Attempts {
		probe := ""
		if a.IsProbe {
			probe = " [PROBE]"
		}
		dispatched := "dispatched"
		if !a.Dispatched {
			dispatched = "NOT_DISPATCHED"
		}
		fmt.Fprintf(&b, "  attempt#%d target=%s breaker=%s->%s%s remaining=%s limit=%s result=%s classify=%s %s | %s\n",
			a.Index, a.Target, a.StateBefore, a.StateAfter, probe,
			a.Remaining.Round(time.Millisecond), a.Limit.Round(time.Millisecond),
			a.Result, a.Attribution, dispatched, a.Decision)
	}
	return b.String()
}
