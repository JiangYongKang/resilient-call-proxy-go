package resilient

import "time"

// RetryPolicy 重试策略。
type RetryPolicy struct {
	// MaxAttempts 含首次在内的总尝试上限，硬性限制请求放大。
	MaxAttempts int
	// Retryable 判定某次失败是否允许重试（已结合请求结果/原因）。
	Retryable func(AttemptResult, Reason) bool
	// BackoffBase 指数退避基数；0 表示失败后立即重试。
	BackoffBase time.Duration
	// BackoffMax 单次退避上限。
	BackoffMax time.Duration
}

// DefaultRetryable 仅对可安全重试的失败重试：
//   - 慢故障（抖动型，语义上未被下游接受）可重试；
//   - 下游明确拒绝不重试（失败是确定的结论）；
//   - 预算耗尽/取消不重试（重试只会越过共享预算）。
//
// 非幂等请求是否重试由代理层额外强制（见 Proxy），策略函数只负责
// 结果/原因维度的判定。
func DefaultRetryable(res AttemptResult, r Reason) bool {
	if r == ReasonBudgetExhausted || r == ReasonCallerCanceled {
		return false
	}
	if res == AttemptReject {
		return false
	}
	return res == AttemptSlowFail
}

// CanRetry 综合幂等性与结果/原因判定是否允许重试。
func (p RetryPolicy) CanRetry(idempotent bool, res AttemptResult, r Reason) bool {
	if !idempotent {
		return false
	}
	if p.Retryable == nil {
		return false
	}
	return p.Retryable(res, r)
}

// Backoff 返回第 attempt 次失败后的等待时长（attempt 从 1 起）。
// 纯计算 min(指数退避, 剩余预算)：退避绝不越过剩余预算。是否需要提前
// 一个时钟精度以区分“预算打断”与“自然醒来”，由代理层统一处理。
func (p RetryPolicy) Backoff(attempt int, remaining time.Duration) time.Duration {
	if p.BackoffBase <= 0 || attempt < 1 {
		return 0
	}
	d := p.BackoffBase << (attempt - 1)
	if d <= 0 || (p.BackoffMax > 0 && d > p.BackoffMax) {
		d = p.BackoffMax
	}
	if remaining >= 0 && d > remaining {
		d = remaining
	}
	if d < 0 {
		return 0
	}
	return d
}

// Fallback 降级函数：熔断打开/半开拒绝或所有尝试失败后调用。
type Fallback func(req *Request) (*Response, error)

// Request 代理请求。Idempotent=false 的请求失败后绝不重试。
type Request struct {
	Route      string
	Idempotent bool
}

// Response 代理响应。
type Response struct {
	Target string
	OK     bool
	Body   string
}
