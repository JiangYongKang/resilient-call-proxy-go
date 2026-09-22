package proxy

import "fmt"

// Reason 以可判别的方式标注一次代理调用最终失败的原因。
type Reason string

const (
	// ReasonNone 尚未失败。
	ReasonNone Reason = ""
	// ReasonBudgetExceeded 本次请求共享的总超时预算已耗尽。
	ReasonBudgetExceeded Reason = "budget_exceeded"
	// ReasonCanceled 发起方主动取消了请求。
	ReasonCanceled Reason = "canceled"
	// ReasonDownstreamRejected 下游明确拒绝（业务拒绝/显式失败），区别于超时。
	ReasonDownstreamRejected Reason = "downstream_rejected"
	// ReasonDownstreamTimeout 下游调用在剩余预算内未能及时返回。
	ReasonDownstreamTimeout Reason = "downstream_timeout"
	// ReasonBreakerOpen 熔断处于打开/半开名额占满，请求被快速失败。
	ReasonBreakerOpen Reason = "breaker_open"
	// ReasonNonRetryable 请求不可安全重试，失败后不再尝试。
	ReasonNonRetryable Reason = "non_retryable"
	// ReasonAttemptsExhausted 达到最大尝试次数仍未成功。
	ReasonAttemptsExhausted Reason = "attempts_exhausted"
	// ReasonNoRoute 路由不存在或无可用目标。
	ReasonNoRoute Reason = "no_route"
)

// CallError 携带可判别的失败原因与底层错误，便于上层用 errors.As 精确判别。
type CallError struct {
	Reason Reason
	Err    error
}

func (e *CallError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("proxy call failed: reason=%s: %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("proxy call failed: reason=%s", e.Reason)
}

func (e *CallError) Unwrap() error { return e.Err }

// Is 支持按 Reason 判别：errors.Is(err, &CallError{Reason: ReasonX})。
func (e *CallError) Is(target error) bool {
	t, ok := target.(*CallError)
	if !ok {
		return false
	}
	return t.Reason == ReasonNone || t.Reason == e.Reason
}

// ErrRejected 是下游"明确拒绝"的哨兵错误，与 context 超时严格区分。
var ErrRejected = fmt.Errorf("downstream rejected request")
