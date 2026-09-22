package resilient

import (
	"errors"
	"fmt"
)

// Reason 描述一次代理调用最终失败的可区分原因。
type Reason int

const (
	ReasonUnknown Reason = iota
	// ReasonBudgetExhausted 总超时预算耗尽（所有尝试共享同一预算）。
	ReasonBudgetExhausted
	// ReasonDownstreamReject 下游明确拒绝（快速、确定的失败）。
	ReasonDownstreamReject
	// ReasonCallerCanceled 发起方主动取消。
	ReasonCallerCanceled
	// ReasonAllAttemptsFailed 所有尝试失败（受熔断/非幂等等策略限制）。
	ReasonAllAttemptsFailed
	// ReasonFallbackFailed 降级路径本身失败。
	ReasonFallbackFailed
)

// CallError 携带可判别的失败原因与上下文。
type CallError struct {
	Reason  Reason
	Message string
}

func (e *CallError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("call error: reason=%s: %s", e.Reason, e.Message)
}

func (r Reason) String() string {
	switch r {
	case ReasonBudgetExhausted:
		return "budget_exhausted"
	case ReasonDownstreamReject:
		return "downstream_reject"
	case ReasonCallerCanceled:
		return "caller_canceled"
	case ReasonAllAttemptsFailed:
		return "all_attempts_failed"
	case ReasonFallbackFailed:
		return "fallback_failed"
	default:
		return "unknown"
	}
}

// AsCallError 尝试从错误链中提取 *CallError。
func AsCallError(err error) (*CallError, bool) {
	var ce *CallError
	if errors.As(err, &ce) {
		return ce, true
	}
	return nil, false
}
