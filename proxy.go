package proxy

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

// Config 代理参数。
type Config struct {
	// MaxAttempts 最大尝试次数（含首次）。<=0 表示不重试（仅 1 次）。
	MaxAttempts int
	// TotalTimeout 每个请求的共享总超时预算；Request.Timeout 为正时优先使用请求级值。
	TotalTimeout time.Duration
	// PerAttemptSafety 单次尝试保护上限，实际取 min(剩余预算, 该值)；<=0 表示只用剩余预算。
	PerAttemptSafety time.Duration
	// LogOutput 每次尝试的判定日志输出；nil 时写 stderr。
	LogOutput io.Writer
}

// FallbackFunc 降级函数：在快速失败或最终失败时提供可解释的兜底响应。
type FallbackFunc func(req *Request, reason Reason) (*Response, error)

// Proxy 编排路由、共享预算、熔断与重试。它本身无状态、可并发使用。
type Proxy struct {
	router *Router
	cfg    Config

	mu        sync.RWMutex
	fallbacks map[string]FallbackFunc
}

func NewProxy(router *Router, cfg Config) *Proxy {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	if cfg.TotalTimeout <= 0 {
		cfg.TotalTimeout = time.Second
	}
	if cfg.LogOutput == nil {
		cfg.LogOutput = os.Stderr
	}
	return &Proxy{router: router, cfg: cfg, fallbacks: map[string]FallbackFunc{}}
}

// SetFallback 为某条路由注册降级路径。
func (p *Proxy) SetFallback(route string, fb FallbackFunc) {
	p.mu.Lock()
	p.fallbacks[route] = fb
	p.mu.Unlock()
}

func (p *Proxy) getFallback(route string) FallbackFunc {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.fallbacks[route]
}

// Call 执行一次经过代理的调用。
//
// 返回值：
//   - resp：成功或降级响应
//   - records：每次尝试的判定记录（含剩余预算、熔断状态、判定依据）
//   - err：无法成功且无降级时，返回 *CallError，可用 errors.Is 按 Reason 判别
func (p *Proxy) Call(ctx context.Context, req *Request) (*Response, []AttemptRecord, error) {
	logOut := p.cfg.LogOutput
	if req.LogOutput != nil {
		logOut = req.LogOutput
	}
	tracer := NewTracerTo(logOut)

	total := p.cfg.TotalTimeout
	if req.Timeout > 0 {
		total = req.Timeout
	}
	budget, cancel := NewBudget(ctx, total)
	defer cancel()

	finish := func(resp *Response, reason Reason, cause error, ok bool) (*Response, []AttemptRecord, error) {
		tracer.Summary(req, reason, ok)
		if ok {
			return resp, tracer.Records(), nil
		}
		if fb := p.getFallback(req.Route); fb != nil {
			fbResp, fbErr := fb(req, reason)
			if fbErr == nil {
				tracer.Add(AttemptRecord{
					Index: 0, RemainingBudget: budget.Remaining(),
					Decision: DecisionFallback, Reason: reason,
				})
				return fbResp, tracer.Records(), nil
			}
			return nil, tracer.Records(), &CallError{Reason: reason, Err: cause}
		}
		return nil, tracer.Records(), &CallError{Reason: reason, Err: cause}
	}

	var lastReason Reason = ReasonAttemptsExhausted
	var lastCause error

	for attempt := 0; attempt < p.cfg.MaxAttempts; attempt++ {
		idx := attempt + 1
		remaining := budget.Remaining()

		// 1) 尝试前先检查共享预算：预算已尽或已取消，绝不再发起调用、不占用任何名额。
		if r := budget.Reason(); r != ReasonNone {
			tracer.Add(AttemptRecord{
				Index: idx, RemainingBudget: remaining,
				Decision: map[bool]Decision{true: DecisionCanceled, false: DecisionBudgetGone}[r == ReasonCanceled],
				Reason:   r,
			})
			lastReason, lastCause = r, budget.Context().Err()
			return finish(nil, r, lastCause, false)
		}

		// 2) 确定性选目标：attempt -> 候选目标，全程可追溯。
		target, err := p.router.pick(req.Route, attempt)
		if err != nil {
			tracer.Add(AttemptRecord{Index: idx, RemainingBudget: remaining,
				Decision: DecisionNoRoute, Reason: ReasonNoRoute, Err: err.Error()})
			return finish(nil, ReasonNoRoute, err, false)
		}

		state := target.breaker.State()

		// 3) 熔断判定：open 或 half_open 探测名额占满 -> 快速失败，不放大请求量。
		report, allowErr := target.breaker.Allow(budget.Context())
		if allowErr != nil {
			tracer.Add(AttemptRecord{
				Index: idx, Target: target.ds.Name(), RemainingBudget: remaining,
				BreakerState: target.breaker.State(), Decision: DecisionFastFail,
				Reason: ReasonBreakerOpen, Err: allowErr.Error(),
			})
			return finish(nil, ReasonBreakerOpen, allowErr, false)
		}

		// 4) 单次尝试时限 = min(剩余预算, PerAttemptSafety)；不设单独定时器时就是剩余预算本身。
		//    所有尝试共用 budget.Context()，因此任何尝试都不可能越过总截止时间。
		attemptCtx := budget.Context()
		cancelAttempt := func() {}
		if p.cfg.PerAttemptSafety > 0 && remaining > p.cfg.PerAttemptSafety {
			c2, c2cancel := context.WithTimeout(budget.Context(), p.cfg.PerAttemptSafety)
			attemptCtx, cancelAttempt = c2, c2cancel
		}

		tracer.Add(AttemptRecord{
			Index: idx, Target: target.ds.Name(),
			RemainingBudget: remaining, BreakerState: state, Decision: DecisionStart,
		})

		resp, callErr := target.ds.Call(attemptCtx, req)
		cancelAttempt()

		// 5) 结果归因与熔断回报。
		if callErr == nil {
			report(OutcomeSuccess)
			tracer.Add(AttemptRecord{
				Index: idx, Target: target.ds.Name(),
				RemainingBudget: budget.Remaining(), BreakerState: target.breaker.State(),
				Decision: DecisionSuccess,
			})
			return finish(resp, ReasonNone, nil, true)
		}

		reason, canceled := classifyError(callErr, budget, ctx)
		if canceled {
			// 发起方取消 / 预算耗尽：结论不可归因于下游，立即终止，不重试、不占用名额。
			report(OutcomeCanceled)
			decision := DecisionTimeout
			switch reason {
			case ReasonCanceled:
				decision = DecisionCanceled
			case ReasonBudgetExceeded:
				decision = DecisionBudgetGone
			}
			tracer.Add(AttemptRecord{
				Index: idx, Target: target.ds.Name(),
				RemainingBudget: budget.Remaining(), BreakerState: target.breaker.State(),
				Decision: decision,
				Reason:   reason, Err: callErr.Error(),
			})
			return finish(nil, reason, callErr, false)
		}

		report(OutcomeFailure)
		tracer.Add(AttemptRecord{
			Index: idx, Target: target.ds.Name(),
			RemainingBudget: budget.Remaining(), BreakerState: target.breaker.State(),
			Decision: map[bool]Decision{true: DecisionReject, false: DecisionTimeout}[reason == ReasonDownstreamRejected],
			Reason:   reason, Err: callErr.Error(),
		})
		lastReason, lastCause = reason, callErr

		// 6) 重试判定：只有幂等请求可重试；非幂等请求失败一次即终止，绝不放大。
		canRetry := req.Idempotent && attempt+1 < p.cfg.MaxAttempts
		if !canRetry {
			if !req.Idempotent {
				tracer.Add(AttemptRecord{
					Index: idx, Target: target.ds.Name(),
					RemainingBudget: budget.Remaining(), BreakerState: target.breaker.State(),
					Decision: DecisionNoRetry, Reason: reason,
				})
			} else {
				tracer.Add(AttemptRecord{
					Index: idx, Target: target.ds.Name(),
					RemainingBudget: budget.Remaining(), BreakerState: target.breaker.State(),
					Decision: DecisionGiveUp, Reason: ReasonAttemptsExhausted,
				})
			}
			return finish(nil, lastReason, lastCause, false)
		}

		tracer.Add(AttemptRecord{
			Index: idx, Target: target.ds.Name(),
			RemainingBudget: budget.Remaining(), BreakerState: target.breaker.State(),
			Decision: DecisionRetry, Reason: reason,
		})
		// 回到循环顶部：先再次检查剩余预算，再发起下一次尝试。
	}

	return finish(nil, lastReason, lastCause, false)
}

// classifyError 把下游返回的错误归一化：
//   - 下游明确拒绝（ErrRejected）-> ReasonDownstreamRejected（可归因、可重试）
//   - 下游慢导致预算/单次时限到期 -> ReasonDownstreamTimeout（可归因、可重试）
//   - 发起方主动取消 -> ReasonCanceled（不可归因、立即终止）
//   - 总预算耗尽 -> ReasonBudgetExceeded（不可归因于单次下游、立即终止）
//
// 返回的 canceled=true 表示本次失败属于"取消/预算耗尽"类，不应计入熔断失败。
func classifyError(err error, budget *Budget, caller context.Context) (reason Reason, canceled bool) {
	if errors.Is(err, ErrRejected) {
		return ReasonDownstreamRejected, false
	}
	// 发起方主动取消（parent 被外部 cancel）优先判定，避免被预算口径覆盖。
	if caller.Err() == context.Canceled {
		return ReasonCanceled, true
	}
	switch budget.Reason() {
	case ReasonCanceled:
		return ReasonCanceled, true
	case ReasonBudgetExceeded:
		// 共享总预算到期：整个请求终止，不能再发起任何尝试。
		return ReasonBudgetExceeded, true
	}
	// budget 尚有余量而调用返回 DeadlineExceeded，说明是单次尝试保护到期 -> 下游超时。
	if errors.Is(err, context.DeadlineExceeded) {
		return ReasonDownstreamTimeout, false
	}
	if errors.Is(err, context.Canceled) {
		// 非 parent 取消：通常是单次尝试 ctx 被取消，按下游超时归因。
		return ReasonDownstreamTimeout, false
	}
	// 其他下游错误一律视为明确拒绝类失败。
	return ReasonDownstreamRejected, false
}
