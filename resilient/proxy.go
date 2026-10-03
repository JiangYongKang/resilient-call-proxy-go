package resilient

import (
	"sync"
	"time"
)

// Proxy 是本地服务调用代理层：路由 -> 熔断器 -> 可控下游，
// 在一个共享超时预算内统一管理熔断、重试与降级。
type Proxy struct {
	clock  Clock
	router *Router
	down   *Downstream
	tracer *Tracer

	mu       sync.RWMutex
	breakers map[Target]*Breaker

	totalTimeout    time.Duration
	perAttemptLimit time.Duration
	epsilon         time.Duration
	breakerCfg      BreakerConfig
	retry           RetryPolicy
	fallback        Fallback
}

// Config 代理配置。
type Config struct {
	TotalTimeout    time.Duration
	PerAttemptLimit time.Duration // 0 表示每次尝试使用剩余共享预算
	// ClockEpsilon 是时间测量的最小精度：任何会恰好触及预算终点的等待
	// （尝试时限、退避）都提前该时长结束，保证“预算到期”在同刻竞争中
	// 稳定地先于“等待自然醒来”被裁决，使归因确定、可复现。0 时使用 1ns。
	ClockEpsilon time.Duration
	Breaker      BreakerConfig
	Retry        RetryPolicy
	Fallback     Fallback
}

// New 构造代理；entries 给出路由表。
func New(clock Clock, entries []RouteEntry, cfg Config) *Proxy {
	if cfg.ClockEpsilon <= 0 {
		cfg.ClockEpsilon = time.Nanosecond
	}
	p := &Proxy{
		clock:           clock,
		router:          NewRouter(entries...),
		down:            NewDownstream(),
		tracer:          NewTracer(),
		breakers:        make(map[Target]*Breaker),
		totalTimeout:    cfg.TotalTimeout,
		perAttemptLimit: cfg.PerAttemptLimit,
		epsilon:         cfg.ClockEpsilon,
		breakerCfg:      cfg.Breaker,
		retry:           cfg.Retry,
		fallback:        cfg.Fallback,
	}
	for _, e := range entries {
		for _, t := range e.Targets {
			p.breakers[t] = NewBreaker(t, cfg.Breaker, clock)
		}
	}
	return p
}

// Downstream 暴露下游以便测试注入脚本。
func (p *Proxy) Downstream() *Downstream { return p.down }

// Do 执行一次代理调用。
//   - budgetTotal<=0 时使用配置的 TotalTimeout；
//   - callerDone 非空且关闭表示发起方主动取消；
//   - 返回响应、最终原因（成功/降级成功时为 ReasonUnknown）与完整轨迹。
func (p *Proxy) Do(req *Request, callerDone <-chan struct{}, budgetTotal time.Duration) (*Response, Reason, Trace) {
	total := budgetTotal
	if total <= 0 {
		total = p.totalTimeout
	}
	budget := NewBudget(p.clock, total, callerDone)
	return p.DoWithBudget(req, budget)
}

// DoWithBudget 使用调用方预先创建的共享预算执行调用。主要用于确定性测试：
// 预算定时器在启动调用协程前就已注册，假时钟推进不会漏掉它。
func (p *Proxy) DoWithBudget(req *Request, budget *Budget) (*Response, Reason, Trace) {
	defer budget.Dispose()

	tr := Trace{Route: req.Route, Idempotent: req.Idempotent}

	// 调用前预算已经终止（例如取消信号先到），直接给出可区分原因。
	if err := budget.Err(); err != nil {
		tr.FinalReason = err.Reason
		p.tracer.add(tr)
		return nil, err.Reason, tr
	}

	maxAttempts := p.retry.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastRes AttemptResult = -1
	allTargetsRejected := true

	// 为本次请求分配路由内基号（一次性），保证请求内尝试顺延确定、可追溯。
	base, routeOK := p.router.OpenRequest(req.Route)

	for round := 0; round < maxAttempts; round++ {
		// 每次尝试前检查共享预算：不得越过剩余预算发起新尝试。
		if err := budget.Err(); err != nil {
			tr.FinalReason = err.Reason
			p.tracer.add(tr)
			return nil, err.Reason, tr
		}
		remaining := budget.Remaining()
		// 剩余预算已不足以容纳一次有意义的调用（不足一个时钟精度）：
		// 视为预算耗尽，绝不发起新的下游尝试（不放大请求量）。
		if remaining <= p.epsilon {
			tr.FinalReason = ReasonBudgetExhausted
			p.tracer.add(tr)
			return nil, ReasonBudgetExhausted, tr
		}

		target, ok := p.router.PickBase(req.Route, base, round)
		if !ok || !routeOK {
			tr.FinalReason = ReasonAllAttemptsFailed
			tr.Cause = ReasonAllAttemptsFailed
			p.tracer.add(tr)
			return p.finishWithFallback(req, tr)
		}
		br := p.breakerOf(target)

		ar0 := AttemptRecord{
			Index:       round + 1,
			Target:      target,
			StateBefore: br.State(),
			Remaining:   remaining,
			Limit:       p.attemptLimit(remaining),
		}
		// 先放入轨迹并持有引用：本尝试后续的判定依据会持续写入该记录，
		// 保证最终轨迹完整反映“剩余预算/熔断状态/判定依据”。
		tr.Attempts = append(tr.Attempts, ar0)
		rec := &tr.Attempts[len(tr.Attempts)-1]

		permit, allowed := br.Allow()
		if !allowed {
			// OPEN/HALF_OPEN 名额满：快速失败，不占用下游资源、不放大流量。
			rec.Result = AttemptBreakerReject
			rec.StateAfter = br.State()
			rec.IsProbe = false
			rec.Decision = "breaker fast-fail (OPEN or HALF_OPEN probe allowance exhausted); try next target without downstream call"
			lastRes = AttemptBreakerReject
			continue
		}
		rec.IsProbe = permit.IsProbe()
		allTargetsRejected = false

		// 放行后、调用下游前再次确认预算（极端情况下恰在此时到期）。
		if err := budget.Err(); err != nil {
			// 非目标责任终止：名额中立归还（探测额度退回、不推动状态迁移），
			// 本次未发往下游，不算目标被调用过，也不计目标失败。
			permit.Abandon(err.Reason)
			rec.Result = AttemptAborted
			rec.StateAfter = br.State()
			rec.Decision = "budget terminated before downstream call; permit abandoned neutrally (not a target failure, no downstream call made)"
			tr.FinalReason = err.Reason
			p.tracer.add(tr)
			return nil, err.Reason, tr
		}

		res, _ := p.down.Call(target, budget, rec.Limit)

		// 归因边界：预算在调用期间终止（到期/取消）时，下游返回的 TIMEOUT
		// 不含目标健康信息——名额中立归还，不计目标失败、不推动熔断迁移，
		// 即使在半开探测窗口内也不因此重新 OPEN。
		if err := budget.Err(); err != nil {
			permit.Abandon(err.Reason)
			rec.Result = AttemptAborted
			rec.StateAfter = br.State()
			rec.Decision = "downstream call cut off by shared budget; classified as non-target termination (permit abandoned, breaker untouched): " + err.Reason.String()
			tr.FinalReason = err.Reason
			p.tracer.add(tr)
			return nil, err.Reason, tr
		}

		permit.Release(res) // 目标责任结果：严格归还，成功、失败、超时都不继续占用名额
		rec.Result = res
		rec.StateAfter = br.State()
		lastRes = res

		if res == AttemptOK {
			rec.Decision = "downstream accepted; return success, no further attempts"
			tr.FinalReason = ReasonUnknown
			p.tracer.add(tr)
			return &Response{Target: target.String(), OK: true, Body: "ok"}, ReasonUnknown, tr
		}
		// 失败归因与是否还能继续。
		// 1) 下游明确拒绝：确定结论，不重试，直接走失败/降级。
		if res == AttemptReject {
			rec.Decision = "downstream explicitly rejected; rejection is definitive, never retry"
			tr.Cause = ReasonDownstreamReject
			tr.FinalReason = ReasonDownstreamReject
			return p.finishWithFallback(req, tr)
		}

		// 2) 慢故障（抖动）：
		//    - 非幂等请求不得重试；
		//    - 探测请求不得再触发额外尝试（避免半开放大流量）；
		//    - 已达总尝试上限：停止。
		canRetry := p.retry.CanRetry(req.Idempotent, res, ReasonAllAttemptsFailed)
		switch {
		case permit.IsProbe():
			canRetry = false
			rec.Decision = "half-open probe failed; probe does not trigger additional attempts"
		case !req.Idempotent:
			rec.Decision = "slow failure on non-idempotent request; retry forbidden"
		default:
			rec.Decision = "slow failure (transient); safe to retry"
		}

		if round+1 >= maxAttempts {
			canRetry = false
			rec.Decision += "; max attempts reached, stop and go to failure/fallback"
		}
		if !canRetry {
			tr.Cause = ReasonAllAttemptsFailed
			tr.FinalReason = ReasonAllAttemptsFailed
			return p.finishWithFallback(req, tr)
		}

		// 3) 退避后重试；退避受共享预算约束。若退避会恰好睡到预算终点，
		// 提前一个时钟精度结束，使预算到期稳定先触发（归因确定可复现）。
		bo := p.retry.Backoff(round+1, budget.Remaining())
		if bo > 0 && bo >= budget.Remaining() && bo > p.epsilon {
			bo = budget.Remaining() - p.epsilon
		}
		rec.Decision += "; retry after backoff " + bo.String()
		if bo > 0 {
			if !p.clock.Sleep(budget, bo) {
				err := budget.Err()
				rec.Decision += "; budget terminated during backoff: " + err.Reason.String()
				tr.FinalReason = err.Reason
				p.tracer.add(tr)
				return nil, err.Reason, tr
			}
		}
	}

	// 全部尝试结束仍未成功（所有目标都被熔断快速拒绝）。
	if lastRes == AttemptBreakerReject || allTargetsRejected {
		tr.Cause = ReasonAllAttemptsFailed
		tr.FinalReason = ReasonAllAttemptsFailed
	}
	return p.finishWithFallback(req, tr)
}

// BreakerOf 返回某目标的熔断器（观测用）。
func (p *Proxy) BreakerOf(t Target) *Breaker { return p.breakerOf(t) }

// Tracer 返回轨迹收集器。
func (p *Proxy) Tracer() *Tracer { return p.tracer }

func (p *Proxy) breakerOf(t Target) *Breaker {
	p.mu.RLock()
	br, ok := p.breakers[t]
	p.mu.RUnlock()
	if ok {
		return br
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if br, ok = p.breakers[t]; ok {
		return br
	}
	br = NewBreaker(t, p.breakerCfg, p.clock)
	p.breakers[t] = br
	return br
}

func (p *Proxy) attemptLimit(remaining time.Duration) time.Duration {
	lim := remaining
	if p.perAttemptLimit > 0 && p.perAttemptLimit < lim {
		lim = p.perAttemptLimit
	}
	// 尝试时限不得恰好落在预算终点：提前一个时钟精度，保证预算到期
	// 稳定先于调用自然醒来被裁决。
	if lim > 0 && lim >= remaining && remaining > p.epsilon {
		lim = remaining - p.epsilon
	}
	return lim
}

// finishWithFallback 对下游侧失败执行降级；预算耗尽/取消不走这里。
// 返回 (resp, finalReason, tr) 所需的 resp/reason：
//   - 配置了降级且成功：降级响应 + ReasonUnknown（原始原因保留在 tr.Cause）；
//   - 配置了降级但失败：nil + ReasonFallbackFailed；
//   - 未配置降级：nil + 原始失败原因。
func (p *Proxy) finishWithFallback(req *Request, tr Trace) (*Response, Reason, Trace) {
	if p.fallback == nil {
		p.tracer.add(tr)
		return nil, tr.FinalReason, tr
	}
	tr.FromFallback = true
	resp, err := p.fallback(req)
	if err != nil {
		tr.FinalReason = ReasonFallbackFailed
		p.tracer.add(tr)
		return nil, ReasonFallbackFailed, tr
	}
	tr.FinalReason = ReasonUnknown // 降级成功；原始下游原因保留在 tr.Cause
	p.tracer.add(tr)
	return resp, ReasonUnknown, tr
}
