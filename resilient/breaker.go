package resilient

import (
	"sync"
	"time"
)

// BreakerState 熔断状态。
type BreakerState int

const (
	BreakerClosed BreakerState = iota
	BreakerOpen
	BreakerHalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case BreakerOpen:
		return "OPEN"
	case BreakerHalfOpen:
		return "HALF_OPEN"
	default:
		return "CLOSED"
	}
}

// BreakerConfig 熔断配置。
type BreakerConfig struct {
	// FailureThreshold 连续失败次数达到该值时 CLOSED -> OPEN。
	// 偶发抖动（次数不足）不会误熔断；一次成功即清零。
	FailureThreshold int
	// Cooldown OPEN 持续时长；冷却到期后惰性迁移到 HALF_OPEN。
	Cooldown time.Duration
	// ProbeAllowance HALF_OPEN 下同时/累计放行的探测请求数；
	// 其余普通请求在 OPEN/HALF_OPEN 期间一律快速拒绝。
	ProbeAllowance int
}

// Transition 记录一次可观测的状态迁移。
type Transition struct {
	Target Target
	From   BreakerState
	To     BreakerState
	At     time.Time
	Why    string
}

// Breaker 单目标熔断器。所有状态判断与迁移都在锁内完成，可并发安全使用。
type Breaker struct {
	target Target
	cfg    BreakerConfig
	clock  Clock

	mu sync.Mutex

	state       BreakerState
	failStreak  int
	openedAt    time.Time
	probesUsed  int
	probesLive  int
	transitions []Transition

	// 目标侧结论计数（仅在凭证携裁决归还时累计）。
	successes      int64
	rejects        int64
	slowFailures   int64
	dispatched     int64
	breakerRejects int64

	// 非目标责任终止计数（取消/预算耗尽）：不进失败、不推动熔断。
	callerCanceled  int64
	budgetExhausted int64
}

func NewBreaker(t Target, cfg BreakerConfig, clock Clock) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.ProbeAllowance <= 0 {
		cfg.ProbeAllowance = 1
	}
	return &Breaker{target: t, cfg: cfg, clock: clock}
}

// Permit 是一次放行凭证。Release 必须恰好调用一次，保证半开探测名额
// 不泄漏（超时后也不继续占用名额）。
type Permit struct {
	br    *Breaker
	probe bool
	used  bool
}

// IsProbe 报告本次放行是否为 HALF_OPEN 探测请求。
func (p *Permit) IsProbe() bool { return p != nil && p.probe }

// Release 归还名额并按调用结果驱动状态迁移。
// 仅用于“下游确实给出了目标侧裁决”的调用（成功/明确拒绝/慢故障）。
func (p *Permit) Release(result AttemptResult) {
	if p == nil || p.used {
		return
	}
	p.used = true
	p.br.record(p.probe, result)
}

// ReleaseAborted 用于“非目标责任终止”：发起方取消或共享预算耗尽。
//   - 绝不推动熔断状态：半开探测因此被取消时不会重新 OPEN；
//   - 探测名额按“归还”处理，使后续探测仍可被放行（名额不泄漏、不被消耗）；
//   - 仅做中性统计（与失败计数分离）。
//
// dispatched 表示下游调用是否真的已经发出：放行后、发起调用前就被终止
// 时传 false，这次不算目标被调用过，也不计入 dispatched。
// reason 只能是 ReasonCallerCanceled / ReasonBudgetExhausted。
func (p *Permit) ReleaseAborted(dispatched bool, reason Reason) {
	if p == nil || p.used {
		return
	}
	p.used = true
	p.br.recordAbort(p.probe, dispatched, reason)
}

// State 返回当前状态；惰性评估冷却到期（OPEN -> HALF_OPEN）。
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.evaluateLocked()
	return b.state
}

// Allow 申请名额。返回 (permit, true) 表示放行；(nil, false) 表示被
// 熔断快速拒绝（调用方应立即走降级/下一个目标，不占用下游资源）。
func (b *Breaker) Allow() (*Permit, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.evaluateLocked()
	switch b.state {
	case BreakerClosed:
		return &Permit{br: b}, true
	case BreakerHalfOpen:
		// 半开期间仅放行 ProbeAllowance 个探测请求；普通请求一律快速失败。
		if b.probesUsed < b.cfg.ProbeAllowance {
			b.probesUsed++
			b.probesLive++
			return &Permit{br: b, probe: true}, true
		}
		b.breakerRejects++
		return nil, false
	default: // OPEN
		b.breakerRejects++
		return nil, false
	}
}

// Transitions 返回迁移记录快照。
func (b *Breaker) Transitions() []Transition {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Transition, len(b.transitions))
	copy(out, b.transitions)
	return out
}

// evaluateLocked 惰性推进 OPEN -> HALF_OPEN。调用方持锁。
func (b *Breaker) evaluateLocked() {
	if b.state != BreakerOpen {
		return
	}
	if b.clock.Now().Sub(b.openedAt) >= b.cfg.Cooldown {
		b.transitionLocked(BreakerHalfOpen, "cooldown elapsed; probing with allowance")
		b.probesUsed = 0
		b.probesLive = 0
	}
}

// record 根据一次已放行、且下游确实给出了目标侧裁决的调用更新状态与计数。
// 取消 / 预算耗尽不走这里（见 recordAbort），因此这里的任何非成功结果都
// 是真正的目标侧失败，可以安全地推动熔断。
func (b *Breaker) record(probe bool, result AttemptResult) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ok := result == AttemptOK
	b.dispatched++
	switch result {
	case AttemptOK:
		b.successes++
	case AttemptReject:
		b.rejects++
	default:
		// AttemptSlowFail / AttemptTimeout（后者仅在预算仍存活时才会走到
		// 这里，即目标自己的尝试时限先到，语义等同慢故障/抖动）。
		b.slowFailures++
	}

	if probe {
		// 探测名额归还：无论成功失败，占用名额都要释放。
		if b.probesLive > 0 {
			b.probesLive--
		}
		if b.state != BreakerHalfOpen {
			// 状态已被其他探测改变，结果仅作观测，不重复迁移。
			return
		}
		if ok {
			b.failStreak = 0
			b.transitionLocked(BreakerClosed, "probe succeeded; circuit recovered")
		} else {
			b.transitionLocked(BreakerOpen, "probe failed; reopen circuit")
			b.openedAt = b.clock.Now()
			b.probesUsed = 0
			b.probesLive = 0
		}
		return
	}

	switch b.state {
	case BreakerClosed:
		if ok {
			b.failStreak = 0
			return
		}
		b.failStreak++
		if b.failStreak >= b.cfg.FailureThreshold {
			b.transitionLocked(BreakerOpen, "consecutive failures reached threshold")
			b.openedAt = b.clock.Now()
		}
	case BreakerHalfOpen:
		// 理论上非探测调用不会在半开被放行；防御性处理。
		if !ok {
			b.transitionLocked(BreakerOpen, "non-probe failure in half-open; reopen")
			b.openedAt = b.clock.Now()
		}
	}
}

// recordAbort 处理一次“非目标责任终止”的归还（取消/预算耗尽）。
// 与 record 的本质区别：
//   - 不累计任何目标侧失败计数、不推动熔断状态；
//   - 半开探测因此终止时不重新 OPEN，且探测名额完整归还
//     （probesUsed/probesLive 都回退），后续探测仍可被放行；
//   - 只在中性分类里单独计数。
func (b *Breaker) recordAbort(probe, dispatched bool, reason Reason) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if dispatched {
		b.dispatched++
	}
	switch reason {
	case ReasonCallerCanceled:
		b.callerCanceled++
	case ReasonBudgetExhausted:
		b.budgetExhausted++
	}

	if !probe {
		return
	}
	// 探测名额归还：在途中名额先释放。
	if b.probesLive > 0 {
		b.probesLive--
	}
	// 仅当仍停留在 HALF_OPEN（尚未被其他探测的成败改变状态）时，把本次
	// 探测名额退还为可用：取消/超时不能“消耗”探测名额，也不能重开熔断。
	if b.state == BreakerHalfOpen && b.probesUsed > 0 {
		b.probesUsed--
	}
}

// fillHealthLocked 在调用方已持有 b.mu 时填充该目标的健康字段。
// 会先惰性评估冷却到期，保证 State 与计数在同一把锁的同一临界区读出。
func (b *Breaker) fillHealthLocked(h *TargetHealth) {
	b.evaluateLocked()
	h.Target = b.target
	h.State = b.state
	h.FailStreak = b.failStreak
	h.Probing = b.state == BreakerHalfOpen
	h.ProbeAllowance = b.cfg.ProbeAllowance
	h.ProbesUsed = b.probesUsed
	h.ProbesLive = b.probesLive
	rem := b.cfg.ProbeAllowance - b.probesUsed
	if rem < 0 {
		rem = 0
	}
	h.ProbesRemaining = rem
	h.Successes = b.successes
	h.Rejects = b.rejects
	h.SlowFailures = b.slowFailures
	h.BreakerRejects = b.breakerRejects
	h.Dispatched = b.dispatched
	h.CallerCanceled = b.callerCanceled
	h.BudgetExhausted = b.budgetExhausted
}

func (b *Breaker) transitionLocked(to BreakerState, why string) {
	if b.state == to {
		return
	}
	from := b.state
	b.state = to
	b.transitions = append(b.transitions, Transition{
		Target: b.target,
		From:   from,
		To:     to,
		At:     b.clock.Now(),
		Why:    why,
	})
}
