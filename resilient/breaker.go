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

	// 健康视图计数：与状态机同锁更新，Snapshot 读出即一致快照。
	successes      uint64 // 下游实际接受
	rejects        uint64 // 下游明确拒绝
	slowFails      uint64 // 慢故障/抖动（含目标自身超时）
	breakerRejects uint64 // 熔断快速拒绝（未发往下游）
	abortCaller    uint64 // 发起方取消截断（非目标责任）
	abortBudget    uint64 // 共享预算耗尽截断（非目标责任）
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

// Release 归还名额并按“目标责任”的调用结果驱动状态迁移与计数。
// 仅当下游给出了可归因于目标的裁决（成功/拒绝/慢故障/自身超时）时使用；
// 发起方取消或共享预算耗尽截断的尝试必须用 Abandon 归还。
func (p *Permit) Release(result AttemptResult) {
	if p == nil || p.used {
		return
	}
	p.used = true
	p.br.record(p.probe, result)
}

// Abandon 以“非目标责任”的原因归还名额：本次尝试被发起方取消或共享
// 预算耗尽截断，结果不含任何关于目标健康的信息。因此：
//   - 名额（含半开探测名额）如数归还，探测额度退回，可再放行新探测；
//   - 不计入目标失败、不改变连续失败计数、不驱动任何状态迁移——
//     即使在 HALF_OPEN 探测窗口内被取消，也不会因此重新 OPEN；
//   - 仅在健康视图中记入对应的非目标责任终止计数，单独可见。
//
// r 只取 ReasonCallerCanceled / ReasonBudgetExhausted 两种终止原因。
func (p *Permit) Abandon(r Reason) {
	if p == nil || p.used {
		return
	}
	p.used = true
	p.br.abandon(p.probe, r)
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

// record 根据一次已放行调用的“目标责任”结果更新状态与健康计数。
func (b *Breaker) record(probe bool, result AttemptResult) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ok := result == AttemptOK

	// 健康计数：无论状态机是否还会消费该结果（如迟到的探测归还），
	// 目标确实给出了这个裁决，都如实归类。
	switch result {
	case AttemptOK:
		b.successes++
	case AttemptReject:
		b.rejects++
	case AttemptSlowFail, AttemptTimeout:
		b.slowFails++
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

// abandon 归还一次“非目标责任”终止的名额：只退名额、只记终止计数，
// 不触碰 failStreak，不驱动任何状态迁移。
func (b *Breaker) abandon(probe bool, r Reason) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if probe {
		// 探测名额归还：本次探测未获得任何目标健康信息，
		// 占用额度退回，允许后续请求重新探测（熔断器不会因此卡死在半开）。
		if b.probesLive > 0 {
			b.probesLive--
		}
		if b.probesUsed > 0 {
			b.probesUsed--
		}
	}
	if r == ReasonCallerCanceled {
		b.abortCaller++
	} else {
		b.abortBudget++
	}
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
