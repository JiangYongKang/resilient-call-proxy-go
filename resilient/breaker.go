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
func (p *Permit) Release(result AttemptResult) {
	if p == nil || p.used {
		return
	}
	p.used = true
	p.br.record(p.probe, result)
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
		return nil, false
	default: // OPEN
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

// record 根据一次已放行调用的结果更新状态。
func (b *Breaker) record(probe bool, result AttemptResult) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ok := result == AttemptOK

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
