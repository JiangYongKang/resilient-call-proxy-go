package proxy

import (
	"context"
	"sync"
	"time"
)

// BreakerState 熔断器状态。
type BreakerState string

const (
	StateClosed   BreakerState = "closed"
	StateOpen     BreakerState = "open"
	StateHalfOpen BreakerState = "half_open"
)

// BreakerConfig 熔断参数。
type BreakerConfig struct {
	FailureThreshold  int           // 连续失败达到该值进入打开
	Cooldown          time.Duration // 打开状态冷却时间
	HalfOpenMaxProbes int           // 半开期间允许并发通过的探测请求数
}

// Breaker 是单目标的熔断器。
//
// 状态迁移规则：
//   - closed：累计"连续失败"次数；一次成功立即清零（短暂抖动不会误熔断）；
//     连续失败达到 FailureThreshold 时迁移到 open。
//   - open：所有普通请求快速失败；距进入 open 超过 Cooldown 后，下一个请求触发迁移到 half_open。
//   - half_open：最多放行 HalfOpenMaxProbes 个在途探测请求，其余快速失败；
//     任一探测成功 -> closed（并清零失败计数）；任一探测失败 -> open 并重置冷却。
type Breaker struct {
	cfg BreakerConfig

	mu         sync.Mutex
	state      BreakerState
	consecFail int
	openUntil  time.Time
	probeTaken int // half_open 下已发放且尚未回报的探测名额

	listeners []func(from, to BreakerState)
}

func NewBreaker(cfg BreakerConfig) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.HalfOpenMaxProbes <= 0 {
		cfg.HalfOpenMaxProbes = 1
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = time.Second
	}
	return &Breaker{cfg: cfg, state: StateClosed}
}

// State 返回当前状态（open 冷却到期会即时迁移到 half_open）。
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	evs := b.maybeEnterHalfOpenLocked(time.Now())
	s := b.state
	b.mu.Unlock()
	dispatch(evs)
	return s
}

// OnTransition 注册状态迁移观测回调（closed->open、open->half_open、half_open->closed/open）。
func (b *Breaker) OnTransition(fn func(from, to BreakerState)) {
	b.mu.Lock()
	b.listeners = append(b.listeners, fn)
	b.mu.Unlock()
}

// transitionEvent 是一次待派发的状态迁移事件，监听器在持锁时快照、锁外同步派发。
type transitionEvent struct {
	from, to  BreakerState
	listeners []func(from, to BreakerState)
}

func (e transitionEvent) dispatch() {
	for _, fn := range e.listeners {
		fn(e.from, e.to)
	}
}

func dispatch(evs []transitionEvent) {
	for _, ev := range evs {
		ev.dispatch()
	}
}

// snapshotListenersLocked 调用时持锁。
func (b *Breaker) snapshotListenersLocked() []func(from, to BreakerState) {
	lns := make([]func(from, to BreakerState), len(b.listeners))
	copy(lns, b.listeners)
	return lns
}

// eventLocked 调用时持锁，构造一条携带监听器快照的迁移事件。
func (b *Breaker) eventLocked(from, to BreakerState) transitionEvent {
	return transitionEvent{from: from, to: to, listeners: b.snapshotListenersLocked()}
}

// maybeEnterHalfOpenLocked 调用时持锁；冷却到期则迁移并返回待派发事件。
func (b *Breaker) maybeEnterHalfOpenLocked(now time.Time) []transitionEvent {
	if b.state == StateOpen && !now.Before(b.openUntil) {
		b.state = StateHalfOpen
		b.probeTaken = 0
		b.consecFail = 0
		return []transitionEvent{b.eventLocked(StateOpen, StateHalfOpen)}
	}
	return nil
}

// ReportOutcome 是一次被放行请求的回报结果。
type ReportOutcome int

const (
	// OutcomeSuccess 下游成功：清零连续失败 / 半开探测成功 -> closed。
	OutcomeSuccess ReportOutcome = iota
	// OutcomeFailure 下游失败（拒绝/超时等可归因于目标的失败）。
	OutcomeFailure
	// OutcomeCanceled 发起方取消或预算耗尽：结论不可归因于目标，
	// 只释放探测名额，不触发状态迁移、不计入连续失败。
	OutcomeCanceled
)

// ErrBreakerOpen 熔断器快速失败哨兵。
type ErrBreakerOpen struct{}

func (e *ErrBreakerOpen) Error() string { return "breaker open: request fast-failed" }

// Allow 判断当前是否允许一个请求通过。
// 拒绝时返回非 nil error（*ErrBreakerOpen）；允许时返回 report，
// 必须在请求结束时且仅调用一次，回报成功/失败/取消。
func (b *Breaker) Allow(_ context.Context) (func(ReportOutcome), error) {
	b.mu.Lock()
	evs := b.maybeEnterHalfOpenLocked(time.Now())

	switch b.state {
	case StateClosed:
		b.mu.Unlock()
		dispatch(evs)
		return b.reportClosed(), nil
	case StateHalfOpen:
		if b.probeTaken >= b.cfg.HalfOpenMaxProbes {
			b.mu.Unlock()
			dispatch(evs)
			return nil, &ErrBreakerOpen{}
		}
		b.probeTaken++
		b.mu.Unlock()
		dispatch(evs)
		return b.reportProbe(), nil
	default: // open
		b.mu.Unlock()
		dispatch(evs)
		return nil, &ErrBreakerOpen{}
	}
}

// reportClosed 处理 closed 状态下普通请求的回报。
func (b *Breaker) reportClosed() func(ReportOutcome) {
	return func(o ReportOutcome) {
		b.mu.Lock()
		var evs []transitionEvent
		switch o {
		case OutcomeSuccess:
			// 成功立即清零连续失败：短暂抖动不会累积到阈值。
			b.consecFail = 0
		case OutcomeFailure:
			b.consecFail++
			if b.consecFail >= b.cfg.FailureThreshold {
				b.state = StateOpen
				b.openUntil = time.Now().Add(b.cfg.Cooldown)
				evs = append(evs, b.eventLocked(StateClosed, StateOpen))
			}
		case OutcomeCanceled:
			// 不可归因于目标：不改变连续失败计数。
		}
		b.mu.Unlock()
		dispatch(evs)
	}
}

// reportProbe 处理 half_open 探测请求的回报。
func (b *Breaker) reportProbe() func(ReportOutcome) {
	return func(o ReportOutcome) {
		b.mu.Lock()
		var evs []transitionEvent
		// 无论结论如何，先释放占用的探测名额，避免超时/取消后名额泄漏。
		if b.probeTaken > 0 {
			b.probeTaken--
		}
		if b.state == StateHalfOpen {
			switch o {
			case OutcomeSuccess:
				b.consecFail = 0
				b.state = StateClosed
				evs = append(evs, b.eventLocked(StateHalfOpen, StateClosed))
			case OutcomeFailure:
				b.state = StateOpen
				b.openUntil = time.Now().Add(b.cfg.Cooldown)
				evs = append(evs, b.eventLocked(StateHalfOpen, StateOpen))
			case OutcomeCanceled:
				// 探测结论不明：保持 half_open，已释放名额，后续可继续探测。
			}
		}
		b.mu.Unlock()
		dispatch(evs)
	}
}
