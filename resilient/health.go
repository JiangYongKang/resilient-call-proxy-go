package resilient

import (
	"sort"
	"time"
)

// Attribution 是一次尝试的单一归因类别：返回给调用方的最终原因、
// 轨迹里记录的判定依据、健康视图里的分类统计都以它为准（三处对齐）。
type Attribution int

const (
	// AttrNone 尚未裁决（轨迹初始值）。
	AttrNone Attribution = iota
	// AttrSuccess 下游接受并成功。
	AttrSuccess
	// AttrDownstreamReject 下游明确拒绝（确定的目标侧失败）。
	AttrDownstreamReject
	// AttrSlowFailure 慢故障/抖动/目标侧超时（目标侧失败）。
	AttrSlowFailure
	// AttrBreakerReject 被熔断拦下，根本没发出去。
	AttrBreakerReject
	// AttrCallerCanceled 发起方取消：非目标责任终止。
	AttrCallerCanceled
	// AttrBudgetExhausted 共享预算耗尽：非目标责任终止。
	AttrBudgetExhausted
)

func (a Attribution) String() string {
	switch a {
	case AttrSuccess:
		return "success"
	case AttrDownstreamReject:
		return "downstream_reject"
	case AttrSlowFailure:
		return "slow_failure"
	case AttrBreakerReject:
		return "breaker_reject"
	case AttrCallerCanceled:
		return "caller_canceled"
	case AttrBudgetExhausted:
		return "budget_exhausted"
	default:
		return "none"
	}
}

// isTargetFailure 报告该归因是否属于“目标侧失败”（计入目标失败统计、
// 允许推动熔断）。取消 / 预算耗尽 / 熔断拦截都不是目标失败。
func (a Attribution) isTargetFailure() bool {
	return a == AttrDownstreamReject || a == AttrSlowFailure
}

// isTargetNeutral 报告该归因是否为“非目标责任终止”：不计失败、不推动
// 熔断，且在健康视图里单独统计。
func (a Attribution) isTargetNeutral() bool {
	return a == AttrCallerCanceled || a == AttrBudgetExhausted
}

// finalReason 把一次尝试的归因映射成返回给调用方的最终原因。
// 这是“调用方结论”的唯一出口，保证与轨迹、健康视图同源。
// 成功返回 ReasonUnknown；熔断拦截不单独终止请求（顺延/聚合处理）。
func (a Attribution) finalReason() Reason {
	switch a {
	case AttrSuccess:
		return ReasonUnknown
	case AttrDownstreamReject:
		return ReasonDownstreamReject
	case AttrCallerCanceled:
		return ReasonCallerCanceled
	case AttrBudgetExhausted:
		return ReasonBudgetExhausted
	default:
		return ReasonAllAttemptsFailed
	}
}

// TargetHealth 是单个目标在某一时刻的一致健康快照。
type TargetHealth struct {
	Target Target

	// 熔断视图。
	State      BreakerState
	FailStreak int  // 当前连续失败累计
	Probing    bool // 是否正处于半开放探测窗口

	// 探测名额。
	ProbeAllowance  int
	ProbesUsed      int // 本半开窗口已放行（尚未归还为可用）的探测数
	ProbesLive      int // 正在途的探测数
	ProbesRemaining int // 还能再放多少个探测

	// 目标侧结论计数。
	Successes      int64 // 成功次数
	Rejects        int64 // 被下游明确拒绝次数
	SlowFailures   int64 // 慢故障/抖动/目标侧超时次数
	BreakerRejects int64 // 被熔断拦下、未发出次数
	Dispatched     int64 // 实际真正发到下游的次数

	// 非目标责任终止计数（不进失败、不推动熔断）。
	CallerCanceled  int64 // 归到该目标尝试上的发起方取消次数
	BudgetExhausted int64 // 归到该目标尝试上的预算耗尽次数
}

// HealthView 是全部目标在同一时刻的一致健康快照。
type HealthView struct {
	SnapshotAt time.Time
	Targets    []TargetHealth

	// 无法归到任何具体目标的非目标责任终止（尝试开始前/退避中等）。
	OrphanCallerCanceled  int64
	OrphanBudgetExhausted int64
}

// HealthSnapshot 返回全部目标在“同一时刻”的一致健康快照。
//
// 一致性：按全局固定顺序（route/index/name 排序）依次短暂持有每个熔断器
// 的锁读取。调用路径上任何时刻最多只持有一个熔断器锁，因此快照按固定顺序
// 加锁不会与调用死锁；快照中每个目标的状态/计数都取自各自的同一临界区，
// 而所有目标合起来对应一个确定的线性化点，调用方读到的是一组自洽的数字。
//
// 该查询不在调用热路径上：它只做内存拷贝，不等待下游、不持有统一大锁，
// 因而不会拖慢正常调用。
func (p *Proxy) HealthSnapshot() HealthView {
	p.mu.RLock()
	bs := make([]*Breaker, 0, len(p.breakers))
	for _, b := range p.breakers {
		bs = append(bs, b)
	}
	orphanC, orphanB := p.orphanCanceled, p.orphanBudgetEnd
	p.mu.RUnlock()

	sort.Slice(bs, func(i, j int) bool {
		return targetLess(bs[i].target, bs[j].target)
	})

	view := HealthView{
		Targets:               make([]TargetHealth, 0, len(bs)),
		OrphanCallerCanceled:  orphanC,
		OrphanBudgetExhausted: orphanB,
	}
	for _, b := range bs {
		b.mu.Lock()
		var h TargetHealth
		b.fillHealthLocked(&h)
		b.mu.Unlock()
		view.Targets = append(view.Targets, h)
	}
	view.SnapshotAt = p.clock.Now()
	return view
}

// TargetHealthOf 返回单个目标的健康快照；目标不存在时返回零值与 false。
// 只读观测，不会为未知目标惰性创建熔断器。
func (p *Proxy) TargetHealthOf(t Target) (TargetHealth, bool) {
	p.mu.RLock()
	br, ok := p.breakers[t]
	p.mu.RUnlock()
	if !ok {
		return TargetHealth{}, false
	}
	br.mu.Lock()
	var h TargetHealth
	br.fillHealthLocked(&h)
	br.mu.Unlock()
	return h, true
}

func targetLess(a, b Target) bool {
	if a.Route != b.Route {
		return a.Route < b.Route
	}
	if a.Index != b.Index {
		return a.Index < b.Index
	}
	return a.Name < b.Name
}
