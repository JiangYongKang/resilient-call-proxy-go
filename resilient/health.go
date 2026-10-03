package resilient

import "sort"

// HealthSnapshot 是某个目标在某一时刻的一致健康视图：
// 所有计数与熔断状态在同一临界区内读出，彼此不自相矛盾。
// 计数自代理启动（或目标首次被路由到）起累计，只增不减。
type HealthSnapshot struct {
	Target Target

	// 熔断状态机观测（与 Breaker 同源、同锁读出）。
	State          BreakerState // 当前熔断状态
	FailStreak     int          // 连续失败累计（CLOSED 下有效；成功即清零）
	Probing        bool         // 是否正在放探测（HALF_OPEN 且有在途探测）
	ProbesUsed     int          // 本半开窗口已放行的探测数
	ProbesLive     int          // 当前在途探测数
	ProbeAllowance int          // 半开探测名额上限

	// 归类计数（归因边界见 docs：只有确属目标责任的结果才计入失败）。
	Successes      uint64 // 下游实际接受
	Rejects        uint64 // 下游明确拒绝（确定结论，不重试）
	SlowFailures   uint64 // 慢故障/抖动，含目标自身超时
	BreakerRejects uint64 // 被熔断快速拒绝、根本未发往下游

	// 非目标责任的终止：发起方取消 / 共享预算耗尽截断了本次尝试。
	// 不计入失败、不推动熔断状态变化，单独可见。
	AbortedByCaller uint64
	AbortedByBudget uint64
}

// Snapshot 返回该目标熔断器的一致健康快照（锁内一次读出）。
func (b *Breaker) Snapshot() HealthSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.evaluateLocked()
	return HealthSnapshot{
		Target:          b.target,
		State:           b.state,
		FailStreak:      b.failStreak,
		Probing:         b.state == BreakerHalfOpen && b.probesLive > 0,
		ProbesUsed:      b.probesUsed,
		ProbesLive:      b.probesLive,
		ProbeAllowance:  b.cfg.ProbeAllowance,
		Successes:       b.successes,
		Rejects:         b.rejects,
		SlowFailures:    b.slowFails,
		BreakerRejects:  b.breakerRejects,
		AbortedByCaller: b.abortCaller,
		AbortedByBudget: b.abortBudget,
	}
}

// Health 返回单个目标的一致健康快照；未知目标返回零值快照（惰性建档）。
func (p *Proxy) Health(t Target) HealthSnapshot {
	return p.breakerOf(t).Snapshot()
}

// HealthAll 返回所有已知目标的健康快照，按目标名排序（输出确定）。
// 可与正常调用并发执行：每个目标的快照在各自锁内原子读出，
// 不在调用热路径上新增任何锁。
func (p *Proxy) HealthAll() []HealthSnapshot {
	p.mu.RLock()
	breakers := make([]*Breaker, 0, len(p.breakers))
	for _, br := range p.breakers {
		breakers = append(breakers, br)
	}
	p.mu.RUnlock()

	out := make([]HealthSnapshot, 0, len(breakers))
	for _, br := range breakers {
		out = append(out, br.Snapshot())
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Target.String() < out[j].Target.String()
	})
	return out
}
