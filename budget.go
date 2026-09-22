package proxy

import (
	"context"
	"time"
)

// Budget 是在所有重试尝试与下游调用之间共享的总超时预算。
// 它基于发起方 context 派生出一个统一截止时间：所有尝试都复用该 context，
// 因而任何一次尝试都不可能越过剩余预算；预算到期后，在途的下游调用也会被立即取消，
// 不会继续占用名额。
type Budget struct {
	ctx    context.Context
	cancel context.CancelFunc
	parent context.Context
	total  time.Duration
	start  time.Time
}

// NewBudget 基于 parent 创建总时限为 total 的共享预算。
// 返回的 CancelFunc 必须由调用方 defer（通常在成功或彻底失败后释放定时器）。
// 若 parent 自身截止时间更早，实际截止时间取二者较早者。
func NewBudget(parent context.Context, total time.Duration) (*Budget, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, total)
	return &Budget{
		ctx:    ctx,
		cancel: cancel,
		parent: parent,
		total:  total,
		start:  time.Now(),
	}, cancel
}

// Context 返回与预算绑定的 context，所有下游调用必须使用它。
func (b *Budget) Context() context.Context { return b.ctx }

// Remaining 返回剩余预算；预算耗尽或已取消返回 <=0 的值。
// 截止时间取 context 的实际 deadline，因此父 context 更早到期时也能正确反映。
func (b *Budget) Remaining() time.Duration {
	if d, ok := b.ctx.Deadline(); ok {
		return time.Until(d)
	}
	return b.total - time.Since(b.start)
}

// Elapsed 返回预算自创建以来已经过去的时间。
func (b *Budget) Elapsed() time.Duration { return time.Since(b.start) }

// Total 返回预算总时限。
func (b *Budget) Total() time.Duration { return b.total }

// Expired 在剩余预算不足时返回 true（不留任何可用于下游调用的时间）。
func (b *Budget) Expired() bool {
	if err := b.ctx.Err(); err != nil {
		return true
	}
	return b.Remaining() <= 0
}

// Reason 判别预算终止原因：
//   - 发起方主动取消 parent 或显式 cancel -> ReasonCanceled
//   - 到达统一截止时间（总预算耗尽 / parent 自身 deadline）-> ReasonBudgetExceeded
//   - 尚未终止 -> ReasonNone
func (b *Budget) Reason() Reason {
	// 优先判断发起方是否主动取消：parent 已结束且其错误为 Canceled。
	if b.parent.Err() == context.Canceled {
		return ReasonCanceled
	}
	switch b.ctx.Err() {
	case context.DeadlineExceeded:
		// 可能是预算自身到期，也可能是 parent 的 deadline 到期，两者都归为预算耗尽。
		return ReasonBudgetExceeded
	case context.Canceled:
		// parent 未取消但派生 context 被取消，说明是通过返回的 CancelFunc 主动取消。
		return ReasonCanceled
	default:
		return ReasonNone
	}
}
