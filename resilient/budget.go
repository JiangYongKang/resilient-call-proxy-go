package resilient

import (
	"sync"
	"time"
)

// Budget 是在多次尝试与下游调用之间共享的总超时预算。
// 它同时承载两个终止信号：
//   - 预算到期（deadline）：ReasonBudgetExhausted
//   - 发起方主动取消（cancel/callerDone）：ReasonCallerCanceled
//
// 任一信号触发后 Done 立即关闭，Err 给出可区分原因；Dispose 释放底层
// 定时器，保证超时后不再占用资源。
type Budget struct {
	clock Clock

	deadline time.Time

	mu       sync.Mutex
	canceled bool
	expired  bool

	closeOnce sync.Once
	done      chan struct{}

	timerOff func()
	onDone   func() // 终止时（到期/取消）回调，用于唤醒时钟等待者
}

// NewBudget 创建总预算为 total 的 Budget。callerDone 非空且关闭时表示
// 发起方主动取消。
func NewBudget(clock Clock, total time.Duration, callerDone <-chan struct{}) *Budget {
	b := &Budget{
		clock:    clock,
		deadline: clock.Now().Add(total),
		done:     make(chan struct{}),
	}
	switch fc := clock.(type) {
	case *FakeClock:
		b.timerOff = fc.after(b.deadline, b.markExpired)
	default:
		t := time.AfterFunc(total, b.markExpired)
		b.timerOff = func() { t.Stop() }
	}
	if callerDone != nil {
		go func() {
			<-callerDone
			b.markCanceled()
		}()
	}
	return b
}

// Remaining 返回剩余预算；<=0 表示预算已耗尽（或已终止）。
func (b *Budget) Remaining() time.Duration {
	return b.RemainingAt(b.clock.Now())
}

// RemainingAt 以指定时刻计算剩余预算。
func (b *Budget) RemainingAt(at time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.canceled || b.expired {
		return 0
	}
	return b.deadline.Sub(at)
}

// Cancel 由发起方主动取消。
func (b *Budget) Cancel() { b.markCanceled() }

// Done 在预算到期或被取消时关闭。
func (b *Budget) Done() <-chan struct{} { return b.done }

// Err 返回预算终止原因；未终止返回 nil。取消与预算耗尽可区分。
func (b *Budget) Err() *CallError {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.canceled:
		return &CallError{Reason: ReasonCallerCanceled, Message: "caller canceled the request"}
	case b.expired:
		return &CallError{Reason: ReasonBudgetExhausted, Message: "shared timeout budget exhausted"}
	default:
		return nil
	}
}

func (b *Budget) markCanceled() {
	b.mu.Lock()
	if b.canceled || b.expired {
		b.mu.Unlock()
		return
	}
	b.canceled = true
	b.mu.Unlock()
	b.closeDone()
}

func (b *Budget) markExpired() {
	b.mu.Lock()
	if b.canceled || b.expired {
		b.mu.Unlock()
		return
	}
	b.expired = true
	b.mu.Unlock()
	b.closeDone()
}

func (b *Budget) closeDone() {
	b.closeOnce.Do(func() { close(b.done) })
}

// Dispose 释放定时器资源。终止后调用安全幂等；超时后不再保留任何定时器。
func (b *Budget) Dispose() {
	if b.timerOff != nil {
		b.timerOff()
	}
}
