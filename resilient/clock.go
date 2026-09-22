package resilient

import (
	"sort"
	"sync"
	"time"
)

// Clock 抽象时间与等待，使测试可以用确定性的假时钟稳定复现超时。
type Clock interface {
	Now() time.Time
	// Sleep 等待 d，返回 true 表示正常等满；返回 false 表示等待期间
	// 被预算（到期或取消）打断。d<=0 时立即返回 true。
	Sleep(ctx *Budget, d time.Duration) bool
}

// RealClock 使用真实时间（示例/手工验证用）。
type RealClock struct{}

func NewRealClock() *RealClock { return &RealClock{} }

func (c *RealClock) Now() time.Time { return time.Now() }

func (c *RealClock) Sleep(ctx *Budget, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// fakeTimer 是预算到期等需要在精确时刻主动触发的回调。
type fakeTimer struct {
	at       time.Time
	seq      int
	fire     func()
	disarmed bool
}

// FakeClock 是确定性假时钟：Now 只随 Advance 变化；Sleep 不使用真实定时器，
// 而是在条件变量上等待“假时钟跨过其 deadline”。
//
// 推进与睡眠者用“代际栅栏”严格配对，杜绝漏唤醒/抢跑：
//   - 每个睡眠者记录自己已处理到的代际 round；
//   - Advance 递增 round、广播，然后等待“当前所有相关睡眠者都已确认这一代”
//     （ack 计数达到本轮应确认人数）才返回；
//   - 睡眠者若在该代已到期或预算终止则离开，并计一次确认。
//
// 全程零墙钟时间，结果确定可复现。
type FakeClock struct {
	mu   sync.Mutex
	cond *sync.Cond
	now  time.Time

	timers  []*fakeTimer
	nextSeq int

	round   int64 // 当前推进代际
	waiters []*sleeper
}

type sleeper struct {
	deadline time.Time
	seen     int64 // 已确认到的代际
	left     bool
	canceled bool
}

func NewFakeClock(start time.Time) *FakeClock {
	c := &FakeClock{now: start}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// after 在 at 时刻执行 f，返回幂等注销函数。
func (c *FakeClock) after(at time.Time, f func()) func() {
	c.mu.Lock()
	t := &fakeTimer{at: at, seq: c.nextSeq, fire: f}
	c.nextSeq++
	c.timers = append(c.timers, t)
	sort.SliceStable(c.timers, func(i, j int) bool {
		if c.timers[i].at.Equal(c.timers[j].at) {
			return c.timers[i].seq < c.timers[j].seq
		}
		return c.timers[i].at.Before(c.timers[j].at)
	})
	c.mu.Unlock()
	c.cond.Broadcast()
	return sync.OnceFunc(func() {
		c.mu.Lock()
		t.disarmed = true
		c.mu.Unlock()
	})
}

// Sleep 等待假时钟跨过 d（true），期间预算终止返回 false。
func (c *FakeClock) Sleep(ctx *Budget, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	default:
	}
	c.mu.Lock()
	w := &sleeper{deadline: c.now.Add(d), seen: c.round}
	c.waiters = append(c.waiters, w)
	c.cond.Broadcast() // 通知测试屏障：已进入等待点

	// 预算终止（到期或取消）时唤醒本睡眠者，使其无需等待下一次 Advance。
	stopWatch := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			c.mu.Lock()
			if !w.left {
				w.canceled = true
				c.cond.Broadcast()
			}
			c.mu.Unlock()
		case <-stopWatch:
		}
	}()

	for {
		if w.canceled {
			w.left = true
			close(stopWatch)
			c.cond.Broadcast()
			c.mu.Unlock()
			return false
		}
		if !c.now.Before(w.deadline) {
			w.left = true
			close(stopWatch)
			c.cond.Broadcast() // 到期离开，计为对当前代的确认
			c.mu.Unlock()
			return true
		}
		if c.round > w.seen {
			w.seen = c.round // 确认已看到这一代（仍未到期，继续等待）
			c.cond.Broadcast()
		}
		c.cond.Wait()
	}
}

// advanceOneLocked 在已持锁状态处理“当前时刻之前”的定时器，并推进到 to。
// 返回锁外待触发的回调。
func (c *FakeClock) fireDueTimersLocked() []func() {
	var fires []func()
	for {
		idx := -1
		for i, t := range c.timers {
			if !t.disarmed && !t.at.After(c.now) {
				idx = i
				break
			}
		}
		if idx < 0 {
			return fires
		}
		t := c.timers[idx]
		c.timers = append(c.timers[:idx], c.timers[idx+1:]...)
		fires = append(fires, t.fire)
	}
}

// Advance 将时钟向前推进 d，触发到期定时器，并与睡眠者完成代际握手。
func (c *FakeClock) Advance(d time.Duration) {
	target := c.Now().Add(d)
	// 分步推进，保证恰好在某个睡眠者 deadline 处也能结算。
	for {
		c.mu.Lock()
		next := target
		// 找最早到期的睡眠者时刻（不早于当前 now，不晚于 target）。
		for _, w := range c.waiters {
			if w.left {
				continue
			}
			if w.deadline.After(c.now) && !w.deadline.After(target) && w.deadline.Before(next) {
				next = w.deadline
			}
		}
		c.now = next
		fires := c.fireDueTimersLocked()
		// 新一代：所有未离开的睡眠者都需要确认看到这一代（重检 deadline 或继续等）。
		c.round++
		pending := c.ackPendingLocked()
		c.cond.Broadcast()
		for pending > 0 {
			c.cond.Wait()
			pending = c.ackPendingLocked()
		}
		c.mu.Unlock()
		for _, f := range fires {
			f()
		}
		if !next.Before(target) {
			return
		}
	}
}

// ackPendingLocked 统计尚未确认当前代的在场睡眠者（持锁调用）。
// 注意：睡眠者在被唤醒后会先在锁内重新检查 deadline：到期则 left=true，
// 未到期则把 seen 更新为当前 round；两种情况都算确认。
func (c *FakeClock) ackPendingLocked() int {
	n := 0
	for _, w := range c.waiters {
		if !w.left && w.seen < c.round {
			n++
		}
	}
	return n
}

// WaitForSleepers 等待至少 n 个睡眠者进入等待；done 先关闭返回 false。
// 供测试在启动阶段与调用协程同步。
func (c *FakeClock) WaitForSleepers(n int, done <-chan struct{}) bool {
	reached := make(chan struct{})
	go func() {
		c.mu.Lock()
		for c.activeWaiters() < n {
			c.cond.Wait()
		}
		c.mu.Unlock()
		close(reached)
	}()
	select {
	case <-reached:
		return true
	case <-done:
		c.cond.Broadcast()
		return false
	}
}

func (c *FakeClock) activeWaiters() int {
	n := 0
	for _, w := range c.waiters {
		if !w.left {
			n++
		}
	}
	return n
}
