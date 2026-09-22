package resilient

import (
	"strconv"
	"sync"
	"time"
)

// Target 描述一个可路由的下游目标。
type Target struct {
	Route string
	Index int
	Name  string
}

func (t Target) String() string {
	if t.Name != "" {
		return t.Name
	}
	return t.Route + "#" + strconv.Itoa(t.Index)
}

// AttemptResult 描述下游对一次调用的裁决。
type AttemptResult int

const (
	AttemptOK            AttemptResult = iota // 成功
	AttemptReject                             // 下游明确拒绝（快速、确定失败，不重试）
	AttemptSlowFail                           // 慢故障：延迟后失败（模拟抖动，可安全重试）
	AttemptTimeout                            // 占用直到时限/预算耗尽
	AttemptBreakerReject                      // 未进入下游：被熔断器快速拒绝（OPEN/HALF_OPEN 名额满）
)

// ScriptedBehavior 是脚本化的下游行为：按“该目标被调用的序号”返回预设
// 结果，未覆盖的序号使用 Default。
type ScriptedBehavior struct {
	Delay    time.Duration // SlowFail/Timeout 占用下游的时长
	Default  AttemptResult
	Sequence []AttemptResult

	mu          sync.Mutex
	callCount   int
	activeCount int
	maxActive   int
}

// Downstream 是进程内可控下游。
type Downstream struct {
	mu      sync.Mutex
	scripts map[Target]*ScriptedBehavior
}

func NewDownstream() *Downstream {
	return &Downstream{scripts: make(map[Target]*ScriptedBehavior)}
}

// SetScript 为目标配置脚本行为。
func (d *Downstream) SetScript(t Target, s *ScriptedBehavior) {
	d.mu.Lock()
	d.scripts[t] = s
	d.mu.Unlock()
}

func (d *Downstream) script(t Target) *ScriptedBehavior {
	d.mu.Lock()
	s := d.scripts[t]
	d.mu.Unlock()
	return s
}

// Call 对目标执行一次调用。
//   - attemptLimit 为本次尝试的时限，且任何情况下都不得越过共享预算；
//   - 预算到期/取消时立即返回 AttemptTimeout 并释放占用名额，不继续占资源；
//   - 返回调用期间该目标观测到的并发占用峰值。
func (d *Downstream) Call(t Target, b *Budget, attemptLimit time.Duration) (AttemptResult, int) {
	s := d.script(t)
	if s == nil {
		s = &ScriptedBehavior{Default: AttemptOK}
	}

	res := s.enter()
	defer s.leave()

	// 预算在进入时已经终止：直接截断，不占用额外时间。
	if b.Remaining() <= 0 {
		return AttemptTimeout, s.peak()
	}

	switch res {
	case AttemptReject:
		// 明确拒绝：快速返回，不消耗时间预算。
		return AttemptReject, s.peak()
	case AttemptOK:
		return AttemptOK, s.peak()
	default:
		// AttemptSlowFail / AttemptTimeout：占用 Delay，但同时受
		// “剩余共享预算”和“本次尝试时限”双重约束。
		wait := s.Delay
		if rem := b.Remaining(); wait > rem {
			wait = rem
		}
		if attemptLimit > 0 && wait > attemptLimit {
			wait = attemptLimit
		}
		if wait > 0 {
			if !b.clock.Sleep(b, wait) {
				// 被预算到期或取消打断：本次调用被预算截断。
				return AttemptTimeout, s.peak()
			}
		}
		if res == AttemptTimeout {
			return AttemptTimeout, s.peak()
		}
		return AttemptSlowFail, s.peak()
	}
}

// PeakActive 返回目标历史最大并发占用。
func (d *Downstream) PeakActive(t Target) int {
	if s := d.script(t); s != nil {
		return s.peak()
	}
	return 0
}

// Calls 返回目标被实际调用次数。
func (d *Downstream) Calls(t Target) int {
	if s := d.script(t); s != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.callCount
	}
	return 0
}

func (s *ScriptedBehavior) enter() AttemptResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callCount++
	s.activeCount++
	if s.activeCount > s.maxActive {
		s.maxActive = s.activeCount
	}
	return s.nextResultLocked(s.callCount - 1)
}

func (s *ScriptedBehavior) leave() {
	s.mu.Lock()
	s.activeCount--
	s.mu.Unlock()
}

func (s *ScriptedBehavior) peak() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxActive
}

// nextResult 必须在持锁状态调用。
func (s *ScriptedBehavior) nextResultLocked(idx int) AttemptResult {
	if idx < len(s.Sequence) {
		return s.Sequence[idx]
	}
	return s.Default
}
