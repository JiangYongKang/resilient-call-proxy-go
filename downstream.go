package proxy

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Request 是经过代理的一次请求。
type Request struct {
	Route      string // 路由键
	Idempotent bool   // 是否幂等；只有幂等请求才允许重试
	Payload    string
	Timeout    time.Duration // 请求级总超时（>0 时覆盖代理默认 TotalTimeout）
	// LogOutput 为本次调用单独指定判定日志输出；nil 时使用代理默认输出。
	// 并发调用各自传入独立 writer 即可避免共享输出上的竞争。
	LogOutput io.Writer
}

// Response 是下游返回的结果。
type Response struct {
	Target  string
	Payload string
}

// Downstream 是进程内可控下游的抽象，用于替代真实服务。
type Downstream interface {
	Name() string
	Call(ctx context.Context, req *Request) (*Response, error)
}

// ScriptAction 描述脚本下游的单步行为。
type ScriptAction int

const (
	// ActionOK 立即成功返回。
	ActionOK ScriptAction = iota
	// ActionReject 立即明确拒绝（业务失败，区别于超时）。
	ActionReject
	// ActionDelay 延迟 Delay 后成功；若 ctx 先结束则按 ctx 返回，不继续占用资源。
	ActionDelay
	// ActionBlock 一直阻塞直到 ctx 结束，用于模拟慢调用吃掉剩余预算。
	ActionBlock
)

// ScriptStep 是一次调用的剧本。
type ScriptStep struct {
	Action ScriptAction
	Delay  time.Duration
	Out    string // 成功时返回的载荷
}

// ScriptedDownstream 是按脚本逐次可控的进程内下游：
// 第 n 次调用按 Steps[n%len] 执行，CallCount 可观测真实放大情况。
type ScriptedDownstream struct {
	name  string
	mu    sync.Mutex
	steps []ScriptStep
	calls atomic.Int64
	// inflight 用于观测当前在途调用数（超时后必须为 0）。
	inflight atomic.Int64
}

func NewScriptedDownstream(name string, steps ...ScriptStep) *ScriptedDownstream {
	return &ScriptedDownstream{name: name, steps: steps}
}

func (d *ScriptedDownstream) Name() string { return d.name }

// CallCount 返回该下游被实际调用的总次数。
func (d *ScriptedDownstream) CallCount() int64 { return d.calls.Load() }

// Inflight 返回当前仍在途的调用数。
func (d *ScriptedDownstream) Inflight() int64 { return d.inflight.Load() }

func (d *ScriptedDownstream) Call(ctx context.Context, req *Request) (*Response, error) {
	n := d.calls.Add(1) - 1
	d.inflight.Add(1)
	defer d.inflight.Add(-1)

	d.mu.Lock()
	step := ScriptStep{Action: ActionOK}
	if len(d.steps) > 0 {
		step = d.steps[int(n)%len(d.steps)]
	}
	d.mu.Unlock()

	switch step.Action {
	case ActionReject:
		return nil, fmt.Errorf("%w: target=%s call=%d", ErrRejected, d.name, n)
	case ActionBlock:
		<-ctx.Done()
		return nil, ctx.Err()
	case ActionDelay:
		t := time.NewTimer(step.Delay)
		defer t.Stop()
		select {
		case <-ctx.Done():
			// ctx 结束立即返回并释放名额，不继续占用资源。
			return nil, ctx.Err()
		case <-t.C:
		}
	}
	return &Response{Target: d.name, Payload: step.Out}, nil
}
