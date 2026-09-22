package resilient_test

import (
	"testing"
	"time"

	"github.com/highcumontoa/resilient-call-proxy-go/resilient"
)

type callResult struct {
	resp   *resilient.Response
	reason resilient.Reason
	tr     resilient.Trace

	ok bool
}

// runPumped 在协程中执行代理调用，主协程以固定小步推进假时钟直到调用结束。
// 调用协程在 FakeClock 的条件变量上等待推进（零墙钟时间），因此整个过程
// 完全确定、可复现，不依赖真实时间、调度睡眠或外部服务。
//
// onStep 在每次推进后以“已流逝时间”回调，可在特定时刻注入取消等事件。
func runPumped(t *testing.T, clk *resilient.FakeClock, p *resilient.Proxy,
	req *resilient.Request, callerDone chan struct{}, total time.Duration,
	onStep func(elapsed time.Duration),
) callResult {
	t.Helper()
	budget := resilient.NewBudget(clk, total, callerDone)
	ch := make(chan callResult, 1)
	done := make(chan struct{})
	go func() {
		r, why, tr := p.DoWithBudget(req, budget)
		ch <- callResult{resp: r, reason: why, tr: tr, ok: true}
		close(done)
	}()

	// quiescent 等待直到：调用协程已产出结果（done 关闭），或已进入一个
	// Sleep 等待点（下游调用/退避）。条件变量等待，零墙钟时间、无忙转。
	quiescent := func() (callResult, bool) {
		if !clk.WaitForSleepers(1, done) {
			return <-ch, true
		}
		select {
		case res := <-ch:
			return res, true
		default:
			return callResult{}, false
		}
	}

	step := 100 * time.Microsecond
	deadline := total + 20*time.Millisecond
	steps := int(deadline / step)
	if res, done := quiescent(); done {
		return res
	}
	for i := 1; i <= steps; i++ {
		clk.Advance(step)
		if onStep != nil && (time.Duration(i)*step)%time.Millisecond == 0 {
			onStep(time.Duration(i) * step)
		}
		if res, done := quiescent(); done {
			return res
		}
	}
	select {
	case res := <-ch:
		return res
	case <-time.After(2 * time.Second):
		t.Fatal("pumped call did not finish in time (possible deadlock)")
		return callResult{}
	}
}
