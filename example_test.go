package proxy_test

import (
	"context"
	"errors"
	"fmt"
	"io"

	proxy "github.com/highcumontoa/resilient-call-proxy-go"
)

func Example() {
	r := proxy.NewRouter(proxy.BreakerConfig{
		FailureThreshold:  3,
		Cooldown:          200 * 1_000_000, // 200ms
		HalfOpenMaxProbes: 1,
	})
	down := proxy.NewScriptedDownstream("a",
		proxy.ScriptStep{Action: proxy.ActionReject},
		proxy.ScriptStep{Action: proxy.ActionOK, Out: "ok"},
	)
	r.Register("svc", down)

	p := proxy.NewProxy(r, proxy.Config{
		MaxAttempts:  3,
		TotalTimeout: 500 * 1_000_000, // 500ms
		LogOutput:    io.Discard,
	})
	// 幂等请求：第 1 次被拒绝，第 2 次成功。
	resp, _, err := p.Call(context.Background(),
		&proxy.Request{Route: "svc", Idempotent: true})
	fmt.Println("call1:", err, resp.Payload, "downstreamCalls=", down.CallCount())

	// 非幂等请求：失败也不重试，错误原因可判别。
	_, _, err2 := p.Call(context.Background(),
		&proxy.Request{Route: "svc", Idempotent: false, LogOutput: io.Discard})
	var ce *proxy.CallError
	if errors.As(err2, &ce) {
		fmt.Println("call2 reason:", ce.Reason, "downstreamCalls=", down.CallCount())
	}

	// Output:
	// call1: <nil> ok downstreamCalls= 2
	// call2 reason: downstream_rejected downstreamCalls= 3
}
