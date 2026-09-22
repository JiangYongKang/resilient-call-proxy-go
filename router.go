package proxy

import (
	"fmt"
	"sync"
)

// routeTarget 绑定一个下游实例与其专属熔断器。
type routeTarget struct {
	ds      Downstream
	breaker *Breaker
}

// Router 维护路由到候选目标的映射，目标顺序稳定且全程可追溯。
type Router struct {
	mu     sync.RWMutex
	routes map[string][]routeTarget
	bcfg   BreakerConfig
}

// NewRouter 创建路由器；bcfg 为每个目标熔断器的默认配置。
func NewRouter(bcfg BreakerConfig) *Router {
	return &Router{routes: map[string][]routeTarget{}, bcfg: bcfg}
}

// Register 为一条路由按顺序登记候选目标；每个目标获得独立熔断器。
func (r *Router) Register(route string, targets ...Downstream) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ds := range targets {
		r.routes[route] = append(r.routes[route], routeTarget{
			ds:      ds,
			breaker: NewBreaker(r.bcfg),
		})
	}
}

// ErrNoRoute 路由不存在或无可用目标。
type ErrNoRoute struct{ route string }

func (e *ErrNoRoute) Error() string { return fmt.Sprintf("no route for %q", e.route) }

// pick 按"尝试序号"确定性地选择目标：attempt 0 使用第一个候选，
// attempt 1 使用第二个候选……超出候选数量后回绕（round-robin）。
// 这样每次尝试打到哪个目标完全可追溯，且失败归因（含对应熔断器）前后一致。
func (r *Router) pick(route string, attempt int) (routeTarget, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	targets, ok := r.routes[route]
	if !ok || len(targets) == 0 {
		return routeTarget{}, &ErrNoRoute{route: route}
	}
	return targets[attempt%len(targets)], nil
}

// Breaker 返回指定目标名的熔断器（主要用于测试观测）。
func (r *Router) Breaker(route, targetName string) *Breaker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, t := range r.routes[route] {
		if t.ds.Name() == targetName {
			return t.breaker
		}
	}
	return nil
}
