package resilient

import "sync"

// RouteEntry 描述一条路由到其候选目标列表（按声明顺序）。
type RouteEntry struct {
	Route   string
	Targets []Target
}

// Router 维护路由表，以“每请求一个基号、请求内按尝试序号顺延”的方式选目标。
// 基号在请求开始时一次性分配（跨并发请求公平轮询）；同一请求的第 round 次
// 尝试确定地落在 targets[(base+round)%n]，与其他并发请求无关、可追溯。
type Router struct {
	mu      sync.Mutex
	routes  map[string][]Target
	counter map[string]int
}

func NewRouter(entries ...RouteEntry) *Router {
	r := &Router{
		routes:  make(map[string][]Target),
		counter: make(map[string]int),
	}
	for _, e := range entries {
		targets := make([]Target, len(e.Targets))
		copy(targets, e.Targets)
		r.routes[e.Route] = targets
	}
	return r
}

// OpenRequest 为一次新请求分配路由内基号（并发安全的公平轮询）。
func (r *Router) OpenRequest(route string) (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.routes[route]; !ok {
		return 0, false
	}
	base := r.counter[route]
	r.counter[route] = base + 1
	return base, true
}

// PickBase 为“基号 base 的请求”的第 round 次尝试选择目标（确定性、可追溯）。
func (r *Router) PickBase(route string, base, round int) (Target, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	targets, ok := r.routes[route]
	if !ok || len(targets) == 0 {
		return Target{}, false
	}
	idx := base + round
	if round < 0 {
		idx = base
	}
	return targets[((idx%len(targets))+len(targets))%len(targets)], true
}

// Targets 返回路由的全部目标副本。
func (r *Router) Targets(route string) ([]Target, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	targets, ok := r.routes[route]
	if !ok {
		return nil, false
	}
	out := make([]Target, len(targets))
	copy(out, targets)
	return out, true
}
