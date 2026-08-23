package lifecycle

import (
	"errors"
	"sync"
)

var ErrDraining = errors.New("lifecycle: ingress is draining")

// Ingress 控制单个服务的新请求准入，并追踪排水开始前已经接受的请求。
// 它刻意不感知存储、事务或业务处理器。
type Ingress struct {
	work *ChildWaitGroupContext

	mu       sync.Mutex
	draining bool
}

// NewIngress 创建一个加入父级关闭流程的服务本地请求闸门。
func NewIngress(parent *WaitGroupContext) *Ingress {
	return &Ingress{work: NewChildWaitGroupContext(parent)}
}

// Enter 在尚未开始排水时接受一个请求。请求处理完成后必须调用返回函数。
func (g *Ingress) Enter() (func(), error) {
	if g == nil || g.work == nil {
		return nil, ErrStopping
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.draining {
		return nil, ErrDraining
	}
	return g.work.Enter()
}

// BeginDrain 停止接受新请求。该操作具备幂等性。
func (g *Ingress) BeginDrain() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.draining = true
	g.mu.Unlock()
}

// Draining 判断当前入口是否已经停止接受新请求。
func (g *Ingress) Draining() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.draining
}

// Wait 等待 BeginDrain 前已经接受的请求完成。调用方必须先开始排水，避免等待期间并发加入新工作。
func (g *Ingress) Wait() {
	if g == nil || g.work == nil {
		return
	}
	g.work.Wait()
}
