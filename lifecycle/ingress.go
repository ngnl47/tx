package lifecycle

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrDraining        = errors.New("lifecycle: ingress is draining")
	ErrDrainNotStarted = errors.New("lifecycle: ingress drain has not started")
)

// Ingress 控制单个服务的新请求准入，并追踪排水开始前已经接受的请求。
// 它刻意不感知存储、事务或业务处理器。
type Ingress struct {
	work *WaitGroupContext

	mu       sync.Mutex
	draining bool
	drained  bool
	pending  int
	done     chan struct{}
}

// NewIngress 创建一个加入父级关闭流程的服务本地请求闸门。
func NewIngress(parent *WaitGroupContext) *Ingress {
	if parent == nil {
		parent = NewWaitGroupContext(context.Background())
	}
	return &Ingress{work: parent}
}

// Enter 在尚未开始排水时接受一个请求。请求处理完成后必须调用返回函数。
func (g *Ingress) Enter() (func(), error) {
	if err := g.BeginWork(); err != nil {
		return nil, err
	}

	var once sync.Once
	return func() { once.Do(g.EndWork) }, nil
}

// BeginWork 登记一项已进入入口队列的工作。它与 EndWork 成对使用，供高频
// 框架队列避免为每项工作创建完成闭包；普通 Receiver 应使用 Enter。
func (g *Ingress) BeginWork() error {
	if g == nil || g.work == nil {
		return ErrStopping
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.draining {
		return ErrDraining
	}
	if err := g.work.BeginWork(); err != nil {
		return err
	}
	g.pending++
	return nil
}

// EndWork 完成一项由 BeginWork 登记的工作。每次成功登记必须且只能调用一次。
func (g *Ingress) EndWork() {
	if g == nil || g.work == nil {
		return
	}
	g.work.EndWork()
	g.finish()
}

// BeginDrain 停止接受新请求。该操作具备幂等性。
func (g *Ingress) BeginDrain() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.draining {
		return
	}
	g.draining = true
	g.done = make(chan struct{})
	if g.pending == 0 {
		g.drained = true
		close(g.done)
	}
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
func (g *Ingress) Wait() error {
	return g.WaitContext(context.Background())
}

// WaitContext 等待排水前已经接受的工作完成，并允许调用方设置排水上限。
func (g *Ingress) WaitContext(ctx context.Context) error {
	if g == nil || g.work == nil {
		return ErrStopping
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g.mu.Lock()
	if !g.draining {
		g.mu.Unlock()
		return ErrDrainNotStarted
	}
	if g.drained {
		g.mu.Unlock()
		return nil
	}
	done := g.done
	g.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Pending 返回已经接受但尚未完成的工作数量。
func (g *Ingress) Pending() int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	pending := g.pending
	g.mu.Unlock()
	return pending
}

func (g *Ingress) finish() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pending > 0 {
		g.pending--
	}
	if g.draining && !g.drained && g.pending == 0 {
		g.drained = true
		close(g.done)
	}
}
