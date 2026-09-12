// Package actortrial 验证最小本地实体运行时，不是稳定公共 API。
package actortrial

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrActivationDenied = errors.New("actortrial: activation denied")
	ErrDeactivated      = errors.New("actortrial: activation closed")
	ErrMailboxFull      = errors.New("actortrial: mailbox full")
	ErrDraining         = errors.New("actortrial: runtime draining")
	ErrPanicked         = errors.New("actortrial: callback panicked")
)

// Identity 是业务实体身份；物理激活句柄由 Context 隔离。
type Identity struct{ Kind, Key string }

// Behavior 每次激活独立创建；缺省事件不需要空实现。
type Behavior struct {
	Receive            func(Context, any) (any, error)
	OnInitAfter        func(Context) error
	OnActivateBefore   func(Context) error
	OnActivateAfter    func(Context) error
	OnDeactivateBefore func(Context) error
	OnDeactivateAfter  func(Context) error
}

// Context 绑定本次激活，过期句柄不能投递到重建实例。
type Context struct {
	context.Context
	Identity Identity
	entry    *activation
}

func (c Context) Post(message any) (*Future, error) { return c.entry.runtime.submit(c.entry, message) }

// Deactivate 由串行业务处理或定时消息触发；关闭后仍处理已经入队的消息。
func (c Context) Deactivate() { c.entry.runtime.close(c.entry) }

// After 只投递，不在后台修改业务状态；激活结束取消等待。
func (c Context) After(delay time.Duration, message any) {
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-c.entry.life.Done():
			return
		case <-timer.C:
			if _, err := c.Post(message); err != nil && !errors.Is(err, ErrDeactivated) {
				slog.Error("actortrial timer delivery failed", "identity", c.Identity, "err", err)
			}
		}
	}()
}

type outcome struct {
	value any
	err   error
}

// Future 的取消仅结束调用方等待，不撤销已经接受的处理。
type Future struct {
	done   chan struct{}
	result outcome
	onDone func(any, error)
}

func (f *Future) Await(ctx context.Context) (any, error) {
	select {
	case <-f.done:
		return f.result.value, f.result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *Future) finish(value any, err error) {
	if f.onDone != nil {
		f.onDone(value, err)
	}
	f.result = outcome{value, err}
	close(f.done)
}

type request struct {
	message any
	future  *Future
}
type activation struct {
	runtime       *Runtime
	identity      Identity
	queue         chan request
	closed        bool // 由 Runtime.mu 保护；关闭通道与入队使用同一锁。
	life          context.Context
	cancel        context.CancelFunc
	initializing  bool
	reservedBytes int64
}

// Runtime 只管理本进程实体，不做发现或跨实例归属判定。
type Runtime struct {
	mu        sync.Mutex
	entries   map[Identity]*activation
	draining  bool
	placement *Placement
	workers   sync.WaitGroup
	capacity  int
	timeout   time.Duration
	factory   func(Context) (*Behavior, error)
	limits    ActivationLimits
	usage     ActivationUsage
	failures  map[Identity]*RemoteError
}

// New 的工厂先做业务加载准入再构造对象；返回对象后由运行时负责清理。
func New(capacity int, timeout time.Duration, factory func(Context) (*Behavior, error)) *Runtime {
	if capacity < 1 || timeout <= 0 || factory == nil {
		panic("actortrial: invalid runtime configuration")
	}
	return &Runtime{entries: make(map[Identity]*activation), capacity: capacity, timeout: timeout, factory: factory}
}
func (r *Runtime) Call(ctx context.Context, id Identity, message any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := r.Submit(id, message)
	if err != nil {
		return nil, err
	}
	return f.Await(ctx)
}
func (r *Runtime) Submit(id Identity, message any) (*Future, error) {
	return r.submitRequest(id, message, nil)
}

// 响应回调在实体 worker 内完成，纳入已有排水屏障。
func (r *Runtime) submitRequest(id Identity, message any, done func(any, error)) (*Future, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, err := r.ensureLocked(id)
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, r.failures[id]
	}
	return r.enqueue(e, message, done)
}

func (r *Runtime) ensureLocked(id Identity) (*activation, error) {
	if id.Kind == "" || id.Key == "" {
		return nil, errors.New("actortrial: empty actor identity")
	}
	if r.draining {
		return nil, ErrDraining
	}
	if r.placement != nil {
		target, err := r.placement.Target(id)
		if err != nil || target.Instance != r.placement.local {
			return nil, ErrNotOwner
		}
	}
	if e := r.entries[id]; e != nil {
		return e, nil
	}
	if r.failures[id] != nil {
		return nil, nil
	}
	bytes, err := r.reserveLocked(id)
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	e := &activation{runtime: r, identity: id, queue: make(chan request, r.capacity), life: life, cancel: cancel,
		initializing: true, reservedBytes: bytes}
	r.entries[id] = e
	r.workers.Add(1)
	go e.run()
	return e, nil
}
func (r *Runtime) submit(e *activation, message any) (*Future, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.enqueue(e, message, nil)
}
func (r *Runtime) enqueue(e *activation, message any, done func(any, error)) (*Future, error) {
	if e.closed {
		return nil, ErrDeactivated
	}
	f := &Future{done: make(chan struct{}), onDone: done}
	select {
	case e.queue <- request{message, f}:
		return f, nil
	default:
		return nil, ErrMailboxFull
	}
}
func (r *Runtime) close(e *activation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closeLocked(e)
}
func (r *Runtime) closeLocked(e *activation) {
	if !e.closed {
		e.closed = true
		close(e.queue)
	}
}

// Drain 关闸并等待所有实体；超时不并发释放仍在执行的业务资源。
func (r *Runtime) Drain(ctx context.Context) error {
	r.mu.Lock()
	r.draining = true
	for _, e := range r.entries {
		r.closeLocked(e)
	}
	r.mu.Unlock()
	done := make(chan struct{})
	go func() { r.workers.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *activation) invoke(fn func(Context) error) (err error) {
	if fn == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(e.life, e.runtime.timeout)
	defer cancel()
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("%w: %v", ErrPanicked, value)
		}
	}()
	return fn(Context{ctx, e.identity, e})
}
func (e *activation) run() {
	defer e.runtime.workers.Done()
	var behavior *Behavior
	activated := false
	var failure error
	defer func() {
		e.runtime.close(e)
		if behavior != nil {
			if activated {
				e.report(behavior.OnDeactivateBefore)
			}
			e.report(behavior.OnDeactivateAfter)
		}
		e.cancel()
		e.runtime.mu.Lock()
		delete(e.runtime.entries, e.identity)
		e.runtime.usage.ReservedBytes -= e.reservedBytes
		if e.initializing {
			e.runtime.usage.Initializing--
		}
		if failure != nil {
			e.runtime.rememberFailureLocked(e.identity, failure)
		}
		e.runtime.mu.Unlock()
	}()
	err := e.invoke(func(ctx Context) error {
		var err error
		behavior, err = e.runtime.factory(ctx)
		return err
	})
	if err == nil {
		if behavior == nil || behavior.Receive == nil {
			err = errors.New("actortrial: factory returned no receiver")
		} else {
			for _, hook := range []func(Context) error{behavior.OnInitAfter, behavior.OnActivateBefore, behavior.OnActivateAfter} {
				if err = e.invoke(hook); err != nil {
					break
				}
			}
		}
	}
	if err != nil {
		failure = err
		e.failPending(err)
		return
	}
	activated = true
	e.runtime.mu.Lock()
	e.initializing = false
	e.runtime.usage.Initializing--
	e.runtime.mu.Unlock()
	for req := range e.queue {
		var value any
		err := e.invoke(func(ctx Context) error { var err error; value, err = behavior.Receive(ctx, req.message); return err })
		req.future.finish(value, err)
		if errors.Is(err, ErrPanicked) {
			failure = err
			activated = false // 异常状态只清理，不执行正常退出前业务通知。
			e.failPending(err)
			return
		}
	}
}
func (e *activation) failPending(err error) {
	e.runtime.close(e)
	for req := range e.queue {
		req.future.finish(nil, err)
	}
}
func (e *activation) report(hook func(Context) error) {
	if err := e.invoke(hook); err != nil {
		slog.Error("actortrial lifecycle callback failed", "identity", e.identity, "err", err)
	}
}
