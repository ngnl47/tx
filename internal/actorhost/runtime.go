// Package actorhost 提供私有的本地实体宿主，统一激活、邮箱和生命周期，不持有传输连接。
package actorhost

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
	// IdlePassivation 是完成上一次处理后的空闲卸载时间；零值禁用，不替代显式生命周期策略。
	IdlePassivation    time.Duration
	Receive            func(Context, any) (any, error)
	OnInitAfter        func(Context) error
	OnActivateBefore   func(Context) error
	OnActivateAfter    func(Context) error
	OnDeactivateBefore func(Context) error
	OnDeactivateAfter  func(Context) error
}

// BehaviorFactory 在每次激活时判断加载条件并创建独立行为；失败时返回错误。
// 工厂与初始化、激活事件不附加统一 deadline；具体 I/O 由其调用方设置期限。
type BehaviorFactory func(Context) (*Behavior, error)

// Context 绑定本次激活，过期句柄不能投递到重建实例。
type Context struct {
	context.Context
	Identity Identity
	entry    *activation
}

// Post 投递到本次激活的邮箱，不受当前回调 Context 结束影响。
// 不要在同一实体回调内等待它完成，否则会阻塞处理该消息的邮箱。
func (c Context) Post(message any) (LocalFuture, error) {
	future, err := c.entry.runtime.submit(c.entry, message)
	return LocalFuture{future}, err
}

// LocalFuture 只开放等待能力，完成结果由运行时持有；取消等待不撤销已投递消息。
// 仅在 Post 成功后使用。它与跨实体 RPC 的 Future 不是同一种结果协议。
type LocalFuture struct{ future *Future }

func (f LocalFuture) Await(ctx context.Context) (any, error) { return f.future.Await(ctx) }
func (f LocalFuture) Done() <-chan struct{}                  { return f.future.Done() }

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

// NewFuture 和 Complete 供库内传输适配完成同一结果协议；每个 Future 只能完成一次。
func NewFuture(onDone func(any, error)) *Future {
	return &Future{done: make(chan struct{}), onDone: onDone}
}
func (f *Future) Done() <-chan struct{} { return f.done }

func (f *Future) Await(ctx context.Context) (any, error) {
	select {
	case <-f.done:
		return f.result.value, f.result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *Future) Complete(value any, err error) {
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
	runtime      *Runtime
	identity     Identity
	queue        chan request
	closed       bool // 由 Runtime.mu 保护；关闭通道与入队使用同一锁。
	life         context.Context
	cancel       context.CancelFunc
	initializing bool
}

// Runtime 只管理本进程实体，不做发现或跨实例归属判定。
type Runtime struct {
	mu           sync.Mutex
	entries      map[Identity]*activation
	draining     bool
	placement    *Placement
	workers      sync.WaitGroup
	capacity     int
	timeout      time.Duration
	factory      BehaviorFactory
	limits       ActivationLimits
	initializing int
	failures     map[Identity]*RemoteError
}

// New 的工厂先做业务加载准入再构造对象；返回对象后由运行时负责清理。
// timeout 限制权威归属查询、Receive 和卸载回调的上下文，不限制业务初始化耗时。
func New(capacity int, timeout time.Duration, factory BehaviorFactory) *Runtime {
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
	return r.SubmitWithReply(id, message, nil)
}

// 响应回调在实体 worker 内完成，纳入已有排水屏障。
func (r *Runtime) SubmitWithReply(id Identity, message any, done func(any, error)) (*Future, error) {
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
	if err := r.checkCapacityLocked(); err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	e := &activation{runtime: r, identity: id, queue: make(chan request, r.capacity), life: life, cancel: cancel,
		initializing: true}
	r.entries[id] = e
	r.initializing++
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
	f := NewFuture(done)
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

// BeginDrain 同步关闭所有实体准入，不等待已接受工作与卸载回调，也不关闭传输。
// 与激活、入队共用同一锁；重复调用不会重新开放或重复关闭邮箱。
func (r *Runtime) BeginDrain() {
	r.mu.Lock()
	r.draining = true
	for _, e := range r.entries {
		r.closeLocked(e)
	}
	r.mu.Unlock()
}

// Drain 关闸并等待所有实体；超时不并发释放仍在执行的业务资源。
func (r *Runtime) Drain(ctx context.Context) error {
	r.BeginDrain()
	done := make(chan struct{})
	go func() { r.workers.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *activation) invoke(timeout time.Duration, fn func(Context) error) (err error) {
	if fn == nil {
		return nil
	}
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(e.life, timeout)
	} else {
		ctx, cancel = context.WithCancel(e.life)
	}
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
		if e.initializing {
			e.runtime.initializing--
		}
		if failure != nil {
			e.runtime.rememberFailureLocked(e.identity, failure)
		}
		e.runtime.mu.Unlock()
	}()
	err := e.invoke(0, func(ctx Context) error {
		var err error
		behavior, err = e.runtime.factory(ctx)
		return err
	})
	if err == nil {
		if behavior == nil || behavior.Receive == nil {
			err = errors.New("actortrial: factory returned no receiver")
		} else if behavior.IdlePassivation < 0 {
			err = errors.New("actortrial: negative idle passivation")
		} else {
			for _, hook := range []func(Context) error{behavior.OnInitAfter, behavior.OnActivateBefore, behavior.OnActivateAfter} {
				if err = e.invoke(0, hook); err != nil {
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
	e.runtime.initializing--
	e.runtime.mu.Unlock()
	var timer *time.Timer
	if behavior.IdlePassivation > 0 {
		timer = time.NewTimer(behavior.IdlePassivation)
		defer timer.Stop()
	}
	for {
		var req request
		var ok bool
		if timer == nil {
			req, ok = <-e.queue
		} else {
			select {
			case req, ok = <-e.queue:
			case <-timer.C:
				// 到期只关闭准入，已接受消息继续沿同一邮箱排空。
				e.runtime.close(e)
				timer = nil
				continue
			}
		}
		if !ok {
			return
		}
		var value any
		err := e.invoke(e.runtime.timeout, func(ctx Context) error { var err error; value, err = behavior.Receive(ctx, req.message); return err })
		if timer != nil {
			timer.Reset(behavior.IdlePassivation)
		}
		req.future.Complete(value, err)
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
		req.future.Complete(nil, err)
	}
}
func (e *activation) report(hook func(Context) error) {
	if err := e.invoke(e.runtime.timeout, hook); err != nil {
		slog.Error("actortrial lifecycle callback failed", "identity", e.identity, "err", err)
	}
}

// Timeout 返回 Receive 和卸载回调的上下文期限，不代表初始化的最长耗时。
func (r *Runtime) Timeout() time.Duration { return r.timeout }

// Draining 返回本地准入状态，不等待排水完成。
func (r *Runtime) Draining() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.draining }
