package txn

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"github.com/ngnl47/tx/lifecycle"
	"runtime/debug"
	"time"

	txlog "github.com/ngnl47/tx/log"
)

const (
	defaultAsyncConcurrency = 64
	defaultAsyncPending     = 8192
	defaultAsyncTimeout     = 10 * time.Second
)

var ErrAsyncFull = errors.New("transaction: async admission limit reached")
var ErrSubmitInTransaction = errors.New("transaction: Submit cannot use a transaction context")

// Future 的完成包含事务、同步提交后续过程及清理；等待取消不取消执行。
// 不保存全局结果表，调用方释放 Future 后即可回收。
type Future struct {
	done chan struct{}
	err  error
}

func (f *Future) Done() <-chan struct{} { return f.done }

// Await 可以重复或并发等待；任务已完成时优先返回执行结果。
func (f *Future) Await(ctx context.Context) error {
	select {
	case <-f.done:
		return f.err
	default:
	}
	select {
	case <-f.done:
		return f.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PanicError 保存异步执行的 panic 诊断；同步 Call 继续向调用边界传播 panic。
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string       { return fmt.Sprintf("transaction: panic: %v", e.Value) }
func newPanicError(value any) *PanicError { return &PanicError{Value: value, Stack: debug.Stack()} }

// ConfigureAsync 在首次 Call/Submit 前配置实例额度；不修改其他 Runtime。
func (runtime *Runtime) ConfigureAsync(config AsyncConfig) error {
	var err error
	config, err = normalizeAsyncConfig(config)
	if err != nil {
		return err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.started {
		return errors.New("transaction: cannot configure async execution after start")
	}
	runtime.asyncConfig = config
	return nil
}

// Submit 接纳一个独立异步根事务；执行额度不足时进入有界 FIFO 队列，总接纳额度满时拒绝。
// 低层保留 ctx 的值，但不继承取消和期限；应用应通过 txruntime.Submit 绑定干净的服务能力。
func (runtime *Runtime) Submit(ctx context.Context, procedure Procedure) (*Future, error) {
	if runtime == nil {
		return nil, errors.New("txn runtime: runtime is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if tx, _ := FromContext(ctx); tx != nil {
		return nil, ErrSubmitInTransaction
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if isNil(procedure) {
		return nil, errors.New("txn runtime: procedure is nil")
	}
	work := runtime.workIngress()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.asyncConfig.MaxConcurrent == 0 {
		runtime.asyncConfig, _ = normalizeAsyncConfig(runtime.asyncConfig)
	}
	if runtime.asyncRunning+runtime.asyncQueue.Len() >= runtime.asyncConfig.MaxPending {
		return nil, ErrAsyncFull
	}
	if err := work.BeginWork(); err != nil {
		return nil, err
	}
	runtime.started = true
	taskCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runtime.asyncConfig.Timeout)
	task := &asyncTask{ctx: taskCtx, cancel: cancel, procedure: procedure, future: &Future{done: make(chan struct{})}, work: work}
	if runtime.asyncRunning < runtime.asyncConfig.MaxConcurrent {
		runtime.asyncRunning++
		go runtime.runAsync(task)
	} else {
		task.entry = runtime.asyncQueue.PushBack(task)
		task.stopExpiry = context.AfterFunc(taskCtx, func() { runtime.expireQueued(task) })
	}
	return task.future, nil
}

func normalizeAsyncConfig(config AsyncConfig) (AsyncConfig, error) {
	if config.MaxConcurrent < 0 || config.MaxPending < 0 || config.Timeout < 0 {
		return config, errors.New("transaction: async limits and timeout must not be negative")
	}
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = defaultAsyncConcurrency
	}
	if config.MaxPending == 0 {
		config.MaxPending = max(defaultAsyncPending, config.MaxConcurrent)
	}
	if config.Timeout == 0 {
		config.Timeout = defaultAsyncTimeout
	}
	if config.MaxPending < config.MaxConcurrent {
		return config, errors.New("transaction: async maxPending must be at least maxConcurrent")
	}
	return config, nil
}

// asyncTask 仅等待时持有队列节点；所有队列状态由 Runtime.mu 保护。
type asyncTask struct {
	ctx        context.Context
	cancel     context.CancelFunc
	procedure  Procedure
	future     *Future
	work       *lifecycle.Ingress
	entry      *list.Element
	stopExpiry func() bool
}

func (task *asyncTask) complete(err error) {
	task.cancel()
	task.future.err = err
	close(task.future.done)
	task.work.EndWork()
}

func logAsyncError(task *asyncTask, err error) {
	if err == nil {
		return
	}
	var panicErr *PanicError
	if errors.As(err, &panicErr) {
		txlog.ErrorContext(task.ctx, "asynchronous Procedure failed", "procedure", procedureName(task.procedure), "err", err, "stack", string(panicErr.Stack))
	} else {
		txlog.ErrorContext(task.ctx, "asynchronous Procedure failed", "procedure", procedureName(task.procedure), "err", err)
	}
}

func (runtime *Runtime) expireQueued(task *asyncTask) {
	runtime.mu.Lock()
	if task.entry == nil {
		runtime.mu.Unlock()
		return
	}
	runtime.asyncQueue.Remove(task.entry)
	task.entry = nil
	err := task.ctx.Err()
	task.complete(err)
	runtime.mu.Unlock()
	logAsyncError(task, err)
}

func (runtime *Runtime) runAsync(task *asyncTask) {
	var err error
	defer func() {
		if recovered := recover(); recovered != nil {
			err = newPanicError(recovered)
		}
		logAsyncError(task, err)
		runtime.mu.Lock()
		runtime.asyncRunning--
		task.complete(err)
		var next *asyncTask
		if entry := runtime.asyncQueue.Front(); entry != nil {
			next = entry.Value.(*asyncTask)
			runtime.asyncQueue.Remove(entry)
			next.entry = nil
			next.stopExpiry()
			runtime.asyncRunning++
		}
		runtime.mu.Unlock()
		if next != nil {
			go runtime.runAsync(next)
		}
	}()
	// 出队和期限可能同时发生；已过期的任务不得进入业务。
	if err = task.ctx.Err(); err != nil {
		return
	}
	err = runtime.callAsync(task.ctx, task.procedure)
}
