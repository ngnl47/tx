// Package lifecycle 提供支持取消的上下文，并在优雅关闭期间等待已接受的工作执行完毕。
package lifecycle

import (
	"context"
	"errors"
	"sync"
)

var ErrStopping = errors.New("lifecycle: context is stopping")

// Tasks 是面向服务的生命周期契约。模块可以登记自己负责的后台任务，但无权取消或等待进程根上下文。
type Tasks interface {
	context.Context
	Go(func(context.Context)) error
}

// WaitGroupContext 将取消信号与信号发出前已经接受的工作组合起来。
// 长期运行的任务应调用 Go，而不是直接维护 sync.WaitGroup。
type WaitGroupContext struct {
	context.Context

	cancel context.CancelFunc

	mu       sync.Mutex
	stopping bool
	wg       sync.WaitGroup
}

// NewWaitGroupContext 创建生命周期根节点。parent 为 nil 时使用 context.Background。
func NewWaitGroupContext(parent context.Context) *WaitGroupContext {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &WaitGroupContext{Context: ctx, cancel: cancel}
}

// Enter 登记已经在调用方 goroutine 中执行的工作。工作完成后必须调用一次返回函数。
func (c *WaitGroupContext) Enter() (func(), error) {
	if c == nil {
		return nil, ErrStopping
	}
	c.mu.Lock()
	if c.stopping || c.Err() != nil {
		c.mu.Unlock()
		return nil, ErrStopping
	}
	c.wg.Add(1)
	c.mu.Unlock()

	var once sync.Once
	return func() { once.Do(c.wg.Done) }, nil
}

// Go 登记并启动属于当前生命周期的工作。关闭开始后不再接受新工作，避免等待过程与后续登记发生竞争。
func (c *WaitGroupContext) Go(task func(context.Context)) error {
	if task == nil {
		return ErrStopping
	}
	done, err := c.Enter()
	if err != nil {
		return err
	}

	go func() {
		defer done()
		task(c)
	}()
	return nil
}

// Cancel 开始优雅关闭。调用前已经接受的工作仍会被追踪，Wait 仍会等待它们完成。
func (c *WaitGroupContext) Cancel() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopping {
		return false
	}
	c.stopping = true
	c.cancel()
	return true
}

// Canceled 判断当前上下文或任一父上下文是否已经取消。
func (c *WaitGroupContext) Canceled() bool {
	return c == nil || c.Err() != nil
}

// WaitDone 等待取消信号，但不等待已经接受的工作。
func (c *WaitGroupContext) WaitDone() {
	if c == nil {
		return
	}
	<-c.Done()
}

// Wait 等待取消前接受的全部任务完成。
func (c *WaitGroupContext) Wait() {
	if c == nil {
		return
	}
	c.wg.Wait()
}

// WaitDoneAndWaitGroupWait 先等待关闭信号，再等待全部已接受任务完成。
func (c *WaitGroupContext) WaitDoneAndWaitGroupWait() {
	if c == nil {
		return
	}
	c.WaitDone()
	c.Wait()
}

// ChildWaitGroupContext 是子生命周期。它会自动加入父节点，因此根节点关闭时会等待子上下文登记的工作。
type ChildWaitGroupContext struct {
	*WaitGroupContext
}

// NewChildWaitGroupContext 创建子生命周期。子节点会感知父节点取消，父节点会等待子节点排空。
func NewChildWaitGroupContext(parent *WaitGroupContext) *ChildWaitGroupContext {
	if parent == nil {
		return &ChildWaitGroupContext{WaitGroupContext: NewWaitGroupContext(context.Background())}
	}
	child := &ChildWaitGroupContext{WaitGroupContext: NewWaitGroupContext(parent)}
	if err := parent.Go(func(context.Context) {
		child.WaitDoneAndWaitGroupWait()
	}); err != nil {
		child.Cancel()
	}
	return child
}
