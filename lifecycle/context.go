// Package lifecycle provides cancellable contexts that keep accepted work alive
// until it has finished during graceful shutdown.
package lifecycle

import (
	"context"
	"errors"
	"sync"
)

var ErrStopping = errors.New("lifecycle: context is stopping")

// WaitGroupContext combines a cancellation signal with the work that was
// accepted before that signal. Call Go for long-running tasks instead of
// managing a sync.WaitGroup directly.
type WaitGroupContext struct {
	context.Context

	cancel context.CancelFunc

	mu       sync.Mutex
	stopping bool
	wg       sync.WaitGroup
}

// NewWaitGroupContext creates a lifecycle root. A nil parent uses Background.
func NewWaitGroupContext(parent context.Context) *WaitGroupContext {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &WaitGroupContext{Context: ctx, cancel: cancel}
}

// Enter registers work that is already executing on the caller goroutine. Its
// returned function must be called once after that work has completed.
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

// Go registers and starts work that belongs to this lifecycle. Once shutdown
// begins, no further work is accepted, so waiting cannot race with a later Add.
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

// Cancel starts graceful shutdown. Work accepted before this call remains
// tracked and Wait still waits for it to finish.
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

// Canceled reports whether this context or one of its parents has been canceled.
func (c *WaitGroupContext) Canceled() bool {
	return c == nil || c.Err() != nil
}

// WaitDone waits for cancellation without waiting for accepted work.
func (c *WaitGroupContext) WaitDone() {
	if c == nil {
		return
	}
	<-c.Done()
}

// Wait waits for every task accepted before cancellation.
func (c *WaitGroupContext) Wait() {
	if c == nil {
		return
	}
	c.wg.Wait()
}

// WaitDoneAndWaitGroupWait waits for shutdown and then for every accepted task.
func (c *WaitGroupContext) WaitDoneAndWaitGroupWait() {
	if c == nil {
		return
	}
	c.WaitDone()
	c.Wait()
}

// ChildWaitGroupContext is a child lifecycle. It is automatically joined to
// its parent, so root shutdown waits for work registered on child contexts.
type ChildWaitGroupContext struct {
	*WaitGroupContext
}

// NewChildWaitGroupContext creates a child lifecycle. The child observes the
// parent's cancellation and the parent waits until the child has drained.
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
