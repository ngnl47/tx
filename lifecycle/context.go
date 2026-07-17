package lifecycle

import (
	"context"
	"sync"
)

type CancelContext struct {
	context.Context

	cancel context.CancelFunc
	mu     sync.Mutex
	local  bool
}

func NewCancelContext(parent context.Context) *CancelContext {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &CancelContext{Context: ctx, cancel: cancel}
}

func (c *CancelContext) Cancel() bool {
	return c.ThisCancel()
}

func (c *CancelContext) ThisCancel() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.local {
		return false
	}
	c.local = true
	c.cancel()
	return true
}

func (c *CancelContext) Canceled() bool {
	select {
	case <-c.Done():
		return true
	default:
		return false
	}
}

func (c *CancelContext) WaitDone() {
	<-c.Done()
}

type WaitGroupContext struct {
	*CancelContext

	wg sync.WaitGroup
}

func NewWaitGroupContext(parent context.Context) *WaitGroupContext {
	return &WaitGroupContext{CancelContext: NewCancelContext(parent)}
}

func (c *WaitGroupContext) GetWaitGroup() *sync.WaitGroup {
	return &c.wg
}

func (c *WaitGroupContext) WaitGroupAdd(delta int) {
	c.wg.Add(delta)
}

func (c *WaitGroupContext) WaitGroupDone() {
	c.wg.Done()
}

func (c *WaitGroupContext) WaitGroupWait() {
	c.wg.Wait()
}

func (c *WaitGroupContext) WaitDoneAndWaitGroupWait() {
	c.WaitDone()
	c.WaitGroupWait()
}

type waitGroupParent interface {
	context.Context
	GetWaitGroup() *sync.WaitGroup
}

type ChildWaitGroupContext struct {
	*WaitGroupContext

	parentWG *sync.WaitGroup
}

func NewChildWaitGroupContext(parent waitGroupParent) *ChildWaitGroupContext {
	return NewChildWaitGroupContextWithParent(parent, parent.GetWaitGroup())
}

func NewChildWaitGroupContextWithParent(parent context.Context, parentWG *sync.WaitGroup) *ChildWaitGroupContext {
	return &ChildWaitGroupContext{
		WaitGroupContext: NewWaitGroupContext(parent),
		parentWG:         parentWG,
	}
}

func (c *ChildWaitGroupContext) ParentWaitGroupAdd(delta int) {
	c.parentWG.Add(delta)
}

func (c *ChildWaitGroupContext) ParentWaitGroupDone() {
	c.parentWG.Done()
}

func (c *ChildWaitGroupContext) ParentWaitGroupWait() {
	c.parentWG.Wait()
}
