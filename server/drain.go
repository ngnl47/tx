package server

import (
	"context"
	"errors"
	"sync"
	"time"

	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/modules/control"
)

var errDrainNotStarted = errors.New("server: drain has not started")

const defaultDrainTimeout = 5 * time.Minute

type hostDrainer interface {
	Drain(context.Context) error
}

// drainCoordinator 将项目服务入口和 discovery 适配到通用运维控制面。
// 它刻意不持有生命周期取消、最终存储刷盘或 Procedure 准入权限。
type drainCoordinator struct {
	host     hostDrainer
	mu       sync.Mutex
	status   control.Status
	drainErr error
	done     chan struct{}
	timeout  time.Duration
}

func newDrainCoordinator(host hostDrainer, timeout time.Duration) *drainCoordinator {
	return &drainCoordinator{
		host:    host,
		status:  control.Status{State: control.StateRunning},
		timeout: effectiveDrainTimeout(timeout),
	}
}

// BeginDrain 将实例标记为不可用于普通路由，再异步启动入口排水。
// 控制 RPC 可以在已接受工作完成前返回，调用方通过 WaitDrained 等待最终状态。
func (c *drainCoordinator) BeginDrain(context.Context) error {
	if c == nil || c.host == nil {
		return errors.New("server: drain host is not configured")
	}
	c.mu.Lock()
	if c.done != nil {
		if c.status.State == control.StateFailed {
			err := c.drainErr
			c.mu.Unlock()
			return err
		}
		c.mu.Unlock()
		return nil
	}
	c.status = control.Status{State: control.StateDraining}
	c.done = make(chan struct{})
	c.mu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		err := c.host.Drain(ctx)
		if err != nil {
			txlog.Error("server drain failed", "timeout", c.timeout, "err", err)
		}
		c.finish(err)
	}()
	return nil
}

// WaitDrained 等待此前启动的排水请求完成。
func (c *drainCoordinator) WaitDrained(ctx context.Context) error {
	if c == nil {
		return errDrainNotStarted
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	done := c.done
	c.mu.Unlock()
	if done == nil {
		return errDrainNotStarted
	}
	select {
	case <-done:
		c.mu.Lock()
		err := c.drainErr
		c.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *drainCoordinator) Status() control.Status {
	if c == nil {
		return control.Status{State: control.StateFailed, Error: "drain coordinator is not configured"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status
}

func (c *drainCoordinator) finish(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done == nil || c.status.State == control.StateDrained || c.status.State == control.StateFailed {
		return
	}
	c.drainErr = err
	if err != nil {
		c.status = control.Status{State: control.StateFailed, Error: err.Error()}
	} else {
		c.status = control.Status{State: control.StateDrained}
	}
	close(c.done)
}

func effectiveDrainTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return defaultDrainTimeout
	}
	return timeout
}
