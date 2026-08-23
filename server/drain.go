package server

import (
	"context"
	"errors"
	"sync"

	"github.com/ngnl47/tx/modules/control"
)

var errDrainNotStarted = errors.New("server: drain has not started")

type hostDrainer interface {
	Drain(context.Context) error
}

// drainCoordinator adapts project service ingress and discovery to the generic
// operational control plane. It intentionally does not own lifecycle cancel,
// final storage flush, or Procedure admission.
type drainCoordinator struct {
	host     hostDrainer
	mu       sync.Mutex
	status   control.Status
	drainErr error
	done     chan struct{}
}

func newDrainCoordinator(host hostDrainer) *drainCoordinator {
	return &drainCoordinator{
		host:   host,
		status: control.Status{State: control.StateRunning},
	}
}

// BeginDrain marks the instance unavailable to normal routing, then starts
// ingress drain asynchronously. The control RPC can return while accepted work
// finishes; callers use WaitDrained for the terminal state.
func (c *drainCoordinator) BeginDrain(ctx context.Context) error {
	if c == nil || c.host == nil {
		return errors.New("server: drain host is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
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
		c.finish(c.host.Drain(context.Background()))
	}()
	return nil
}

// WaitDrained waits for the drain request that was previously started.
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
