package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/control"
)

func TestDrainCoordinatorWaitsForHost(t *testing.T) {
	host := &blockingDrainHost{started: make(chan struct{}), release: make(chan struct{})}
	coordinator := newDrainCoordinator(host, time.Second)
	if err := coordinator.BeginDrain(context.Background()); err != nil {
		t.Fatalf("BeginDrain returned error: %v", err)
	}
	select {
	case <-host.started:
	case <-time.After(time.Second):
		t.Fatal("host Drain was not started")
	}
	if got := coordinator.Status().State; got != control.StateDraining {
		t.Fatalf("state = %v, want draining", got)
	}
	waiting := make(chan error, 1)
	go func() { waiting <- coordinator.WaitDrained(context.Background()) }()
	select {
	case err := <-waiting:
		t.Fatalf("WaitDrained returned before host drain: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(host.release)
	if err := <-waiting; err != nil {
		t.Fatalf("WaitDrained returned error: %v", err)
	}
	if got := coordinator.Status().State; got != control.StateDrained {
		t.Fatalf("state = %v, want drained", got)
	}
}

func TestDrainCoordinatorFailsWhenServerDrainTimesOut(t *testing.T) {
	coordinator := newDrainCoordinator(contextDrainHost{}, 5*time.Millisecond)
	if err := coordinator.BeginDrain(context.Background()); err != nil {
		t.Fatalf("BeginDrain returned error: %v", err)
	}
	err := coordinator.WaitDrained(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitDrained error = %v, want deadline exceeded", err)
	}
	status := coordinator.Status()
	if status.State != control.StateFailed || status.Error == "" {
		t.Fatalf("status = %#v, want failed with diagnostic", status)
	}
}

type contextDrainHost struct{}

func (contextDrainHost) Drain(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

type blockingDrainHost struct {
	started chan struct{}
	release chan struct{}
}

func (host *blockingDrainHost) Drain(context.Context) error {
	if host.started != nil {
		close(host.started)
	}
	if host.release != nil {
		<-host.release
	}
	return nil
}
