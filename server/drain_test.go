package server

import (
	"context"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/control"
)

func TestDrainCoordinatorWaitsForHost(t *testing.T) {
	host := &blockingDrainHost{started: make(chan struct{}), release: make(chan struct{})}
	coordinator := newDrainCoordinator(host)
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
