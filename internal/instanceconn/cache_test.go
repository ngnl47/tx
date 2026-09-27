package instanceconn

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
)

type testConnection struct{ closed atomic.Bool }

func (c *testConnection) Close() error { c.closed.Store(true); return nil }

func TestCacheRotatesRestartedInstanceAndRejectsLateOldDial(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	old := discovery.Instance{ID: "old", Endpoint: "127.0.0.1:19000", Services: []string{"game"}, Status: discovery.StatusRunning}
	newInstance := old
	newInstance.ID = "new"
	if err := directory.Replace([]discovery.Instance{old}); err != nil {
		t.Fatal(err)
	}
	cache := New[*testConnection](directory)
	started := make(chan struct{})
	finish := make(chan struct{})
	late := make(chan error, 1)
	lateConnection := &testConnection{}
	go func() {
		_, err := cache.Get(context.Background(), old, func(context.Context, discovery.Instance) (*testConnection, error) {
			close(started)
			<-finish
			return lateConnection, nil
		})
		late <- err
	}()
	<-started
	if err := directory.Replace([]discovery.Instance{newInstance}); err != nil {
		t.Fatal(err)
	}
	current := &testConnection{}
	got, err := cache.Get(context.Background(), newInstance, func(context.Context, discovery.Instance) (*testConnection, error) { return current, nil })
	if err != nil || got != current {
		t.Fatalf("new instance connection = %v, %v", got, err)
	}
	close(finish)
	if err := <-late; !errors.Is(err, ErrStaleInstance) {
		t.Fatalf("late old dial = %v", err)
	}
	if !lateConnection.closed.Load() || current.closed.Load() {
		t.Fatal("late old connection replaced current connection")
	}
	reused, err := cache.Get(context.Background(), newInstance, func(context.Context, discovery.Instance) (*testConnection, error) {
		t.Fatal("unexpected redial")
		return nil, nil
	})
	if err != nil || reused != current {
		t.Fatalf("reuse = %v, %v", reused, err)
	}
	if err := cache.Close(); err != nil || !current.closed.Load() {
		t.Fatalf("close = %v", err)
	}
}

func TestCacheRetiresPreviousIdentity(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	first := discovery.Instance{ID: "first", Endpoint: "127.0.0.1:19000", Services: []string{"game"}, Status: discovery.StatusRunning}
	second := first
	second.ID = "second"
	if err := directory.Replace([]discovery.Instance{first}); err != nil {
		t.Fatal(err)
	}
	cache := New[*testConnection](directory)
	previous := &testConnection{}
	if _, err := cache.Get(context.Background(), first, func(context.Context, discovery.Instance) (*testConnection, error) { return previous, nil }); err != nil {
		t.Fatal(err)
	}
	if err := directory.Replace([]discovery.Instance{second}); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Get(context.Background(), second, func(context.Context, discovery.Instance) (*testConnection, error) { return &testConnection{}, nil }); err != nil {
		t.Fatal(err)
	}
	if !previous.closed.Load() {
		t.Fatal("previous identity connection remained open")
	}
}

func TestCacheCloseDuringDialClosesLateConnection(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	instance := discovery.Instance{ID: "game-a", Endpoint: "127.0.0.1:19000", Services: []string{"game"}, Status: discovery.StatusRunning}
	if err := directory.Replace([]discovery.Instance{instance}); err != nil {
		t.Fatal(err)
	}
	cache := New[*testConnection](directory)
	started, finish := make(chan struct{}), make(chan struct{})
	created := &testConnection{}
	result := make(chan error, 1)
	go func() {
		_, err := cache.Get(context.Background(), instance, func(context.Context, discovery.Instance) (*testConnection, error) {
			close(started)
			<-finish
			return created, nil
		})
		result <- err
	}()
	<-started
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	close(finish)
	if err := <-result; !errors.Is(err, ErrClosed) || !created.closed.Load() {
		t.Fatalf("late dial after close = %v, closed=%v", err, created.closed.Load())
	}
}
