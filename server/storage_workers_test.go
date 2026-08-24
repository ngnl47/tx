package server

import (
	"context"
	"testing"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/storage/fusion"
)

type emptyDelayQueue struct{}

func (emptyDelayQueue) Enqueue(context.Context, fusion.DelayedOperation) error { return nil }

func newEmptyDelayTaskDispatcher() *fusion.DelayTaskDispatcher {
	return fusion.NewDelayTaskDispatcher(emptyDelayQueue{})
}

func TestStartDelayTaskDispatcherUsesLifecycle(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	store := &storageRuntime{delayDispatcher: newEmptyDelayTaskDispatcher()}
	if err := store.startDelayTaskDispatcher(root); err != nil {
		t.Fatalf("startDelayTaskDispatcher returned error: %v", err)
	}
	root.Cancel()
	root.WaitDoneAndWaitGroupWait()
}

func TestStorageWorkersRejectMissingDependencies(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	tests := []struct {
		name string
		run  func() error
	}{
		{name: "nil dispatcher storage", run: func() error { return (*storageRuntime)(nil).startDelayTaskDispatcher(root) }},
		{name: "missing dispatcher", run: func() error { return (&storageRuntime{}).startDelayTaskDispatcher(root) }},
		{name: "missing dispatcher lifecycle", run: func() error {
			return (&storageRuntime{delayDispatcher: newEmptyDelayTaskDispatcher()}).startDelayTaskDispatcher(nil)
		}},
		{name: "nil scheduler storage", run: func() error { return (*storageRuntime)(nil).startDelayScheduler(root) }},
		{name: "missing scheduler", run: func() error { return (&storageRuntime{}).startDelayScheduler(root) }},
		{name: "missing scheduler lifecycle", run: func() error { return (&storageRuntime{}).startDelayScheduler(nil) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); err == nil {
				t.Fatal("worker start error = nil")
			}
		})
	}
	root.Cancel()
	root.WaitDoneAndWaitGroupWait()
}
