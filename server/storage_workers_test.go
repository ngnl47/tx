package server

import (
	"context"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
)

type emptyDelayQueue struct{}

func (emptyDelayQueue) Enqueue(context.Context, fusion.DelayedOperation) error { return nil }

func newEmptyDelayTaskDispatcher() *fusion.DelayTaskDispatcher {
	return fusion.NewDelayTaskDispatcher(emptyDelayQueue{})
}

type recordingDelayQueue struct {
	operations []fusion.DelayedOperation
}

func (queue *recordingDelayQueue) Enqueue(_ context.Context, operation fusion.DelayedOperation) error {
	queue.operations = append(queue.operations, operation)
	return nil
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

func TestFusionCModDrainPublishesPendingDelayTask(t *testing.T) {
	queue := &recordingDelayQueue{}
	dispatcher := fusion.NewDelayTaskDispatcher(queue)
	dispatcher.Submit(
		meta.TypeSpec{
			Type:    meta.Type{Ref: meta.TypeRef{FullName: "data.GDRole"}},
			Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Minute}},
		},
		storage.RecordKey{ID: int64(1001)},
	)
	app := &Server{
		config:  Config{Runtime: RuntimeConfig{Lifecycle: LifecycleConfig{DrainTimeout: time.Second}}},
		runtime: &runtime{storage: &storageRuntime{delayDispatcher: dispatcher}},
	}
	drainer, ok := app.fusionCMod().(CModDrainer)
	if !ok {
		t.Fatal("fusion CMod does not implement CModDrainer")
	}
	if err := drainer.Drain(context.Background()); err != nil {
		t.Fatalf("Drain returned error: %v", err)
	}
	if len(queue.operations) != 1 || queue.operations[0].Kind != fusion.DelayedSave {
		t.Fatalf("operations = %#v, want one delayed save", queue.operations)
	}
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
		{name: "nil dispatcher storage drain", run: func() error {
			return (*storageRuntime)(nil).drainDelayTaskDispatcher(context.Background())
		}},
		{name: "missing dispatcher drain", run: func() error {
			return (&storageRuntime{}).drainDelayTaskDispatcher(context.Background())
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
