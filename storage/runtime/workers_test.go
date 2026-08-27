package runtime

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

func (emptyDelayQueue) Enqueue(context.Context, fusion.DelayedSave) error { return nil }

func newEmptyDelayTaskDispatcher() *fusion.DelayTaskDispatcher {
	return fusion.NewDelayTaskDispatcher(emptyDelayQueue{})
}

type recordingDelayQueue struct {
	operations []fusion.DelayedSave
}

func (queue *recordingDelayQueue) Enqueue(_ context.Context, operation fusion.DelayedSave) error {
	queue.operations = append(queue.operations, operation)
	return nil
}

func TestStartDelayTaskDispatcherUsesLifecycle(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	store := &Runtime{delayDispatcher: newEmptyDelayTaskDispatcher()}
	if err := store.StartDelayTaskDispatcher(root); err != nil {
		t.Fatalf("StartDelayTaskDispatcher returned error: %v", err)
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
		{name: "nil dispatcher storage", run: func() error { return (*Runtime)(nil).StartDelayTaskDispatcher(root) }},
		{name: "missing dispatcher", run: func() error { return (&Runtime{}).StartDelayTaskDispatcher(root) }},
		{name: "missing dispatcher lifecycle", run: func() error {
			return (&Runtime{delayDispatcher: newEmptyDelayTaskDispatcher()}).StartDelayTaskDispatcher(nil)
		}},
		{name: "nil dispatcher storage drain", run: func() error {
			return (*Runtime)(nil).DrainDelayTaskDispatcher(context.Background())
		}},
		{name: "missing dispatcher drain", run: func() error {
			return (&Runtime{}).DrainDelayTaskDispatcher(context.Background())
		}},
		{name: "nil scheduler storage", run: func() error { return (*Runtime)(nil).StartDelayScheduler(root) }},
		{name: "missing scheduler", run: func() error { return (&Runtime{}).StartDelayScheduler(root) }},
		{name: "missing scheduler lifecycle", run: func() error { return (&Runtime{}).StartDelayScheduler(nil) }},
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

func TestDrainDelayTaskDispatcherPublishesPendingTask(t *testing.T) {
	queue := &recordingDelayQueue{}
	dispatcher := fusion.NewDelayTaskDispatcher(queue)
	dispatcher.Submit(
		meta.TypeSpec{
			Type:    meta.Type{Ref: meta.TypeRef{FullName: "data.GDRole"}},
			Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Minute}},
		},
		storage.RecordKey{ID: int64(1001)},
	)
	store := &Runtime{delayDispatcher: dispatcher}
	if err := store.DrainDelayTaskDispatcher(context.Background()); err != nil {
		t.Fatalf("DrainDelayTaskDispatcher returned error: %v", err)
	}
	if len(queue.operations) != 1 {
		t.Fatalf("operations = %#v, want one delayed save", queue.operations)
	}
}
