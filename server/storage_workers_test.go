package server

import (
	"context"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
	txredis "github.com/ngnl47/tx/storage/redis"
	"github.com/ngnl47/tx/storage/task"
	taskmemory "github.com/ngnl47/tx/storage/task/memory"
)

type emptyHandoffStore struct{}

func (emptyHandoffStore) ReadHandoffs(context.Context, uint64, int, int) ([]txredis.HandoffEntry, uint64, error) {
	return nil, 0, nil
}

func (emptyHandoffStore) ReadRecordRevision(context.Context, meta.TypeSpec, storage.RecordKey) (string, error) {
	return "1", nil
}

func (emptyHandoffStore) ConfirmHandoff(context.Context, meta.TypeSpec, storage.RecordKey, string, txredis.HandoffReceipt) (bool, error) {
	return true, nil
}

type emptyDelayQueue struct{}

func (emptyDelayQueue) Enqueue(context.Context, fusion.DelayedOperation) error { return nil }

func newEmptyHandoffRecoverer() *fusion.HandoffRecoverer {
	return fusion.NewHandoffRecoverer(meta.NewRegistry(), emptyHandoffStore{}, emptyDelayQueue{})
}

func TestStartBackgroundWorkersWaitsForActiveTask(t *testing.T) {
	backend := taskmemory.NewBackend()
	now := time.Unix(100, 0)
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	root := lifecycle.NewWaitGroupContext(context.Background())
	store := &storageRuntime{handoffWorker: newEmptyHandoffRecoverer(), delayWorker: fusion.NewSchedulerWithOptions(backend, fusion.SchedulerOptions{
		BatchSize:    10,
		PollInterval: time.Millisecond,
		Now:          func() time.Time { return now },
		Handler: fusion.DelayTaskHandlerFunc(func(context.Context, task.DelayTask) error {
			close(started)
			<-release
			return nil
		}),
	})}
	if err := store.delayWorker.Schedule(root, task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}); err != nil {
		t.Fatalf("Schedule returned error: %v", err)
	}
	if err := store.startBackgroundWorkers(root); err != nil {
		t.Fatalf("StartBackgroundWorkers returned error: %v", err)
	}
	<-started
	go func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
		close(done)
	}()
	assertChannelOpen(t, done, "worker lifecycle returned before active task completed")
	close(release)
	assertChannelClosed(t, done, "worker lifecycle did not finish after active task completed")
}

func TestStartBackgroundWorkersRejectsMissingDependencies(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	if err := (*storageRuntime)(nil).startBackgroundWorkers(root); err == nil {
		t.Fatal("nil storage error = nil")
	}
	if err := (&storageRuntime{}).startBackgroundWorkers(root); err == nil {
		t.Fatal("missing worker error = nil")
	}
	if err := (&storageRuntime{delayWorker: fusion.NewScheduler(taskmemory.NewBackend())}).startBackgroundWorkers(root); err == nil {
		t.Fatal("missing handoff worker error = nil")
	}
	if err := (&storageRuntime{}).startBackgroundWorkers(nil); err == nil {
		t.Fatal("nil lifecycle error = nil")
	}
	root.Cancel()
	root.WaitDoneAndWaitGroupWait()
}

func TestStartBackgroundWorkersStartsOnlyOnce(t *testing.T) {
	backend := taskmemory.NewBackend()
	now := time.Unix(100, 0)
	started := make(chan string, 2)
	release := make(chan struct{})
	root := lifecycle.NewWaitGroupContext(context.Background())
	store := &storageRuntime{handoffWorker: newEmptyHandoffRecoverer(), delayWorker: fusion.NewSchedulerWithOptions(backend, fusion.SchedulerOptions{
		BatchSize:    1,
		PollInterval: time.Millisecond,
		Now:          func() time.Time { return now },
		Handler: fusion.DelayTaskHandlerFunc(func(_ context.Context, item task.DelayTask) error {
			started <- item.ID.(string)
			<-release
			return nil
		}),
	})}
	for _, id := range []string{"1", "2"} {
		if err := store.delayWorker.Schedule(root, task.DelayTask{Type: "role", ID: id, Action: "save", DueAt: now}); err != nil {
			t.Fatalf("Schedule(%s) returned error: %v", id, err)
		}
	}
	if err := store.startBackgroundWorkers(root); err != nil {
		t.Fatalf("first StartBackgroundWorkers returned error: %v", err)
	}
	if err := store.startBackgroundWorkers(root); err != nil {
		t.Fatalf("second StartBackgroundWorkers returned error: %v", err)
	}
	if id := <-started; id != "1" {
		t.Fatalf("first task ID = %q, want 1", id)
	}
	select {
	case id := <-started:
		t.Fatalf("duplicate worker started task %q concurrently", id)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case id := <-started:
		if id != "2" {
			t.Fatalf("second task ID = %q, want 2", id)
		}
	case <-time.After(time.Second):
		t.Fatal("single worker did not continue with second task")
	}
	root.Cancel()
	root.WaitDoneAndWaitGroupWait()
}

func assertChannelClosed(t *testing.T, channel <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}

func assertChannelOpen(t *testing.T, channel <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-channel:
		t.Fatal(message)
	case <-time.After(20 * time.Millisecond):
	}
}
