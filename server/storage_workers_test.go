package server

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
	txredis "github.com/ngnl47/tx/storage/redis"
)

type emptyHandoffStore struct {
	readErr error
}

func (s emptyHandoffStore) ReadHandoffs(context.Context, uint64, int, int) ([]txredis.HandoffEntry, uint64, error) {
	return nil, 0, s.readErr
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

func TestStartHandoffWorkerReturnsRecoveryFailure(t *testing.T) {
	recoveryErr := errors.New("handoff recovery unavailable")
	root := lifecycle.NewWaitGroupContext(context.Background())
	store := &storageRuntime{
		handoffWorker: fusion.NewHandoffRecoverer(
			meta.NewRegistry(),
			emptyHandoffStore{readErr: recoveryErr},
			emptyDelayQueue{},
		),
	}
	if err := store.startHandoffWorker(root); !errors.Is(err, recoveryErr) {
		t.Fatalf("startHandoffWorker error = %v, want %v", err, recoveryErr)
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
		{name: "nil handoff storage", run: func() error { return (*storageRuntime)(nil).startHandoffWorker(root) }},
		{name: "missing handoff worker", run: func() error { return (&storageRuntime{}).startHandoffWorker(root) }},
		{name: "missing handoff lifecycle", run: func() error {
			return (&storageRuntime{handoffWorker: newEmptyHandoffRecoverer()}).startHandoffWorker(nil)
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
