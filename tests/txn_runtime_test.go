package tests

import (
	"context"
	"errors"
	"strings"
	"testing"

	cachemodule "github.com/ngnl47/tx/modules/cache"
	"github.com/ngnl47/tx/modules/txn"
)

type rejectingAdmission struct {
	err error
}

func (admission rejectingAdmission) RequireWritable() error {
	return admission.err
}

type recordingCommitSink struct {
	prepared bool
	commits  [][]txn.LockTarget
	aborts   int
}

func (sink *recordingCommitSink) Prepare(_ context.Context, targets []txn.LockTarget) (txn.CommitReservation, error) {
	sink.prepared = true
	return &recordingCommitReservation{
		sink:    sink,
		targets: append([]txn.LockTarget(nil), targets...),
	}, nil
}

type recordingCommitReservation struct {
	sink    *recordingCommitSink
	targets []txn.LockTarget
}

func (reservation *recordingCommitReservation) Commit(targets []txn.LockTarget) {
	reservation.sink.commits = append(
		reservation.sink.commits,
		append([]txn.LockTarget(nil), targets...),
	)
}

func (reservation *recordingCommitReservation) Abort() {
	reservation.sink.aborts++
}

type commitSinkProcedure struct {
	lock       txn.LockTarget
	store      *runtimeTestStore
	prepared   *bool
	processErr error
}

func (procedure commitSinkProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{procedure.lock}, nil
}

func (procedure commitSinkProcedure) Process(*txn.Tx) error {
	if procedure.prepared != nil && !*procedure.prepared {
		return errors.New("commit sink was not prepared before process")
	}
	procedure.store.Set()
	return procedure.processErr
}

func TestTxnRuntimeRejectsTypedNilProcedure(t *testing.T) {
	runtime := txn.NewRuntime()
	var procedure *runtimeTestProcedure
	err := runtime.Call(context.Background(), procedure)
	if err == nil || !strings.Contains(err.Error(), "procedure is nil") {
		t.Fatalf("Call error = %v, want nil procedure", err)
	}
}

func TestTxnRuntimeCommitsFinalDirtyTargetsToBoundSink(t *testing.T) {
	store := &runtimeTestStore{}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	sink := &recordingCommitSink{}
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitSink(sink); err != nil {
		t.Fatalf("BindCommitSink returned error: %v", err)
	}

	err := runtime.Call(context.Background(), commitSinkProcedure{
		lock:     lock,
		store:    store,
		prepared: &sink.prepared,
	})
	if err != nil {
		t.Fatalf("Call returned error: %v", err)
	}
	if len(sink.commits) != 1 || len(sink.commits[0]) != 1 || sink.commits[0][0].Key != lock.Key {
		t.Fatalf("committed targets = %#v, want role root", sink.commits)
	}
	if sink.aborts != 0 {
		t.Fatalf("Abort calls = %d, want 0", sink.aborts)
	}
}

func TestTxnRuntimeAbortsCommitReservationAfterRollback(t *testing.T) {
	store := &runtimeTestStore{}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	sink := &recordingCommitSink{}
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitSink(sink); err != nil {
		t.Fatalf("BindCommitSink returned error: %v", err)
	}

	err := runtime.Call(context.Background(), commitSinkProcedure{
		lock:       lock,
		store:      store,
		prepared:   &sink.prepared,
		processErr: errors.New("rollback"),
	})
	if err == nil {
		t.Fatal("Call returned nil error")
	}
	if len(sink.commits) != 0 || sink.aborts != 1 {
		t.Fatalf("sink state = commits:%#v aborts:%d, want one abort", sink.commits, sink.aborts)
	}
}

func TestTxnRuntimeRejectsInvalidOrDuplicateCommitSink(t *testing.T) {
	runtime := txn.NewRuntime()
	var typedNil *recordingCommitSink
	if err := runtime.BindCommitSink(typedNil); err == nil || !strings.Contains(err.Error(), "commit sink is nil") {
		t.Fatalf("typed nil BindCommitSink error = %v, want nil sink", err)
	}
	if err := runtime.BindCommitSink(&recordingCommitSink{}); err != nil {
		t.Fatalf("first BindCommitSink returned error: %v", err)
	}
	if err := runtime.BindCommitSink(&recordingCommitSink{}); err == nil || !strings.Contains(err.Error(), "already bound") {
		t.Fatalf("second BindCommitSink error = %v, want already bound", err)
	}
}

func TestTxnRuntimeRejectsProcedureBeforeMutationWhenAdmissionIsClosed(t *testing.T) {
	store := &runtimeTestStore{}
	writer := &recordingChangeStoreApplier{}
	admissionErr := errors.New("storage unavailable")
	manager := cachemodule.NewManager(cachemodule.Dependencies{
		Writer:         writer,
		WriteAdmission: rejectingAdmission{err: admissionErr},
	}, cachemodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitSink(manager); err != nil {
		t.Fatalf("BindCommitSink returned error: %v", err)
	}
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	called := false

	err := runtime.Call(context.Background(), observingRuntimeTestProcedure{
		runtimeTestProcedure: runtimeTestProcedure{lock: lock, store: store},
		called:               &called,
	})
	if !errors.Is(err, admissionErr) {
		t.Fatalf("Call error = %v, want admission error", err)
	}
	if called || len(store.changes) != 0 || manager.Pending() != 0 {
		t.Fatalf("closed admission executed procedure: called=%t changes=%#v pending=%d", called, store.changes, manager.Pending())
	}
}

func TestTxnRuntimeAllowsPureMemoryProcedureWhenAdmissionIsClosed(t *testing.T) {
	store := &runtimeMemoryStore{}
	manager := cachemodule.NewManager(cachemodule.Dependencies{
		WriteAdmission: rejectingAdmission{err: errors.New("storage unavailable")},
	}, cachemodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitSink(manager); err != nil {
		t.Fatalf("BindCommitSink returned error: %v", err)
	}
	lock := txn.NewLockTarget(txn.NewLockKey("memory", 1001), store)
	if err := runtime.Call(context.Background(), commitSinkProcedure{
		lock:  lock,
		store: &store.runtimeTestStore,
	}); err != nil {
		t.Fatalf("pure memory Call returned error: %v", err)
	}
	if manager.Pending() != 0 || !store.HasChanges() {
		t.Fatalf("pure memory state = pending:%d changes:%t", manager.Pending(), store.HasChanges())
	}
}

func TestCacheManagerTreatsTypedNilWriteAdmissionAsAbsent(t *testing.T) {
	store := &runtimeTestStore{}
	var typedNil *rejectingAdmission
	manager := cachemodule.NewManager(cachemodule.Dependencies{
		Writer:         &recordingChangeStoreApplier{},
		WriteAdmission: typedNil,
	}, cachemodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitSink(manager); err != nil {
		t.Fatalf("BindCommitSink returned error: %v", err)
	}
	err := runtime.Call(context.Background(), runtimeTestProcedure{
		lock:  txn.NewLockTarget(txn.NewLockKey("role", 1001), store),
		store: store,
	})
	if err != nil {
		t.Fatalf("Call with typed nil write admission returned error: %v", err)
	}
}
