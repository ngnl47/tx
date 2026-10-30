package tests

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/txn"
	writebackmodule "github.com/ngnl47/tx/modules/writeback"
)

type rejectingAdmission struct {
	err error
}

func (admission rejectingAdmission) RequireWritable([]txn.StoreTarget) error {
	return admission.err
}

type recordingCommitParticipant struct {
	prepared bool
	commits  [][]txn.StoreTarget
	aborts   int
}

func (sink *recordingCommitParticipant) Prepare(_ context.Context, targets []txn.StoreTarget) (txn.CommitReservation, error) {
	sink.prepared = true
	return &recordingCommitReservation{
		sink:    sink,
		targets: append([]txn.StoreTarget(nil), targets...),
	}, nil
}

type recordingCommitReservation struct {
	sink    *recordingCommitParticipant
	targets []txn.StoreTarget
}

func (reservation *recordingCommitReservation) Commit(targets []txn.StoreTarget) {
	reservation.sink.commits = append(
		reservation.sink.commits,
		append([]txn.StoreTarget(nil), targets...),
	)
}

func (reservation *recordingCommitReservation) Abort() {
	reservation.sink.aborts++
}

type commitParticipantProcedure struct {
	lock       txn.StoreTarget
	store      *runtimeTestStore
	prepared   *bool
	processErr error
}

type chainedCommitProcedure struct {
	commitParticipantProcedure
	next txn.Procedure
}

func (p chainedCommitProcedure) Process(tx *txn.Tx) error {
	if err := p.commitParticipantProcedure.Process(tx); err != nil {
		return err
	}
	return tx.CallAfterCommit(p.next)
}

func TestTxnRuntimeContinuationKeepsWriteBackAndIndependentRollback(t *testing.T) {
	for _, failNext := range []bool{false, true} {
		parent, child := &runtimeTestStore{id: 10}, &runtimeTestStore{id: 11}
		key := txn.NewLockey("role", 1, 10)
		parentTarget, childTarget := txn.NewStoreTarget(key, parent), txn.NewStoreTarget(key, child)
		sink := &recordingCommitParticipant{}
		runtime := txn.NewRuntime()
		if err := runtime.BindCommitParticipant(sink); err != nil {
			t.Fatal(err)
		}
		next := commitParticipantProcedure{lock: childTarget, store: child}
		if failNext {
			next.processErr = errors.New("follow-up failed")
		}
		err := runtime.Call(t.Context(), chainedCommitProcedure{
			commitParticipantProcedure: commitParticipantProcedure{lock: parentTarget, store: parent},
			next:                       next,
		})
		if !parent.HasChanges() {
			t.Fatal("原事务已提交的修改丢失")
		}
		if failNext {
			var committed *txn.AfterCommitError
			if !errors.As(err, &committed) || child.HasChanges() || sink.aborts != 1 || len(sink.commits) != 1 {
				t.Fatalf("提交后失败：err=%v childDirty=%v aborts=%d commits=%d", err, child.HasChanges(), sink.aborts, len(sink.commits))
			}
		} else if err != nil || len(sink.commits) != 2 || sink.commits[1][0].Key != childTarget.Key {
			t.Fatalf("后续事务未复用写回：err=%v commits=%v", err, sink.commits)
		}
	}
}

func (procedure commitParticipantProcedure) Locks(context.Context) (txn.Lockeys, error) {
	return txn.Lockeys{procedure.lock.Lock}, nil
}

func (procedure commitParticipantProcedure) Process(tx *txn.Tx) error {
	if err := tx.Enlist(procedure.lock); err != nil {
		return err
	}
	if procedure.prepared != nil && !*procedure.prepared {
		return errors.New("commit participant was not prepared before process")
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

func TestTxnRuntimeBindsGenericCommitParticipant(t *testing.T) {
	runtime := txn.NewRuntime()
	var participant txn.CommitParticipant = &recordingCommitParticipant{}
	if err := runtime.BindCommitParticipant(participant); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
}

func TestTxnRuntimeCommitsFinalDirtyTargetsToBoundSink(t *testing.T) {
	store := &runtimeTestStore{}
	lock := txn.NewStoreTarget(txn.NewLockey("role", 0, 1001), store)
	sink := &recordingCommitParticipant{}
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(sink); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}

	err := runtime.Call(context.Background(), commitParticipantProcedure{
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
	lock := txn.NewStoreTarget(txn.NewLockey("role", 0, 1001), store)
	sink := &recordingCommitParticipant{}
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(sink); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}

	err := runtime.Call(context.Background(), commitParticipantProcedure{
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

func TestTxnRuntimeRejectsInvalidOrDuplicateCommitParticipant(t *testing.T) {
	runtime := txn.NewRuntime()
	var typedNil *recordingCommitParticipant
	if err := runtime.BindCommitParticipant(typedNil); err == nil || !strings.Contains(err.Error(), "commit participant is nil") {
		t.Fatalf("typed nil BindCommitParticipant error = %v, want nil participant", err)
	}
	if err := runtime.BindCommitParticipant(&recordingCommitParticipant{}); err != nil {
		t.Fatalf("first BindCommitParticipant returned error: %v", err)
	}
	if err := runtime.BindCommitParticipant(&recordingCommitParticipant{}); err == nil || !strings.Contains(err.Error(), "already bound") {
		t.Fatalf("second BindCommitParticipant error = %v, want already bound", err)
	}
}

func TestTxnRuntimeRejectsProcedureBeforeMutationWhenAdmissionIsClosed(t *testing.T) {
	store := &runtimeTestStore{}
	writer := &recordingChangeStoreApplier{}
	admissionErr := errors.New("storage unavailable")
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{
		Writer:         writer,
		WriteAdmission: rejectingAdmission{err: admissionErr},
	}, writebackmodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	lock := txn.NewStoreTarget(txn.NewLockey("role", 0, 1001), store)
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
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{
		WriteAdmission: rejectingAdmission{err: errors.New("storage unavailable")},
	}, writebackmodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	lock := txn.NewStoreTarget(txn.NewLockey("memory", 0, 1001), store)
	if err := runtime.Call(context.Background(), commitParticipantProcedure{
		lock:  lock,
		store: &store.runtimeTestStore,
	}); err != nil {
		t.Fatalf("pure memory Call returned error: %v", err)
	}
	if manager.Pending() != 0 || !store.HasChanges() {
		t.Fatalf("pure memory state = pending:%d changes:%t", manager.Pending(), store.HasChanges())
	}
}

func TestWriteBackManagerTreatsTypedNilWriteAdmissionAsAbsent(t *testing.T) {
	store := &runtimeTestStore{}
	var typedNil *rejectingAdmission
	manager := writebackmodule.NewManager(writebackmodule.Dependencies{
		Writer:         &recordingChangeStoreApplier{},
		WriteAdmission: typedNil,
	}, writebackmodule.Config{})
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(manager); err != nil {
		t.Fatalf("BindCommitParticipant returned error: %v", err)
	}
	err := runtime.Call(context.Background(), runtimeTestProcedure{
		lock:  txn.NewStoreTarget(txn.NewLockey("role", 0, 1001), store),
		store: store,
	})
	if err != nil {
		t.Fatalf("Call with typed nil write admission returned error: %v", err)
	}
}

// 子过程先完成受控修改，再模拟业务 panic。
type panickingRuntimeProcedure struct{ runtimeTestProcedure }

func (procedure panickingRuntimeProcedure) Process(tx *txn.Tx) error {
	if err := tx.Enlist(procedure.lock); err != nil {
		return err
	}
	if err := procedure.runtimeTestProcedure.Process(tx); err != nil {
		return err
	}
	panic("child panic")
}

func TestTxnRuntimeChildPanicRestoresPendingChangesAndAbortsReservations(t *testing.T) {
	parent, child := &runtimeTestStore{}, &runtimeTestStore{}
	parent.SetField(7)
	child.SetField(8)
	parentBefore, childBefore := parent.changes.clone(), child.changes.clone()
	parentLock := txn.NewStoreTarget(txn.NewLockey(t.Name(), 0, "parent"), parent)
	child.id = 1002
	childLock := txn.NewStoreTarget(txn.NewLockey(t.Name(), 0, "child"), child)
	sink := &recordingCommitParticipant{}
	runtime := txn.NewRuntime()
	if err := runtime.BindCommitParticipant(sink); err != nil {
		t.Fatal(err)
	}
	procedure := runtimeDynamicParentProcedure{
		lock: parentLock, store: parent,
		child: panickingRuntimeProcedure{runtimeTestProcedure{lock: childLock, store: child}},
	}
	func() {
		defer func() {
			if got := recover(); got != "child panic" {
				t.Fatalf("panic = %v, want child panic", got)
			}
		}()
		_ = runtime.Call(context.Background(), procedure)
		t.Fatal("Call did not propagate child panic")
	}()
	if !reflect.DeepEqual(parent.changes, parentBefore) || !reflect.DeepEqual(child.changes, childBefore) {
		t.Fatalf("panic lost prior pending changes: parent=%v child=%v", parent.changes, child.changes)
	}
	if parent.recorder != nil || child.recorder != nil {
		t.Fatal("panic left undo recorder bound")
	}
	if sink.aborts != 2 || len(sink.commits) != 0 {
		t.Fatalf("reservations after panic: aborts=%d commits=%d, want 2/0", sink.aborts, len(sink.commits))
	}
	// 同一运行时重新进入父子过程，验证两把锁已释放且预约可以重新提交。
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	procedure.child = runtimeTestProcedure{lock: childLock, store: child}
	if err := runtime.Call(ctx, procedure); err != nil {
		t.Fatalf("Call after panic: %v", err)
	}
	if sink.aborts != 2 || len(sink.commits) != 2 {
		t.Fatalf("reservations after recovery: aborts=%d commits=%d, want 2/2", sink.aborts, len(sink.commits))
	}
}
