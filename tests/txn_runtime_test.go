package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/modules/txn"
)

type rejectingAdmission struct {
	err error
}

func (admission rejectingAdmission) RequireWritable() error {
	return admission.err
}

func TestTxnRuntimeRejectsProcedureBeforeMutationWhenAdmissionIsClosed(t *testing.T) {
	store := &runtimeTestStore{}
	writer := &recordingDeltaStoreApplier{}
	admissionErr := errors.New("storage unavailable")
	runtime := txn.NewRuntime(writer, txn.RuntimeConfig{WriteAdmission: rejectingAdmission{err: admissionErr}})
	lock := txn.NewLockTarget(txn.NewLockKey("role", 1001), store)
	called := false

	err := runtime.Call(context.Background(), observingRuntimeTestProcedure{
		runtimeTestProcedure: runtimeTestProcedure{lock: lock, store: store},
		called:               &called,
	})
	if !errors.Is(err, admissionErr) {
		t.Fatalf("Call error = %v, want admission error", err)
	}
	if called || len(store.Deltas()) != 0 || runtime.Pending() != 0 {
		t.Fatalf("closed admission executed procedure: called=%t deltas=%#v pending=%d", called, store.Deltas(), runtime.Pending())
	}
}
