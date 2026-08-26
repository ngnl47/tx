package tests

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ngnl47/tx/modules/txn"
)

type rejectingAdmission struct {
	err error
}

func (admission rejectingAdmission) RequireWritable() error {
	return admission.err
}

func TestTxnRuntimeRejectsTypedNilProcedure(t *testing.T) {
	runtime := txn.NewRuntime(nil)
	var procedure *runtimeTestProcedure
	err := runtime.Call(context.Background(), procedure)
	if err == nil || !strings.Contains(err.Error(), "procedure is nil") {
		t.Fatalf("Call error = %v, want nil procedure", err)
	}
}

func TestTxnRuntimeRejectsProcedureBeforeMutationWhenAdmissionIsClosed(t *testing.T) {
	store := &runtimeTestStore{}
	writer := &recordingChangeStoreApplier{}
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
	if called || len(store.changes) != 0 || runtime.Pending() != 0 {
		t.Fatalf("closed admission executed procedure: called=%t changes=%#v pending=%d", called, store.changes, runtime.Pending())
	}
}
