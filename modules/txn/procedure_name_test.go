package txn

import (
	"context"
	"testing"
)

type procedureNameTest struct{}

func (*procedureNameTest) Locks(context.Context) ([]LockTarget, error) {
	return nil, nil
}

func (*procedureNameTest) Process(*Tx) error {
	return nil
}

func TestManagedProcedureUsesWrappedBusinessProcedureName(t *testing.T) {
	managed := &managedProcedure{procedure: &procedureNameTest{}}
	if got := procedureName(managed); got != "procedureNameTest" {
		t.Fatalf("procedureName(managed) = %q, want wrapped business Procedure name", got)
	}
}
