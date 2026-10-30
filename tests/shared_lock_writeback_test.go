package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
)

type sharedStoreProcedure struct {
	targets []txn.StoreTarget
	stores  []*runtimeTestStore
	fail    bool
}

func (p sharedStoreProcedure) Locks(context.Context) (txn.Lockeys, error) {
	return txn.Lockeys{p.targets[0].Lock}, nil
}
func (p sharedStoreProcedure) Process(tx *txn.Tx) error {
	for _, target := range p.targets {
		if err := tx.Enlist(target); err != nil {
			return err
		}
	}
	for _, store := range p.stores {
		store.Set()
	}
	if p.fail {
		return errors.New("rollback")
	}
	return nil
}

func TestSharedLockKeepsStoreCommitAndRollbackIndependent(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback"}[rollback], func(t *testing.T) {
			role, bag := &runtimeTestStore{typeName: "GDRole"}, &runtimeTestStore{typeName: "GDBag"}
			lock := txn.NewLockey("RoleLock", 1, 1001)
			writer := &recordingChangeStoreApplier{}
			runtime, manager := newWriteBackRuntime(t, writer, writeback.Config{})
			err := runtime.Call(t.Context(), sharedStoreProcedure{targets: []txn.StoreTarget{txn.NewStoreTarget(lock, role), txn.NewStoreTarget(lock, bag)}, stores: []*runtimeTestStore{role, bag}, fail: rollback})
			if rollback {
				if err == nil || manager.Pending() != 0 || role.HasChanges() || bag.HasChanges() {
					t.Fatalf("rollback: %v pending=%d", err, manager.Pending())
				}
				return
			}
			if err != nil || manager.Pending() != 2 {
				t.Fatalf("commit: %v pending=%d", err, manager.Pending())
			}
			if err := manager.Flush(t.Context()); err != nil {
				t.Fatal(err)
			}
			if role.HasChanges() || bag.HasChanges() || manager.Pending() != 0 {
				t.Fatal("共有锁的两个 Store 未分别写回")
			}
		})
	}
}
