package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
	"github.com/ngnl47/tx/storage"
)

type createWriter struct {
	recordingChangeStoreApplier
	inserts int
	exists  bool
	err     error
}

func (w *createWriter) BackfillDocumentWithOptions(context.Context, meta.TypeSpec, storage.RecordKey, map[string]any, storage.ApplyOptions) (bool, error) {
	w.inserts++
	if w.err != nil {
		return false, w.err
	}
	if w.exists {
		return false, nil
	}
	w.exists = true
	return true, nil
}

type createCache struct{ visible, reserved bool }

func (c *createCache) ReserveCreate(context.Context, any) (txn.CreationReservation, error) {
	if c.reserved || c.visible {
		return nil, errors.New("exists")
	}
	c.reserved = true
	return c, nil
}
func (c *createCache) Validate() error { return nil }
func (c *createCache) Commit()         { c.reserved = false; c.visible = true }
func (c *createCache) Abort()          { c.reserved = false }

type newRoot struct {
	*runtimeTestStore
	id string
}

func (v *newRoot) LockTarget() txn.LockTarget {
	return txn.NewLockTarget(txn.NewLockKey("new", v.id), v.runtimeTestStore)
}

type createRootProcedure struct {
	root  *runtimeTestStore
	value *newRoot
	after func(*txn.Tx) error
}

func (p createRootProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{txn.NewLockTarget(txn.NewLockKey("parent", 1), p.root)}, nil
}
func (p createRootProcedure) Process(tx *txn.Tx) error {
	if err := txn.Create(tx.Context(), p.value); err != nil {
		return err
	}
	if p.after != nil {
		return p.after(tx)
	}
	return nil
}

func TestWriteBackCreationWithoutSettersAndFailedInsert(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry", true: "conflict"}[existing], func(t *testing.T) {
			writer := &createWriter{exists: existing, err: errors.New("unavailable")}
			manager := writeback.NewManager(writeback.Dependencies{Writer: writer}, writeback.Config{})
			cache := &createCache{}
			if err := manager.BindCreationCache("new", cache); err != nil {
				t.Fatal(err)
			}
			runtime := txn.NewRuntime()
			_ = runtime.BindCommitParticipant(manager)
			value := &newRoot{runtimeTestStore: &runtimeTestStore{}, id: "1"}
			if err := runtime.Call(context.Background(), createRootProcedure{root: &runtimeTestStore{}, value: value}); err != nil {
				t.Fatal(err)
			}
			if !cache.visible || manager.Pending() != 1 {
				t.Fatal("creation not published and retained")
			}
			if err := manager.Flush(context.Background()); err == nil || manager.Pending() != 1 {
				t.Fatal("failed insert discarded")
			}
			if released, err := manager.TryRelease(context.Background(), value.LockTarget()); err != nil || released {
				t.Fatal("pending object released", err)
			}
			writer.err = nil
			err := manager.Flush(context.Background())
			if existing {
				if !errors.Is(err, writeback.ErrRecordExists) || manager.Pending() != 1 {
					t.Fatal("existing record overwritten", err)
				}
			} else if err != nil || manager.Pending() != 0 || writer.inserts != 2 {
				t.Fatal("retry failed", err)
			}
		})
	}
}

func TestWriteBackCreationRollback(t *testing.T) {
	writer := &createWriter{}
	manager := writeback.NewManager(writeback.Dependencies{Writer: writer}, writeback.Config{})
	cache := &createCache{}
	_ = manager.BindCreationCache("new", cache)
	runtime := txn.NewRuntime()
	_ = runtime.BindCommitParticipant(manager)
	failure := errors.New("rollback")
	err := runtime.Call(context.Background(), createRootProcedure{root: &runtimeTestStore{}, value: &newRoot{&runtimeTestStore{}, "1"}, after: func(*txn.Tx) error { return failure }})
	if !errors.Is(err, failure) || cache.visible || cache.reserved || manager.Pending() != 0 || writer.inserts != 0 {
		t.Fatal("rollback leaked state", err)
	}
}

type rejectingCreateCache struct {
	createCache
	fail           bool
	panicOnReserve bool
}

func (c *rejectingCreateCache) ReserveCreate(ctx context.Context, value any) (txn.CreationReservation, error) {
	if c.panicOnReserve {
		panic("loader panic")
	}
	if c.fail {
		return nil, errors.New("loader failed")
	}
	return c.createCache.ReserveCreate(ctx, value)
}
func TestFailedCreationReleasesCapacityEvenWhenCallerContinues(t *testing.T) {
	manager := writeback.NewManager(writeback.Dependencies{Writer: &createWriter{}}, writeback.Config{MaxPendingRoots: 2})
	cache := &rejectingCreateCache{fail: true}
	_ = manager.BindCreationCache("new", cache)
	runtime := txn.NewRuntime()
	_ = runtime.BindCommitParticipant(manager)
	parent := &runtimeTestStore{}
	p := fusionRoleProcedure{lock: txn.NewLockTarget(txn.NewLockKey("parent", 1), parent), process: func(tx *txn.Tx) error {
		if err := txn.Create(tx.Context(), &newRoot{&runtimeTestStore{}, "failed"}); err == nil {
			t.Fatal("expected load failure")
		}
		cache.fail = false
		return txn.Create(tx.Context(), &newRoot{&runtimeTestStore{}, "success"})
	}}
	if err := runtime.Call(t.Context(), p); err != nil {
		t.Fatal("failed reservation consumed capacity", err)
	}
	if manager.Pending() != 1 {
		t.Fatal("wrong pending count")
	}
}
func TestCreationPanicReleasesReservations(t *testing.T) {
	manager := writeback.NewManager(writeback.Dependencies{Writer: &createWriter{}}, writeback.Config{MaxPendingRoots: 2})
	cache := &rejectingCreateCache{panicOnReserve: true}
	_ = manager.BindCreationCache("new", cache)
	runtime := txn.NewRuntime()
	_ = runtime.BindCommitParticipant(manager)
	p := createRootProcedure{root: &runtimeTestStore{}, value: &newRoot{&runtimeTestStore{}, "new"}}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected panic")
			}
		}()
		_ = runtime.Call(t.Context(), p)
	}()
	cache.panicOnReserve = false
	if err := runtime.Call(t.Context(), p); err != nil {
		t.Fatal("panic retained reservation", err)
	}
	if manager.Pending() != 1 {
		t.Fatal("new root missing")
	}
}
