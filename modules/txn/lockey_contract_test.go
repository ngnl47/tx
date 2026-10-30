package txn

import (
	"context"
	"errors"
	"github.com/ngnl47/tx/change"
	"strings"
	"sync"
	"testing"
	"time"
)

type prelockedProcedure struct {
	lock Lockey
	run  func(*Tx) error
}

func (p prelockedProcedure) Locks(context.Context) (Lockeys, error) { return Lockeys{p.lock}, nil }
func (p prelockedProcedure) Process(tx *Tx) error                   { return p.run(tx) }

func TestPredeclaredLockWithoutStore(t *testing.T) {
	lock := NewLockey("RoleLock", 1, int64(1001))
	called := false
	err := Call(context.Background(), prelockedProcedure{lock, func(tx *Tx) error {
		called = true
		return tx.Call(prelockedProcedure{lock, func(*Tx) error { return nil }})
	}})
	if err != nil || !called {
		t.Fatalf("prelock: called=%v err=%v", called, err)
	}
}

func TestLockeyOrderAndDuplicate(t *testing.T) {
	locks, err := prepareLockeys(Lockeys{NewLockey("RoleLock", 1, 10), NewLockey("LegionLock", 2, 100), NewLockey("RoleLock", 1, 2), NewLockey("RoleLock", 1, 2)})
	if err != nil {
		t.Fatal(err)
	}
	if len(locks) != 3 || locks[0].Type != "LegionLock" || locks[1].Key != "2" || locks[2].Key != "10" {
		t.Fatalf("order: %+v", locks)
	}
	_, err = prepareLockeys(Lockeys{NewLockey("RoleLock", 1, 2), NewLockey("RoleLock", 2, 2)})
	if err == nil {
		t.Fatal("conflicting order accepted")
	}
}

func TestSharedLockStoresRollback(t *testing.T) {
	lock := NewLockey("RoleLock", 1, 1001)
	a, b := &lockeyTestStore{}, &lockeyTestStore{}
	want := errors.New("rollback")
	err := Call(context.Background(), prelockedProcedure{lock, func(tx *Tx) error {
		if err := tx.Enlist(NewStoreTarget(lock, a)); err != nil {
			return err
		}
		if err := tx.Enlist(NewStoreTarget(lock, b)); err != nil {
			return err
		}
		a.set(1)
		b.set(2)
		return want
	}})
	if !errors.Is(err, want) || a.value != 0 || b.value != 0 {
		t.Fatalf("rollback: %v", err)
	}
}

type lockeyTestStore struct {
	value    int
	recorder change.UndoRecorder
}

func (s *lockeyTestStore) SetUndoRecorder(r change.UndoRecorder) { s.recorder = r }

func (s *lockeyTestStore) set(v int) {
	old := s.value
	s.recorder.RecordUndo(func() { s.value = old })
	s.value = v
}

func TestLockeyMixedKeysHaveOneTotalOrder(t *testing.T) {
	for _, keys := range [][]string{{"2", "10", "1a"}, {"10", "1a", "2"}, {"1a", "2", "10"}} {
		var locks Lockeys
		for _, key := range keys {
			locks = append(locks, NewLockey("MixedLock", 1, key))
		}
		got, err := prepareLockeys(locks)
		if err != nil {
			t.Fatal(err)
		}
		if got[0].Key != "2" || got[1].Key != "10" || got[2].Key != "1a" {
			t.Fatalf("keys=%v order=%v", keys, got)
		}
	}
}

type dynamicLockRoot struct {
	first, second StoreTarget
	ready         *sync.WaitGroup
}

func (p dynamicLockRoot) Locks(context.Context) (Lockeys, error) { return nil, nil }
func (p dynamicLockRoot) Process(tx *Tx) error {
	if err := tx.Enlist(p.first); err != nil {
		return err
	}
	p.ready.Done()
	p.ready.Wait()
	return tx.Call(dynamicLockChild{p.second})
}

type dynamicLockChild struct{ target StoreTarget }

func (p dynamicLockChild) Locks(context.Context) (Lockeys, error) { return nil, nil }
func (p dynamicLockChild) Process(tx *Tx) error                   { return tx.Enlist(p.target) }
func TestDynamicEnlistDeadlockKeepsProcedureTrace(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	a := NewStoreTarget(NewLockey("RoleLock", 1, 1), &lockeyTestStore{})
	b := NewStoreTarget(NewLockey("RoleLock", 1, 2), &lockeyTestStore{})
	var ready sync.WaitGroup
	ready.Add(2)
	done := make(chan error, 2)
	go func() { done <- CallWithOptions(ctx, dynamicLockRoot{a, b, &ready}, CallOptions{}) }()
	go func() { done <- CallWithOptions(ctx, dynamicLockRoot{b, a, &ready}, CallOptions{}) }()
	found := false
	for range 2 {
		err := <-done
		if err == nil {
			continue
		}
		var cycle *DeadlockError
		if !errors.As(err, &cycle) {
			t.Fatal(err)
		}
		found = true
		for _, step := range cycle.Cycle {
			if !strings.Contains(step.ProcedurePath, "dynamicLockRoot -> dynamicLockChild") {
				t.Fatalf("lost trace: %s", step.ProcedurePath)
			}
		}
	}
	if !found {
		t.Fatal("未检测到动态补锁环路")
	}
}
