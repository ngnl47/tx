package txn

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ngnl47/tx/change"
)

const benchmarkLockTargetCount = 1024

var benchmarkHeldLockCount int
var benchmarkTransactionError error

type benchmarkLockStore struct{}

func (*benchmarkLockStore) SetUndoRecorder(change.UndoRecorder) {}

func BenchmarkLockTableAcquireRelease(b *testing.B) {
	ctx := context.Background()
	single := []LockTarget{{Key: LockKey{Scope: "role", Value: "470001"}}}
	multiple := []LockTarget{
		{Key: LockKey{Scope: "guild", Value: "7001"}},
		{Key: LockKey{Scope: "role", Value: "470001"}},
		{Key: LockKey{Scope: "role", Value: "470002"}},
		{Key: LockKey{Scope: "role", Value: "470003"}},
	}
	disjoint := make([][]LockTarget, benchmarkLockTargetCount)
	for index := range disjoint {
		disjoint[index] = []LockTarget{{
			Key: LockKey{Scope: "role", Value: fmt.Sprintf("benchmark-%d", index)},
		}}
	}

	b.Run("SingleKeySerial", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			held, err := acquireLocks(ctx, 1, single, "BenchmarkSingle")
			if err != nil {
				b.Fatal(err)
			}
			benchmarkHeldLockCount = held.count
			releaseLocks(1, &held)
		}
	})

	b.Run("FourKeysSerial", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			held, err := acquireLocks(ctx, 1, multiple, "BenchmarkFour")
			if err != nil {
				b.Fatal(err)
			}
			benchmarkHeldLockCount = held.count
			releaseLocks(1, &held)
		}
	})

	b.Run("SharedKeyParallel", func(b *testing.B) {
		var failed atomic.Bool
		var workerID atomic.Uint64
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			transaction := TransactionID(workerID.Add(1))
			for pb.Next() {
				held, err := acquireLocks(ctx, transaction, single, "BenchmarkShared")
				if err != nil {
					failed.Store(true)
					return
				}
				releaseLocks(transaction, &held)
			}
		})
		if failed.Load() {
			b.Fatal("acquire shared benchmark lock failed")
		}
	})

	b.Run("DisjointKeysParallel", func(b *testing.B) {
		var failed atomic.Bool
		var workerID atomic.Uint64
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			id := workerID.Add(1)
			locks := disjoint[(id-1)%uint64(len(disjoint))]
			transaction := TransactionID(id)
			for pb.Next() {
				held, err := acquireLocks(ctx, transaction, locks, "BenchmarkDisjoint")
				if err != nil {
					failed.Store(true)
					return
				}
				releaseLocks(transaction, &held)
			}
		})
		if failed.Load() {
			b.Fatal("acquire disjoint benchmark lock failed")
		}
	})

	b.Run("RawMutexSerial", func(b *testing.B) {
		var mutex sync.Mutex
		b.ReportAllocs()
		for b.Loop() {
			mutex.Lock()
			mutex.Unlock()
		}
	})

	b.Run("RawMutexDisjointParallel", func(b *testing.B) {
		mutexes := make([]sync.Mutex, benchmarkLockTargetCount)
		var workerID atomic.Uint64
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			id := workerID.Add(1) - 1
			mutex := &mutexes[id%uint64(len(mutexes))]
			for pb.Next() {
				mutex.Lock()
				mutex.Unlock()
			}
		})
	})
}

func BenchmarkWithLocks(b *testing.B) {
	ctx := context.Background()
	single := []LockTarget{NewLockTarget(
		LockKey{Scope: "role", Value: "470001"},
		&benchmarkLockStore{},
	)}
	multiple := []LockTarget{
		NewOrderedLockTarget(10, LockKey{Scope: "guild", Value: "7001"}, &benchmarkLockStore{}),
		NewOrderedLockTarget(20, LockKey{Scope: "role", Value: "470001"}, &benchmarkLockStore{}),
		NewOrderedLockTarget(20, LockKey{Scope: "role", Value: "470002"}, &benchmarkLockStore{}),
		NewOrderedLockTarget(20, LockKey{Scope: "role", Value: "470003"}, &benchmarkLockStore{}),
	}

	for _, benchmarkCase := range []struct {
		name  string
		locks []LockTarget
	}{
		{name: "SingleKey", locks: single},
		{name: "FourKeys", locks: multiple},
	} {
		b.Run(benchmarkCase.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := WithLocks(ctx, benchmarkCase.locks, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type benchmarkProcedure struct {
	locks []LockTarget
	child Procedure
}

func (procedure *benchmarkProcedure) Locks(context.Context) ([]LockTarget, error) {
	return procedure.locks, nil
}

func (procedure *benchmarkProcedure) Process(tx *Tx) error {
	if procedure.child == nil {
		return nil
	}
	return tx.Call(procedure.child)
}

func BenchmarkProcedureCall(b *testing.B) {
	firstStore := &benchmarkLockStore{}
	secondStore := &benchmarkLockStore{}
	first := NewLockTarget(NewLockKey("role", 470001), firstStore)
	second := NewLockTarget(NewLockKey("role", 470002), secondStore)
	cases := []struct {
		name      string
		procedure Procedure
	}{
		{
			name:      "RootOnly",
			procedure: &benchmarkProcedure{locks: []LockTarget{first}},
		},
		{
			name: "ChildReusesRootLock",
			procedure: &benchmarkProcedure{
				locks: []LockTarget{first},
				child: &benchmarkProcedure{locks: []LockTarget{first}},
			},
		},
		{
			name: "ChildAcquiresDynamicLock",
			procedure: &benchmarkProcedure{
				locks: []LockTarget{first},
				child: &benchmarkProcedure{locks: []LockTarget{second}},
			},
		},
	}
	for _, benchmarkCase := range cases {
		b.Run(benchmarkCase.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkTransactionError = CallWithOptions(context.Background(), benchmarkCase.procedure, CallOptions{})
				if benchmarkTransactionError != nil {
					b.Fatal(benchmarkTransactionError)
				}
			}
		})
	}
}

func BenchmarkWaitGraph(b *testing.B) {
	b.Run("AcyclicAddRemove", func(b *testing.B) {
		graph := waitGraph{waitsFor: make(map[TransactionID]waitEdge, 1)}
		lock := NewLockKey("role", 470001)
		b.ReportAllocs()
		for b.Loop() {
			benchmarkTransactionError = graph.add(1, 2, lock, "PTrade -> PGrant")
			graph.remove(1)
		}
	})

	b.Run("DetectTwoTransactionCycle", func(b *testing.B) {
		graph := waitGraph{waitsFor: make(map[TransactionID]waitEdge, 1)}
		first := NewLockKey("role", 470001)
		second := NewLockKey("role", 470002)
		if err := graph.add(1, 2, first, "PFirst -> PSecond"); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			benchmarkTransactionError = graph.add(2, 1, second, "PSecond -> PFirst")
		}
	})
}
