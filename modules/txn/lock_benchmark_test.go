package txn

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ngnl47/tx/change"
)

const benchmarkStoreTargetCount = 1024

var benchmarkHeldLockCount int
var benchmarkTransactionError error

type benchmarkLockStore struct{}

func (*benchmarkLockStore) SetUndoRecorder(change.UndoRecorder) {}

func BenchmarkLockTableAcquireRelease(b *testing.B) {
	ctx := context.Background()
	single := Lockeys{{Type: "role", Key: "470001"}}
	multiple := Lockeys{
		{Type: "guild", Key: "7001"},
		{Type: "role", Key: "470001"},
		{Type: "role", Key: "470002"},
		{Type: "role", Key: "470003"},
	}
	disjoint := make([]Lockeys, benchmarkStoreTargetCount)
	for index := range disjoint {
		disjoint[index] = Lockeys{{
			Type: "role", Key: fmt.Sprintf("benchmark-%d", index),
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
		mutexes := make([]sync.Mutex, benchmarkStoreTargetCount)
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
	single := Lockeys{NewLockey("role", 0, "470001")}
	multiple := Lockeys{
		NewLockey("guild", 10, "7001"),
		NewLockey("role", 20, "470001"),
		NewLockey("role", 20, "470002"),
		NewLockey("role", 20, "470003"),
	}

	for _, benchmarkCase := range []struct {
		name  string
		locks Lockeys
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
	locks Lockeys
	child Procedure
}

func (procedure *benchmarkProcedure) Locks(context.Context) (Lockeys, error) {
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
	first := NewStoreTarget(NewLockey("role", 0, 470001), firstStore)
	second := NewStoreTarget(NewLockey("role", 0, 470002), secondStore)
	cases := []struct {
		name      string
		procedure Procedure
	}{
		{
			name:      "RootOnly",
			procedure: &benchmarkProcedure{locks: Lockeys{first.Lock}},
		},
		{
			name: "ChildReusesRootLock",
			procedure: &benchmarkProcedure{
				locks: Lockeys{first.Lock},
				child: &benchmarkProcedure{locks: Lockeys{first.Lock}},
			},
		},
		{
			name: "ChildAcquiresDynamicLock",
			procedure: &benchmarkProcedure{
				locks: Lockeys{first.Lock},
				child: &benchmarkProcedure{locks: Lockeys{second.Lock}},
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
		lock := NewLockey("role", 0, 470001)
		b.ReportAllocs()
		for b.Loop() {
			benchmarkTransactionError = graph.add(1, 2, lock, "PTrade -> PGrant")
			graph.remove(1)
		}
	})

	b.Run("DetectTwoTransactionCycle", func(b *testing.B) {
		graph := waitGraph{waitsFor: make(map[TransactionID]waitEdge, 1)}
		first := NewLockey("role", 0, 470001)
		second := NewLockey("role", 0, 470002)
		if err := graph.add(1, 2, first, "PFirst -> PSecond"); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			benchmarkTransactionError = graph.add(2, 1, second, "PSecond -> PFirst")
		}
	})
}
