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
			held, err := acquireLocks(ctx, single)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkHeldLockCount = held.count
			releaseLocks(&held)
		}
	})

	b.Run("FourKeysSerial", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			held, err := acquireLocks(ctx, multiple)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkHeldLockCount = held.count
			releaseLocks(&held)
		}
	})

	b.Run("SharedKeyParallel", func(b *testing.B) {
		var failed atomic.Bool
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				held, err := acquireLocks(ctx, single)
				if err != nil {
					failed.Store(true)
					return
				}
				releaseLocks(&held)
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
			id := workerID.Add(1) - 1
			locks := disjoint[id%uint64(len(disjoint))]
			for pb.Next() {
				held, err := acquireLocks(ctx, locks)
				if err != nil {
					failed.Store(true)
					return
				}
				releaseLocks(&held)
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
