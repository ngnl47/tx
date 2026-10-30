package entitycache

import (
	"context"
	"errors"
	"github.com/ngnl47/tx/modules/txn"
	"testing"
)

// 普通查询的使用权随实际操作退出释放，取消信号不能使仍在执行的对象被回收。
func TestFindRetainsUntilOperationEnds(t *testing.T) {
	cache := newTestCache(t, 16, 0, func(context.Context, int64) (*cachedValue, bool, error) {
		return &cachedValue{id: 47}, true, nil
	})
	ctx, end := Begin(t.Context())
	first, exists, err := cache.Find(ctx, 47)
	if err != nil || !exists {
		t.Fatal(exists, err)
	}
	entry := cache.values[47]
	candidate := deferredEntry[int64, *cachedValue]{key: 47, entry: entry, generation: entry.generation.Load()}
	cache.tryRelease(t.Context(), candidate)
	second, _, err := cache.Find(ctx, 47)
	if err != nil || first != second {
		t.Fatal("操作内对象身份改变", err)
	}
	end()
	end()
	if _, _, err := cache.Find(ctx, 47); !errors.Is(err, ErrScopeClosed) {
		t.Fatal(err)
	}
	cache.tryRelease(t.Context(), deferredEntry[int64, *cachedValue]{key: 47, entry: entry, generation: entry.generation.Load()})
	if cache.values[47] != nil {
		t.Fatal("操作退出后使用权未释放")
	}
}

func TestFindRequiresScopeAndDoesNotReleaseOnCancellation(t *testing.T) {
	cache := newTestCache(t, 16, 0, func(context.Context, int64) (*cachedValue, bool, error) { return &cachedValue{id: 1}, true, nil })
	if _, _, err := cache.Find(t.Context(), 1); !errors.Is(err, ErrScopeRequired) {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(t.Context())
	ctx, end := Begin(parent)
	defer end()
	if _, _, err := cache.Find(ctx, 1); err != nil {
		t.Fatal(err)
	}
	cancel()
	entry := cache.values[1]
	cache.tryRelease(t.Context(), deferredEntry[int64, *cachedValue]{key: 1, entry: entry, generation: entry.generation.Load()})
	if cache.values[1] != entry {
		t.Fatal("取消提前释放仍在使用的对象")
	}
}

// 事务内取得的使用权不能被嵌套操作结束提前释放，panic 也必须归还。
func TestTransactionFindHoldsUntilCallbacksAndPanicFinish(t *testing.T) {
	for _, panicRun := range []bool{false, true} {
		cache := newTestCache(t, 16, 0, func(context.Context, int64) (*cachedValue, bool, error) { return &cachedValue{id: 1}, true, nil })
		callbacks := 0
		func() {
			defer func() {
				if v := recover(); (v != nil) != panicRun {
					t.Errorf("panic=%v", v)
				}
			}()
			err := txn.NewRuntime().Call(t.Context(), scopeProcedure(func(tx *txn.Tx) error {
				ctx, end := Begin(tx.Context())
				if _, _, err := cache.Find(ctx, 1); err != nil {
					return err
				}
				end()
				entry := cache.values[1]
				assertHeld := func(context.Context) {
					callbacks++
					cache.tryRelease(t.Context(), deferredEntry[int64, *cachedValue]{key: 1, entry: entry, generation: entry.generation.Load()})
					if cache.values[1] != entry {
						t.Error("事务回调前提前释放")
					}
				}
				if panicRun {
					tx.AfterRollback(assertHeld)
					panic("rollback")
				}
				tx.AfterCommit(func(ctx context.Context) error { assertHeld(ctx); return nil })
				return nil
			}))
			if err != nil {
				t.Fatal(err)
			}
		}()
		entry := cache.values[1]
		if callbacks != 1 || entry.users != 0 {
			t.Fatalf("callbacks=%d users=%d", callbacks, entry.users)
		}
		cache.tryRelease(t.Context(), deferredEntry[int64, *cachedValue]{key: 1, entry: entry, generation: entry.generation.Load()})
		if cache.values[1] != nil {
			t.Fatal("事务结束仍持有对象")
		}
	}
}

type scopeProcedure func(*txn.Tx) error

func (p scopeProcedure) Locks(context.Context) (txn.Lockeys, error) { return nil, nil }
func (p scopeProcedure) Process(tx *txn.Tx) error                   { return p(tx) }
