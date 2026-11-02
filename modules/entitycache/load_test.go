package entitycache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ngnl47/tx/modules/txn"
)

// 同一冷加载的首调用者和后加入者都只能取消自己的等待。
func TestColdLoadWaitersCancelIndependently(t *testing.T) {
	for _, cancelLeader := range []bool{true, false} {
		name := "follower"
		if cancelLeader {
			name = "leader"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				gate := make(chan struct{})
				started := make(chan struct{})
				loads := 0
				cache := newBubbleReleaseTestCache(t, func(context.Context, *cachedValue) (bool, error) { return true, nil })
				cache.loader = func(ctx context.Context, id int64) (*cachedValue, bool, error) {
					loads++
					if loads == 1 {
						close(started)
					}
					select {
					case <-ctx.Done():
						return nil, false, ctx.Err()
					case <-gate:
						return &cachedValue{id: id}, true, nil
					}
				}
				tasks := startReleaseTestCache(t, cache)
				defer stopReleaseTestCache(tasks, cache)
				canceled, cancel := context.WithCancel(context.Background())
				defer cancel()
				leader, follower := context.Background(), context.Background()
				if cancelLeader {
					leader = canceled
				} else {
					follower = canceled
				}
				first, second := make(chan error, 1), make(chan error, 1)
				read := func(ctx context.Context, result chan<- error) {
					_, err := cache.Use(ctx, 47, func(*cachedValue) error { return nil })
					result <- err
				}
				go read(leader, first)
				<-started
				go read(follower, second)
				synctest.Wait()
				cancel()
				synctest.Wait()
				canceledResult, remaining := first, second
				if !cancelLeader {
					canceledResult, remaining = second, first
				}
				select {
				case err := <-canceledResult:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("cancel result = %v", err)
					}
				default:
					t.Error("已取消的调用仍等待存储加载")
				}
				close(gate)
				if err := <-remaining; err != nil {
					t.Errorf("其他调用受到取消影响: %v", err)
				}
				if loads != 1 {
					t.Errorf("loads = %d, want 1", loads)
				}
			})
		})
	}
}

// 调用者退出后存储工作仍由宿主追踪；既不携带事务，也不提前清除加载占位。
func TestColdLoadContextAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type capabilityKey struct{}
		started, finish := make(chan struct{}), make(chan struct{})
		cache := newBubbleReleaseTestCache(t, func(context.Context, *cachedValue) (bool, error) { return true, nil })
		cache.loader = func(ctx context.Context, id int64) (*cachedValue, bool, error) {
			if ctx.Value(capabilityKey{}) != "storage" {
				t.Error("存储能力丢失")
			}
			if tx, _ := txn.FromContext(ctx); tx != nil {
				t.Error("共享加载仍携带调用事务")
			}
			if ctx.Value(scopeKey{}) != nil {
				t.Error("共享加载仍携带调用使用期")
			}
			close(started)
			<-finish // 模拟未及时响应取消的 I/O。
			return &cachedValue{id: id}, true, nil
		}
		tasks := startReleaseTestCache(t, cache)
		defer stopReleaseTestCache(tasks, cache)
		ctx, cancel := context.WithCancel(context.WithValue(context.Background(), capabilityKey{}, "storage"))
		defer cancel()
		ctx, end := Begin(ctx)
		defer end()
		result := make(chan error, 1)
		go func() {
			result <- txn.NewRuntime().Call(ctx, scopeProcedure(func(tx *txn.Tx) error {
				_, err := cache.Use(tx.Context(), 47, func(*cachedValue) error { return nil })
				return err
			}))
		}()
		<-started
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		stopCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := tasks.CancelAndWait(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("close = %v", err)
		}
		// 生命周期取消后仍保留在途占位，不能绕过加载创建第二个对象。
		cache.mu.RLock()
		entry := cache.values[47]
		cache.mu.RUnlock()
		if entry == nil || !entry.loading {
			t.Error("加载退出前丢失占位")
		}
		close(finish)
		if err := tasks.CancelAndWait(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertKeyNotRetained(t, cache, 47)
	})
}

func TestColdLoadTimeoutRetainsWorkUntilExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		finish := make(chan struct{})
		cache := newBubbleReleaseTestCache(t, func(context.Context, *cachedValue) (bool, error) { return true, nil })
		var loads atomic.Int32
		cache.loader = func(context.Context, int64) (*cachedValue, bool, error) {
			loads.Add(1)
			<-finish
			return &cachedValue{id: 47}, true, nil
		}
		tasks := startReleaseTestCache(t, cache)
		defer stopReleaseTestCache(tasks, cache)
		for range 2 {
			_, err := cache.Use(context.Background(), 47, func(*cachedValue) error { return nil })
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("load = %v", err)
			}
		}
		if loads.Load() != 1 {
			t.Errorf("未结束的加载被重新执行: %d", loads.Load())
		}
		close(finish)
		synctest.Wait()
		assertKeyNotRetained(t, cache, 47)
	})
}

func TestColdLoadPanicReleasesReservation(t *testing.T) {
	cache := newTestCache(t, 16, 0, func(context.Context, int64) (*cachedValue, bool, error) { panic("broken loader") })
	if _, err := cache.Use(t.Context(), 47, func(*cachedValue) error { return nil }); err == nil {
		t.Fatal("panic 被伪装为成功")
	}
	assertKeyNotRetained(t, cache, 47)
}
