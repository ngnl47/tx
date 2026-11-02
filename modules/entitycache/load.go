package entitycache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/txn"
)

// 冷加载使用独立 I/O 期限；等待取消不等于加载已经退出。
const loadTimeout = 10 * time.Second

type cacheLoad struct {
	done   chan struct{}
	query  context.Context
	loaded bool
	err    error
}

func (cache *Cache[K, V]) start(tasks lifecycle.Tasks, onStop func()) error {
	if tasks == nil {
		return errors.New("entity cache: lifecycle tasks are nil")
	}
	cache.loadMu.Lock()
	defer cache.loadMu.Unlock()
	if cache.tasks != nil {
		return errors.New("entity cache: already started")
	}
	cache.tasks = lifecycle.TaskView(tasks)
	if err := tasks.Go(func(ctx context.Context) {
		if onStop != nil {
			defer onStop()
		}
		cache.runReleaseWorker(ctx)
	}); err != nil {
		cache.tasks = nil
		return err
	}
	return nil
}

// load 仅在未命中时登记工作；全部等待者退出也不提前移除在途加载。
func (cache *Cache[K, V]) load(ctx context.Context, key K) (bool, error) {
	cache.loadMu.Lock()
	call := cache.loads[key]
	if call == nil {
		if cache.tasks == nil {
			cache.loadMu.Unlock()
			return false, errors.New("entity cache: Start is required before loading")
		}
		// 只借用存储能力等值，不让共享 I/O 携带调用者的取消、事务或实体使用期。
		values := context.WithoutCancel(txn.WithoutTransaction(ctx))
		query, cancel := context.WithTimeout(loadContext{Context: cache.tasks, values: values}, loadTimeout)
		call = &cacheLoad{done: make(chan struct{}), query: query}
		cache.loads[key] = call
		if err := cache.tasks.Go(func(context.Context) {
			defer cancel()
			defer func() {
				if failure := recover(); failure != nil {
					call.err = fmt.Errorf("entity cache: loader panic: %v", failure)
				}
				cache.loadMu.Lock()
				delete(cache.loads, key)
				close(call.done)
				cache.loadMu.Unlock()
			}()
			call.loaded, call.err = cache.loadMissing(query, key)
		}); err != nil {
			cancel()
			delete(cache.loads, key)
			cache.loadMu.Unlock()
			return false, err
		}
	}
	cache.loadMu.Unlock()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-call.query.Done():
		// 成功后的 defer cancel 不覆盖已经发布的结果。
		select {
		case <-call.done:
		default:
			return false, call.query.Err()
		}
	case <-call.done:
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return call.loaded, call.err
}

type loadContext struct {
	context.Context
	values context.Context
}

func (ctx loadContext) Value(key any) any {
	if _, ok := key.(scopeKey); ok {
		return nil
	}
	return ctx.values.Value(key)
}
