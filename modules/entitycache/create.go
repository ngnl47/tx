package entitycache

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
)

// ErrCreating 表示该身份正在加载或创建，调用方不能取得另一个对象副本。
var ErrCreating = errors.New("entity cache: identity is loading or being created")
var ErrExists = errors.New("entity cache: identity already exists")

// Creation 为当前唯一缓存提供写回登记适配；keyOf 复用所属模块的身份定义。
func (cache *Cache[K, V]) Creation(keyOf func(V) (K, error)) writeback.CreationCache {
	return creationCache[K, V]{cache: cache, keyOf: keyOf}
}

type creationCache[K comparable, V any] struct {
	cache *Cache[K, V]
	keyOf func(V) (K, error)
}

func (binding creationCache[K, V]) ReserveCreate(ctx context.Context, value any) (txn.CreationReservation, error) {
	owner, active := txn.FromContext(ctx)
	if owner != nil && !active {
		return nil, txn.ErrInactive
	}
	v, ok := value.(V)
	if !ok {
		return nil, errors.New("entity cache: incompatible creation type")
	}
	cache := binding.cache
	key, err := binding.keyOf(v)
	if err != nil {
		return nil, err
	}
	entry := &cacheEntry[V]{value: v, owner: owner, creating: true}
	entry.generation.Store(1)
	cache.mu.Lock()
	if old := cache.values[key]; old != nil {
		busy := old.creating || old.loading
		cache.mu.Unlock()
		if busy {
			return nil, ErrCreating
		}
		return nil, ErrExists
	}
	cache.nextIncarnation++
	entry.incarnation = cache.nextIncarnation
	cache.values[key] = entry
	cache.mu.Unlock()
	reservation := &cacheCreation[K, V]{cache: cache, key: key, entry: entry, keyOf: binding.keyOf}
	success := false
	defer func() {
		if !success {
			reservation.Abort()
		}
	}()
	_, exists, err := cache.loader(ctx, key)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrExists
	}

	release := func() { cache.mu.Lock(); entry.users--; cache.mu.Unlock() }
	if active {
		cache.mu.Lock()
		entry.users++
		cache.mu.Unlock()
		owner.OnFinish(release)
	} else {
		scope, _ := ctx.Value(scopeKey{}).(*scope)
		if scope == nil {
			return nil, ErrScopeRequired
		}
		scope.mu.Lock()
		if scope.closed {
			scope.mu.Unlock()
			return nil, ErrScopeClosed
		}
		cache.mu.Lock()
		entry.users++
		cache.mu.Unlock()
		scope.releases[entry] = release
		scope.mu.Unlock()
	}
	success = true
	return reservation, nil
}

type cacheCreation[K comparable, V any] struct {
	cache *Cache[K, V]
	key   K
	entry *cacheEntry[V]
	keyOf func(V) (K, error)
}

func (r *cacheCreation[K, V]) Validate() error {
	key, err := r.keyOf(r.entry.value)
	if err != nil {
		return err
	}
	if key != r.key {
		return errors.New("entity cache: created identity changed")
	}
	return nil
}
func (r *cacheCreation[K, V]) Commit() {
	r.cache.mu.Lock()
	r.entry.owner = nil
	r.entry.creating = false
	count := len(r.cache.values)
	r.cache.mu.Unlock()
	r.cache.policy.Set(r.key, r.entry)
	r.cache.activatePolicyReads(count)
}
func (r *cacheCreation[K, V]) Abort() {
	r.cache.mu.Lock()
	if r.cache.values[r.key] == r.entry {
		delete(r.cache.values, r.key)
	}
	r.cache.mu.Unlock()
}
