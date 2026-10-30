package framework

import (
	"context"

	"github.com/ngnl47/tx/modules/entitycache"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
)

// EntityCache 按首次生成查询创建，多个子服务 Provider 共用当前应用的对象身份。
func (p *runtimeAccess) EntityCache() (*entitycache.Runtime, error) {
	p.requireOpen()
	r := p.runtime
	r.entityCacheMu.Lock()
	defer r.entityCacheMu.Unlock()
	if r.entityCacheClosed {
		return nil, entitycache.ErrManagementClosed
	}
	if r.entityCache != nil {
		return r.entityCache, nil
	}
	cache, err := entitycache.NewRuntime(r.config.Runtime.EntityCache, func(ctx context.Context, value any) (bool, error) {
		if lockable, ok := value.(txn.StoreProvider); ok {
			if release, ok := r.commitParticipant.(interface {
				TryRelease(context.Context, txn.StoreTarget) (bool, error)
			}); ok {
				return release.TryRelease(ctx, lockable.StoreTarget())
			}
		}
		d := value.(entitycache.Described).EntityDescriptor()
		var clean bool
		err := txn.WithLocks(ctx, txn.Lockeys{value.(txn.StoreProvider).StoreTarget().Lock}, func(context.Context) error { clean = !d.Store(value).HasChanges(); return nil })
		return clean, err
	})
	if err != nil {
		return nil, err
	}
	if binder, ok := r.commitParticipant.(interface {
		BindCreationResolver(writeback.CreationCache) error
	}); ok {
		if err := binder.BindCreationResolver(cache); err != nil {
			return nil, err
		}
	}
	if err := cache.Start(r.lifecycle); err != nil {
		return nil, err
	}
	if r.entityCacheDraining {
		_ = cache.DrainManagement(context.Background())
	}
	r.entityCache = cache
	return cache, nil
}

// stopEntityCacheManagement 与惰性创建共用状态锁；停止管理不撤销排水中的业务能力。
func (r *runtime) stopEntityCacheManagement(ctx context.Context, final bool) error {
	r.entityCacheMu.Lock()
	r.entityCacheDraining = true
	r.entityCacheClosed = r.entityCacheClosed || final
	cache := r.entityCache
	r.entityCacheMu.Unlock()
	if cache == nil {
		return nil
	}
	if final {
		return cache.CloseManagement(ctx)
	}
	return cache.DrainManagement(ctx)
}
