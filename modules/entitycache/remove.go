package entitycache

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
	"github.com/ngnl47/tx/storage"
)

func (r *Runtime) ReserveRemove(ctx context.Context, value any) (writeback.RemovalReservation, error) {
	e, err := r.mutable(ctx, value)
	if err != nil {
		return nil, err
	}
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()
	if e.removed || e.deleting {
		return nil, storage.ErrDeletionPending
	}
	owner, _ := txn.FromContext(ctx)
	e.removing = owner
	e.deleting = true
	return &removal{runtime: r, entry: e}, nil
}

// mutable 验证原对象身份，并将其使用权与记录锁保留到本事务结束。
func (r *Runtime) mutable(ctx context.Context, value any) (*entity, error) {
	if _, err := requireProcedure(ctx); err != nil {
		return nil, err
	}
	e, err := r.managed(value)
	if err != nil {
		return nil, err
	}
	managed, exists, err := r.Find(ctx, e.id.descriptor, e.id.record)
	if err != nil {
		return nil, err
	}
	if !exists || managed != value {
		return nil, storage.ErrDeletionPending
	}
	return e, nil
}
func (r *Runtime) managed(value any) (*entity, error) {
	d, ok := value.(Described)
	if !ok {
		return nil, errors.New("entity cache: generated descriptor is required")
	}
	descriptor := d.EntityDescriptor()
	id := identity{descriptor, descriptor.Store(value).RecordKey()}
	r.cache.mu.RLock()
	defer r.cache.mu.RUnlock()
	cached := r.cache.values[id]
	if cached == nil || cached.loading || cached.value.value != value {
		return nil, errors.New("entity cache: value is not the managed instance")
	}
	return cached.value, nil
}

type removal struct {
	runtime *Runtime
	entry   *entity
}

func (r *removal) Validate() error { return nil }
func (r *removal) Commit() {
	r.runtime.cache.mu.Lock()
	r.entry.removed = true
	r.entry.removing = nil
	r.runtime.cache.mu.Unlock()
}
func (r *removal) Abort() {
	r.runtime.cache.mu.Lock()
	r.entry.deleting = false
	r.entry.removing = nil
	r.runtime.cache.mu.Unlock()
}
func (r *removal) Complete() {
	c := r.runtime.cache
	c.mu.Lock()
	entry := c.values[r.entry.id]
	if entry != nil && entry.value == r.entry {
		// 显式删除同时退休 policy 身份，旧淘汰回调不得阻挡同 ID 的新对象。
		entry.retired.Store(true)
		c.policy.Invalidate(r.entry.id)
		c.deferredMu.Lock()
		if candidate, ok := c.deferred[r.entry.id]; ok && candidate.entry == entry {
			delete(c.deferred, r.entry.id)
			c.deferredN.Add(-1)
		}
		c.deferredMu.Unlock()
		delete(c.values, r.entry.id)
	}
	c.mu.Unlock()
}
