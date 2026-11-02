package entitycache

import (
	"context"

	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
	"github.com/ngnl47/tx/storage"
)

// Access 只提供实体访问与管理操作，生命周期由持有 Runtime 的宿主控制。
type Access struct{ runtime *Runtime }

// Access 返回同一缓存的受限视图，不创建缓存或后台任务。
func (r *Runtime) Access() *Access { return &r.access }

func (a *Access) Find(ctx context.Context, descriptor *Descriptor, key storage.RecordKey) (any, bool, error) {
	return a.runtime.Find(ctx, descriptor, key)
}

func (a *Access) Snapshot(ctx context.Context, descriptor *Descriptor, key storage.RecordKey) (any, bool, error) {
	return a.runtime.Snapshot(ctx, descriptor, key)
}

func (a *Access) ReserveCreate(ctx context.Context, value any) (txn.CreationReservation, error) {
	return a.runtime.ReserveCreate(ctx, value)
}

func (a *Access) ReserveRemove(ctx context.Context, value any) (writeback.RemovalReservation, error) {
	return a.runtime.ReserveRemove(ctx, value)
}

func (a *Access) Stats(ctx context.Context) (Stats, error) { return a.runtime.Stats(ctx) }

func (a *Access) List(ctx context.Context, options ListOptions) (EntryList, error) {
	return a.runtime.List(ctx, options)
}

func (a *Access) SnapshotCached(ctx context.Context, ref EntryRef) (any, error) {
	return a.runtime.SnapshotCached(ctx, ref)
}

func (a *Access) TryRelease(ctx context.Context, ref EntryRef) (ReleaseResult, error) {
	return a.runtime.TryRelease(ctx, ref)
}
