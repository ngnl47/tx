package framework

import (
	"context"
	"testing"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/modules/entitycache"
	"github.com/ngnl47/tx/modules/writeback"
	txruntime "github.com/ngnl47/tx/runtime"
)

func TestEntityCacheAccessDoesNotExposeOwner(t *testing.T) {
	tasks := lifecycle.NewWaitGroupContext(t.Context())
	t.Cleanup(func() { tasks.Cancel(); tasks.Wait() })
	r := &runtime{lifecycle: tasks}
	provider := newRuntimeAccess(r, Spec{Name: "game"})
	ctx := txruntime.Bind(t.Context(), provider)
	access, err := txruntime.EntityCache(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 显式缓存绑定与服务入口必须返回相同的受限能力，不能取回宿主。
	bound, err := txruntime.EntityCache(entitycache.Bind(ctx, r.entityCache))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{access, bound} {
		if _, ok := value.(interface{ Start(lifecycle.Tasks) error }); ok {
			t.Error("业务缓存入口暴露了 Start")
		}
		if _, ok := value.(interface{ DrainManagement(context.Context) error }); ok {
			t.Error("业务缓存入口暴露了 DrainManagement")
		}
		if _, ok := value.(interface{ CloseManagement(context.Context) error }); ok {
			t.Error("业务缓存入口暴露了 CloseManagement")
		}
		if _, ok := value.(writeback.CreationCache); ok {
			t.Error("业务缓存入口暴露了创建预留，允许绕过事务与写回登记")
		}
		if _, ok := value.(writeback.RemovalCache); ok {
			t.Error("业务缓存入口暴露了删除预留，允许绕过事务与写回登记")
		}
	}
}

func BenchmarkEntityCacheCapabilityAccess(b *testing.B) {
	tasks := lifecycle.NewWaitGroupContext(b.Context())
	b.Cleanup(func() { tasks.Cancel(); tasks.Wait() })
	r := &runtime{lifecycle: tasks, selectedCMods: map[CModID]struct{}{CModMongoStorage: {}}}
	provider := newRuntimeAccess(r, Spec{Name: "game", Requires: []CModID{CModMongoStorage}})
	ctx := txruntime.Bind(b.Context(), provider)
	if _, err := txruntime.EntityCache(ctx); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		provider.RequireStorage(meta.StorageModeMongo)
		if _, err := txruntime.EntityCache(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
