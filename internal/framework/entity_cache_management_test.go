package framework

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/modules/entitycache"
)

// 提前取得与排水中惰性创建的缓存，都必须继承管理准入；业务资源仍由原排水流程持有。
func TestEntityCacheManagementFollowsServerLifecycle(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		t.Run(map[bool]string{false: "existing", true: "lazy"}[lazy], func(t *testing.T) {
			var cached *entitycache.Runtime
			app, err := New(Config{AppID: "cache-management"}, WithService(testService{spec: Spec{Name: "game"}}))
			if err != nil {
				t.Fatal(err)
			}
			if err = app.Start(t.Context(), RunOptions{Services: []string{"game"}}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := app.Stop(context.Background()); err != nil {
					t.Error(err)
				}
			})
			access := newRuntimeAccess(app.runtime, Spec{Name: "game"})
			if !lazy {
				cached, err = access.EntityCache()
				if err != nil {
					t.Fatal(err)
				}
			}
			if err = app.Drain(t.Context()); err != nil {
				t.Fatal(err)
			}
			if lazy {
				cached, err = access.EntityCache()
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err = cached.Stats(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err = cached.SnapshotCached(t.Context(), entitycache.EntryRef{}); !errors.Is(err, entitycache.ErrManagementDraining) {
				t.Fatal("排水仍允许管理快照", err)
			}
			if err = app.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err = cached.Stats(t.Context()); !errors.Is(err, entitycache.ErrManagementClosed) {
				t.Fatal("旧句柄在关闭后仍可用", err)
			}
		})
	}
}
