package framework

import (
	"context"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/txn"
	txruntime "github.com/ngnl47/tx/runtime"
	"testing"
)

func TestProcedureAccessDoesNotExposeOwner(t *testing.T) {
	r := &runtime{transaction: txn.NewRuntime(), selectedCMods: map[CModID]struct{}{CModProcedure: {}}}
	ctx := txruntime.Bind(t.Context(), newRuntimeAccess(r, Spec{Name: "game", Requires: []CModID{CModProcedure}}))
	for _, executor := range []txn.Executor{txruntime.Procedure(ctx), newServiceContext(r, nil).Transaction()} {
		if _, ok := executor.(interface{ Drain(context.Context) error }); ok {
			t.Fatal("普通事务执行入口暴露了运行时关闭能力")
		}
		calls := 0
		p := lifecycleProcedure(func(*txn.Tx) error { calls++; return nil })
		if err := executor.Call(ctx, p); err != nil {
			t.Fatal(err)
		}
		future, err := executor.Submit(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		if err := future.Await(ctx); err != nil {
			t.Fatal(err)
		}
		if calls != 2 {
			t.Fatalf("受限视图未执行 Call/Submit: %d", calls)
		}
	}
}

func TestServiceAccessIsolationAndLifetime(t *testing.T) {
	a := &runtime{config: Config{AppID: "app-a"}, selectedCMods: map[CModID]struct{}{CModActor: {}}}
	b := &runtime{config: Config{AppID: "app-b"}, selectedCMods: map[CModID]struct{}{CModActor: {}}}
	pa := newRuntimeAccess(a, Spec{Name: "game", Requires: []CModID{CModActor}})
	pb := newRuntimeAccess(b, Spec{Name: "game", Requires: []CModID{CModActor}})
	ca := txruntime.Bind(context.Background(), pa)
	cb := txruntime.Bind(context.Background(), pb)
	if txruntime.AppID(ca) != "app-a" || txruntime.AppID(cb) != "app-b" {
		t.Fatal("应用标识未跟随上下文")
	}
	if txruntime.ProviderFrom(ca) == txruntime.ProviderFrom(cb) {
		t.Fatal("应用作用域混用")
	}
	assertPanic := func(f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Error("应拒绝无效能力")
			}
		}()
		f()
	}
	assertPanic(func() { txruntime.MongoReader(ca) })
	assertPanic(func() { _, _ = txruntime.MongoDatabase(ca, "world", 1) })
	a.accessClosed.Store(true)
	assertPanic(func() { txruntime.Actor(ca) })
	assertPanic(func() { txruntime.AppID(ca) })
	assertPanic(func() { _, _ = txruntime.MongoDatabase(ca, "world", 1) })
}

func TestNativeMongoModeIsolation(t *testing.T) {
	access := modeStorageAccess{mode: "redis"}
	if _, err := access.MongoDatabase(t.Context(), "world", 1); err == nil {
		t.Fatal("Redis 能力视图泄露 Mongo 原生入口")
	}
}
func TestServiceTasksKeepAccess(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	p := newRuntimeAccess(&runtime{}, Spec{Name: "test"})
	tasks := taskAccess{Context: txruntime.Bind(root, p), root: root}
	result := make(chan bool, 1)
	if err := tasks.Go(func(ctx context.Context) { result <- txruntime.ProviderFrom(ctx) == p }); err != nil {
		t.Fatal(err)
	}
	if !<-result {
		t.Fatal("后台任务丢失服务作用域")
	}
	root.Cancel()
	root.Wait()
}

func TestServiceShutdownKeepsAccess(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(t.Context())
	r := &runtime{lifecycle: root}
	p := newRuntimeAccess(r, Spec{Name: "game"})
	r.serviceAccess = map[string]*runtimeAccess{"game": p}
	check := func(ctx context.Context) error {
		if txruntime.ProviderFrom(ctx) != p {
			t.Error("生命周期回调丢失服务能力")
		}
		return nil
	}
	registry, err := newRegistry(testService{spec: Spec{Name: "game"}, start: func(c Context) error { return check(c.Tasks()) }, drain: check, stop: check})
	if err != nil {
		t.Fatal(err)
	}
	host := newServiceHost(registry, r)
	if err := host.start([]string{"game"}); err != nil {
		t.Fatal(err)
	}
	if err := host.stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkBoundActorAccess(b *testing.B) {
	r := &runtime{selectedCMods: map[CModID]struct{}{CModActor: {}}}
	ctx := txruntime.Bind(context.Background(), newRuntimeAccess(r, Spec{Name: "game", Requires: []CModID{CModActor}}))
	b.ReportAllocs()
	for b.Loop() {
		_ = txruntime.Actor(ctx)
	}
}

// 启动绑定共享本次进程能力；服务绑定仍检查声明，关闭后全部失效。
type boundActorRuntime struct{ txactor.Runtime }

func TestStartupRuntimeBinding(t *testing.T) {
	r := &runtime{config: Config{AppID: "bound"}, actor: &boundActorRuntime{}, selectedCMods: map[CModID]struct{}{CModActor: {}}}
	kernel := newServiceContext(r, nil)
	setup := kernel.RuntimeAccess("setup", nil)
	declared := kernel.RuntimeAccess("game", []CModID{CModActor})
	restricted := kernel.RuntimeAccess("login", []CModID{})
	if setup.AppID() != "bound" || setup.Actor() != declared.Actor() {
		t.Fatal("绑定未复用同一进程")
	}
	reject := func(fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Error("能力应拒绝访问")
			}
		}()
		fn()
	}
	reject(func() { restricted.Actor() })
	reject(func() { setup.MongoBackfiller() })
	r.accessClosed.Store(true)
	reject(func() { setup.AppID() })
	reject(func() { declared.Actor() })
}

func TestEntityCacheSharedAcrossServicesAndIsolatedAcrossApplications(t *testing.T) {
	newApp := func() *runtime {
		tasks := lifecycle.NewWaitGroupContext(t.Context())
		t.Cleanup(func() { tasks.Cancel(); tasks.Wait() })
		return &runtime{lifecycle: tasks}
	}
	a, b := newApp(), newApp()
	game := newRuntimeAccess(a, Spec{Name: "game"})
	world := newRuntimeAccess(a, Spec{Name: "world"})
	other := newRuntimeAccess(b, Spec{Name: "game"})
	ca, err := game.EntityCache()
	if err != nil {
		t.Fatal(err)
	}
	cw, err := world.EntityCache()
	if err != nil {
		t.Fatal(err)
	}
	cb, err := other.EntityCache()
	if err != nil {
		t.Fatal(err)
	}
	if ca != cw || ca == cb {
		t.Fatal("缓存没有遵守应用资源边界")
	}
	a.accessClosed.Store(true)
	defer func() {
		if recover() == nil {
			t.Error("关闭应用仍能取得缓存")
		}
	}()
	_, _ = game.EntityCache()
}
