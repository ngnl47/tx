package framework

import (
	"context"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	txruntime "github.com/ngnl47/tx/runtime"
	"testing"
)

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
	a.accessClosed.Store(true)
	assertPanic(func() { txruntime.Actor(ca) })
	assertPanic(func() { txruntime.AppID(ca) })
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
