package framework

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/rpc"
	"google.golang.org/grpc/credentials/insecure"
)

func placementFixture(t *testing.T, resolver txactor.GroupResolver) (*actorPlacement, *runtime, []Service) {
	t.Helper()
	factory := func(txactor.Context) (*txactor.Behavior, error) { return &txactor.Behavior{}, nil }
	spec := Spec{Name: "game", ActorTypes: []txactor.Type{
		txactor.MustType("role", factory, txactor.WithPlacement("server")),
		txactor.MustType("legion", factory, txactor.WithPlacement("server")),
	}, Placements: []Placement{{Name: "server", Factory: func(Context) (txactor.GroupResolver, error) { return resolver, nil }}}}
	services := []Service{testService{spec: spec}}
	catalog, err := collectActorTypes(services)
	if err != nil {
		t.Fatal(err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	directory := discovery.NewServiceDirectory()
	dialer, _ := rpc.NewGRPCDialer(insecure.NewCredentials())
	sender, err := rpc.NewSender(directory, dialer)
	if err != nil {
		t.Fatal(err)
	}
	host, err := rpc.NewHost("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rt := &runtime{lifecycle: root, actorCatalog: catalog, rpcSender: sender, rpc: host, directory: directory}
	p, err := newActorPlacement(rt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = p.close(context.Background())
		_ = host.Stop(context.Background())
		_ = sender.Close()
		root.Cancel()
		root.Wait()
	})
	return p, rt, services
}

func TestPlacementLocalRemoteAndProviderReplacement(t *testing.T) {
	var reads atomic.Int32
	resolve := func(_ context.Context, id txactor.Identity) (string, error) {
		reads.Add(1)
		if id.ID == "missing" {
			return "", errors.New("not found")
		}
		return "1001", nil
	}
	local, rt, services := placementFixture(t, resolve)
	if err := local.initialize(rt, services); err != nil {
		t.Fatal(err)
	}
	if err := rt.rpc.Start(rt.lifecycle, nil); err != nil {
		t.Fatal(err)
	}
	remote, caller, _ := placementFixture(t, func(context.Context, txactor.Identity) (string, error) {
		t.Fatal("远端不得构造或执行业务查询")
		return "", nil
	})
	if err := remote.initialize(caller, nil); err != nil {
		t.Fatal(err)
	}
	replace := func(endpoint string) {
		t.Helper()
		if err := caller.directory.Replace([]discovery.Instance{{ID: endpoint, Endpoint: endpoint, Status: discovery.StatusRunning, Services: []string{"game"}}}); err != nil {
			t.Fatal(err)
		}
	}
	replace(rt.rpc.Endpoint())
	want := actorhost.PlacementSlot("game", "server", "1001")
	for _, p := range []*actorPlacement{local, remote} {
		for _, typ := range []string{"role", "legion"} {
			for range 3 {
				if got, err := p.resolve(t.Context(), txactor.Identity{Type: typ, ID: "7"}); err != nil || got != want {
					t.Fatalf("%s %v", got, err)
				}
			}
		}
	}
	if reads.Load() != 2 {
		t.Fatalf("缓存未复用: %d", reads.Load())
	}
	for range 2 {
		if _, err := remote.resolve(t.Context(), txactor.Identity{Type: "role", ID: "missing"}); err == nil {
			t.Fatal("未传播失败")
		}
	}
	if reads.Load() != 4 {
		t.Fatal("失败被缓存")
	}
	local.ingress.BeginDrain()
	if _, err := remote.resolve(t.Context(), txactor.Identity{Type: "role", ID: "8"}); err == nil {
		t.Fatal("排水后仍接纳冷查询")
	}
	if got, err := remote.resolve(t.Context(), txactor.Identity{Type: "role", ID: "7"}); err != nil || got != want {
		t.Fatal("已有缓存应继续可读")
	}
	next, nextRT, nextServices := placementFixture(t, resolve)
	if err := next.initialize(nextRT, nextServices); err != nil {
		t.Fatal(err)
	}
	if err := nextRT.rpc.Start(nextRT.lifecycle, nil); err != nil {
		t.Fatal(err)
	}
	replace(nextRT.rpc.Endpoint())
	if got, err := remote.resolve(t.Context(), txactor.Identity{Type: "role", ID: "8"}); err != nil || got != want {
		t.Fatalf("替换提供方: %s %v", got, err)
	}
}

func TestPlacementWaitersAndDrainTrackActualQuery(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	p, rt, services := placementFixture(t, func(context.Context, txactor.Identity) (string, error) { close(entered); <-release; return "1001", nil })
	if err := p.initialize(rt, services); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := p.resolve(ctx, txactor.Identity{Type: "role", ID: "7"}); done <- err }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stopped := make(chan struct{})
	go func() { _ = p.close(context.Background()); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("查询未退出便释放资源")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("未收尾")
	}
}

func TestPlacementCatalogRejectsMissingProviderAndSkipsDefaultRuntime(t *testing.T) {
	factory := func(txactor.Context) (*txactor.Behavior, error) { return &txactor.Behavior{}, nil }
	_, err := collectActorTypes([]Service{testService{spec: Spec{Name: "game", ActorTypes: []txactor.Type{txactor.MustType("role", factory, txactor.WithPlacement("server"))}}}})
	if err == nil {
		t.Fatal("未声明提供方却允许引用")
	}
	p, err := newActorPlacement(&runtime{actorCatalog: map[string]actorTypeSpec{"role": {actorType: txactor.MustType("role", factory)}}})
	if err != nil || p != nil {
		t.Fatal("默认分区不应创建分组缓存或查询服务")
	}
}

func TestPlacementCloseDeadlineRetainsWorkForRetry(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	p, rt, services := placementFixture(t, func(context.Context, txactor.Identity) (string, error) { close(entered); <-release; return "1001", nil })
	if err := p.initialize(rt, services); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = p.resolve(t.Context(), txactor.Identity{Type: "role", ID: "7"}) }()
	<-entered
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	rt.placement = p
	server := &Server{runtime: rt}
	err := server.finishWork(ctx, false)
	close(release)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errStopDeferred) {
		t.Fatalf("首次关闭: %v", err)
	}
	if err := server.finishWork(t.Context(), false); err != nil {
		t.Fatalf("实际退出后重试: %v", err)
	}
}
