package framework

import (
	"context"
	"crypto/rand"
	"errors"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	clientv3 "go.etcd.io/etcd/client/v3"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 正式双实例验证元数据控制不排队，预算在加载、失败清理和卸载间保持一致。
func TestFormalActorActivationControl(t *testing.T) {
	for _, scope := range []txactor.CapacityScope{txactor.CapacityActivations, txactor.CapacityAssignments} {
		t.Run(string(scope), func(t *testing.T) { testFormalActorActivationControl(t, scope) })
	}
}

func testFormalActorActivationControl(t *testing.T, scope txactor.CapacityScope) {
	url, endpoint := os.Getenv("TX_ACTORTRIAL_NATS_URL"), os.Getenv("TX_ETCD_ENDPOINT")
	if url == "" || endpoint == "" {
		t.Skip("设置 NATS/etcd 地址验证正式激活入口")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	owner := rand.Text()
	prefix := "/tx/tests/activation/" + owner
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	marker := prefix + "/owner"
	created, err := client.Txn(ctx).If(clientv3.Compare(clientv3.Version(marker), "=", 0)).Then(clientv3.OpPut(marker, owner)).Commit()
	if err != nil || !created.Succeeded {
		t.Fatalf("隔离范围: %v", err)
	}
	defer func() {
		if t.Failed() {
			t.Log("保留失败范围", prefix)
			return
		}
		c, end := context.WithTimeout(context.Background(), time.Second)
		defer end()
		result, err := client.Txn(c).If(clientv3.Compare(clientv3.Value(marker), "=", owner)).Then(clientv3.OpDelete(prefix+"/", clientv3.WithPrefix())).Commit()
		if err != nil || !result.Succeeded {
			t.Errorf("清理隔离范围: %v", err)
		}
	}()
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var loads atomic.Int32
	var allow atomic.Bool
	kind := txactor.MustType("world", func(c txactor.Context) (*txactor.Behavior, error) {
		loads.Add(1)
		if c.Identity.ID == "blocked" {
			<-release
		}
		if c.Identity.ID == "denied" && !allow.Load() {
			return nil, txactor.ErrActivationDenied
		}
		return &txactor.Behavior{Receive: func(c txactor.Context, _ any) (any, error) {
			c.Deactivate()
			return txactor.Result{MessageType: "done"}, nil
		}}, nil
	}, txactor.WithSlotMapping(func(id txactor.Identity) string { return "world-" + id.ID }))
	var access txactor.Runtime
	registrations := []Service{testService{spec: Spec{Name: "slgworld", ActorTypes: []txactor.Type{kind}}}, testService{spec: Spec{Name: "game", Requires: []CModID{CModActor}}, start: func(c Context) error { access = c.Actor(); return nil }}}
	start := func(service string) *Server {
		app := newDiscoveryIdentityTestServer(t, endpoint, prefix, "", registrations...)
		app.config.Actor.Runtime = txactor.Config{NATSURL: url, Namespace: owner, Capacity: txactor.Capacity{Maximum: 1, Scope: scope, Initializations: 1}}
		if err := app.Start(ctx, RunOptions{Services: []string{service}, RPCListen: "127.0.0.1:0"}); err != nil {
			t.Fatal(err)
		}
		return app
	}
	world := start("slgworld")
	defer world.Stop(context.Background())
	game := start("game")
	defer game.Stop(context.Background())
	defer unblock()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	wait := func(check func() bool) {
		for !check() {
			select {
			case <-tick.C:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
	wait(func() bool { return len(game.discovery.Directory().RunningInstances("slgworld")) == 1 })
	id := txactor.Identity{Type: "world", ID: "blocked"}
	state, err := access.Probe(ctx, id)
	if err != nil || state.State != txactor.Unloaded || loads.Load() != 0 {
		t.Fatalf("探测触发加载: %+v %v", state, err)
	}
	if _, err := world.discovery.AuthoritativeSlots().ResolveSlot(ctx, "world-blocked"); !errors.Is(err, discovery.ErrSlotNotFound) {
		t.Fatalf("探测创建归属: %v", err)
	}
	state, err = access.EnsureActivation(ctx, id)
	if err != nil || state.State != txactor.Initializing {
		t.Fatalf("发起加载: %+v %v", state, err)
	}
	wait(func() bool { return loads.Load() == 1 })
	for range 3 {
		state, err = access.Probe(ctx, id)
		if err != nil || state.State != txactor.Initializing {
			t.Fatalf("初始化阻塞探测: %+v %v", state, err)
		}
		if _, err = access.EnsureActivation(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	other := txactor.Identity{Type: "world", ID: "denied"}
	if _, err = access.EnsureActivation(ctx, other); !errors.Is(err, txactor.ErrCapacity) {
		t.Fatalf("超预算: %v", err)
	}
	if _, err := world.discovery.AuthoritativeSlots().ResolveSlot(ctx, "world-denied"); !errors.Is(err, discovery.ErrSlotNotFound) {
		t.Fatalf("预检失败创建归属: %v", err)
	}
	unblock()
	wait(func() bool { s, e := access.Probe(ctx, id); return e == nil && s.State == txactor.Ready })
	if _, err = access.Call(ctx, id, txactor.Delivery{MessageType: "finish"}); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { s, e := access.Probe(ctx, id); return e == nil && s.State == txactor.Unloaded })
	if scope == txactor.CapacityAssignments {
		if _, err := access.EnsureActivation(ctx, other); !errors.Is(err, txactor.ErrCapacity) {
			t.Fatalf("卸载错误释放世界归属名额: %v", err)
		}
		extra := start("slgworld")
		defer extra.Stop(context.Background())
		wait(func() bool { return len(game.discovery.Directory().RunningInstances("slgworld")) == 2 })
	}
	if _, err = access.EnsureActivation(ctx, other); err != nil {
		t.Fatal(err)
	}
	wait(func() bool {
		s, e := access.Probe(ctx, other)
		return e == nil && s.State == txactor.Failed && errors.Is(s.Failure, txactor.ErrActivationDenied)
	})
	allow.Store(true)
	state, err = access.EnsureActivation(ctx, other)
	if err != nil || state.State != txactor.Failed || loads.Load() != 2 {
		t.Fatalf("隐式重试了失败激活: %+v %v %d", state, err, loads.Load())
	}
	if _, err = access.RetryActivation(ctx, other); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { s, e := access.Probe(ctx, other); return e == nil && s.State == txactor.Ready })
	if loads.Load() != 3 {
		t.Fatalf("共享加载/显式重试次数: %d", loads.Load())
	}
	if scope == txactor.CapacityAssignments {
		before, err := world.discovery.AuthoritativeSlots().ResolveSlot(ctx, "world-blocked")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := access.EnsureActivation(ctx, id); err != nil {
			t.Fatalf("原世界重载占用了新名额: %v", err)
		}
		wait(func() bool { s, e := access.Probe(ctx, id); return e == nil && s.State == txactor.Ready })
		after, err := world.discovery.AuthoritativeSlots().ResolveSlot(ctx, "world-blocked")
		if err != nil || before != after {
			t.Fatalf("重载改变了归属: %+v %v", after, err)
		}
	}

}

func TestAssignmentCapacityRequiresIndependentSlotsBeforeConnecting(t *testing.T) {
	kind := txactor.MustType("world", func(txactor.Context) (*txactor.Behavior, error) { t.Fatal("声明阶段不应加载"); return nil, nil })
	app, err := New(Config{AppID: "budget", Discovery: DiscoveryConfig{Etcd: discovery.EtcdConfig{Endpoints: []string{"127.0.0.1:1"}, Prefix: "/tx/test-budget", LeaseTTL: time.Second, DialTimeout: time.Second}}, Actor: ActorConfig{Runtime: txactor.Config{NATSURL: "nats://127.0.0.1:1", Capacity: txactor.Capacity{Maximum: 1, Scope: txactor.CapacityAssignments}}}}, WithService(testService{spec: Spec{Name: "world", ActorTypes: []txactor.Type{kind}}}))
	if err != nil {
		t.Fatal(err)
	}
	if err = app.Start(t.Context(), RunOptions{Services: []string{"world"}}); err == nil || !strings.Contains(err.Error(), "independent slots") {
		t.Fatalf("应在连接前拒绝缺失独立映射: %v", err)
	}
}
