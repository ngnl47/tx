package actortrial

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ngnl47/tx/internal/actorrpc"
	txactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
)

// 同分区共享归属而不共享实体状态，两条传输共用默认路由和权威加载校验。
func TestPartitionRoutingSharesAssignmentNotEntity(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mapping txactor.SlotMapping
		ids     []Identity
		slot    string
	}{
		{"default", nil, []Identity{{"role", "8"}, {"role", "26"}}, "actor-v1.cm9sZQ.p100"},
		{"zone", func(id Identity) string { return map[string]string{"1": "role-zone-10", "2": "role-zone-10"}[id.Key] }, []Identity{{"role", "1"}, {"role", "2"}}, "role-zone-10"},
	} {
		t.Run(tc.name, func(t *testing.T) { testSharedPartition(t, tc.mapping, tc.ids, tc.slot) })
	}
}

func testSharedPartition(t *testing.T, mapping txactor.SlotMapping, ids []Identity, slot string) {
	url, ctx := natsURL(t), trialContext(t)
	store, ns := trialSlots(t), nats.NewInbox()
	runtime := New(8, time.Second, func(Context) (*Behavior, error) {
		count := uint64(0)
		return &Behavior{Receive: func(Context, any) (any, error) {
			count++
			return &actorpb.Result{MessageType: "count", CommandId: count}, nil
		}}, nil
	})
	runtime.BindSlots("a", store, mapping)
	host, err := actorrpc.ConnectNATS(url, ns, "a", runtime, 32)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Drain(c); err != nil {
			t.Error(err)
		}
		host.Close()
	})
	direct := directPeer(t, "a", runtime)
	directory := discovery.NewServiceDirectory()
	a := discovery.Instance{ID: "a", Services: []string{"game"}, Endpoint: direct.Endpoint(), Status: discovery.StatusRunning}
	if err := directory.Replace([]discovery.Instance{a}); err != nil {
		t.Fatal(err)
	}
	callerRuntime := echoRuntime()
	caller, err := actorrpc.ConnectNATS(url, ns, "caller", callerRuntime, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer caller.Close()
	defer callerRuntime.Drain(ctx)
	resolver, _ := watchedRoutes(t, store, directory)
	allocator := &actorrpc.EntityRouter{Resolver: resolver, Ownership: slotOwnership(t, store, directory), Transport: caller, Service: "game", SlotID: mapping}
	// 拒绝容量时不得先写 assignment 或触发业务加载。
	runtime.SetActivationLimits(ActivationLimits{Admit: func() bool { return false }})
	if _, _, err := allocator.Ensure(ctx, ids[0]); !errors.Is(err, ErrCapacity) {
		t.Fatalf("容量预检: %v", err)
	}
	if _, err := store.ResolveSlot(ctx, slot); !errors.Is(err, discovery.ErrSlotNotFound) {
		t.Fatalf("容量拒绝仍创建归属: %v", err)
	}
	if runtime.Probe(ids[0]).State != Unloaded {
		t.Fatal("容量预检触发了加载")
	}
	runtime.SetActivationLimits(ActivationLimits{})
	cold := &actorrpc.RoutedCaller{Router: allocator, Transport: directSender(t, directory)}
	if result, err := cold.Call(ctx, ids[0], &actorpb.Delivery{MessageType: "increment"}); err != nil || result.CommandId != 1 {
		t.Fatalf("首次直连调用: %+v %v", result, err)
	}
	assignment, err := store.ResolveSlot(ctx, slot)
	if err != nil {
		t.Fatal(err)
	}
	if assignment.SlotID != slot || assignment.RuntimeInstanceID != "a" {
		t.Fatalf("assignment: %+v", assignment)
	}
	// 新增实例不重算已经存在的归属；无需连接未被选中的 b。
	if err := directory.Replace([]discovery.Instance{a, {ID: "b", Services: []string{"game"}, Endpoint: "127.0.0.1:1", Status: discovery.StatusRunning}}); err != nil {
		t.Fatal(err)
	}
	second, _, err := allocator.Ensure(ctx, ids[1])
	if err != nil || second != assignment {
		t.Fatalf("partition reassigned: %+v %v", second, err)
	}
	routes := []*RoutedCaller{{Resolver: resolver, SlotID: mapping, Transport: directSender(t, directory)}, {Resolver: resolver, SlotID: mapping, Transport: caller}}
	for round, route := range routes {
		for _, id := range ids {
			result, err := route.Call(ctx, id, &actorpb.Delivery{MessageType: "increment"})
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(round + 1)
			if id == ids[0] {
				want++
			}
			if result.CommandId != want {
				t.Fatalf("entity state mixed: %+v %+v", id, result)
			}
		}
	}
	// 使用独立快照验证热路由与失效行为，避免 Watch 推进干扰断言。
	cache := discovery.NewSlotDirectory()
	cached, err := discovery.NewCachedSlotStore(store, cache)
	if err != nil {
		t.Fatal(err)
	}
	hotResolver, err := discovery.NewSlotResolver(cached, directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Replace([]discovery.SlotAssignment{assignment}); err != nil {
		t.Fatal(err)
	}
	hot := &actorrpc.EntityRouter{Resolver: hotResolver, SlotID: mapping, Transport: caller}
	current := &actorrpc.NATSMessenger{Router: hot}
	for _, id := range ids {
		message := &actorpb.Delivery{MessageType: "increment"}
		if err := current.Tell(ctx, id, message); err != nil {
			t.Fatal(err)
		}
		if err := current.TellAccepted(ctx, id, message); err != nil {
			t.Fatal(err)
		}
		future, err := current.CallFuture(ctx, id, message)
		if err != nil {
			t.Fatal(err)
		}
		result, err := future.Await(ctx)
		want := uint64(5)
		if id == ids[0] {
			want++
		}
		if err != nil || result.CommandId != want {
			t.Fatalf("四种语义未共用归属或发生重投: %+v %v", result, err)
		}
		result, err = current.Call(ctx, id, message)
		if err != nil || result.CommandId != want+1 {
			t.Fatalf("同步结果: %+v %v", result, err)
		}
	}

	if mapping != nil {
		unknown := Identity{"role", "missing-zone"}
		if _, _, err := allocator.Ensure(ctx, unknown); err == nil {
			t.Fatal("missing group fell back to default partition")
		}
		if runtime.Probe(unknown).State != Unloaded {
			t.Fatal("missing group activated an entity")
		}
	}
	cache.Invalidate()
	if _, err := hot.Resolve(ctx, ids[0]); !errors.Is(err, discovery.ErrSlotDirectoryUnavailable) {
		t.Fatalf("失效快照触发了分配: %v", err)
	}
	// 指向错误实例的加载仍被权威记录拒绝，不能因共享分区而另建实体。
	wrong := echoRuntime()
	wrong.BindSlots("b", store, mapping)
	defer wrong.Drain(ctx)
	if _, err := wrong.Call(ctx, ids[0], &actorpb.Delivery{}); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("wrong owner activated: %v", err)
	}
}
