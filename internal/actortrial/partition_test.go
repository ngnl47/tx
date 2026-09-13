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
	host, err := ConnectSlotHost(url, ns, "a", runtime, 32, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := host.Drain(c); err != nil {
			t.Error(err)
		}
		host.conn.Close()
	})
	direct := directPeer(t, "a", runtime)
	directory := discovery.NewServiceDirectory()
	a := discovery.Instance{ID: "a", Services: []string{"game"}, Endpoint: direct.Endpoint(), Status: discovery.StatusRunning}
	if err := directory.Replace([]discovery.Instance{a}); err != nil {
		t.Fatal(err)
	}
	caller := peer(t, url, ns, "caller", echoRuntime())
	allocator := NewSlotAllocator(slotOwnership(t, store, directory), caller, "game", mapping)
	assignment, _, err := allocator.Ensure(ctx, ids[0])
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
	resolver, _ := watchedRoutes(t, store, directory)
	routes := []*RoutedCaller{{Resolver: resolver, SlotID: mapping, Transport: directSender(t, directory)}, {Resolver: resolver, SlotID: mapping, Transport: caller}}
	for round, route := range routes {
		for _, id := range ids {
			result, err := route.Call(ctx, id, &actorpb.Delivery{MessageType: "increment"})
			if err != nil {
				t.Fatal(err)
			}
			if result.CommandId != uint64(round+1) {
				t.Fatalf("entity state mixed: %+v %+v", id, result)
			}
		}
	}
	current := &actorrpc.RoutedCaller{Resolver: resolver, SlotID: mapping, Transport: caller}
	for _, id := range ids {
		result, err := current.Call(ctx, id, &actorpb.Delivery{MessageType: "increment"})
		if err != nil {
			t.Fatal(err)
		}
		if result.CommandId != 3 {
			t.Fatalf("migrated routing changed owner: %+v", result)
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
	// 指向错误实例的加载仍被权威记录拒绝，不能因共享分区而另建实体。
	wrong := echoRuntime()
	wrong.BindSlots("b", store, mapping)
	defer wrong.Drain(ctx)
	if _, err := wrong.Call(ctx, ids[0], &actorpb.Delivery{}); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("wrong owner activated: %v", err)
	}
}
