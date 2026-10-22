package actorrpc

import (
	"context"
	"testing"
	"time"

	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/rpc"
)

type routeBenchmarkCaller struct{ result *actorpb.Result }

func (c routeBenchmarkCaller) Call(context.Context, actorhost.Target, *actorpb.Delivery) (*actorpb.Result, error) {
	return c.result, nil
}

// 持久端留空，若路由错误地回源就会失败；本基准只测 Watch 缓存到传输调用边界。
type routeBenchmarkStore struct{ discovery.SlotStore }

func BenchmarkSharedCachedRoute(b *testing.B) {
	cache := discovery.NewSlotDirectory()
	if err := cache.Replace([]discovery.SlotAssignment{{SlotID: "world-51", RuntimeInstanceID: "a", Generation: 1}}); err != nil {
		b.Fatal(err)
	}
	store, err := discovery.NewCachedSlotStore(routeBenchmarkStore{}, cache)
	if err != nil {
		b.Fatal(err)
	}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{ID: "a", Services: []string{"world"}, Endpoint: "127.0.0.1:1", Status: discovery.StatusRunning}}); err != nil {
		b.Fatal(err)
	}
	resolver, err := discovery.NewSlotResolver(store, directory)
	if err != nil {
		b.Fatal(err)
	}
	caller := &RoutedCaller{Router: &EntityRouter{Resolver: resolver, SlotID: worldSlot, Service: func(string) (string, error) { return "world", nil }}, Transport: routeBenchmarkCaller{&actorpb.Result{MessageType: "ok"}}}
	id, delivery := actorhost.Identity{Type: "world", ID: "51"}, &actorpb.Delivery{MessageType: "read"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if result, err := caller.Call(b.Context(), id, delivery); err != nil || result.MessageType != "ok" {
			b.Fatalf("%v %v", result, err)
		}
	}
}

// 单机 gRPC 完成调用基线，不与绕行 Docker broker 的 NATS 延迟直接相除。
func BenchmarkDirectActorCall(b *testing.B) {
	r := echoRuntime()
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Drain(ctx); err != nil {
			b.Error(err)
		}
	})
	host := directPeer(b, "a", r)
	directory := discovery.NewServiceDirectory()
	routeMembers(b, directory, map[string]*rpc.Host{"a": host})
	caller := directSender(b, directory)
	ctx, cancel := context.WithTimeout(b.Context(), time.Minute)
	defer cancel()
	target, delivery := actorhost.Target{Instance: "a", Identity: actorhost.Identity{Type: "world", ID: "51"}}, &actorpb.Delivery{MessageType: "ack", Payload: make([]byte, 1024)}
	if _, err := caller.Call(ctx, target, delivery); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := caller.Call(ctx, target, delivery)
		if err != nil || result.MessageType != "ok" {
			b.Fatalf("%v %v", result, err)
		}
	}
}
