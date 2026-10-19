package actorrpc

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
)

// 同一路由入口按 Kind 选择服务池；共享服务的 Kind 保持独立归属和实体状态。
func TestEntityRouterSelectsKindService(t *testing.T) {
	url, ctx := natsURL(t), actorTestContext(t)
	store, ns := actorTestSlots(t), nats.NewInbox()
	directory := discovery.NewServiceDirectory()
	var members []discovery.Instance
	var caller *NATSTransport
	for _, service := range []string{"game", "slgworld"} {
		local := actorhost.New(8, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
			count := uint64(0)
			return &actorhost.Behavior{Receive: func(actorhost.Context, any) (any, error) {
				count++
				return &actorpb.Result{CommandHash: uint32(count), MessageType: service}, nil
			}}, nil
		})
		local.BindSlots(service, store, nil)
		transport, err := ConnectNATS(url, ns, service, local, 32)
		if err != nil {
			t.Fatal(err)
		}
		defer transport.Close()
		defer local.Drain(ctx)
		caller = transport
		members = append(members, discovery.Instance{ID: service, Services: []string{service}, Endpoint: "127.0.0.1:1", Status: discovery.StatusRunning})
	}
	if err := directory.Replace(members); err != nil {
		t.Fatal(err)
	}
	resolver, _ := watchedRoutes(t, store, directory)
	services := map[string]string{"role": "game", "world": "slgworld", "seasonWorld": "slgworld"}
	router := &EntityRouter{Resolver: resolver, Ownership: slotOwnership(t, store, directory), Transport: caller,
		Service: func(kind string) (string, error) {
			service, ok := services[kind]
			if !ok {
				return "", fmt.Errorf("unknown kind %q", kind)
			}
			return service, nil
		},
	}
	messenger := &NATSMessenger{Router: router}
	for kind, service := range services {
		id := actorhost.Identity{Kind: kind, Key: "1"}
		for count := uint64(1); count <= 2; count++ {
			result, err := messenger.Call(ctx, id, &actorpb.Delivery{})
			if err != nil || result.MessageType != service || result.CommandHash != uint32(count) {
				t.Fatalf("Kind %s 路由或状态串用: %+v %v", kind, result, err)
			}
		}
		assignment, err := store.ResolveSlot(ctx, actorhost.PartitionSlot(id))
		if err != nil || assignment.RuntimeInstanceID != service {
			t.Fatalf("归属: %+v %v", assignment, err)
		}
	}
	unknown := actorhost.Identity{Kind: "unknown", Key: "1"}
	if _, err := router.Resolve(ctx, unknown); err == nil {
		t.Fatal("未知 Kind 被接受")
	}
	if _, _, err := router.Ensure(ctx, unknown); err == nil {
		t.Fatal("未知 Kind 被分配")
	}
	if _, err := store.ResolveSlot(ctx, actorhost.PartitionSlot(unknown)); !errors.Is(err, discovery.ErrSlotNotFound) {
		t.Fatalf("未知 Kind 写入归属: %v", err)
	}
	// 已有世界归属仍在线，但不承载目录要求的服务；冷、热路径均应拒绝。
	services["world"] = "game"
	world := actorhost.Identity{Kind: "world", Key: "1"}
	if _, _, err := router.Ensure(ctx, world); !errors.Is(err, discovery.ErrSlotRuntimeUnavailable) {
		t.Fatalf("冷路径服务池校验: %v", err)
	}
	if _, err := router.Resolve(ctx, world); !errors.Is(err, discovery.ErrSlotRuntimeUnavailable) {
		t.Fatalf("缓存路径绕过服务池校验: %v", err)
	}

	// 即使残留 assignment 可解析，目录移除 Kind 后也不能继续投递。
	delete(services, "world")
	if _, err := router.Resolve(ctx, actorhost.Identity{Kind: "world", Key: "1"}); err == nil {
		t.Fatal("缓存绕过 Kind 目录")
	}
}
