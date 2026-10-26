package actorrpc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func directPeer(t testing.TB, instance string, r *actorhost.Runtime) *rpc.Host {
	t.Helper()
	host, err := rpc.NewHost("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	t.Cleanup(func() {
		if err := host.Stop(context.Background()); err != nil {
			t.Error(err)
		}
		root.Cancel()
		root.Wait()
	})
	if err := host.Register(func(registrar grpc.ServiceRegistrar) error {
		return rpc.RegisterDeliveryService(registrar, nil, NewDirectReceiver(instance, r))
	}); err != nil {
		t.Fatal(err)
	}
	if err := host.Start(root); err != nil {
		t.Fatal(err)
	}
	return host
}

func directSender(t testing.TB, directory discovery.Directory) *DirectTransport {
	t.Helper()
	dialer, err := rpc.NewGRPCDialer(insecure.NewCredentials(), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	sender, err := rpc.NewSender(directory, dialer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sender.Close(); err != nil {
			t.Error(err)
		}
	})
	return &DirectTransport{Sender: sender}
}

func routeMembers(t testing.TB, directory *discovery.ServiceDirectory, hosts map[string]*rpc.Host) {
	t.Helper()
	var members []discovery.Instance
	for id, host := range hosts {
		members = append(members, discovery.Instance{ID: id, Services: []string{"world"}, Endpoint: host.Endpoint(), Status: discovery.StatusRunning})
	}
	if err := directory.Replace(members); err != nil {
		t.Fatal(err)
	}
}

func TestDirectActorCallKeepsIdentityErrorsAndCancellation(t *testing.T) {
	ctx := actorTestContext(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var completed atomic.Int32
	r := actorhost.New(8, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
		return &actorhost.Behavior{Receive: func(_ actorhost.Context, value any) (any, error) {
			d := value.(*actorpb.Delivery)
			switch d.MessageType {
			case "block":
				close(entered)
				<-release
				completed.Add(1)
			case "denied":
				return nil, actorhost.ErrActivationDenied
			case "invalid":
				return nil, nil
			case "panic":
				panic("injected")
			}
			return &actorpb.Result{MessageType: "ok"}, nil
		}}, nil
	})
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Drain(cleanup); err != nil {
			t.Error(err)
		}
	})
	host := directPeer(t, "a", r)
	directory := discovery.NewServiceDirectory()
	// 旧实例地址已被 a 复用，消息仍必须按启动实例 ID 拒绝。
	routeMembers(t, directory, map[string]*rpc.Host{"a": host, "old": host})
	transport := directSender(t, directory)
	id := actorhost.Identity{Type: "world", ID: "中文世界/51"}
	if _, err := transport.Call(ctx, actorhost.Target{Instance: "old", Identity: id}, &actorpb.Delivery{MessageType: "ack"}); !errors.Is(err, actorhost.ErrNotOwner) {
		t.Fatalf("旧实例身份未拒绝: %v", err)
	}
	if r.Probe(id).State != actorhost.Unloaded {
		t.Fatal("错误实例触发了激活")
	}
	for _, message := range []string{"denied", "invalid", "panic"} {
		_, err := transport.Call(ctx, actorhost.Target{Instance: "a", Identity: actorhost.Identity{Type: "world", ID: message}}, &actorpb.Delivery{MessageType: message})
		if err == nil || (message == "denied" && !errors.Is(err, actorhost.ErrActivationDenied)) || (message == "panic" && !errors.Is(err, actorhost.ErrPanicked)) {
			t.Fatalf("%s 错误丢失: %v", message, err)
		}
	}
	callCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := transport.Call(callCtx, actorhost.Target{Instance: "a", Identity: id}, &actorpb.Delivery{MessageType: "block"})
		done <- err
	}()
	signal(t, ctx, entered)
	cancel()
	if err := <-done; err == nil {
		t.Fatal("取消仍返回成功")
	}
	once.Do(func() { close(release) })
	if err := r.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if completed.Load() != 1 {
		t.Fatal("取消丢弃或重投了已接受业务")
	}
	if _, err := transport.Call(ctx, actorhost.Target{Instance: "a", Identity: id}, &actorpb.Delivery{MessageType: "late"}); !errors.Is(err, actorhost.ErrDraining) {
		t.Fatal("排水后直连未拒绝", err)
	}
}

func watchedRoutes(t *testing.T, store discovery.SlotStore, directory discovery.Directory) (*discovery.SlotResolver, *discovery.SlotDirectory) {
	t.Helper()
	cache := discovery.NewSlotDirectory()
	cached, err := discovery.NewCachedSlotStore(store, cache)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- store.(interface {
			Watch(context.Context, *discovery.SlotDirectory) error
		}).Watch(ctx, cache)
	}()
	t.Cleanup(func() { cancel(); <-done })
	signal(t, actorTestContext(t), cache.Ready())
	resolver, err := discovery.NewSlotResolver(cached, directory)
	if err != nil {
		t.Fatal(err)
	}
	return resolver, cache
}

// 两种传输都必须生成业务解码器接受的信封；请求关联仍由各传输自行维护。
func TestSharedRoutingDecodesBusinessDelivery(t *testing.T) {
	url, ctx := natsURL(t), actorTestContext(t)
	received := make(chan txactor.Delivery, 8)
	local := actorhost.New(8, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
		return &actorhost.Behavior{Receive: func(_ actorhost.Context, value any) (any, error) {
			d, err := txactor.DeliveryFromProto(value.(*actorpb.Delivery))
			if err != nil {
				return nil, err
			}
			received <- d
			return &actorpb.Result{MessageType: "decoded", Payload: d.Payload}, nil
		}}, nil
	})
	server := peer(t, url, nats.NewInbox(), "decode", local)
	host := directPeer(t, server.instance, local)
	directory := discovery.NewServiceDirectory()
	routeMembers(t, directory, map[string]*rpc.Host{server.instance: host})
	remote := peer(t, url, server.namespace, "caller", echoRuntime())
	delivery, err := (txactor.Delivery{MessageType: "decode", Payload: []byte("业务")}).ToProto()
	if err != nil {
		t.Fatal(err)
	}
	target := actorhost.Target{Instance: server.instance, Identity: actorhost.Identity{Type: "world", ID: "51"}}
	for name, caller := range map[string]Caller{"direct": directSender(t, directory), "nats-remote": remote, "nats-local": server} {
		t.Run(name, func(t *testing.T) {
			result, err := caller.Call(ctx, target, delivery)
			if err != nil || result.MessageType != "decoded" {
				t.Fatalf("业务解码失败: %v %v", result, err)
			}
			<-received
			if delivery.Mode != actorpb.DeliveryMode_DELIVERY_MODE_TELL || delivery.RequestId != 0 {
				t.Fatal("修改了调用方信封")
			}
		})
	}
	// 传输模式由入口决定，不沿用入参中另一种模式的关联标记。
	delivery.RequestId = 99
	if err := remote.Tell(ctx, target, delivery); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal("Tell 未通过业务解码", ctx.Err())
	}
}

// 直连独立启动，不依赖 NATS Connect 的副作用来保护加载。
func TestDirectActorOwnershipBeforeNATS(t *testing.T) {
	ctx := actorTestContext(t)
	reader := &countedSlotReader{assignment: discovery.SlotAssignment{SlotID: "world-1", RuntimeInstanceID: "new", Generation: 2}}
	var loads atomic.Int32
	r := actorhost.New(8, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
		loads.Add(1)
		return &actorhost.Behavior{Receive: func(actorhost.Context, any) (any, error) { return &actorpb.Result{MessageType: "ok"}, nil }}, nil
	})
	r.BindSlots("old", reader, actorhost.SlotMapping(worldSlot).Resolver())
	t.Cleanup(func() {
		if err := r.Drain(context.Background()); err != nil {
			t.Error(err)
		}
	})
	host := directPeer(t, "old", r)
	directory := discovery.NewServiceDirectory()
	routeMembers(t, directory, map[string]*rpc.Host{"old": host})
	caller := directSender(t, directory)
	_, err := caller.Call(ctx, actorhost.Target{Instance: "old", Identity: actorhost.Identity{Type: "world", ID: "1"}}, &actorpb.Delivery{MessageType: "move"})
	if !errors.Is(err, actorhost.ErrNotOwner) || loads.Load() != 0 || reader.reads.Load() != 1 {
		t.Fatalf("wrong owner: err=%v loads=%d reads=%d", err, loads.Load(), reader.reads.Load())
	}

	// 正确归属的首次请求完成校验，后续消息复用同一激活。
	current := echoRuntime()
	current.BindSlots("new", reader, actorhost.SlotMapping(worldSlot).Resolver())
	t.Cleanup(func() {
		if err := current.Drain(context.Background()); err != nil {
			t.Error(err)
		}
	})
	nextHost := directPeer(t, "new", current)
	routeMembers(t, directory, map[string]*rpc.Host{"old": host, "new": nextHost})
	for range 2 {
		if _, err := caller.Call(ctx, actorhost.Target{Instance: "new", Identity: actorhost.Identity{Type: "world", ID: "1"}}, &actorpb.Delivery{MessageType: "move"}); err != nil {
			t.Fatal(err)
		}
	}
	if reader.reads.Load() != 2 {
		t.Fatalf("重复读取权威归属: %d", reader.reads.Load())
	}
}
