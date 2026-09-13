package actortrial

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ngnl47/tx/lifecycle"
	txactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/agent"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func directPeer(t testing.TB, instance string, r *Runtime) *rpc.Host {
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
	ctx := trialContext(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var completed atomic.Int32
	r := New(8, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(_ Context, value any) (any, error) {
			d := value.(*actorpb.Delivery)
			switch d.MessageType {
			case "block":
				close(entered)
				<-release
				completed.Add(1)
			case "denied":
				return nil, ErrActivationDenied
			case "invalid":
				return nil, nil
			case "panic":
				panic("injected")
			}
			return &actorpb.Result{ProcessedUpSeq: d.UpSeq}, nil
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
	id := Identity{"world", "中文世界/51"}
	if _, err := transport.Call(ctx, Target{"old", id}, &actorpb.Delivery{MessageType: "ack", UpSeq: 1}); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("旧实例身份未拒绝: %v", err)
	}
	if r.Probe(id).State != Unloaded {
		t.Fatal("错误实例触发了激活")
	}
	for _, message := range []string{"denied", "invalid", "panic"} {
		_, err := transport.Call(ctx, Target{"a", Identity{"world", message}}, &actorpb.Delivery{MessageType: message})
		if err == nil || (message == "denied" && !errors.Is(err, ErrActivationDenied)) || (message == "panic" && !errors.Is(err, ErrPanicked)) {
			t.Fatalf("%s 错误丢失: %v", message, err)
		}
	}
	callCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := transport.Call(callCtx, Target{"a", id}, &actorpb.Delivery{MessageType: "block", UpSeq: 9})
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
	if _, err := transport.Call(ctx, Target{"a", id}, &actorpb.Delivery{MessageType: "late"}); !errors.Is(err, ErrDraining) {
		t.Fatal("排水后直连未拒绝", err)
	}
}

func TestDirectAgentRejectsInvalidCompletion(t *testing.T) {
	ctx := trialContext(t)
	r := New(8, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(_ Context, value any) (any, error) {
			d := value.(*actorpb.Delivery)
			switch string(d.Payload) {
			case "wrong-seq":
				return &actorpb.Result{ProcessedUpSeq: d.UpSeq + 1}, nil
			case "mixed":
				return &actorpb.Result{ProcessedUpSeq: d.UpSeq, MessageType: "business"}, nil
			case "business":
				return &actorpb.Result{MessageType: "business"}, nil
			case "failure":
				return nil, &RemoteError{Code: 4001, Message: "道具不足"}
			}
			return &actorpb.Result{ProcessedUpSeq: d.UpSeq}, nil
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
	routeMembers(t, directory, map[string]*rpc.Host{"a": host})
	reader := &countedSlotReader{assignment: discovery.SlotAssignment{SlotID: "world-51", RuntimeInstanceID: "a", Generation: 1}}
	resolver, err := discovery.NewSlotResolver(reader, directory)
	if err != nil {
		t.Fatal(err)
	}
	caller := &RoutedCaller{Resolver: resolver, SlotID: worldSlot, Transport: directSender(t, directory)}
	dispatcher := NewAgentDispatcher(caller, func(_ context.Context, request agent.DispatchRequest) (Identity, txactor.Delivery, error) {
		return Identity{"world", "51"}, txactor.Delivery{MessageType: "up", Payload: request.Payload}, nil
	}, time.Second)
	request := agent.DispatchRequest{Identity: agent.Identity{SessionPrincipal: protocol.SessionPrincipal{AccountID: 1, ServerID: 1, RoleID: 2, SessionID: 3}, ConnectionID: 4}, Seq: 7}
	for _, payload := range []string{"wrong-seq", "mixed", "business", "failure", "valid"} {
		t.Run(payload, func(t *testing.T) {
			request.Payload = []byte(payload)
			result, err := dispatcher.Dispatch(ctx, request)
			if payload == "valid" {
				if err != nil || result.ProcessedUpSeq != 7 {
					t.Fatalf("合法 ACK: %v %v", result, err)
				}
			} else if err == nil || result.ProcessedUpSeq != 0 {
				t.Fatalf("失败推进 ACK: %v %v", result, err)
			}
			if payload == "failure" {
				var remote *RemoteError
				if !errors.As(err, &remote) || remote.Code != 4001 {
					t.Fatalf("业务错误码丢失: %v", err)
				}
			}
		})
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
	signal(t, trialContext(t), cache.Ready())
	resolver, err := discovery.NewSlotResolver(cached, directory)
	if err != nil {
		t.Fatal(err)
	}
	return resolver, cache
}

func TestSharedRoutingDirectAgentAndNATSHandoff(t *testing.T) {
	url, ctx, store := natsURL(t), trialContext(t), trialSlots(t)
	ns := nats.NewInbox()
	directory := discovery.NewServiceDirectory()
	caller := peer(t, url, ns, "caller", echoRuntime())
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var loadsA, loadsB, flushes, completed atomic.Int32
	var seen actorpb.SessionIdentity
	aRuntime := New(8, time.Second, func(Context) (*Behavior, error) {
		loadsA.Add(1)
		return &Behavior{Receive: func(_ Context, value any) (any, error) {
			d := value.(*actorpb.Delivery)
			if d.UpSeq > 0 {
				seen.RoleId, seen.SessionId, seen.ConnectionId = d.Identity.RoleId, d.Identity.SessionId, d.Identity.ConnectionId
				close(entered)
				<-release
				completed.Add(1)
				return &actorpb.Result{ProcessedUpSeq: d.UpSeq}, nil
			}
			return &actorpb.Result{MessageType: "value", Payload: []byte("a")}, nil
		}}, nil
	})
	a := slotPeer(t, url, ns, "a", aRuntime, store, func(context.Context) error {
		if flushes.Add(1) == 1 {
			return errors.New("injected flush failure")
		}
		return nil
	})
	b := slotPeer(t, url, ns, "b", New(8, time.Second, func(Context) (*Behavior, error) {
		loadsB.Add(1)
		return &Behavior{Receive: func(_ Context, v any) (any, error) {
			d := v.(*actorpb.Delivery)
			if d.UpSeq > 0 {
				return &actorpb.Result{ProcessedUpSeq: d.UpSeq}, nil
			}
			return &actorpb.Result{MessageType: "value", Payload: []byte("b")}, nil
		}}, nil
	}), store, nil)
	aRPC, bRPC := directPeer(t, "a", a.runtime), directPeer(t, "b", b.runtime)
	routeMembers(t, directory, map[string]*rpc.Host{"a": aRPC})
	id := Identity{"world", "51"}
	allocator := NewSlotAllocator(store, directory, caller, "world", worldSlot)
	current, _, err := allocator.Ensure(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	resolver, cache := watchedRoutes(t, store, directory)
	direct := directSender(t, directory)
	up := &RoutedCaller{Resolver: resolver, SlotID: worldSlot, Transport: direct}
	remote := &RoutedCaller{Resolver: resolver, SlotID: worldSlot, Transport: caller}
	resolve := func(_ context.Context, request agent.DispatchRequest) (Identity, txactor.Delivery, error) {
		return id, txactor.Delivery{MessageType: "up", CommandID: uint64(request.CommandID), Payload: request.Payload}, nil
	}
	dispatcher := NewAgentDispatcher(up, resolve, time.Second)
	routeMembers(t, directory, map[string]*rpc.Host{"a": aRPC, "b": bRPC})
	if _, err := direct.Call(ctx, Target{"b", id}, &actorpb.Delivery{MessageType: "wrong-owner"}); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("直连绕过加载前归属校验: %v", err)
	}
	for _, client := range []*RoutedCaller{up, remote} {
		result, err := client.Call(ctx, id, &actorpb.Delivery{MessageType: "read"})
		if err != nil || string(result.Payload) != "a" {
			t.Fatalf("新增成员改变归属: %v %v", result, err)
		}
	}
	if loadsA.Load() != 1 || loadsB.Load() != 0 {
		t.Fatal("双入口重复激活")
	}
	before := a.conn.Stats()
	ack := make(chan error, 1)
	go func() {
		result, err := dispatcher.Dispatch(ctx, agent.DispatchRequest{Identity: agent.Identity{SessionPrincipal: protocol.SessionPrincipal{AccountID: 1, ServerID: 1, RoleID: 2, SessionID: 3}, ConnectionID: 4}, Seq: 7, CommandID: 10})
		if err == nil && result.ProcessedUpSeq != 7 {
			err = fmt.Errorf("错误 ACK: %v", result)
		}
		ack <- err
	}()
	signal(t, ctx, entered)
	select {
	case err := <-ack:
		t.Fatalf("业务完成前 ACK: %v", err)
	default:
	}
	if seen.RoleId != 2 || seen.SessionId != 3 || seen.ConnectionId != 4 {
		t.Fatal("可信会话身份丢失")
	}
	if after := a.conn.Stats(); after.InMsgs != before.InMsgs || after.OutMsgs != before.OutMsgs {
		t.Fatal("直连上行经过 NATS")
	}
	queued, err := caller.CallFuture(ctx, Target{"a", id}, &actorpb.Delivery{MessageType: "read"})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, ctx, func() bool {
		a.runtime.mu.Lock()
		defer a.runtime.mu.Unlock()
		return len(a.runtime.entries[id].queue) == 1
	})
	select {
	case <-queued.future.done:
		t.Fatal("NATS 绕过了直连占用的同一邮箱")
	default:
	}
	handoff := make(chan error, 1)
	go func() { _, _, err := allocator.Handoff(ctx, id, current, "b"); handoff <- err }()
	eventually(t, ctx, func() bool { a.runtime.mu.Lock(); defer a.runtime.mu.Unlock(); return a.runtime.draining })
	for _, client := range []*RoutedCaller{up, remote} {
		if _, err := client.Call(ctx, id, &actorpb.Delivery{MessageType: "late"}); !errors.Is(err, ErrDraining) {
			t.Fatalf("共用闸门未拒绝: %v", err)
		}
	}
	if loadsB.Load() != 0 {
		t.Fatal("排空前在新实例加载")
	}
	once.Do(func() { close(release) })
	if err := <-ack; err != nil {
		t.Fatal(err)
	}
	if result, err := queued.Await(ctx); err != nil || string(result.Payload) != "a" {
		t.Fatalf("排水丢弃已排队 NATS: %v %v", result, err)
	}
	if err := <-handoff; err == nil {
		t.Fatal("收尾失败仍晋升")
	}
	if got, err := store.ResolveSlot(ctx, worldSlot(id)); err != nil || got != current {
		t.Fatalf("失败后归属变化: %v %v", got, err)
	}
	if completed.Load() != 1 || loadsB.Load() != 0 {
		t.Fatal("错误重投或提前激活")
	}
	if _, _, err := allocator.Handoff(ctx, id, current, "b"); err != nil {
		t.Fatal(err)
	}
	eventually(t, ctx, func() bool {
		target, err := resolver.ResolveSlot(ctx, worldSlot(id))
		return err == nil && target.Assignment.RuntimeInstanceID == "b"
	})
	for _, client := range []*RoutedCaller{up, remote} {
		result, err := client.Call(ctx, id, &actorpb.Delivery{MessageType: "read"})
		if err != nil || string(result.Payload) != "b" {
			t.Fatalf("Watch 未更新双入口: %v %v", result, err)
		}
	}
	if loadsB.Load() != 1 {
		t.Fatal("新方双入口重复激活")
	}
	for _, transport := range []Caller{direct, caller} {
		if _, err := transport.Call(ctx, Target{"a", id}, &actorpb.Delivery{MessageType: "stale"}); !errors.Is(err, ErrDraining) {
			t.Fatalf("旧路由未拒绝: %v", err)
		}
	}
	cache.Invalidate()
	for _, client := range []*RoutedCaller{up, remote} {
		if _, err := client.Call(ctx, id, &actorpb.Delivery{MessageType: "read"}); !errors.Is(err, discovery.ErrSlotDirectoryUnavailable) {
			t.Fatalf("失效后绕过缓存: %v", err)
		}
	}
}

// 两种传输都必须生成业务解码器接受的信封；请求关联仍由各传输自行维护。
func TestSharedRoutingDecodesBusinessDelivery(t *testing.T) {
	url, ctx := natsURL(t), trialContext(t)
	received := make(chan txactor.Delivery, 8)
	local := New(8, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(_ Context, value any) (any, error) {
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
	target := Target{server.instance, Identity{"world", "51"}}
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
