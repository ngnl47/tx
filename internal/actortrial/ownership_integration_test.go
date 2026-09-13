package actortrial

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/meta"
	txactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/agent"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
	"github.com/ngnl47/tx/protocol"
	"github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
	"github.com/ngnl47/tx/storage/redis/goredis"
	"github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// ownershipStore 只提供一个可回滚字段的 Store 契约；生成 GD 的验收仍在独立示例中。
type ownershipStore struct {
	key      string
	value    int64
	dirty    bool
	recorder change.UndoRecorder
}

func (s *ownershipStore) TypeSpec() meta.TypeSpec {
	return meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{FullName: "OwnershipTrial"}},
		Storage: meta.StorageSpec{Mode: meta.StorageModeRedis}, Redis: &meta.RedisSpec{Key: s.key}}
}
func (*ownershipStore) RecordKey() storage.RecordKey { return storage.RecordKey{ID: int64(51)} }
func (s *ownershipStore) HasChanges() bool           { return s.dirty }
func (s *ownershipStore) ClearChanges()              { s.dirty = false }
func (s *ownershipStore) AllTouch()                  { s.dirty = true }
func (*ownershipStore) DefaultApplyOptions() storage.ApplyOptions {
	return storage.ApplyOptions{Upsert: true}
}
func (s *ownershipStore) SetUndoRecorder(recorder change.UndoRecorder) { s.recorder = recorder }
func (*ownershipStore) FieldNameResolver() change.FieldNameResolver {
	return change.FieldNameResolverFunc(func(change.FieldID, change.FieldNameKind) string { return "value" })
}
func (s *ownershipStore) ResolveValue(change.Path) (any, bool) { return s.value, true }
func (s *ownershipStore) VisitChanges(v change.Visitor) error {
	if s.dirty {
		return v.VisitChange(change.Path{Depth: 1, Nodes: [change.MaxPathDepth]change.PathNode{{Field: 1}}}, change.OpSet)
	}
	return nil
}

type ownershipProcedure struct {
	store *ownershipStore
	delta int64
}

func (p ownershipProcedure) Locks(context.Context) ([]txn.LockTarget, error) {
	return []txn.LockTarget{txn.NewLockTarget(txn.NewLockKey(p.store.key, 51), p.store)}, nil
}
func (p ownershipProcedure) Process(*txn.Tx) error {
	value, dirty := p.store.value, p.store.dirty
	p.store.recorder.RecordUndo(func() { p.store.value, p.store.dirty = value, dirty })
	p.store.value += p.delta
	p.store.dirty = true
	return nil
}

// 仅在真实 writer 之前注入错误/等待；成功路径继续使用原 Lua 适配和 Store 清理。
type ownershipWriter struct {
	storage.ChangeApplier
	before func(context.Context) error
}

func (w ownershipWriter) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, names change.FieldNameResolver, values change.ValueResolver, options storage.ApplyOptions) error {
	if err := w.before(ctx); err != nil {
		return err
	}
	return w.ChangeApplier.ApplyChangesWithOptions(ctx, spec, key, source, names, values, options)
}

// ownershipPeer 是测试组合根，正式 framework 的租约/Actor 接线尚未迁移。
type ownershipPeer struct {
	*SlotHost
	discovery *discovery.Runtime
	rpc       *rpc.Host
	leaseDone chan struct{}
}

func newOwnershipPeer(t *testing.T, url, namespace string, config discovery.EtcdConfig, store discovery.SlotStore, local *Runtime, flush func(context.Context) error) *ownershipPeer {
	t.Helper()
	root := lifecycle.NewWaitGroupContext(context.Background())
	host, err := rpc.NewHost("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var directory *discovery.Runtime
	t.Cleanup(func() {
		root.Cancel()
		if err := host.Stop(context.Background()); err != nil {
			t.Error(err)
		}
		root.Wait()
		if directory != nil {
			if err := directory.Close(context.Background()); err != nil {
				t.Error(err)
			}
		}
	})
	directory, err = discovery.NewRuntime(root, config, "ownership", discovery.Instance{
		Services: []string{"world"}, Endpoint: host.Endpoint(), Status: discovery.StatusStarting,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := trialContext(t)
	if _, err := directory.Slots().ResolveSlot(ctx, "world-51"); !errors.Is(err, discovery.ErrSlotDirectoryUnavailable) {
		t.Fatalf("Watch 尚未启动就允许缓存路由: %v", err)
	}
	if err := root.Go(func(ctx context.Context) {
		if err := directory.Watch(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := directory.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := directory.Directory().LookupInstance(directory.Instance().ID); ok {
		t.Fatal("入口就绪前发布 running")
	}
	p := &ownershipPeer{discovery: directory, rpc: host, leaseDone: make(chan struct{})}
	p.SlotHost = slotPeer(t, url, namespace, directory.Instance().ID, local, store, flush)
	if err := host.Register(func(registrar grpc.ServiceRegistrar) error {
		return rpc.RegisterDeliveryService(registrar, nil, NewDirectReceiver(directory.Instance().ID, local))
	}); err != nil {
		t.Fatal(err)
	}
	if err := host.Start(root); err != nil {
		t.Fatal(err)
	}
	// 对应正式框架的致命身份丢失边界：关闸，不把租约消失解释为安全交接。
	if err := root.Go(func(ctx context.Context) {
		select {
		case <-directory.Done():
			if errors.Is(directory.Err(), discovery.ErrRuntimeIdentityLost) {
				drain, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
				if err := local.Drain(drain); err != nil {
					t.Error(err)
				}
			}
		case <-ctx.Done():
		}
		close(p.leaseDone)
	}); err != nil {
		t.Fatal(err)
	}
	if err := directory.MarkRunning(ctx); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestOwnershipIntegrationWritebackHandoff(t *testing.T) {
	url, ctx := natsURL(t), trialContext(t)
	address := os.Getenv("TX_ACTORTRIAL_REDIS_ADDR")
	if address == "" {
		t.Skip("设置 TX_ACTORTRIAL_REDIS_ADDR 验证实际事务写回")
	}
	registry, config := trialSlotRegistry(t)
	store, namespace := registry.Slots(), nats.NewInbox()
	client := redis.NewClient(&redis.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	key := "tx:actortrial:ownership:" + namespace
	marker := key + ":owner"
	if ok, err := client.SetNX(ctx, marker, namespace, 0).Result(); err != nil || !ok {
		t.Fatalf("Redis ownership: %v", err)
	}
	t.Log("Redis 专用键:", marker, key+":{51}")
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if got, err := client.Get(cleanup, marker).Result(); err != nil || got != namespace {
			t.Errorf("Redis ownership 不匹配: %q %v", got, err)
			return
		}
		if err := client.Del(cleanup, key+":{51}", marker).Err(); err != nil {
			t.Error(err)
		}
	})
	adapter, err := txredis.NewAdapter(goredis.NewClient(client))
	if err != nil {
		t.Fatal(err)
	}
	root := &ownershipStore{key: key}
	if err := storage.Create(ctx, adapter, root); err != nil {
		t.Fatal(err)
	}
	blocked, release := make(chan struct{}), make(chan struct{})
	writeEntered, writeRelease := make(chan struct{}), make(chan struct{})
	var unblock, unblockWrite sync.Once
	defer unblock.Do(func() { close(release) })
	defer unblockWrite.Do(func() { close(writeRelease) })
	var writes, notifications atomic.Int32
	writer := ownershipWriter{adapter, func(ctx context.Context) error {
		switch writes.Add(1) {
		case 1:
			return errors.New("injected persistence failure")
		case 2:
			close(writeEntered)
			select {
			case <-writeRelease:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}}
	manager := writeback.NewManager(writeback.Dependencies{Writer: writer}, writeback.Config{
		FlushInterval: time.Hour, ShutdownFlushTimeout: 300 * time.Millisecond,
	})
	executor := txn.NewRuntime()
	if err := executor.BindCommitParticipant(manager); err != nil {
		t.Fatal(err)
	}
	local := New(8, 5*time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(c Context, value any) (any, error) {
			d := value.(*actorpb.Delivery)
			if d.UpSeq > 0 {
				close(blocked)
				<-release
			}
			if err := executor.Call(c, ownershipProcedure{root, int64(d.CommandId)}); err != nil {
				return nil, err
			}
			if d.UpSeq > 0 {
				return &actorpb.Result{ProcessedUpSeq: d.UpSeq}, nil
			}
			return &actorpb.Result{MessageType: "value", Payload: []byte(fmt.Sprint(root.value))}, nil
		}, OnDeactivateBefore: func(Context) error { notifications.Add(1); return nil }}, nil
	})
	a := newOwnershipPeer(t, url, namespace, config, store, local, manager.Shutdown)
	caller := peer(t, url, namespace, "caller", echoRuntime())
	eventually(t, ctx, func() bool { return len(a.discovery.Directory().RunningInstances("world")) == 1 })
	allocator := NewSlotAllocator(store, a.discovery.Directory(), caller, "world", worldSlot)
	id := Identity{"world", "51"}
	current, _, err := allocator.Ensure(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := discovery.NewSlotResolver(a.discovery.Slots(), a.discovery.Directory())
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, ctx, func() bool { _, err := resolver.ResolveSlot(ctx, current.SlotID); return err == nil })
	direct := directSender(t, a.discovery.Directory())
	up := &RoutedCaller{Resolver: resolver, SlotID: worldSlot, Transport: direct}
	remote := &RoutedCaller{Resolver: resolver, SlotID: worldSlot, Transport: caller}
	dispatcher := NewAgentDispatcher(up, func(context.Context, agent.DispatchRequest) (Identity, txactor.Delivery, error) {
		return id, txactor.Delivery{MessageType: "up", CommandID: 3}, nil
	}, 5*time.Second)
	ack := make(chan error, 1)
	go func() {
		result, err := dispatcher.Dispatch(ctx, agent.DispatchRequest{Seq: 7, Identity: protocol.SessionIdentity{
			SessionPrincipal: protocol.SessionPrincipal{AccountID: 1, ServerID: 1, RoleID: 2, SessionID: 3}, ConnectionID: 4,
		}})
		if err == nil && result.ProcessedUpSeq != 7 {
			err = fmt.Errorf("错误 ACK: %+v", result)
		}
		ack <- err
	}()
	signal(t, ctx, blocked)
	future, err := caller.CallFuture(ctx, Target{current.RuntimeInstanceID, id}, &actorpb.Delivery{MessageType: "add", CommandId: 4})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, ctx, func() bool { local.mu.Lock(); defer local.mu.Unlock(); return len(local.entries[id].queue) == 1 })
	select {
	case err := <-ack:
		t.Fatalf("业务完成前 ACK: %v", err)
	default:
	}
	b := newOwnershipPeer(t, url, namespace, config, store, New(8, 5*time.Second, func(c Context) (*Behavior, error) {
		doc, found, err := adapter.LoadDocument(c, root.TypeSpec(), root.RecordKey())
		if err != nil || !found {
			return nil, fmt.Errorf("加载持久值: found=%v err=%v", found, err)
		}
		return &Behavior{Receive: func(Context, any) (any, error) {
			return &actorpb.Result{MessageType: "value", Payload: []byte(fmt.Sprint(doc["value"]))}, nil
		}}, nil
	}), nil)
	eventually(t, ctx, func() bool { return len(a.discovery.Directory().RunningInstances("world")) == 2 })
	if got, _, err := allocator.Ensure(ctx, id); err != nil || got != current {
		t.Fatalf("新成员改写已有归属: %+v %v", got, err)
	}
	handoff := make(chan error, 1)
	go func() { _, _, err := allocator.Handoff(ctx, id, current, b.instance); handoff <- err }()
	eventually(t, ctx, func() bool { local.mu.Lock(); defer local.mu.Unlock(); return local.draining })
	if err := a.discovery.MarkDraining(ctx); err != nil {
		t.Fatal(err)
	}
	if writes.Load() != 0 {
		t.Fatal("已接受业务结束前开始最终写回")
	}
	for _, transport := range []Caller{direct, caller} {
		if _, err := transport.Call(ctx, Target{a.instance, id}, &actorpb.Delivery{MessageType: "late"}); !errors.Is(err, ErrDraining) {
			t.Fatalf("排水入口未拒绝: %v", err)
		}
	}
	unblock.Do(func() { close(release) })
	if err := <-ack; err != nil {
		t.Fatal(err)
	}
	if result, err := future.Await(ctx); err != nil || string(result.Payload) != "7" {
		t.Fatalf("排队业务没有执行: %+v %v", result, err)
	}
	if err := <-handoff; err == nil {
		t.Fatal("写回失败却交接成功")
	}
	if got, err := store.ResolveSlot(ctx, current.SlotID); err != nil || got != current || manager.Pending() != 1 || !root.HasChanges() || b.runtime.Probe(id).State != Unloaded {
		t.Fatalf("失败后未保留归属或待刷根: %+v pending=%d err=%v", got, manager.Pending(), err)
	}
	if doc, _, err := adapter.LoadDocument(ctx, root.TypeSpec(), root.RecordKey()); err != nil || fmt.Sprint(doc["value"]) != "0" {
		t.Fatalf("失败注入后持久值: %+v %v", doc, err)
	}
	// 调用方停止等待后才释放真实写入，使源方完成但交接方收不到成功结果。
	short, cancel := context.WithCancel(ctx)
	go func() { _, _, err := allocator.Handoff(short, id, current, b.instance); handoff <- err }()
	signal(t, ctx, writeEntered)
	cancel()
	if err := <-handoff; !errors.Is(err, context.Canceled) {
		t.Fatalf("响应未知应返回取消: %v", err)
	}
	unblockWrite.Do(func() { close(writeRelease) })
	eventually(t, ctx, func() bool { a.mu.Lock(); defer a.mu.Unlock(); return a.quiesced })
	if got, err := store.ResolveSlot(ctx, current.SlotID); err != nil || got != current {
		t.Fatalf("响应未知仍晋升: %+v %v", got, err)
	}
	next, _, err := allocator.Handoff(ctx, id, current, b.instance)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, ctx, func() bool {
		route, err := resolver.ResolveSlot(ctx, current.SlotID)
		return err == nil && route.Assignment == next && b.runtime.Probe(id).State == Ready
	})
	for _, route := range []*RoutedCaller{up, remote} {
		if result, err := route.Call(ctx, id, &actorpb.Delivery{MessageType: "read"}); err != nil || string(result.Payload) != "7" {
			t.Fatalf("新实例未读到最后变更: %+v %v", result, err)
		}
	}
	if manager.Pending() != 0 || root.HasChanges() || writes.Load() != 2 || notifications.Load() != 1 {
		t.Fatalf("收尾未完成或重复执行: pending=%d writes=%d notifications=%d", manager.Pending(), writes.Load(), notifications.Load())
	}
}

func TestOwnershipIntegrationLeaseLoss(t *testing.T) {
	url, ctx := natsURL(t), trialContext(t)
	registry, config := trialSlotRegistry(t)
	store, namespace := registry.Slots(), nats.NewInbox()
	a := newOwnershipPeer(t, url, namespace, config, store, echoRuntime(), nil)
	caller := peer(t, url, namespace, "caller", echoRuntime())
	eventually(t, ctx, func() bool { return len(a.discovery.Directory().RunningInstances("world")) == 1 })
	allocator := NewSlotAllocator(store, a.discovery.Directory(), caller, "world", worldSlot)
	id := Identity{"world", "51"}
	current, _, err := allocator.Ensure(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: config.Endpoints, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	registration, err := client.Get(ctx, config.Prefix+"/instances/"+a.instance)
	if err != nil || len(registration.Kvs) != 1 {
		t.Fatalf("读取本实例租约: %+v %v", registration, err)
	}
	if _, err := client.Revoke(ctx, clientv3.LeaseID(registration.Kvs[0].Lease)); err != nil {
		t.Fatal(err)
	}
	signal(t, ctx, a.leaseDone)
	if !errors.Is(a.discovery.Err(), discovery.ErrRuntimeIdentityLost) {
		t.Fatal(a.discovery.Err())
	}
	// 固定旧快照强制到达旧 Receiver，避免只验证 Directory 已经摘除了节点。
	stale := discovery.NewServiceDirectory()
	routeMembers(t, stale, map[string]*rpc.Host{a.instance: a.rpc})
	for _, transport := range []Caller{directSender(t, stale), caller} {
		if _, err := transport.Call(ctx, Target{a.instance, id}, &actorpb.Delivery{MessageType: "late"}); !errors.Is(err, ErrDraining) {
			t.Fatalf("租约失效后旧入口仍接收: %v", err)
		}
	}
	b := newOwnershipPeer(t, url, namespace, config, store, echoRuntime(), nil)
	eventually(t, ctx, func() bool {
		_, old := b.discovery.Directory().LookupInstance(a.instance)
		return !old && len(b.discovery.Directory().RunningInstances("world")) == 1
	})
	allocator = NewSlotAllocator(store, b.discovery.Directory(), caller, "world", worldSlot)
	if got, _, err := allocator.Ensure(ctx, id); !errors.Is(err, discovery.ErrSlotRuntimeUnavailable) || got != current {
		t.Fatalf("租约丢失被解释为可自动接管: %+v %v", got, err)
	}
	if got, err := store.ResolveSlot(ctx, current.SlotID); err != nil || got != current || b.runtime.Probe(id).State != Unloaded {
		t.Fatalf("失效后归属发生改变: %+v %v", got, err)
	}
	// 模拟同一旧路由地址实际已由新启动实例接收，必须在业务加载前拒绝旧身份。
	routeMembers(t, stale, map[string]*rpc.Host{a.instance: b.rpc})
	if _, err := directSender(t, stale).Call(ctx, Target{a.instance, id}, &actorpb.Delivery{MessageType: "late"}); !errors.Is(err, ErrNotOwner) || b.runtime.Probe(id).State != Unloaded {
		t.Fatalf("新实例接纳旧启动身份: %v", err)
	}
}
