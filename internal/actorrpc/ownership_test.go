package actorrpc

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
	txetcd "github.com/ngnl47/tx/etcd"
	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
)

func actorTestSlots(t *testing.T) discovery.SlotStore {
	t.Helper()
	endpoint := os.Getenv("TX_ACTORTRIAL_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("设置 TX_ACTORTRIAL_ETCD_ENDPOINT 运行真实槽位测试")
	}
	ctx := actorTestContext(t)
	client, err := txetcd.New(context.Background(), txetcd.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err = client.Probe(ctx); err != nil {
		t.Fatal(err)
	}
	owner := nats.NewInbox()
	prefix := "/tx/tests/actorrpc/" + owner
	if _, ok, err := client.Create(ctx, prefix+"/owner", []byte(owner), 0); err != nil || !ok {
		t.Fatalf("命名空间占用: %v", err)
	}
	t.Log("etcd 测试范围:", prefix)
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		marker, ok, err := client.Get(cleanup, prefix+"/owner")
		if err != nil || !ok || string(marker.Value) != owner {
			t.Errorf("清理 ownership 不匹配: %v", err)
			return
		}
		snapshot, err := client.Snapshot(cleanup, prefix+"/")
		if err != nil {
			t.Error(err)
			return
		}
		for _, kv := range snapshot.Values {
			if err := client.Delete(cleanup, kv.Key); err != nil {
				t.Error(err)
			}
		}
	})
	config := discovery.EtcdConfig{Endpoints: []string{endpoint}, Prefix: prefix, LeaseTTL: 5 * time.Second, DialTimeout: time.Second}
	registry, err := discovery.NewEtcdRegistryWithClient(client, config)
	if err != nil {
		t.Fatal(err)
	}
	return registry.Slots()
}

func worldSlot(id actorhost.Identity) string { return "world-" + id.Key }

func worldMembers(t *testing.T, directory *discovery.ServiceDirectory, ids ...string) {
	t.Helper()
	var members []discovery.Instance
	for _, id := range ids {
		members = append(members, discovery.Instance{ID: id, Services: []string{"world"}, Endpoint: "unused-" + id, Status: discovery.StatusRunning})
	}
	if err := directory.Replace(members); err != nil {
		t.Fatal(err)
	}
}

func TestSlotAllocationKeepsOwnerAndRespectsCapacity(t *testing.T) {
	url, ctx := natsURL(t), actorTestContext(t)
	store, ns := actorTestSlots(t), nats.NewInbox()
	var loads atomic.Int32
	makeRuntime := func() *actorhost.Runtime {
		r := actorhost.New(32, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
			loads.Add(1)
			return &actorhost.Behavior{Receive: func(c actorhost.Context, v any) (any, error) {
				d := v.(*actorpb.Delivery)
				if d.MessageType == "unload" {
					c.Deactivate()
				}
				return &actorpb.Result{MessageType: "ok"}, nil
			}}, nil
		})
		r.SetActivationLimits(actorhost.ActivationLimits{Actors: 1, Initializations: 8})
		return r
	}
	a, b := slotPeer(t, url, ns, "a", makeRuntime(), store), slotPeer(t, url, ns, "b", makeRuntime(), store)
	caller := peer(t, url, ns, "caller", echoRuntime())
	directory := discovery.NewServiceDirectory()
	worldMembers(t, directory, "a")
	allocator := newTestEntityRouter(slotOwnership(t, store, directory), caller, "world", worldSlot)
	id := actorhost.Identity{Kind: "world", Key: "51"}
	first, _, err := allocator.Ensure(ctx, id)
	if err != nil || first.RuntimeInstanceID != "a" {
		t.Fatalf("首次分配: %+v %v", first, err)
	}
	if _, err = caller.Call(ctx, actorhost.Target{Instance: "a", Identity: id}, &actorpb.Delivery{MessageType: "ping"}); err != nil {
		t.Fatal(err)
	}
	worldMembers(t, directory, "a", "b")
	other := peer(t, url, ns, "caller-2", echoRuntime())
	secondAllocator := newTestEntityRouter(slotOwnership(t, store, directory), other, "world", worldSlot)
	secondID := actorhost.Identity{Kind: "world", Key: "52"}
	var group sync.WaitGroup
	for i := range 24 {
		group.Go(func() {
			which := allocator
			if i%2 != 0 {
				which = secondAllocator
			}
			got, _, err := which.Ensure(ctx, secondID)
			if err != nil || got.RuntimeInstanceID != "b" || got.Generation != 1 {
				t.Errorf("并发分配: %+v %v", got, err)
			}
		})
	}
	group.Wait()
	if _, err = caller.Call(ctx, actorhost.Target{Instance: "b", Identity: secondID}, &actorpb.Delivery{MessageType: "ping"}); err != nil {
		t.Fatal(err)
	}
	if loads.Load() != 2 {
		t.Fatalf("重复加载: %d", loads.Load())
	}
	if got, _, err := allocator.Ensure(ctx, id); err != nil || got != first {
		t.Fatalf("新成员改变旧归属: %+v %v", got, err)
	}
	if _, _, err = allocator.Ensure(ctx, actorhost.Identity{Kind: "world", Key: "53"}); !errors.Is(err, actorhost.ErrCapacity) {
		t.Fatal("全满未拒绝", err)
	}
	if _, err = store.ResolveSlot(ctx, "world-53"); !errors.Is(err, discovery.ErrSlotNotFound) {
		t.Fatal("容量拒绝却创建槽位", err)
	}
	if _, err = caller.Call(ctx, actorhost.Target{Instance: "a", Identity: id}, &actorpb.Delivery{MessageType: "unload"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, ctx, func() bool { return a.runtime.Probe(id).State == actorhost.Unloaded })
	if got, _, err := allocator.Ensure(ctx, id); err != nil || got != first {
		t.Fatalf("普通卸载改变归属: %+v %v", got, err)
	}
	if _, err = caller.Call(ctx, actorhost.Target{Instance: "a", Identity: id}, &actorpb.Delivery{MessageType: "ping"}); err != nil {
		t.Fatal(err)
	}
	if loads.Load() != 3 || b.runtime.ActivationUsage().Actors != 1 {
		t.Fatal("重建或预算错误")
	}
}

func TestCapacityPreflightDoesNotReserve(t *testing.T) {
	r := echoRuntime()
	r.SetActivationLimits(actorhost.ActivationLimits{Actors: 1, Initializations: 1})
	for i := range 3 {
		if err := r.CheckActivationCapacity(actorhost.Identity{Kind: "world", Key: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if r.ActivationUsage() != (actorhost.ActivationUsage{}) {
		t.Fatal("预检占用额度")
	}
	if err := r.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.CheckActivationCapacity(actorhost.Identity{Kind: "world", Key: "51"}); !errors.Is(err, actorhost.ErrDraining) {
		t.Fatal(err)
	}
}

// 故障仅影响控制调用方看到的结果，etcd 操作确实已经执行。
type uncertainSlotStore struct {
	discovery.SlotStore
	createLost   atomic.Bool
	afterCreate  func()
	beforeCreate func(context.Context) error
	creates      atomic.Int32
}

func (s *uncertainSlotStore) CreateSlot(ctx context.Context, a discovery.SlotAssignment) error {
	s.creates.Add(1)
	if s.beforeCreate != nil {
		if err := s.beforeCreate(ctx); err != nil {
			return err
		}
	}
	if err := s.SlotStore.CreateSlot(ctx, a); err != nil {
		return err
	}
	if s.afterCreate != nil {
		s.afterCreate()
	}
	if s.createLost.Swap(false) {
		return context.DeadlineExceeded
	}
	return nil
}

func TestSlotCASReplyLossDoesNotReassign(t *testing.T) {
	url, ctx := natsURL(t), actorTestContext(t)
	store := &uncertainSlotStore{SlotStore: actorTestSlots(t)}
	ns := nats.NewInbox()
	var loads atomic.Int32
	makeRuntime := func() *actorhost.Runtime {
		return actorhost.New(8, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
			loads.Add(1)
			return &actorhost.Behavior{Receive: func(actorhost.Context, any) (any, error) { return &actorpb.Result{MessageType: "ok"}, nil }}, nil
		})
	}
	slotPeer(t, url, ns, "a", makeRuntime(), store)
	b := slotPeer(t, url, ns, "b", makeRuntime(), store)
	caller := peer(t, url, ns, "caller", echoRuntime())
	directory := discovery.NewServiceDirectory()
	worldMembers(t, directory, "a")
	allocator := newTestEntityRouter(slotOwnership(t, store, directory), caller, "world", worldSlot)
	id := actorhost.Identity{Kind: "world", Key: "51"}
	store.createLost.Store(true)
	if _, _, err := allocator.Ensure(ctx, id); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	current, err := store.ResolveSlot(ctx, worldSlot(id))
	if err != nil || current.RuntimeInstanceID != "a" || store.creates.Load() != 1 || loads.Load() != 0 {
		t.Fatalf("未知创建结果后重新分配或加载: %+v %v", current, err)
	}
	if _, _, err = allocator.Ensure(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err = caller.Call(ctx, actorhost.Target{Instance: "a", Identity: id}, &actorpb.Delivery{MessageType: "ping"}); err != nil {
		t.Fatal(err)
	}
	if b.runtime.Probe(id).State != actorhost.Unloaded || loads.Load() != 1 {
		t.Fatal("创建结果未知后错误激活其他节点")
	}
}

func TestSlotCapacityChangeAfterCASDoesNotFallback(t *testing.T) {
	url, ctx := natsURL(t), actorTestContext(t)
	store := &uncertainSlotStore{SlotStore: actorTestSlots(t)}
	ns := nats.NewInbox()
	var pressure atomic.Bool
	var loads atomic.Int32
	for _, name := range []string{"a", "b"} {
		r := actorhost.New(8, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
			loads.Add(1)
			return &actorhost.Behavior{Receive: func(actorhost.Context, any) (any, error) { return &actorpb.Result{MessageType: "ok"}, nil }}, nil
		})
		r.SetActivationLimits(actorhost.ActivationLimits{Admit: func() bool { return !pressure.Load() }})
		slotPeer(t, url, ns, name, r, store)
	}
	caller := peer(t, url, ns, "caller", echoRuntime())
	directory := discovery.NewServiceDirectory()
	worldMembers(t, directory, "a", "b")
	allocator := newTestEntityRouter(slotOwnership(t, store, directory), caller, "world", worldSlot)
	store.afterCreate = func() { pressure.Store(true) }
	id := actorhost.Identity{Kind: "world", Key: "51"}
	attempted, _, err := allocator.Ensure(ctx, id)
	if !errors.Is(err, actorhost.ErrCapacity) {
		t.Fatal(err)
	}
	stored, err := store.ResolveSlot(ctx, worldSlot(id))
	if err != nil || stored != attempted || store.creates.Load() != 1 || loads.Load() != 0 {
		t.Fatalf("容量变化后误改派: %+v %v", stored, err)
	}
	pressure.Store(false)
	if got, _, err := allocator.Ensure(ctx, id); err != nil || got != stored {
		t.Fatalf("恢复后归属不稳定: %+v %v", got, err)
	}
}

func TestSlotFirstAssignmentCompetingCandidates(t *testing.T) {
	url, ctx := natsURL(t), actorTestContext(t)
	store := &uncertainSlotStore{SlotStore: actorTestSlots(t)}
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	store.beforeCreate = func(c context.Context) error {
		entered <- struct{}{}
		select {
		case <-release:
			return nil
		case <-c.Done():
			return c.Err()
		}
	}
	ns := nats.NewInbox()
	var loads atomic.Int32
	for _, name := range []string{"a", "b"} {
		slotPeer(t, url, ns, name, actorhost.New(8, time.Second, func(actorhost.Context) (*actorhost.Behavior, error) {
			loads.Add(1)
			return &actorhost.Behavior{Receive: func(actorhost.Context, any) (any, error) { return &actorpb.Result{MessageType: "ok"}, nil }}, nil
		}), store)
	}
	caller := peer(t, url, ns, "caller", echoRuntime())
	id := actorhost.Identity{Kind: "world", Key: "51"}
	directories := []*discovery.ServiceDirectory{discovery.NewServiceDirectory(), discovery.NewServiceDirectory()}
	worldMembers(t, directories[0], "a")
	worldMembers(t, directories[1], "b")
	allocators := []*EntityRouter{newTestEntityRouter(slotOwnership(t, store, directories[0]), caller, "world", worldSlot), newTestEntityRouter(slotOwnership(t, store, directories[1]), peer(t, url, ns, "caller-2", echoRuntime()), "world", worldSlot)}
	results := make(chan error, 2)
	for _, allocator := range allocators {
		go func() { _, _, err := allocator.Ensure(ctx, id); results <- err }()
	}
	signal(t, ctx, entered)
	signal(t, ctx, entered)
	if loads.Load() != 0 {
		t.Fatal("CAS 前执行了业务加载")
	}
	releaseOnce.Do(func() { close(release) })
	winners := 0
	for range allocators {
		err := <-results
		if err == nil {
			winners++
		} else if !errors.Is(err, discovery.ErrSlotRuntimeUnavailable) {
			t.Fatal(err)
		}
	}
	if winners != 1 || store.creates.Load() != 2 {
		t.Fatalf("首次 CAS 竞争异常: winners=%d creates=%d", winners, store.creates.Load())
	}
	stored, err := store.ResolveSlot(ctx, worldSlot(id))
	if err != nil {
		t.Fatal(err)
	}
	for i, allocator := range allocators {
		worldMembers(t, directories[i], "a", "b")
		if got, _, err := allocator.Ensure(ctx, id); err != nil || got != stored {
			t.Fatalf("目录追上后改变归属: %+v %v", got, err)
		}
	}
	if _, err = caller.Call(ctx, actorhost.Target{Instance: stored.RuntimeInstanceID, Identity: id}, &actorpb.Delivery{MessageType: "ping"}); err != nil {
		t.Fatal(err)
	}
	if loads.Load() != 1 {
		t.Fatal("竞争者重复加载", loads.Load())
	}
}

// 独立测试夹具显式构造底层能力；正式组合根直接使用 Discovery actorhost.Runtime.Ownership。
func slotOwnership(t testing.TB, store discovery.SlotStore, directory discovery.Directory) *discovery.SlotOwnership {
	t.Helper()
	ownership, err := discovery.NewSlotOwnership(store, directory)
	if err != nil {
		t.Fatal(err)
	}
	return ownership
}

// 只组装正式传输和归属入口，不实现第二套容量或交接协议。
func slotPeer(t *testing.T, url, ns, name string, r *actorhost.Runtime, store discovery.SlotStore) *NATSTransport {
	t.Helper()
	r.BindSlots(name, store, worldSlot)
	return peer(t, url, ns, name, r)
}
func newTestEntityRouter(ownership *discovery.SlotOwnership, transport *NATSTransport, service string, mapping actorhost.SlotMapping) *EntityRouter {
	return &EntityRouter{Ownership: ownership, Transport: transport, Service: func(string) (string, error) { return service, nil }, SlotID: mapping}
}
