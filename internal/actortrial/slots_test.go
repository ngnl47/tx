package actortrial

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	txetcd "github.com/ngnl47/tx/etcd"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/redis/go-redis/v9"
)

func TestSlotRollingHandoffWaitsForRedisFlush(t *testing.T) {
	url, ctx := natsURL(t), trialContext(t)
	address := os.Getenv("TX_ACTORTRIAL_REDIS_ADDR")
	if address == "" {
		t.Skip("设置 TX_ACTORTRIAL_REDIS_ADDR 验证真实收尾")
	}
	store, ns := trialSlots(t), nats.NewInbox()
	redisClient := redis.NewClient(&redis.Options{Addr: address})
	t.Cleanup(func() { _ = redisClient.Close() })
	key := "tx:actortrial:" + ns
	type storedWorlds struct {
		Owner  string
		Values map[string]int64
	}
	initial, _ := json.Marshal(storedWorlds{Owner: ns, Values: map[string]int64{}})
	if ok, err := redisClient.SetNX(ctx, key, initial, 0).Result(); err != nil || !ok {
		t.Fatalf("Redis 范围占用: %v", err)
	}
	t.Log("Redis 专用键:", key)
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var current storedWorlds
		data, err := redisClient.Get(cleanup, key).Bytes()
		if err != nil || json.Unmarshal(data, &current) != nil || current.Owner != ns {
			t.Errorf("Redis ownership 不匹配: %v", err)
			return
		}
		if err := redisClient.Del(cleanup, key).Err(); err != nil {
			t.Error(err)
		}
	})
	var notifications atomic.Int32
	caller := peer(t, url, ns, "caller", New(8, time.Second, func(Context) (*Behavior, error) {
		return &Behavior{Receive: func(Context, any) (any, error) { notifications.Add(1); return &actorpb.Result{MessageType: "ok"}, nil }}, nil
	}))
	directory := discovery.NewServiceDirectory()
	worldMembers(t, directory, "a")
	allocator := NewSlotAllocator(slotOwnership(t, store, directory), caller, "world", worldSlot)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var values sync.Map
	var flushes, loadedB atomic.Int32
	var a *SlotHost
	r := New(8, 2*time.Second, func(c Context) (*Behavior, error) {
		value := new(int64)
		values.Store(c.Identity.Key, value)
		return &Behavior{
			Receive: func(_ Context, v any) (any, error) {
				d := v.(*actorpb.Delivery)
				if d.MessageType == "block" {
					close(entered)
					<-release
				}
				*value += int64(d.CommandId)
				return &actorpb.Result{MessageType: "value", Payload: []byte(fmt.Sprint(*value))}, nil
			},
			OnDeactivateBefore: func(c Context) error {
				_, err := a.Call(c, Target{"caller", Identity{"notice", "exit"}}, &actorpb.Delivery{MessageType: "exit"})
				return err
			},
		}, nil
	})
	a = slotPeer(t, url, ns, "a", r, store, func(c context.Context) error {
		if flushes.Add(1) == 1 {
			return errors.New("injected flush failure")
		}
		snapshot := storedWorlds{Owner: ns, Values: map[string]int64{}}
		values.Range(func(key, value any) bool { snapshot.Values[key.(string)] = *value.(*int64); return true })
		data, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		return redisClient.Set(c, key, data, 0).Err()
	})
	b := slotPeer(t, url, ns, "b", New(8, time.Second, func(c Context) (*Behavior, error) {
		loadedB.Add(1)
		data, err := redisClient.Get(c, key).Bytes()
		if err != nil {
			return nil, err
		}
		var snapshot storedWorlds
		if err = json.Unmarshal(data, &snapshot); err != nil {
			return nil, err
		}
		value := snapshot.Values[c.Identity.Key]
		return &Behavior{Receive: func(Context, any) (any, error) {
			return &actorpb.Result{MessageType: "value", Payload: []byte(fmt.Sprint(value))}, nil
		}}, nil
	}), store, nil)
	ids := []Identity{{"world", "51"}, {"world", "52"}}
	current := make([]discovery.SlotAssignment, 2)
	for i, id := range ids {
		var err error
		current[i], _, err = allocator.Ensure(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = caller.Call(ctx, Target{"a", id}, &actorpb.Delivery{MessageType: "add", CommandId: 2}); err != nil {
			t.Fatal(err)
		}
	}
	worldMembers(t, directory, "a", "b")
	if _, err := caller.Call(ctx, Target{"b", ids[0]}, &actorpb.Delivery{MessageType: "wrong"}); !errors.Is(err, ErrNotOwner) {
		t.Fatal("错误 owner 执行", err)
	}
	eventually(t, ctx, func() bool { return b.runtime.Probe(ids[0]).State == Failed })
	first, err := caller.CallFuture(ctx, Target{"a", ids[0]}, &actorpb.Delivery{MessageType: "block", CommandId: 5})
	if err != nil {
		t.Fatal(err)
	}
	signal(t, ctx, entered)
	queued, err := caller.CallFuture(ctx, Target{"a", ids[0]}, &actorpb.Delivery{MessageType: "queued", CommandId: 3})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, ctx, func() bool { return r.Probe(ids[0]).PendingMessages == 1 })
	done := make(chan error, 1)
	go func() { _, _, err := allocator.Handoff(ctx, ids[0], current[0], "b"); done <- err }()
	eventually(t, ctx, r.Draining)
	if _, err = caller.Call(ctx, Target{"a", ids[1]}, &actorpb.Delivery{MessageType: "late"}); !errors.Is(err, ErrDraining) {
		t.Fatal("实例关闸未拒绝另一世界", err)
	}
	if got, err := store.ResolveSlot(ctx, current[0].SlotID); err != nil || got != current[0] {
		t.Fatalf("排空前交接: %+v %v", got, err)
	}
	if loadedB.Load() != 0 {
		t.Fatal("旧请求仍在执行时新方已加载")
	}
	releaseOnce.Do(func() { close(release) })
	for _, f := range []*ResultFuture{first, queued} {
		if _, err := f.Await(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-done; err == nil {
		t.Fatal("收尾失败却交接成功")
	}
	if got, err := store.ResolveSlot(ctx, current[0].SlotID); err != nil || got != current[0] {
		t.Fatalf("失败后修改归属: %+v %v", got, err)
	}
	if notifications.Load() != 2 || loadedB.Load() != 0 || a.conn.IsClosed() {
		t.Fatal("卸载 RPC、目标加载或连接保留错误")
	}
	next, _, err := allocator.Handoff(ctx, ids[0], current[0], "b")
	if err != nil || next.Generation != 2 || next.RuntimeInstanceID != "b" {
		t.Fatalf("重试交接: %+v %v", next, err)
	}
	result, err := caller.Call(ctx, Target{"b", ids[0]}, &actorpb.Delivery{MessageType: "read"})
	if err != nil || string(result.Payload) != "10" {
		t.Fatalf("新方未读到排空后持久值: %v %v", result, err)
	}
	if _, _, err = allocator.Ensure(ctx, ids[1]); !errors.Is(err, ErrDraining) {
		t.Fatal("未交接槽位被自动改派", err)
	}
	if _, _, err = allocator.Handoff(ctx, ids[1], current[1], "b"); err != nil {
		t.Fatal(err)
	}
	if flushes.Load() != 2 || notifications.Load() != 2 {
		t.Fatal("重复收尾或卸载通知")
	}
	if _, _, err = allocator.Handoff(ctx, ids[0], current[0], "b"); !errors.Is(err, discovery.ErrSlotConflict) {
		t.Fatal("过时代次未拒绝", err)
	}
}

func trialSlots(t *testing.T) discovery.SlotStore {
	registry, _ := trialSlotRegistry(t)
	return registry.Slots()
}

func trialSlotRegistry(t *testing.T) (*discovery.EtcdRegistry, discovery.EtcdConfig) {
	t.Helper()
	endpoint := os.Getenv("TX_ACTORTRIAL_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("设置 TX_ACTORTRIAL_ETCD_ENDPOINT 运行真实槽位试验")
	}
	ctx := trialContext(t)
	client, err := txetcd.New(context.Background(), txetcd.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err = client.Probe(ctx); err != nil {
		t.Fatal(err)
	}
	owner := nats.NewInbox()
	prefix := "/tx/tests/actortrial/" + owner
	if _, ok, err := client.Create(ctx, prefix+"/owner", []byte(owner), 0); err != nil || !ok {
		t.Fatalf("命名空间占用: %v", err)
	}
	t.Log("etcd 试验范围:", prefix)
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
	return registry, config
}

func worldSlot(id Identity) string { return "world-" + id.Key }

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

func slotPeer(t *testing.T, url, ns, name string, r *Runtime, store discovery.SlotStore, flush func(context.Context) error) *SlotHost {
	t.Helper()
	r.BindSlots(name, store, worldSlot)
	host, err := ConnectSlotHost(url, ns, name, r, 64, flush)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := host.Drain(ctx); err != nil {
			t.Error(err)
		}
		host.conn.Close()
	})
	return host
}

func TestSlotAllocationKeepsOwnerAndRespectsCapacity(t *testing.T) {
	url, ctx := natsURL(t), trialContext(t)
	store, ns := trialSlots(t), nats.NewInbox()
	var loads atomic.Int32
	makeRuntime := func() *Runtime {
		r := New(32, time.Second, func(Context) (*Behavior, error) {
			loads.Add(1)
			return &Behavior{Receive: func(c Context, v any) (any, error) {
				d := v.(*actorpb.Delivery)
				if d.MessageType == "unload" {
					c.Deactivate()
				}
				return &actorpb.Result{MessageType: "ok"}, nil
			}}, nil
		})
		r.SetActivationLimits(ActivationLimits{Actors: 1, Initializations: 8})
		return r
	}
	a, b := slotPeer(t, url, ns, "a", makeRuntime(), store, nil), slotPeer(t, url, ns, "b", makeRuntime(), store, nil)
	caller := peer(t, url, ns, "caller", echoRuntime())
	directory := discovery.NewServiceDirectory()
	worldMembers(t, directory, "a")
	allocator := NewSlotAllocator(slotOwnership(t, store, directory), caller, "world", worldSlot)
	id := Identity{"world", "51"}
	first, _, err := allocator.Ensure(ctx, id)
	if err != nil || first.RuntimeInstanceID != "a" {
		t.Fatalf("首次分配: %+v %v", first, err)
	}
	if _, err = caller.Call(ctx, Target{"a", id}, &actorpb.Delivery{MessageType: "ping"}); err != nil {
		t.Fatal(err)
	}
	worldMembers(t, directory, "a", "b")
	other := peer(t, url, ns, "caller-2", echoRuntime())
	secondAllocator := NewSlotAllocator(slotOwnership(t, store, directory), other, "world", worldSlot)
	secondID := Identity{"world", "52"}
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
	if _, err = caller.Call(ctx, Target{"b", secondID}, &actorpb.Delivery{MessageType: "ping"}); err != nil {
		t.Fatal(err)
	}
	if loads.Load() != 2 {
		t.Fatalf("重复加载: %d", loads.Load())
	}
	if got, _, err := allocator.Ensure(ctx, id); err != nil || got != first {
		t.Fatalf("新成员改变旧归属: %+v %v", got, err)
	}
	if _, _, err = allocator.Ensure(ctx, Identity{"world", "53"}); !errors.Is(err, ErrCapacity) {
		t.Fatal("全满未拒绝", err)
	}
	if _, err = store.ResolveSlot(ctx, "world-53"); !errors.Is(err, discovery.ErrSlotNotFound) {
		t.Fatal("容量拒绝却创建槽位", err)
	}
	if _, err = caller.Call(ctx, Target{"a", id}, &actorpb.Delivery{MessageType: "unload"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, ctx, func() bool { return a.runtime.Probe(id).State == Unloaded })
	if got, _, err := allocator.Ensure(ctx, id); err != nil || got != first {
		t.Fatalf("普通卸载改变归属: %+v %v", got, err)
	}
	if _, err = caller.Call(ctx, Target{"a", id}, &actorpb.Delivery{MessageType: "ping"}); err != nil {
		t.Fatal(err)
	}
	if loads.Load() != 3 || b.runtime.ActivationUsage().Actors != 1 {
		t.Fatal("重建或预算错误")
	}
}

func TestCapacityPreflightDoesNotReserve(t *testing.T) {
	r := echoRuntime()
	r.SetActivationLimits(ActivationLimits{Actors: 1, Initializations: 1})
	for i := range 3 {
		if err := r.CheckActivationCapacity(Identity{"world", fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if r.ActivationUsage() != (ActivationUsage{}) {
		t.Fatal("预检占用额度")
	}
	if err := r.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.CheckActivationCapacity(Identity{"world", "51"}); !errors.Is(err, ErrDraining) {
		t.Fatal(err)
	}
}

// 故障仅影响控制调用方看到的结果，etcd 操作确实已经执行。
type uncertainSlotStore struct {
	discovery.SlotStore
	createLost   atomic.Bool
	promoteLost  atomic.Bool
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
func (s *uncertainSlotStore) PromoteSlot(ctx context.Context, current, next discovery.SlotAssignment) error {
	if err := s.SlotStore.PromoteSlot(ctx, current, next); err != nil {
		return err
	}
	if s.promoteLost.Swap(false) {
		return context.DeadlineExceeded
	}
	return nil
}

func TestSlotCASReplyLossDoesNotReassign(t *testing.T) {
	url, ctx := natsURL(t), trialContext(t)
	store := &uncertainSlotStore{SlotStore: trialSlots(t)}
	ns := nats.NewInbox()
	var loads atomic.Int32
	makeRuntime := func() *Runtime {
		return New(8, time.Second, func(Context) (*Behavior, error) {
			loads.Add(1)
			return &Behavior{Receive: func(Context, any) (any, error) { return &actorpb.Result{MessageType: "ok"}, nil }}, nil
		})
	}
	slotPeer(t, url, ns, "a", makeRuntime(), store, nil)
	b := slotPeer(t, url, ns, "b", makeRuntime(), store, nil)
	caller := peer(t, url, ns, "caller", echoRuntime())
	directory := discovery.NewServiceDirectory()
	worldMembers(t, directory, "a")
	allocator := NewSlotAllocator(slotOwnership(t, store, directory), caller, "world", worldSlot)
	id := Identity{"world", "51"}
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
	if _, err = caller.Call(ctx, Target{"a", id}, &actorpb.Delivery{MessageType: "ping"}); err != nil {
		t.Fatal(err)
	}
	worldMembers(t, directory, "a", "b")
	store.promoteLost.Store(true)
	if _, _, err = allocator.Handoff(ctx, id, current, "b"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	next, err := store.ResolveSlot(ctx, worldSlot(id))
	if err != nil || next.RuntimeInstanceID != "b" || next.Generation != 2 || b.runtime.Probe(id).State != Unloaded || loads.Load() != 1 {
		t.Fatalf("未知晋升结果触发后续加载: %+v %v", next, err)
	}
	if _, _, err = allocator.Handoff(ctx, id, current, "b"); !errors.Is(err, discovery.ErrSlotConflict) {
		t.Fatal(err)
	}
	if _, _, err = allocator.Ensure(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err = caller.Call(ctx, Target{"b", id}, &actorpb.Delivery{MessageType: "ping"}); err != nil {
		t.Fatal(err)
	}
	if loads.Load() != 2 {
		t.Fatal(loads.Load())
	}
}

func TestSlotCapacityChangeAfterCASDoesNotFallback(t *testing.T) {
	url, ctx := natsURL(t), trialContext(t)
	store := &uncertainSlotStore{SlotStore: trialSlots(t)}
	ns := nats.NewInbox()
	var pressure atomic.Bool
	var loads atomic.Int32
	for _, name := range []string{"a", "b"} {
		r := New(8, time.Second, func(Context) (*Behavior, error) {
			loads.Add(1)
			return &Behavior{Receive: func(Context, any) (any, error) { return &actorpb.Result{MessageType: "ok"}, nil }}, nil
		})
		r.SetActivationLimits(ActivationLimits{Admit: func() bool { return !pressure.Load() }})
		slotPeer(t, url, ns, name, r, store, nil)
	}
	caller := peer(t, url, ns, "caller", echoRuntime())
	directory := discovery.NewServiceDirectory()
	worldMembers(t, directory, "a", "b")
	allocator := NewSlotAllocator(slotOwnership(t, store, directory), caller, "world", worldSlot)
	store.afterCreate = func() { pressure.Store(true) }
	id := Identity{"world", "51"}
	attempted, _, err := allocator.Ensure(ctx, id)
	if !errors.Is(err, ErrCapacity) {
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

func TestSlotQuiesceReplyLossDoesNotPromote(t *testing.T) {
	url, ctx := natsURL(t), trialContext(t)
	store, ns := trialSlots(t), nats.NewInbox()
	b := slotPeer(t, url, ns, "b", echoRuntime(), store, nil)
	caller := peer(t, url, ns, "caller", echoRuntime())
	completed := make(chan struct{}, 1)
	// 模拟源端已经完成收尾但成功回复丢失，不能据超时自行推断结果。
	_, err := caller.conn.Subscribe(caller.slotSubject("a"), func(msg *nats.Msg) {
		var command slotControl
		if json.Unmarshal(msg.Data, &command) == nil && command.Action == "quiesce" {
			completed <- struct{}{}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = caller.conn.FlushWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	directory := discovery.NewServiceDirectory()
	worldMembers(t, directory, "a", "b")
	allocator := NewSlotAllocator(slotOwnership(t, store, directory), caller, "world", worldSlot)
	id := Identity{"world", "51"}
	current := discovery.SlotAssignment{SlotID: worldSlot(id), RuntimeInstanceID: "a", Generation: 1}
	if err = store.CreateSlot(ctx, current); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, _, err = allocator.Handoff(short, id, current, "b"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	signal(t, ctx, completed)
	got, err := store.ResolveSlot(ctx, current.SlotID)
	if err != nil || got != current || b.runtime.Probe(id).State != Unloaded {
		t.Fatalf("收尾回复未知却晋升: %+v %v", got, err)
	}
}

func TestConcurrentSlotHandoffHasOneWinner(t *testing.T) {
	url, ctx := natsURL(t), trialContext(t)
	store, ns := trialSlots(t), nats.NewInbox()
	var loads atomic.Int32
	for _, name := range []string{"a", "b", "c"} {
		slotPeer(t, url, ns, name, New(8, time.Second, func(Context) (*Behavior, error) {
			loads.Add(1)
			return &Behavior{Receive: func(Context, any) (any, error) { return &actorpb.Result{MessageType: "ok"}, nil }}, nil
		}), store, nil)
	}
	directory := discovery.NewServiceDirectory()
	worldMembers(t, directory, "a")
	caller := peer(t, url, ns, "caller", echoRuntime())
	allocator := NewSlotAllocator(slotOwnership(t, store, directory), caller, "world", worldSlot)
	id := Identity{"world", "51"}
	current, _, err := allocator.Ensure(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = caller.Call(ctx, Target{"a", id}, &actorpb.Delivery{MessageType: "ping"}); err != nil {
		t.Fatal(err)
	}
	worldMembers(t, directory, "a", "b", "c")
	other := NewSlotAllocator(slotOwnership(t, store, directory), peer(t, url, ns, "caller-2", echoRuntime()), "world", worldSlot)
	var group sync.WaitGroup
	var winners atomic.Int32
	for i, next := range []string{"b", "c"} {
		group.Go(func() {
			which := allocator
			if i == 1 {
				which = other
			}
			_, _, err := which.Handoff(ctx, id, current, next)
			if err == nil {
				winners.Add(1)
			} else if !errors.Is(err, discovery.ErrSlotConflict) {
				t.Error(err)
			}
		})
	}
	group.Wait()
	stored, err := store.ResolveSlot(ctx, current.SlotID)
	if err != nil || winners.Load() != 1 || stored.Generation != 2 {
		t.Fatalf("CAS 胜方: %+v %v count=%d", stored, err, winners.Load())
	}
	if _, err = caller.Call(ctx, Target{stored.RuntimeInstanceID, id}, &actorpb.Delivery{MessageType: "ping"}); err != nil {
		t.Fatal(err)
	}
	if loads.Load() != 2 {
		t.Fatal("多个目标开始加载", loads.Load())
	}
	if _, err = caller.Call(ctx, Target{"a", id}, &actorpb.Delivery{MessageType: "stale"}); !errors.Is(err, ErrDraining) {
		t.Fatal(err)
	}
}

func TestSlotFirstAssignmentCompetingCandidates(t *testing.T) {
	url, ctx := natsURL(t), trialContext(t)
	store := &uncertainSlotStore{SlotStore: trialSlots(t)}
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
		slotPeer(t, url, ns, name, New(8, time.Second, func(Context) (*Behavior, error) {
			loads.Add(1)
			return &Behavior{Receive: func(Context, any) (any, error) { return &actorpb.Result{MessageType: "ok"}, nil }}, nil
		}), store, nil)
	}
	caller := peer(t, url, ns, "caller", echoRuntime())
	id := Identity{"world", "51"}
	directories := []*discovery.ServiceDirectory{discovery.NewServiceDirectory(), discovery.NewServiceDirectory()}
	worldMembers(t, directories[0], "a")
	worldMembers(t, directories[1], "b")
	allocators := []*SlotAllocator{NewSlotAllocator(slotOwnership(t, store, directories[0]), caller, "world", worldSlot), NewSlotAllocator(slotOwnership(t, store, directories[1]), peer(t, url, ns, "caller-2", echoRuntime()), "world", worldSlot)}
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
	if _, err = caller.Call(ctx, Target{stored.RuntimeInstanceID, id}, &actorpb.Delivery{MessageType: "ping"}); err != nil {
		t.Fatal(err)
	}
	if loads.Load() != 1 {
		t.Fatal("竞争者重复加载", loads.Load())
	}
}

// 独立测试夹具显式构造底层能力；正式组合根直接使用 Discovery Runtime.Ownership。
func slotOwnership(t testing.TB, store discovery.SlotStore, directory discovery.Directory) *discovery.SlotOwnership {
	t.Helper()
	ownership, err := discovery.NewSlotOwnership(store, directory)
	if err != nil {
		t.Fatal(err)
	}
	return ownership
}
