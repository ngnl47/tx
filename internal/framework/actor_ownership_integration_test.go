package framework

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/txn"
	"github.com/ngnl47/tx/modules/writeback"
	"github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
	"github.com/ngnl47/tx/storage/redis/goredis"
	"github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ownershipStore 只提供一个可回滚字段的 Store 契约；生成 GD 的验收仍在独立示例中。
type ownershipStore struct {
	key      string
	value    int64
	dirty    bool
	recorder change.UndoRecorder
}

func (s *ownershipStore) TypeSpec() meta.TypeSpec {
	return meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{FullName: "OwnershipIntegration"}},
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

func (p ownershipProcedure) Locks(context.Context) (txn.Lockeys, error) {
	return txn.Lockeys{txn.NewLockey(p.store.key, 0, 51)}, nil
}
func (p ownershipProcedure) Process(tx *txn.Tx) error {
	if err := tx.Enlist(txn.NewStoreTarget(txn.NewLockey(p.store.key, 0, 51), p.store)); err != nil {
		return err
	}
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

// 每次验收使用独立 etcd 前缀；所有节点均由正式 Server 启停。
func ownershipTestConfig(t *testing.T) (Config, *clientv3.Client) {
	t.Helper()
	url, endpoint := os.Getenv("TX_ACTORTRIAL_NATS_URL"), os.Getenv("TX_ETCD_ENDPOINT")
	if url == "" || endpoint == "" {
		t.Skip("设置 TX_ACTORTRIAL_NATS_URL 和 TX_ETCD_ENDPOINT 运行归属集成测试")
	}
	owner := rand.Text()
	config := Config{AppID: "ownership-integration", Discovery: DiscoveryConfig{Etcd: discovery.EtcdConfig{
		Endpoints: []string{endpoint}, Prefix: "/tx/tests/ownership/" + owner, LeaseTTL: 2 * time.Second, DialTimeout: time.Second,
	}}}
	config.Actor.Runtime = txactor.Config{NATSURL: url, Namespace: owner}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer client.Close()
		if t.Failed() {
			t.Log("保留失败范围", config.Discovery.Etcd.Prefix)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := client.Delete(ctx, config.Discovery.Etcd.Prefix+"/", clientv3.WithPrefix()); err != nil {
			t.Error(err)
		}
	})
	return config, client
}

func startOwnershipServer(t *testing.T, ctx context.Context, config Config, factory txactor.BehaviorFactory, flush func() error) *Server {
	t.Helper()
	mod := &retryFlushCMod{flush: flush}
	kind := txactor.MustType("world", factory, txactor.WithSlotMapping(func(id txactor.Identity) string { return "world-" + id.ID }))
	app, err := New(config, WithCMod(mod), WithService(testService{spec: Spec{Name: "world", ActorTypes: []txactor.Type{kind}, Requires: []CModID{mod.Spec().ID}}}))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Start(ctx, RunOptions{Services: []string{"world"}, RPCListen: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := app.Stop(cleanup); err != nil && !errors.Is(err, discovery.ErrRuntimeIdentityLost) {
			t.Error(err)
		}
	})
	return app
}

func waitOwnership(t *testing.T, ctx context.Context, check func() bool) {
	t.Helper()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !check() {
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func TestActorOwnershipWritebackHandoff(t *testing.T) {
	address := os.Getenv("TX_ACTORTRIAL_REDIS_ADDR")
	if address == "" {
		t.Skip("设置 TX_ACTORTRIAL_REDIS_ADDR 验证事务写回")
	}
	config, _ := ownershipTestConfig(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: address})
	defer client.Close()
	key := "tx:ownership:" + rand.Text()
	defer func() {
		if !t.Failed() {
			if err := client.Del(context.Background(), key+":{51}").Err(); err != nil {
				t.Error(err)
			}
		}
	}()
	adapter, err := txredis.NewAdapter(goredis.NewClient(client))
	if err != nil {
		t.Fatal(err)
	}
	root := &ownershipStore{key: key}
	if err := storage.Create(ctx, adapter, root); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("注入最终写回失败")
	var writes, notifications, loads atomic.Int32
	manager := writeback.NewManager(writeback.Dependencies{Writer: ownershipWriter{adapter, func(context.Context) error {
		if writes.Add(1) == 1 {
			return failure
		}
		return nil
	}}}, writeback.Config{FlushInterval: time.Hour, ShutdownFlushTimeout: time.Second})
	executor := txn.NewRuntime()
	if err := executor.BindCommitParticipant(manager); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	a := startOwnershipServer(t, ctx, config, func(txactor.Context) (*txactor.Behavior, error) {
		return &txactor.Behavior{Receive: func(c txactor.Context, value any) (any, error) {
			d := value.(txactor.Delivery)
			if d.MessageType == "block" {
				close(entered)
				<-release
			}
			if err := executor.Call(c, ownershipProcedure{root, int64(d.CommandHash)}); err != nil {
				return nil, err
			}
			return txactor.Result{MessageType: "value", Payload: []byte(fmt.Sprint(root.value))}, nil
		}, OnDeactivateBefore: func(txactor.Context) error { notifications.Add(1); return nil }}, nil
	}, func() error { return manager.Shutdown(context.Background()) })
	id := txactor.Identity{Type: "world", ID: "51"}
	first, err := a.Context().Actor().CallFuture(ctx, id, txactor.Delivery{MessageType: "block", CommandHash: 3})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	queued, err := a.Context().Actor().CallFuture(ctx, id, txactor.Delivery{MessageType: "add", CommandHash: 4})
	if err != nil {
		t.Fatal(err)
	}
	waitOwnership(t, ctx, func() bool { return a.runtime.entities.Probe(id).PendingMessages == 1 })
	current, err := a.discovery.AuthoritativeSlots().ResolveSlot(ctx, "world-51")
	if err != nil {
		t.Fatal(err)
	}
	b := startOwnershipServer(t, ctx, config, func(c txactor.Context) (*txactor.Behavior, error) {
		loads.Add(1)
		doc, found, err := adapter.LoadDocument(c, root.TypeSpec(), root.RecordKey())
		if err != nil || !found {
			return nil, fmt.Errorf("加载持久值: found=%v err=%v", found, err)
		}
		return &txactor.Behavior{Receive: func(txactor.Context, any) (any, error) {
			return txactor.Result{MessageType: "value", Payload: []byte(fmt.Sprint(doc["value"]))}, nil
		}}, nil
	}, func() error { return nil })
	waitOwnership(t, ctx, func() bool { return len(b.discovery.Directory().RunningInstances("world")) == 2 })
	nextID := b.runtime.instanceSnapshot().ID
	handoff := func(c context.Context) (discovery.SlotAssignment, error) {
		return b.discovery.Ownership().Handoff(c, "world", current, nextID, func(context.Context, string) (bool, error) { return true, nil }, a.PrepareHandoff)
	}
	result := make(chan error, 1)
	go func() { _, err := handoff(ctx); result <- err }()
	waitOwnership(t, ctx, a.runtime.entities.Draining)
	if writes.Load() != 0 || loads.Load() != 0 {
		t.Fatal("排空前写回或加载目标")
	}
	unblock()
	for _, f := range []*txactor.Future{first, queued} {
		if _, err := f.Await(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-result; err == nil {
		t.Fatalf("未保留写回失败: %v", err)
	}
	if got, err := b.discovery.AuthoritativeSlots().ResolveSlot(ctx, current.SlotID); err != nil || got != current || manager.Pending() != 1 || !root.HasChanges() || loads.Load() != 0 {
		t.Fatalf("失败提前交接或丢失待写数据: %+v %v", got, err)
	}
	if doc, _, err := adapter.LoadDocument(ctx, root.TypeSpec(), root.RecordKey()); err != nil || fmt.Sprint(doc["value"]) != "0" {
		t.Fatalf("失败后持久值: %+v %v", doc, err)
	}
	next, err := handoff(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waitOwnership(t, ctx, func() bool {
		got, err := b.discovery.Slots().ResolveSlot(ctx, current.SlotID)
		return err == nil && got == next
	})
	got, err := b.Context().Actor().Call(ctx, id, txactor.Delivery{MessageType: "read"})
	if err != nil || string(got.Payload) != "7" {
		t.Fatalf("新 owner 未加载最终持久值: %+v %v", got, err)
	}
	if err := a.PrepareHandoff(ctx, current.RuntimeInstanceID); err != nil {
		t.Fatal(err)
	}
	if writes.Load() != 2 || notifications.Load() != 1 || manager.Pending() != 0 || root.HasChanges() {
		t.Fatalf("收尾重复或未完成: writes=%d exits=%d pending=%d", writes.Load(), notifications.Load(), manager.Pending())
	}
}

func TestActorOwnershipLeaseLossDoesNotReassign(t *testing.T) {
	config, client := ownershipTestConfig(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	factory := func(txactor.Context) (*txactor.Behavior, error) {
		return &txactor.Behavior{Receive: func(txactor.Context, any) (any, error) { return txactor.Result{MessageType: "ok"}, nil }}, nil
	}
	a := startOwnershipServer(t, ctx, config, factory, func() error { return nil })
	id := txactor.Identity{Type: "world", ID: "51"}
	if _, err := a.Context().Actor().Call(ctx, id, txactor.Delivery{MessageType: "ping"}); err != nil {
		t.Fatal(err)
	}
	current, err := a.discovery.AuthoritativeSlots().ResolveSlot(ctx, "world-51")
	if err != nil {
		t.Fatal(err)
	}
	b := startOwnershipServer(t, ctx, config, factory, func() error { return nil })
	registration, err := client.Get(ctx, config.Discovery.Etcd.Prefix+"/instances/"+current.RuntimeInstanceID)
	if err != nil || len(registration.Kvs) != 1 {
		t.Fatalf("读取租约: %+v %v", registration, err)
	}
	local := a.runtime.entities
	if _, err := client.Revoke(ctx, clientv3.LeaseID(registration.Kvs[0].Lease)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-a.stopDone:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !local.Draining() || !errors.Is(a.stopErr, discovery.ErrRuntimeIdentityLost) {
		t.Fatalf("租约丢失未触发正式排水: %v", a.stopErr)
	}
	waitOwnership(t, ctx, func() bool { _, old := b.discovery.Directory().LookupInstance(current.RuntimeInstanceID); return !old })
	if _, err := b.Context().Actor().EnsureActivation(ctx, id); !errors.Is(err, discovery.ErrSlotRuntimeUnavailable) {
		t.Fatalf("租约丢失后自动接管: %v", err)
	}
	if got, err := b.discovery.AuthoritativeSlots().ResolveSlot(ctx, current.SlotID); err != nil || got != current || b.runtime.entities.Probe(id).State != txactor.Unloaded {
		t.Fatalf("租约失效改写归属或加载: %+v %v", got, err)
	}
}
