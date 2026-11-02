package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ngnl47/tx/internal/framework"
	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/discovery"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdRouteRefreshRepairsWithoutWatchEcho(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/tx/tests/route-refresh-%d/instances", time.Now().UnixNano())
	r, err := discovery.NewEtcdRegistry(ctx, discovery.EtcdConfig{Endpoints: []string{endpoint}, Prefix: prefix, LeaseTTL: 5 * time.Second, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cleanup, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup.Close()
	defer func() {
		_, err := cleanup.Delete(context.Background(), prefix[:len(prefix)-len("instances")], clientv3.WithPrefix())
		if err != nil {
			t.Error(err)
		}
	}()
	putSlotTestMembers(t, ctx, endpoint, prefix, "old", "new")
	old := discovery.SlotAssignment{SlotID: "slot", RuntimeInstanceID: "old", Generation: 1}
	next := discovery.SlotAssignment{SlotID: "slot", RuntimeInstanceID: "new", Generation: 2}
	if err := r.Slots().CreateSlot(ctx, old); err != nil {
		t.Fatal(err)
	}
	_, oldRevision, err := r.Slots().ResolveSlotAtRevision(ctx, "slot")
	if err != nil {
		t.Fatal(err)
	}
	d := discovery.NewSlotDirectory()
	_ = d.ReplaceSnapshot([]discovery.SlotAssignment{old}, oldRevision)
	cached, _ := discovery.NewCachedSlotStore(r.Slots(), d)
	members := discovery.NewServiceDirectory()
	_ = members.Replace([]discovery.Instance{
		{ID: "old", Services: []string{"game"}, Endpoint: "old:1", Status: discovery.StatusRunning},
		{ID: "new", Services: []string{"game"}, Endpoint: "new:1", Status: discovery.StatusRunning},
	})
	resolver, _ := discovery.NewSlotResolver(cached, members)
	tasks := lifecycle.NewWaitGroupContext(ctx)
	defer func() { tasks.Cancel(); tasks.Wait() }()
	resolver.BindRefresh(lifecycle.TaskView(tasks))
	if err := r.Slots().PromoteSlot(ctx, old, next); err != nil {
		t.Fatal(err)
	}
	resolver.Invalidate(old)
	got, err := resolver.ResolveServiceSlot(ctx, "game", "slot")
	if err != nil || got.Assignment != next {
		t.Fatalf("未回源修正: %+v %v", got, err)
	}
	_ = d.ReplaceSnapshot([]discovery.SlotAssignment{old}, oldRevision)
	resolver.Invalidate(old)
	got, err = resolver.ResolveServiceSlot(ctx, "game", "slot")
	if err != nil || got.Assignment != next {
		t.Fatalf("旧反馈覆盖新路由: %+v %v", got, err)
	}
}

func TestEtcdRegistryLifecycleIntegration(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	registry, err := discovery.NewEtcdRegistry(ctx, discovery.EtcdConfig{
		Endpoints:   []string{endpoint},
		Prefix:      fmt.Sprintf("/tx/tests/discovery-%d", time.Now().UnixNano()),
		LeaseTTL:    5 * time.Second,
		DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("create etcd registry: %v", err)
	}
	defer registry.Close()

	directory := discovery.NewServiceDirectory()
	watchCtx, stopWatch := context.WithCancel(ctx)
	watchResult := make(chan error, 1)
	go func() {
		watchResult <- registry.Watch(watchCtx, directory)
	}()

	registration, err := registry.Register(ctx, "txgame", discovery.Instance{
		Services: []string{"game"},
		Endpoint: "127.0.0.1:9000",
		Version:  "test",
		Status:   discovery.StatusStarting,
	})
	if err != nil {
		stopWatch()
		t.Fatalf("register etcd instance: %v", err)
	}
	defer registration.Close(context.Background())
	instanceID := registration.Instance().ID

	if _, ok := directory.NextInstance("game"); ok {
		t.Fatal("starting instance must not be selected by service route")
	}
	if _, ok := directory.LookupInstance(instanceID); ok {
		t.Fatal("starting instance must not be selected by exact route")
	}

	if err := registration.SetStatus(ctx, discovery.StatusRunning); err != nil {
		t.Fatalf("mark instance running: %v", err)
	}
	waitDirectory(t, ctx, directory, func() bool {
		instance, ok := directory.NextInstance("game")
		return ok && instance.ID == instanceID
	})

	if err := registration.SetStatus(ctx, discovery.StatusStopping); err != nil {
		t.Fatalf("mark instance stopping: %v", err)
	}
	waitDirectory(t, ctx, directory, func() bool {
		instance, ok := directory.LookupInstance(instanceID)
		return ok && instance.Status == discovery.StatusStopping
	})
	if _, ok := directory.NextInstance("game"); ok {
		t.Fatal("stopping instance must not be selected by service route")
	}

	if err := registration.Close(ctx); err != nil {
		t.Fatalf("close registration: %v", err)
	}
	waitDirectory(t, ctx, directory, func() bool {
		_, ok := directory.LookupInstance(instanceID)
		return !ok
	})

	stopWatch()
	if err := <-watchResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("watch error = %v, want context canceled", err)
	}
}

func TestEtcdSlotStorePromotesWithCompareAndSwap(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("/tx/tests/slots-%d", time.Now().UnixNano())
	registry, err := discovery.NewEtcdRegistry(ctx, discovery.EtcdConfig{
		Endpoints:   []string{endpoint},
		Prefix:      prefix,
		LeaseTTL:    5 * time.Second,
		DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("create etcd registry: %v", err)
	}
	defer registry.Close()

	putSlotTestMembers(t, ctx, endpoint, prefix, "pod-uid-a", "pod-uid-b")
	slots := registry.Slots()
	current := discovery.SlotAssignment{
		SlotID:            "game-slot-1",
		RuntimeInstanceID: "pod-uid-a",
		Generation:        42,
	}
	if err := slots.CreateSlot(ctx, current); err != nil {
		t.Fatalf("CreateSlot() error = %v", err)
	}
	resolved, err := slots.ResolveSlot(ctx, current.SlotID)
	if err != nil {
		t.Fatalf("ResolveSlot() error = %v", err)
	}
	if resolved != current {
		t.Fatalf("resolved slot = %#v, want %#v", resolved, current)
	}

	next := discovery.SlotAssignment{
		SlotID:            current.SlotID,
		RuntimeInstanceID: "pod-uid-b",
		Generation:        43,
	}
	if err := slots.PromoteSlot(ctx, current, next); err != nil {
		t.Fatalf("PromoteSlot() error = %v", err)
	}
	resolved, err = slots.ResolveSlot(ctx, current.SlotID)
	if err != nil {
		t.Fatalf("ResolveSlot() after promotion error = %v", err)
	}
	if resolved != next {
		t.Fatalf("promoted slot = %#v, want %#v", resolved, next)
	}
	if err := slots.PromoteSlot(ctx, current, next); !errors.Is(err, discovery.ErrSlotConflict) {
		t.Fatalf("stale PromoteSlot() error = %v, want ErrSlotConflict", err)
	}
}

func TestEtcdSlotWatchMaintainsCachedDirectoryAndFailsClosed(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("/tx/tests/slot-watch-%d", time.Now().UnixNano())
	registry, err := discovery.NewEtcdRegistry(ctx, discovery.EtcdConfig{
		Endpoints:   []string{endpoint},
		Prefix:      prefix,
		LeaseTTL:    5 * time.Second,
		DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("create etcd registry: %v", err)
	}
	defer registry.Close()

	putSlotTestMembers(t, ctx, endpoint, prefix, "pod-uid-a", "pod-uid-b")
	slots := registry.Slots()
	current := discovery.SlotAssignment{
		SlotID:            "game-slot-1",
		RuntimeInstanceID: "pod-uid-a",
		Generation:        42,
	}
	if err := slots.CreateSlot(ctx, current); err != nil {
		t.Fatalf("CreateSlot() error = %v", err)
	}
	directory := discovery.NewSlotDirectory()
	cached, err := discovery.NewCachedSlotStore(slots, directory)
	if err != nil {
		t.Fatalf("NewCachedSlotStore() error = %v", err)
	}
	watchCtx, stopWatch := context.WithCancel(ctx)
	watchResult := make(chan error, 1)
	go func() {
		watchResult <- slots.Watch(watchCtx, directory)
	}()

	waitSlotAssignment(t, ctx, cached, current)
	next := discovery.SlotAssignment{
		SlotID:            current.SlotID,
		RuntimeInstanceID: "pod-uid-b",
		Generation:        43,
	}
	if err := slots.PromoteSlot(ctx, current, next); err != nil {
		t.Fatalf("PromoteSlot() error = %v", err)
	}
	waitSlotAssignment(t, ctx, cached, next)
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatalf("create etcd cleanup client: %v", err)
	}
	defer client.Close()
	if _, err := client.Delete(ctx, prefix+"/slots/"+current.SlotID); err != nil {
		t.Fatalf("delete slot assignment: %v", err)
	}
	waitSlotMissing(t, ctx, cached, current.SlotID)

	stopWatch()
	if err := <-watchResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("watch error = %v, want context canceled", err)
	}
	if _, err := cached.ResolveSlot(ctx, current.SlotID); !errors.Is(err, discovery.ErrSlotDirectoryUnavailable) {
		t.Fatalf("ResolveSlot() after watch stop error = %v, want ErrSlotDirectoryUnavailable", err)
	}
}

func TestEtcdSlotStoreSurvivesRegistryClose(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	config := discovery.EtcdConfig{
		Endpoints:   []string{endpoint},
		Prefix:      fmt.Sprintf("/tx/tests/slots-durable-%d", time.Now().UnixNano()),
		LeaseTTL:    5 * time.Second,
		DialTimeout: time.Second,
	}
	registry, err := discovery.NewEtcdRegistry(ctx, config)
	if err != nil {
		t.Fatalf("create first etcd registry: %v", err)
	}
	putSlotTestMembers(t, ctx, endpoint, config.Prefix, "pod-uid-a")
	assignment := discovery.SlotAssignment{
		SlotID:            "game-slot-1",
		RuntimeInstanceID: "pod-uid-a",
		Generation:        42,
	}
	if err := registry.Slots().CreateSlot(ctx, assignment); err != nil {
		_ = registry.Close()
		t.Fatalf("CreateSlot() error = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatalf("close first etcd registry: %v", err)
	}

	reopened, err := discovery.NewEtcdRegistry(ctx, config)
	if err != nil {
		t.Fatalf("reopen etcd registry: %v", err)
	}
	defer reopened.Close()
	resolved, err := reopened.Slots().ResolveSlot(ctx, assignment.SlotID)
	if err != nil {
		t.Fatalf("ResolveSlot() after registry close error = %v", err)
	}
	if resolved != assignment {
		t.Fatalf("durable slot = %#v, want %#v", resolved, assignment)
	}
}

func TestConcurrentRuntimesUseDifferentInstanceIDs(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	prefix := uniqueDiscoveryPrefix(t, "concurrent-runtime")
	type result struct {
		runtime *discovery.Runtime
		err     error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			runtime, err := openIntegrationRuntime(t.Context(), endpoint, "txgame", prefix, 2*time.Second)
			results <- result{runtime: runtime, err: err}
		}()
	}
	first := <-results
	second := <-results
	if first.err != nil || second.err != nil {
		if first.runtime != nil {
			_ = first.runtime.Close(context.Background())
		}
		if second.runtime != nil {
			_ = second.runtime.Close(context.Background())
		}
		t.Fatalf("runtime claims failed: first=%v second=%v", first.err, second.err)
	}
	t.Cleanup(func() {
		_ = first.runtime.Close(context.Background())
		_ = second.runtime.Close(context.Background())
	})
	if first.runtime.Instance().ID == second.runtime.Instance().ID {
		t.Fatalf("InstanceID duplicated: %q", first.runtime.Instance().ID)
	}
}

func TestFrameworkDiscoveryAutomaticIdentityAssignsDistinctInstanceIDs(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	prefix := uniqueDiscoveryPrefix(t, "framework-auto-identity")
	first := newFrameworkDiscoveryServer(t, endpoint, prefix, "127.0.0.1:9101", &frameworkDiscoveryIdentityService{name: "game"})
	second := newFrameworkDiscoveryServer(t, endpoint, prefix, "127.0.0.1:9102", &frameworkDiscoveryIdentityService{name: "game"})
	if err := first.Start(t.Context(), framework.RunOptions{Services: []string{"game"}, InstanceEndpoint: "127.0.0.1:9101"}); err != nil {
		t.Fatalf("start first framework server: %v", err)
	}
	t.Cleanup(func() { _ = first.Stop(context.Background()) })
	if err := second.Start(t.Context(), framework.RunOptions{Services: []string{"game"}, InstanceEndpoint: "127.0.0.1:9102"}); err != nil {
		t.Fatalf("start second framework server: %v", err)
	}
	t.Cleanup(func() { _ = second.Stop(context.Background()) })
	firstID := first.Context().Instance().ID
	secondID := second.Context().Instance().ID
	assertGeneratedInstanceIDShape(t, firstID, "txgame")
	assertGeneratedInstanceIDShape(t, secondID, "txgame")
	if firstID == "" || secondID == "" {
		t.Fatalf("automatic InstanceID values = %q, %q", firstID, secondID)
	}
	if firstID == secondID {
		t.Fatalf("automatic InstanceID duplicated: %q", firstID)
	}
}

func TestFrameworkDiscoveryAutomaticIdentityIsSharedAcrossServices(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	first := &frameworkDiscoveryIdentityService{name: "game"}
	second := &frameworkDiscoveryIdentityService{name: "login"}
	server := newFrameworkDiscoveryServer(t, endpoint, uniqueDiscoveryPrefix(t, "framework-shared-identity"), "127.0.0.1:9103", first, second)
	if err := server.Start(t.Context(), framework.RunOptions{Services: []string{"game", "login"}, InstanceEndpoint: "127.0.0.1:9103"}); err != nil {
		t.Fatalf("start framework server: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	assertGeneratedInstanceIDShape(t, first.instanceID, "txgame")
	assertGeneratedInstanceIDShape(t, second.instanceID, "txgame")
	if first.instanceID == "" || second.instanceID == "" {
		t.Fatalf("service InstanceID values = %q, %q", first.instanceID, second.instanceID)
	}
	if first.instanceID != second.instanceID {
		t.Fatalf("service InstanceID values differ: %q, %q", first.instanceID, second.instanceID)
	}
}

func assertGeneratedInstanceIDShape(t *testing.T, value, appID string) {
	t.Helper()
	prefix := appID + "-"
	if !strings.HasPrefix(value, prefix) {
		t.Fatalf("InstanceID = %q, want AppID prefix %q", value, prefix)
	}
	if _, err := uuid.Parse(strings.TrimPrefix(value, prefix)); err != nil {
		t.Fatalf("InstanceID = %q, want AppID + UUID: %v", value, err)
	}
}

func TestEtcdRuntimeCloseRevokesMemberLeaseImmediately(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	for _, running := range []bool{false, true} {
		t.Run(fmt.Sprintf("running-%t", running), func(t *testing.T) {
			prefix := uniqueDiscoveryPrefix(t, "runtime-close")
			runtime := newIntegrationRuntime(t, "txgame", prefix, 5*time.Second)
			if running {
				if err := runtime.MarkRunning(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			key := prefix + "/instances/" + runtime.Instance().ID
			member, err := client.Get(t.Context(), key)
			if err != nil {
				t.Fatal(err)
			}
			if len(member.Kvs) != 1 || member.Kvs[0].Lease == 0 {
				t.Fatalf("成员未绑定租约: %v", member.Kvs)
			}
			leaseID := clientv3.LeaseID(member.Kvs[0].Lease)
			if err := runtime.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			remaining, err := client.Get(t.Context(), prefix+"/", clientv3.WithPrefix())
			if err != nil {
				t.Fatal(err)
			}
			if len(remaining.Kvs) != 0 {
				t.Fatalf("Close 后残留成员或 work slot: %v", remaining.Kvs)
			}
			lease, err := client.TimeToLive(t.Context(), leaseID)
			if err != nil {
				t.Fatal(err)
			}
			if lease.TTL != -1 {
				t.Fatalf("Close 后租约仍存在: TTL=%d", lease.TTL)
			}
			if err := runtime.Err(); !errors.Is(err, discovery.ErrRuntimeClosed) {
				t.Fatalf("Runtime.Err = %v", err)
			}
		})
	}
}

func uniqueDiscoveryPrefix(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("/tx/tests/%s-%d", suffix, time.Now().UnixNano())
}

func newIntegrationRuntime(t *testing.T, appID, prefix string, leaseTTL time.Duration) *discovery.Runtime {
	t.Helper()
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	runtime, err := openIntegrationRuntime(t.Context(), endpoint, appID, prefix, leaseTTL)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	return runtime
}

func openIntegrationRuntime(ctx context.Context, endpoint, appID, prefix string, leaseTTL time.Duration) (*discovery.Runtime, error) {
	return discovery.NewRuntime(ctx, discovery.EtcdConfig{
		Endpoints:   []string{endpoint},
		Prefix:      prefix,
		LeaseTTL:    leaseTTL,
		DialTimeout: time.Second,
	}, appID, discovery.Instance{
		Services: []string{"login"},
		Endpoint: "127.0.0.1:9000",
		Status:   discovery.StatusStarting,
	})
}

type frameworkDiscoveryIdentityService struct {
	name       string
	instanceID string
}

func (service *frameworkDiscoveryIdentityService) Spec() framework.Spec {
	return framework.Spec{Name: service.name, Requires: []framework.CModID{framework.CModDiscovery}}
}

func (service *frameworkDiscoveryIdentityService) Init(ctx framework.Context) error {
	service.instanceID = ctx.Instance().ID
	return nil
}

func newFrameworkDiscoveryServer(t *testing.T, endpoint, prefix, instanceEndpoint string, services ...framework.Service) *framework.Server {
	t.Helper()
	server, err := framework.New(
		framework.Config{
			AppID: "txgame",
			Discovery: framework.DiscoveryConfig{Etcd: discovery.EtcdConfig{
				Endpoints:   []string{endpoint},
				Prefix:      prefix,
				LeaseTTL:    2 * time.Second,
				DialTimeout: time.Second,
			}},
		},
		framework.WithService(services...),
	)
	if err != nil {
		t.Fatalf("new framework server for %s: %v", instanceEndpoint, err)
	}
	return server
}

func waitDirectory(t *testing.T, ctx context.Context, directory *discovery.ServiceDirectory, condition func() bool) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait discovery directory: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitSlotAssignment(t *testing.T, ctx context.Context, store discovery.SlotReader, want discovery.SlotAssignment) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		assignment, err := store.ResolveSlot(ctx, want.SlotID)
		if err == nil && assignment == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait slot assignment %#v: %v", want, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitSlotMissing(t *testing.T, ctx context.Context, store discovery.SlotReader, slotID string) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := store.ResolveSlot(ctx, slotID)
		if errors.Is(err, discovery.ErrSlotNotFound) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait slot %q deletion: %v", slotID, ctx.Err())
		case <-ticker.C:
		}
	}
}

// 持久槽位测试也登记权威成员，避免把仅存在于内存的假成员当作可分配宿主。
func putSlotTestMembers(t *testing.T, ctx context.Context, endpoint, prefix string, ids ...string) {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	prefix = strings.TrimSuffix(prefix, "/instances") + "/instances/"
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for _, id := range ids {
			if _, err := client.Delete(cleanup, prefix+id); err != nil {
				t.Error(err)
			}
		}
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, id := range ids {
		payload, err := json.Marshal(discovery.Instance{ID: id, Services: []string{"game"}, Endpoint: id + ":1", Status: discovery.StatusRunning})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Put(ctx, prefix+id, string(payload)); err != nil {
			t.Fatal(err)
		}
	}
}
