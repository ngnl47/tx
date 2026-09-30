package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ngnl47/tx/internal/framework"
	"github.com/ngnl47/tx/modules/discovery"
	clientv3 "go.etcd.io/etcd/client/v3"
)

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

	registry, err := discovery.NewEtcdRegistry(ctx, discovery.EtcdConfig{
		Endpoints:   []string{endpoint},
		Prefix:      fmt.Sprintf("/tx/tests/slots-%d", time.Now().UnixNano()),
		LeaseTTL:    5 * time.Second,
		DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("create etcd registry: %v", err)
	}
	defer registry.Close()

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
	if err := cached.PromoteSlot(ctx, current, next); err != nil {
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

func TestConcurrentRuntimeClaimsUseDifferentWorkSlots(t *testing.T) {
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
	if first.runtime.Identity().WorkSlot == second.runtime.Identity().WorkSlot {
		t.Fatalf("workSlot duplicated: %d", first.runtime.Identity().WorkSlot)
	}
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
	var workSlot uint16
	var modRevision int64
	matched, err := fmt.Sscanf(value, appID+"-%d-%d", &workSlot, &modRevision)
	if err != nil || matched != 2 || workSlot == 0 || workSlot > discovery.WorkSlotCount || modRevision <= 0 ||
		value != fmt.Sprintf("%s-%d-%d", appID, workSlot, modRevision) {
		t.Fatalf("InstanceID = %q, want %s-<workSlot>-<ModRevision>", value, appID)
	}
}

func TestEtcdRunningRuntimeCloseKeepsWorkSlotUntilTTL(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	prefix := uniqueDiscoveryPrefix(t, "runtime-close")
	const leaseTTL = 2 * time.Second
	runtime := newIntegrationRuntime(t, "txgame", prefix, leaseTTL)
	if err := runtime.MarkRunning(t.Context()); err != nil {
		t.Fatalf("MarkRunning: %v", err)
	}
	identity := runtime.Identity()
	instance := runtime.Instance()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatalf("create etcd assertion client: %v", err)
	}
	defer client.Close()
	if err := runtime.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	instanceKey := prefix + "/instances/" + instance.ID
	workSlotKey := fmt.Sprintf("%s/work-slots/%d", prefix, identity.WorkSlot)
	if response, err := client.Get(t.Context(), instanceKey); err != nil || len(response.Kvs) != 0 {
		t.Fatalf("instance key after Close: count=%d err=%v", len(response.Kvs), err)
	}
	if response, err := client.Get(t.Context(), workSlotKey); err != nil || len(response.Kvs) != 1 {
		t.Fatalf("workSlot key immediately after Close: count=%d err=%v", len(response.Kvs), err)
	}
	waitWorkSlotReleased(t, client, workSlotKey, 2*leaseTTL)
	if err := runtime.Err(); !errors.Is(err, discovery.ErrRuntimeClosed) {
		t.Fatalf("Runtime.Err = %v, want ErrRuntimeClosed", err)
	}
}

func TestEtcdStartingRuntimeCloseRevokesWorkSlotImmediately(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	prefix := uniqueDiscoveryPrefix(t, "runtime-start-failure")
	runtime := newIntegrationRuntime(t, "txgame", prefix, 5*time.Second)
	workSlotKey := fmt.Sprintf("%s/work-slots/%d", prefix, runtime.Identity().WorkSlot)
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatalf("create etcd assertion client: %v", err)
	}
	defer client.Close()
	if err := runtime.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	response, err := client.Get(t.Context(), workSlotKey)
	if err != nil {
		t.Fatalf("read workSlot after Close: %v", err)
	}
	if len(response.Kvs) != 0 {
		t.Fatal("starting Runtime retained workSlot after Close")
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

func (service *frameworkDiscoveryIdentityService) Start(ctx framework.Context) error {
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

func waitWorkSlotReleased(t *testing.T, client *clientv3.Client, key string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		response, err := client.Get(ctx, key)
		if err == nil && len(response.Kvs) == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait workSlot %q release: %v", key, ctx.Err())
		case <-ticker.C:
		}
	}
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

func waitSlotAssignment(t *testing.T, ctx context.Context, store discovery.SlotStore, want discovery.SlotAssignment) {
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

func waitSlotMissing(t *testing.T, ctx context.Context, store discovery.SlotStore, slotID string) {
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
