package framework

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	txetcd "github.com/ngnl47/tx/etcd"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/sessionid"
)

func TestSessionIDServerRestartWithEtcd(t *testing.T) {
	endpoint, prefix, observer := sessionIDEtcdEnvironment(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	seen := make(map[int64]bool)
	var previousInstance string
	var previousHandle sessionid.Allocator
	for range 2 {
		server, handle := startSessionIDEtcdServer(t, ctx, endpoint, prefix)
		instanceID := server.Context().Instance().ID
		if instanceID == "" || instanceID == previousInstance {
			t.Fatalf("restart reused instance ID %q", instanceID)
		}
		if previousHandle != nil {
			if id, err := previousHandle.AllocateSessionID(ctx); id != 0 || !errors.Is(err, ErrSessionIDUnavailable) {
				t.Fatalf("old handle after restart = %d, %v", id, err)
			}
		}
		for range 32 {
			id, err := handle.AllocateSessionID(ctx)
			if err != nil || id <= 0 || id > sessionid.MaxSessionID || seen[id] {
				t.Fatalf("AllocateSessionID = %d, %v, duplicate=%t", id, err, seen[id])
			}
			seen[id] = true
		}
		client := server.discovery.client
		if err := server.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if err := client.Probe(ctx); err == nil {
			t.Fatal("framework Stop did not close its etcd client")
		}
		if id, err := handle.AllocateSessionID(ctx); id != 0 || !errors.Is(err, ErrSessionIDUnavailable) {
			t.Fatalf("stopped handle = %d, %v", id, err)
		}
		snapshot, err := observer.Snapshot(ctx, prefix+"/")
		if err != nil || len(snapshot.Values) != 0 {
			t.Fatalf("members after Stop = %#v, %v", snapshot.Values, err)
		}
		issuer, found, err := observer.Get(ctx, prefix+"-session-ids/issuer")
		if err != nil || !found || issuer.LeaseID != 0 || !seen[issuer.ModRevision] {
			t.Fatalf("issuer after Stop = %#v, %t, %v", issuer, found, err)
		}
		previousInstance, previousHandle = instanceID, handle
	}
}

func TestSessionIDStartupFailureClosesSharedClientWithEtcd(t *testing.T) {
	endpoint, prefix, observer := sessionIDEtcdEnvironment(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	wantErr := errors.New("login Start failed")
	var server *Server
	var client *txetcd.Client
	var handle sessionid.Allocator
	service := phasedService{
		testService: testService{spec: Spec{Name: "login", Requires: []CModID{CModSessionID}}},
		initHook: func(runtime Context) error {
			handle, client = runtime.SessionIDs(), server.discovery.client
			return client.Probe(ctx)
		},
		startHook: func(context.Context) error { return wantErr },
	}
	server = newDiscoveryIdentityTestServer(t, endpoint, prefix, "127.0.0.1:9190", service)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := server.Stop(cleanupCtx); err != nil {
			t.Errorf("stop failed server: %v", err)
		}
	})
	if err := server.Start(ctx, RunOptions{Services: []string{"login"}, InstanceEndpoint: "127.0.0.1:9190"}); !errors.Is(err, wantErr) {
		t.Fatalf("Start = %v, want %v", err, wantErr)
	}
	if err := client.Probe(ctx); err == nil {
		t.Fatal("failed Start did not close its etcd client")
	}
	if id, err := handle.AllocateSessionID(ctx); id != 0 || !errors.Is(err, ErrSessionIDUnavailable) {
		t.Fatalf("handle after failed Start = %d, %v", id, err)
	}
	snapshot, err := observer.Snapshot(ctx, prefix+"/")
	if err != nil || len(snapshot.Values) != 0 {
		t.Fatalf("members after failed Start = %#v, %v", snapshot.Values, err)
	}
	if _, found, err := observer.Get(ctx, prefix+"-session-ids/issuer"); err != nil || found {
		t.Fatalf("failed Start issued a session ID: found=%t, error=%v", found, err)
	}
}

func TestSessionIDDiscoveryClientOwnershipWithEtcd(t *testing.T) {
	endpoint, prefix, _ := sessionIDEtcdEnvironment(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	runtime, err := newDiscovery(ctx, discovery.EtcdConfig{
		Endpoints: []string{endpoint}, Prefix: prefix, LeaseTTL: 2 * time.Second,
	}, "login", discovery.Instance{Endpoint: "127.0.0.1:9190", Services: []string{"login"}, Status: discovery.StatusStarting})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := runtime.Close(cleanupCtx); err != nil {
			t.Errorf("close discovery: %v", err)
		}
	})
	if err := runtime.Runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runtime.client.Probe(ctx); err != nil {
		t.Fatalf("borrowed Runtime closed shared client: %v", err)
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runtime.client.Probe(ctx); err == nil {
		t.Fatal("framework owner did not close shared client")
	}
}

func TestSessionIDAllocationAndSlotWritesWithEtcd(t *testing.T) {
	endpoint, prefix, observer := sessionIDEtcdEnvironment(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	server, handle := startSessionIDEtcdServer(t, ctx, endpoint, prefix)
	root := prefix + "/"
	// 在快照和 CAS 之间明确插入一次发号，避免仅靠并行时序证明隔离。
	snapshot, err := observer.Snapshot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.AllocateSessionID(ctx); err != nil {
		t.Fatal(err)
	}
	guard := root + "cas-guard"
	if ok, err := observer.CompareAndSwapWithinSnapshot(ctx, root, snapshot.Revision, guard, 0, []byte("guard")); err != nil || !ok {
		t.Fatalf("session write invalidated Discovery CAS: %t, %v", ok, err)
	}
	if err := observer.Delete(ctx, guard); err != nil {
		t.Fatal(err)
	}

	const workers, perWorker, slotCount = 4, 64, 16
	ids := make(chan int64, workers*perWorker)
	errs := make(chan error, workers+1)
	start := make(chan struct{})
	var group sync.WaitGroup
	for range workers {
		group.Go(func() {
			<-start
			for range perWorker {
				id, err := handle.AllocateSessionID(ctx)
				if err != nil {
					errs <- err
					return
				}
				ids <- id
			}
		})
	}
	slots := server.discovery.AuthoritativeSlots().(*discovery.EtcdSlotStore)
	instanceID := server.Context().Instance().ID
	group.Go(func() {
		<-start
		for index := range slotCount {
			current := discovery.SlotAssignment{SlotID: fmt.Sprint("slot-", index), RuntimeInstanceID: instanceID, Generation: 1}
			if err := slots.CreateSlot(ctx, current); err != nil {
				errs <- fmt.Errorf("create %s: %w", current.SlotID, err)
				return
			}
			if err := slots.ReleaseSlot(ctx, current); err != nil {
				errs <- fmt.Errorf("release %s: %w", current.SlotID, err)
				return
			}
			got, err := slots.ResolveSlot(ctx, current.SlotID)
			if err != nil || !got.Released() || got.Generation != 2 {
				errs <- fmt.Errorf("released slot = %+v, %v", got, err)
				return
			}
		}
	})
	close(start)
	group.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	seen := make(map[int64]bool, workers*perWorker)
	for id := range ids {
		if id <= 0 || id > sessionid.MaxSessionID || seen[id] {
			t.Fatalf("invalid or duplicate session ID: %d", id)
		}
		seen[id] = true
	}
	if len(seen) != workers*perWorker {
		t.Fatalf("session count = %d, want %d", len(seen), workers*perWorker)
	}
	snapshot, err = observer.Snapshot(ctx, root)
	if err != nil || len(snapshot.Values) != slotCount+1 {
		t.Fatalf("Discovery root keys = %d, %v, want one member and %d slots", len(snapshot.Values), err, slotCount)
	}
	for _, value := range snapshot.Values {
		if value.Key != root+"instances/"+instanceID && !strings.HasPrefix(value.Key, root+"slots/") {
			t.Fatalf("unexpected Discovery key: %q", value.Key)
		}
	}
	for {
		got, err := server.discovery.Slots().ResolveSlot(ctx, "slot-15")
		if err == nil && got.Released() && got.Generation == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("slot directory did not observe release: %+v, %v", got, err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if _, found := server.discovery.Directory().LookupInstance(instanceID); !found {
		t.Fatal("session writes invalidated member directory")
	}
}

func startSessionIDEtcdServer(t *testing.T, ctx context.Context, endpoint, prefix string) (*Server, sessionid.Allocator) {
	t.Helper()
	var handle sessionid.Allocator
	server := newDiscoveryIdentityTestServer(t, endpoint, prefix, "127.0.0.1:9190", testService{
		spec: Spec{Name: "login", Requires: []CModID{CModSessionID}},
		start: func(runtime Context) error {
			handle = runtime.SessionIDs()
			return nil
		},
	})
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := server.Stop(cleanupCtx); err != nil {
			t.Errorf("stop server: %v", err)
		}
	})
	if err := server.Start(ctx, RunOptions{Services: []string{"login"}, InstanceEndpoint: "127.0.0.1:9190"}); err != nil {
		t.Fatal(err)
	}
	return server, handle
}

func sessionIDEtcdEnvironment(t *testing.T) (string, string, *txetcd.Client) {
	t.Helper()
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("设置 TX_ETCD_ENDPOINT 运行真实 etcd 测试")
	}
	prefix := "/tx/tests/framework-sessionid/" + uuid.NewString()
	client, err := txetcd.New(context.Background(), txetcd.Config{Endpoints: []string{endpoint}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer func() {
			if err := client.Close(); err != nil {
				t.Errorf("close observer: %v", err)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		snapshot, err := client.Snapshot(ctx, prefix+"/")
		if err != nil {
			t.Errorf("list owned test keys: %v", err)
			return
		}
		keys := []string{prefix + "-session-ids/issuer"}
		for _, value := range snapshot.Values {
			keys = append(keys, value.Key)
		}
		for _, key := range keys {
			if err := client.Delete(ctx, key); err != nil {
				t.Errorf("delete owned test key %q: %v", key, err)
			}
		}
		if snapshot, err := client.Snapshot(ctx, prefix+"/"); err != nil || len(snapshot.Values) != 0 {
			t.Errorf("owned keys after cleanup: %#v, %v", snapshot.Values, err)
		}
		if _, found, err := client.Get(ctx, keys[0]); err != nil || found {
			t.Errorf("issuer after cleanup: found=%t, error=%v", found, err)
		}
	})
	return endpoint, prefix, client
}
