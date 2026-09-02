package discovery

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdWorkSlotReuseChangesRuntimeIdentity(t *testing.T) {
	registry, raw, root := newIdentityIntegrationRegistry(t, "reuse", time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	firstLease, err := registry.client.Grant(ctx, time.Second)
	if err != nil {
		t.Fatalf("grant first lease: %v", err)
	}
	first, err := registry.claimWorkSlot(ctx, firstLease, "txgame", 17)
	if err != nil {
		t.Fatalf("claim first workSlot: %v", err)
	}
	if first.WorkSlot != 17 {
		t.Fatalf("first workSlot = %d, want 17", first.WorkSlot)
	}
	if err := firstLease.Stop(); err != nil {
		t.Fatalf("stop first lease: %v", err)
	}
	waitEtcdKeyMissing(t, ctx, raw, fmt.Sprintf("%s/work-slots/17", root))

	secondLease, err := registry.client.Grant(ctx, time.Second)
	if err != nil {
		t.Fatalf("grant second lease: %v", err)
	}
	t.Cleanup(func() { _ = secondLease.Revoke(context.Background()) })
	second, err := registry.claimWorkSlot(ctx, secondLease, "txgame", 17)
	if err != nil {
		t.Fatalf("claim second workSlot: %v", err)
	}
	if second.WorkSlot != 17 {
		t.Fatalf("second workSlot = %d, want 17", second.WorkSlot)
	}
	if second.ModRevision == first.ModRevision || second.InstanceID == first.InstanceID {
		t.Fatalf("reused identity did not change: first=%#v second=%#v", first, second)
	}
}

func TestEtcdWorkSlotDomainsAreIndependent(t *testing.T) {
	firstRegistry, _, _ := newIdentityIntegrationRegistry(t, "domain-a", 2*time.Second)
	secondRegistry, _, _ := newIdentityIntegrationRegistry(t, "domain-b", 2*time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	firstLease, err := firstRegistry.client.Grant(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("grant first lease: %v", err)
	}
	t.Cleanup(func() { _ = firstLease.Revoke(context.Background()) })
	secondLease, err := secondRegistry.client.Grant(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("grant second lease: %v", err)
	}
	t.Cleanup(func() { _ = secondLease.Revoke(context.Background()) })
	first, err := firstRegistry.claimWorkSlot(ctx, firstLease, "txgame", 17)
	if err != nil {
		t.Fatalf("claim first domain: %v", err)
	}
	second, err := secondRegistry.claimWorkSlot(ctx, secondLease, "txgame", 17)
	if err != nil {
		t.Fatalf("claim second domain: %v", err)
	}
	if first.WorkSlot != 17 || second.WorkSlot != 17 {
		t.Fatalf("independent workSlots = %d and %d, want both 17", first.WorkSlot, second.WorkSlot)
	}
}

func TestEtcdWorkSlotExhaustionNeverUsesZero(t *testing.T) {
	registry, raw, root := newIdentityIntegrationRegistry(t, "exhausted", 2*time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	for first := 1; first <= WorkSlotCount; first += 100 {
		last := min(first+99, WorkSlotCount)
		operations := make([]clientv3.Op, 0, last-first+1)
		for slot := first; slot <= last; slot++ {
			operations = append(operations, clientv3.OpPut(fmt.Sprintf("%s/work-slots/%d", root, slot), "occupied"))
		}
		if _, err := raw.Txn(ctx).Then(operations...).Commit(); err != nil {
			t.Fatalf("occupy workSlots %d-%d: %v", first, last, err)
		}
	}

	lease, err := registry.client.Grant(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("grant claimant lease: %v", err)
	}
	t.Cleanup(func() { _ = lease.Revoke(context.Background()) })
	if _, err := registry.claimWorkSlot(ctx, lease, "txgame", 1); !errors.Is(err, ErrWorkSlotExhausted) {
		t.Fatalf("claimWorkSlot error = %v, want ErrWorkSlotExhausted", err)
	}
	response, err := raw.Get(ctx, root+"/work-slots/0")
	if err != nil {
		t.Fatalf("read reserved workSlot: %v", err)
	}
	if len(response.Kvs) != 0 {
		t.Fatal("reserved workSlot 0 was created")
	}
}

func TestEtcdRuntimeReportsIdentityLoss(t *testing.T) {
	registry, raw, _ := newIdentityIntegrationRegistry(t, "identity-loss", 2*time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	registration, err := registry.RegisterRuntime(ctx, "txgame", Instance{
		Services: []string{"login"},
		Endpoint: "127.0.0.1:9000",
		Status:   StatusStarting,
	})
	if err != nil {
		t.Fatalf("RegisterRuntime: %v", err)
	}
	runtime, err := newRuntime(registry, registration, registry.Slots())
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}
	if _, err := raw.Revoke(ctx, clientv3.LeaseID(registration.lease.ID())); err != nil {
		t.Fatalf("revoke runtime lease: %v", err)
	}
	select {
	case <-runtime.Done():
	case <-ctx.Done():
		t.Fatalf("wait Runtime.Done: %v", ctx.Err())
	}
	if err := runtime.Err(); !errors.Is(err, ErrRuntimeIdentityLost) || !errors.Is(err, ErrEtcdLeaseLost) {
		t.Fatalf("Runtime.Err = %v, want identity loss with lease cause", err)
	}
}

func newIdentityIntegrationRegistry(t *testing.T, suffix string, ttl time.Duration) (*EtcdRegistry, *clientv3.Client, string) {
	t.Helper()
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_ETCD_ENDPOINT to run the etcd integration test")
	}
	root := fmt.Sprintf("/tx/tests/identity-%s-%d", suffix, time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	registry, err := NewEtcdRegistry(ctx, EtcdConfig{
		Endpoints:   []string{endpoint},
		Prefix:      root,
		LeaseTTL:    ttl,
		DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewEtcdRegistry: %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	raw, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatalf("create raw etcd client: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		_, _ = raw.Delete(cleanupCtx, root+"/", clientv3.WithPrefix())
		_ = raw.Close()
	})
	return registry, raw, root
}

func waitEtcdKeyMissing(t *testing.T, ctx context.Context, client *clientv3.Client, key string) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		response, err := client.Get(ctx, key)
		if err == nil && len(response.Kvs) == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait etcd key %q deletion: %v", key, ctx.Err())
		case <-ticker.C:
		}
	}
}
