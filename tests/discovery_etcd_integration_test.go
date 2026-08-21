package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/discovery"
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

	registration, err := registry.Register(ctx, discovery.Instance{
		ID:       "game-integration",
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

	if _, ok := directory.NextInstance("game"); ok {
		t.Fatal("starting instance must not be selected by service route")
	}
	if _, ok := directory.LookupInstance("game-integration"); ok {
		t.Fatal("starting instance must not be selected by exact route")
	}

	if err := registration.SetStatus(ctx, discovery.StatusRunning); err != nil {
		t.Fatalf("mark instance running: %v", err)
	}
	waitDirectory(t, ctx, directory, func() bool {
		instance, ok := directory.NextInstance("game")
		return ok && instance.ID == "game-integration"
	})

	if err := registration.SetStatus(ctx, discovery.StatusStopping); err != nil {
		t.Fatalf("mark instance stopping: %v", err)
	}
	waitDirectory(t, ctx, directory, func() bool {
		instance, ok := directory.LookupInstance("game-integration")
		return ok && instance.Status == discovery.StatusStopping
	})
	if _, ok := directory.NextInstance("game"); ok {
		t.Fatal("stopping instance must not be selected by service route")
	}

	if err := registration.Close(ctx); err != nil {
		t.Fatalf("close registration: %v", err)
	}
	waitDirectory(t, ctx, directory, func() bool {
		_, ok := directory.LookupInstance("game-integration")
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
		RuntimeInstanceID: "game-1@v42@a",
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
		RuntimeInstanceID: "game-1@v43@b",
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
		RuntimeInstanceID: "game-1@v42@a",
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
