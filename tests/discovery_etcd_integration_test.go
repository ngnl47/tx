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

	if _, ok := directory.ResolveService("game"); ok {
		t.Fatal("starting instance must not be selected by service route")
	}
	if _, ok := directory.ResolveID("game-integration"); ok {
		t.Fatal("starting instance must not be selected by exact route")
	}

	if err := registration.SetStatus(ctx, discovery.StatusRunning); err != nil {
		t.Fatalf("mark instance running: %v", err)
	}
	waitDirectory(t, ctx, directory, func() bool {
		instance, ok := directory.ResolveService("game")
		return ok && instance.ID == "game-integration"
	})

	if err := registration.SetStatus(ctx, discovery.StatusStopping); err != nil {
		t.Fatalf("mark instance stopping: %v", err)
	}
	waitDirectory(t, ctx, directory, func() bool {
		instance, ok := directory.ResolveID("game-integration")
		return ok && instance.Status == discovery.StatusStopping
	})
	if _, ok := directory.ResolveService("game"); ok {
		t.Fatal("stopping instance must not be selected by service route")
	}

	if err := registration.Close(ctx); err != nil {
		t.Fatalf("close registration: %v", err)
	}
	waitDirectory(t, ctx, directory, func() bool {
		_, ok := directory.ResolveID("game-integration")
		return !ok
	})

	stopWatch()
	if err := <-watchResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("watch error = %v, want context canceled", err)
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
