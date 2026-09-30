package msgq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ngnl47/tx/modules/discovery"
)

func TestRoleRouteCacheReusesOwnerAndInvalidatesExactGeneration(t *testing.T) {
	ctx := context.Background()
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{ID: "msgq-a", Endpoint: "localhost:9001", Services: []string{"project-mailbox"}, Status: discovery.StatusRunning}}); err != nil {
		t.Fatal(err)
	}
	owner := RoleBinding{RoleID: 47, InstanceID: "msgq-a", Generation: "first"}
	var reads atomic.Int64
	cache, err := NewRoleRouter("project-mailbox", directory, func(context.Context, int64) (RoleBinding, bool, error) {
		reads.Add(1)
		return owner, true, nil
	}, 128)
	if err != nil {
		t.Fatal(err)
	}
	first, err := cache.Resolve(ctx, 47)
	if err != nil || first.Binding != owner {
		t.Fatalf("resolve = %+v, %v", first, err)
	}
	for range 1000 {
		if _, err := cache.Resolve(ctx, 47); err != nil {
			t.Fatal(err)
		}
	}
	if reads.Load() != 1 {
		t.Fatalf("hot path performed %d reads", reads.Load())
	}
	owner.Generation = "second"
	cache.Invalidate(first.Binding)
	second, err := cache.Resolve(ctx, 47)
	if err != nil || second.Binding != owner {
		t.Fatalf("refresh = %+v, %v", second, err)
	}
	cache.Invalidate(first.Binding)
	if _, err := cache.Resolve(ctx, 47); err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 2 {
		t.Fatalf("old invalidation evicted current owner: %d reads", reads.Load())
	}
	owner = RoleBinding{RoleID: 47, InstanceID: "msgq-b", Generation: "third"}
	if err := directory.Replace([]discovery.Instance{{ID: "msgq-b", Endpoint: "localhost:9002", Services: []string{"project-mailbox"}, Status: discovery.StatusRunning}}); err != nil {
		t.Fatal(err)
	}
	if target, err := cache.Resolve(ctx, 47); err != nil || target.Binding != owner {
		t.Fatalf("stale instance cache prevented new owner routing: %+v, %v", target, err)
	}
	directory.Invalidate()
	if _, err := cache.Resolve(ctx, 47); !errors.Is(err, ErrRouteUnavailable) {
		t.Fatalf("removed owner: %v", err)
	}
}

func TestRoleRouteCacheCoalescesColdQueries(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{ID: "msgq-a", Endpoint: "localhost:9001", Services: []string{"project-mailbox"}, Status: discovery.StatusRunning}}); err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int64
	cache, err := NewRoleRouter("project-mailbox", directory, func(context.Context, int64) (RoleBinding, bool, error) {
		reads.Add(1)
		return RoleBinding{RoleID: 47, InstanceID: "msgq-a", Generation: "first"}, true, nil
	}, 128)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 32 {
		group.Go(func() {
			if _, err := cache.Resolve(context.Background(), 47); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	if reads.Load() != 1 {
		t.Fatalf("cold queries performed %d reads", reads.Load())
	}
}

func BenchmarkRoleRouterWarm(b *testing.B) {
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{ID: "msgq-a", Endpoint: "localhost:9001", Services: []string{"project-mailbox"}, Status: discovery.StatusRunning}}); err != nil {
		b.Fatal(err)
	}
	var reads atomic.Int64
	router, err := NewRoleRouter("project-mailbox", directory, func(context.Context, int64) (RoleBinding, bool, error) {
		reads.Add(1)
		return RoleBinding{RoleID: 47, InstanceID: "msgq-a", Generation: "first"}, true, nil
	}, 128)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if _, err := router.Resolve(ctx, 47); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := router.Resolve(ctx, 47); err != nil {
			b.Fatal(err)
		}
	}
	if reads.Load() != 1 {
		b.Fatalf("hot path performed %d reads", reads.Load())
	}
}
