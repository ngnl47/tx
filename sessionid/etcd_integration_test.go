package sessionid

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ngnl47/tx/etcd"
)

func TestEtcdAllocatorsDoNotReuseSessionIDs(t *testing.T) {
	endpoint := os.Getenv("TX_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("设置 TX_ETCD_ENDPOINT 运行真实 etcd 测试")
	}
	client, err := etcd.New(context.Background(), etcd.Config{Endpoints: []string{endpoint}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close etcd client: %v", err)
		}
	})
	key := "/tx/tests/sessionid/" + uuid.NewString() + "/issuer"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Delete(ctx, key); err != nil {
			t.Errorf("delete test key: %v", err)
		}
		if _, found, err := client.Get(ctx, key); err != nil || found {
			t.Errorf("test key after cleanup: found=%t, error=%v", found, err)
		}
	})
	allocators := make([]*EtcdAllocator, 4)
	for index := range allocators {
		allocators[index], err = NewEtcdAllocator(client, key)
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const workers, perWorker = 8, 32
	ids := make(chan int64, workers*perWorker)
	errs := make(chan error, workers)
	var group sync.WaitGroup
	for worker := range workers {
		group.Go(func() {
			allocator := allocators[worker%len(allocators)]
			for range perWorker {
				id, err := allocator.AllocateSessionID(ctx)
				if err != nil {
					errs <- err
					return
				}
				ids <- id
			}
		})
	}
	group.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Errorf("AllocateSessionID: %v", err)
	}
	seen := make(map[int64]struct{}, workers*perWorker)
	for id := range ids {
		if id <= 0 || id > MaxSessionID {
			t.Fatalf("session ID out of range: %d", id)
		}
		if _, found := seen[id]; found {
			t.Fatalf("duplicate session ID: %d", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != workers*perWorker {
		t.Fatalf("unique session IDs = %d, want %d", len(seen), workers*perWorker)
	}
	restarted, err := NewEtcdAllocator(client, key)
	if err != nil {
		t.Fatal(err)
	}
	id, err := restarted.AllocateSessionID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := seen[id]; found {
		t.Fatalf("recreated allocator reused session ID: %d", id)
	}
	stored, found, err := client.Get(ctx, key)
	if err != nil || !found || stored.LeaseID != 0 || stored.ModRevision != id {
		t.Fatalf("stored issuer = %#v, %t, %v, want unleased revision %d", stored, found, err, id)
	}
}
