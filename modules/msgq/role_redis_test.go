package msgq

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ngnl47/tx/storage/redis/goredis"
	redis "github.com/redis/go-redis/v9"
)

func TestRoleStoreRedisAtomicClaimAndGeneration(t *testing.T) {
	endpoint := os.Getenv("TX_REDIS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_REDIS_ENDPOINT for real Redis verification")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw := redis.NewClient(&redis.Options{Addr: endpoint})
	defer raw.Close()
	prefix := fmt.Sprintf("tx:test:role-owner:%d", time.Now().UnixNano())
	key := prefix + ":role:{47}"
	defer raw.Del(context.Background(), key)
	store, err := NewRedisSessionStore(goredis.NewClient(raw), SessionRedisOptions{Prefix: prefix, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan RoleBinding, 32)
	var group sync.WaitGroup
	for i := range 32 {
		group.Go(func() {
			owner, err := store.ClaimRole(ctx, RoleBinding{RoleID: 47, InstanceID: fmt.Sprintf("msgq-%d", i), Generation: fmt.Sprintf("generation-%d", i)})
			if err != nil {
				t.Error(err)
				return
			}
			results <- owner
		})
	}
	group.Wait()
	close(results)
	var first RoleBinding
	count := 0
	for owner := range results {
		if count == 0 {
			first = owner
		}
		if owner != first {
			t.Fatalf("split ownership: %+v vs %+v", first, owner)
		}
		count++
	}
	if count != 32 {
		t.Fatalf("claims = %d", count)
	}
	if ttl, err := raw.PTTL(ctx, key).Result(); err != nil || ttl <= 0 || ttl > time.Minute {
		t.Fatalf("TTL = %v, %v", ttl, err)
	}
	if ok, err := store.RefreshRole(ctx, first); err != nil || !ok {
		t.Fatalf("refresh = %t, %v", ok, err)
	}
	if ok, err := store.ReleaseRole(ctx, first); err != nil || !ok {
		t.Fatalf("release = %t, %v", ok, err)
	}
	second := first
	second.Generation = "replacement"
	if owner, err := store.ClaimRole(ctx, second); err != nil || owner != second {
		t.Fatalf("replacement = %+v, %v", owner, err)
	}
	if ok, err := store.RefreshRole(ctx, first); err != nil || ok {
		t.Fatalf("stale refresh = %t, %v", ok, err)
	}
	if ok, err := store.ReleaseRole(ctx, first); err != nil || ok {
		t.Fatalf("stale release = %t, %v", ok, err)
	}
	if owner, found, err := store.FindRole(ctx, 47); err != nil || !found || owner != second {
		t.Fatalf("find = %+v, %t, %v", owner, found, err)
	}
	if err := raw.PExpire(ctx, key, time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		_, found, err := store.FindRole(ctx, 47)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("unrefreshed role ownership did not expire")
		}
		time.Sleep(time.Millisecond)
	}
}
