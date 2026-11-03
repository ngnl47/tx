package msgq

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/storage/redis/goredis"
	redis "github.com/redis/go-redis/v9"
)

func TestAdmissionRedisCASRetainsUnconfirmedEvidence(t *testing.T) {
	endpoint := os.Getenv("TX_REDIS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_REDIS_ENDPOINT for real Redis verification")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw := redis.NewClient(&redis.Options{Addr: endpoint})
	defer raw.Close()
	prefix := fmt.Sprintf("tx:test:admission:%d", time.Now().UnixNano())
	store, err := NewRedisSessionStore(goredis.NewClient(raw), SessionRedisOptions{Prefix: prefix, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Del(context.Background(), store.admissionKey(47))
	var group sync.WaitGroup
	var winners atomic.Int64
	for id := int64(1); id <= 16; id++ {
		group.Go(func() {
			changed, err := store.CompareAdmission(ctx, 47, AdmissionRecord{}, AdmissionRecord{SessionID: id, TicketOrder: id, ExpiresAt: time.Now().Add(time.Minute)})
			if err != nil {
				t.Error(err)
			}
			if changed {
				winners.Add(1)
			}
		})
	}
	group.Wait()
	if winners.Load() != 1 {
		t.Fatalf("CAS winners=%d", winners.Load())
	}
	if ttl, err := raw.PTTL(ctx, store.admissionKey(47)).Result(); err != nil || ttl != -1 {
		t.Fatalf("watermark TTL=%v %v", ttl, err)
	}
	current, err := store.LoadAdmission(ctx, 47)
	if err != nil {
		t.Fatal(err)
	}
	pending := AdmissionRecord{TicketOrder: current.TicketOrder, Header: SessionHeader{RoleID: 47, SessionID: current.SessionID, ConnectionID: 1}, Attached: true}
	if changed, err := store.CompareAdmission(ctx, 47, current, pending); err != nil || !changed {
		t.Fatalf("record termination=%t %v", changed, err)
	}
	if changed, err := store.CompareAdmission(ctx, 47, current, AdmissionRecord{}); err != nil || changed {
		t.Fatalf("stale cleanup=%t %v", changed, err)
	}
	if ttl, err := raw.PTTL(ctx, store.admissionKey(47)).Result(); err != nil || ttl != -1 {
		t.Fatalf("pending evidence TTL=%v %v", ttl, err)
	}
	record, err := store.LoadAdmission(ctx, 47)
	if err != nil || !record.Attached {
		t.Fatalf("lost evidence: %+v %v", record, err)
	}
	if changed, err := store.CompareAdmission(ctx, 47, record, AdmissionRecord{TicketOrder: record.TicketOrder}); err != nil || !changed {
		t.Fatalf("confirmed cleanup=%t %v", changed, err)
	}
	record, err = store.LoadAdmission(ctx, 47)
	if err != nil || record.SessionID != 0 || record.Attached || record.TicketOrder != current.TicketOrder {
		t.Fatalf("cleanup=%+v %v", record, err)
	}
}
