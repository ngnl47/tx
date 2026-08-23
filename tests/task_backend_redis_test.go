package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"

	"github.com/ngnl47/tx/storage"
	txgoredis "github.com/ngnl47/tx/storage/redis/goredis"
	"github.com/ngnl47/tx/storage/task"
	taskredis "github.com/ngnl47/tx/storage/task/redis"
)

func TestRedisTaskBackendClassifiesTransportError(t *testing.T) {
	transportErr := errors.New("redis unavailable")
	backend := taskredis.NewBackend(scriptedTaskRedis{err: storage.NewBackendError(storage.BackendRedis, "eval", transportErr)}, taskredis.Options{})
	item := task.DelayTask{Type: "role", ID: int64(1001), Action: "save"}

	err := backend.Enqueue(context.Background(), item)
	if !errors.Is(err, transportErr) {
		t.Fatalf("Enqueue error = %v, want transport error", err)
	}
	if dependency, ok := storage.BackendFromError(err); !ok || dependency != storage.BackendRedis {
		t.Fatalf("BackendFromError = %v, %t, want redis, true", dependency, ok)
	}
}

func TestRedisTaskBackendDoesNotClassifyUnmarkedClientError(t *testing.T) {
	backend := taskredis.NewBackend(scriptedTaskRedis{err: errors.New("invalid redis response")}, taskredis.Options{})
	item := task.DelayTask{Type: "role", ID: int64(1001), Action: "save"}

	err := backend.Enqueue(context.Background(), item)
	if _, ok := storage.BackendFromError(err); ok {
		t.Fatalf("BackendFromError classified unmarked client error: %v", err)
	}
}

func TestRedisTaskBackendRejectsMissingClient(t *testing.T) {
	backend := taskredis.NewBackend(nil, taskredis.Options{})
	item := task.DelayTask{Type: "data.GDRole", ID: int64(1001), Action: "save"}
	if err := backend.Enqueue(context.Background(), item); !errors.Is(err, taskredis.ErrClientNotConfigured) {
		t.Fatalf("Enqueue error = %v, want ErrClientNotConfigured", err)
	}
	if _, err := backend.ClaimDue(context.Background(), time.Now(), 1); !errors.Is(err, taskredis.ErrClientNotConfigured) {
		t.Fatalf("ClaimDue error = %v, want ErrClientNotConfigured", err)
	}
	lease := task.DelayTaskLease{Task: item}
	if err := backend.Complete(context.Background(), lease); !errors.Is(err, taskredis.ErrClientNotConfigured) {
		t.Fatalf("Complete error = %v, want ErrClientNotConfigured", err)
	}
	if err := backend.Retry(context.Background(), lease, time.Now(), errors.New("retry")); !errors.Is(err, taskredis.ErrClientNotConfigured) {
		t.Fatalf("Retry error = %v, want ErrClientNotConfigured", err)
	}
}

func TestRedisTaskBackendRejectsTypedNilClient(t *testing.T) {
	var client *scriptedTaskRedis
	backend := taskredis.NewBackend(client, taskredis.Options{})
	item := task.DelayTask{Type: "data.GDRole", ID: int64(1001), Action: "save"}
	if err := backend.Enqueue(context.Background(), item); !errors.Is(err, taskredis.ErrClientNotConfigured) {
		t.Fatalf("Enqueue error = %v, want ErrClientNotConfigured", err)
	}
	if _, err := backend.ClaimDue(context.Background(), time.Now(), 1); !errors.Is(err, taskredis.ErrClientNotConfigured) {
		t.Fatalf("ClaimDue error = %v, want ErrClientNotConfigured", err)
	}
	lease := task.DelayTaskLease{Task: item}
	if err := backend.Complete(context.Background(), lease); !errors.Is(err, taskredis.ErrClientNotConfigured) {
		t.Fatalf("Complete error = %v, want ErrClientNotConfigured", err)
	}
	if err := backend.Retry(context.Background(), lease, time.Now(), errors.New("retry")); !errors.Is(err, taskredis.ErrClientNotConfigured) {
		t.Fatalf("Retry error = %v, want ErrClientNotConfigured", err)
	}
}

func TestRedisTaskBackendRejectsSubMillisecondLeaseBeforeIO(t *testing.T) {
	client := &sequenceTaskRedis{}
	backend := taskredis.NewBackend(client, taskredis.Options{LeaseTimeout: time.Nanosecond})
	item := task.DelayTask{Type: "data.GDRole", ID: int64(1001), Action: "save"}

	if err := backend.Enqueue(context.Background(), item); !errors.Is(err, taskredis.ErrInvalidOptions) {
		t.Fatalf("Enqueue error = %v, want ErrInvalidOptions", err)
	}
	if _, err := backend.ClaimDue(context.Background(), time.Now(), 1); !errors.Is(err, taskredis.ErrInvalidOptions) {
		t.Fatalf("ClaimDue error = %v, want ErrInvalidOptions", err)
	}
	if client.next != 0 {
		t.Fatalf("eval calls = %d, want no Redis I/O", client.next)
	}
}

func TestRedisDelayTaskBackendRejectsUnknownScriptResults(t *testing.T) {
	backend := taskredis.NewBackend(scriptedTaskRedis{result: "unexpected"}, taskredis.Options{})
	item := task.DelayTask{Type: "role", ID: int64(1001), Action: "save"}
	lease := task.DelayTaskLease{Task: item, Token: "token", Attempts: 1}

	for _, tt := range []struct {
		name string
		run  func() error
	}{
		{name: "enqueue", run: func() error { return backend.Enqueue(context.Background(), item) }},
		{name: "complete", run: func() error { return backend.Complete(context.Background(), lease) }},
		{name: "retry", run: func() error { return backend.Retry(context.Background(), lease, time.Now(), errors.New("retry")) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if err == nil || !strings.Contains(err.Error(), "unexpected script result") || !strings.Contains(err.Error(), "unexpected") {
				t.Fatalf("error = %v, want unexpected script result", err)
			}
		})
	}
}

func TestRedisDelayTaskBackendClaimDueRejectsNonArrayResults(t *testing.T) {
	for _, result := range []string{"", "null", "{}", "unexpected"} {
		t.Run(fmt.Sprintf("%q", result), func(t *testing.T) {
			backend := taskredis.NewBackend(scriptedTaskRedis{result: result}, taskredis.Options{})
			_, err := backend.ClaimDue(context.Background(), time.Now(), 1)
			if err == nil || !strings.Contains(err.Error(), "decode leases") {
				t.Fatalf("ClaimDue error = %v, want decode leases error", err)
			}
		})
	}
}

func TestRedisDelayTaskBackendClaimDueAcceptsEmptyArray(t *testing.T) {
	backend := taskredis.NewBackend(scriptedTaskRedis{result: "[]"}, taskredis.Options{})
	leases, err := backend.ClaimDue(context.Background(), time.Now(), 1)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases = %#v, want empty", leases)
	}
}

func TestRedisDelayTaskBackendClaimDueReturnsLeasesBeforeLaterBucketError(t *testing.T) {
	itemBody, err := json.Marshal(task.DelayTask{Type: "role", ID: "1", Action: "save"})
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	leaseBody, err := json.Marshal([]map[string]any{{
		"body":     string(itemBody),
		"token":    "lease-1",
		"attempts": 1,
	}})
	if err != nil {
		t.Fatalf("marshal lease: %v", err)
	}
	bucketErr := errors.New("bucket unavailable")
	backend := taskredis.NewBackend(&sequenceTaskRedis{results: []scriptedTaskRedis{
		{result: string(leaseBody)},
		{err: bucketErr},
	}}, taskredis.Options{BucketCount: 2})

	leases, err := backend.ClaimDue(context.Background(), time.Now(), 2)
	if !errors.Is(err, bucketErr) {
		t.Fatalf("ClaimDue error = %v, want bucket error", err)
	}
	if len(leases) != 1 || leases[0].Task.ID != "1" || leases[0].Token != "lease-1" {
		t.Fatalf("ClaimDue leases = %#v, want successfully claimed first bucket lease", leases)
	}
}

func TestRedisDelayTaskBackendClaimDueReturnsValidRowsWithInvalidLeaseError(t *testing.T) {
	itemBody, err := json.Marshal(task.DelayTask{Type: "role", ID: "1", Action: "save"})
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	result, err := json.Marshal([]map[string]any{
		{"body": string(itemBody), "token": "lease-1", "attempts": 1},
		{"body": string(itemBody), "token": "", "attempts": 1},
	})
	if err != nil {
		t.Fatalf("marshal leases: %v", err)
	}
	backend := taskredis.NewBackend(scriptedTaskRedis{result: string(result)}, taskredis.Options{BucketCount: 1})

	leases, err := backend.ClaimDue(context.Background(), time.Now(), 2)
	if err == nil || !strings.Contains(err.Error(), "invalid task lease") {
		t.Fatalf("ClaimDue error = %v, want invalid task lease", err)
	}
	if len(leases) != 1 || leases[0].Token != "lease-1" {
		t.Fatalf("ClaimDue leases = %#v, want valid row", leases)
	}
}

func TestRedisDelayTaskBackendRejectsIncompleteTask(t *testing.T) {
	backend := taskredis.NewBackend(scriptedTaskRedis{result: "ok"}, taskredis.Options{})
	for _, item := range []task.DelayTask{
		{ID: "1", Action: "save"},
		{Type: "role", Action: "save"},
		{Type: "role", ID: "1"},
		{Type: "role", ID: "", Action: "save"},
	} {
		if err := backend.Enqueue(context.Background(), item); err == nil {
			t.Fatalf("Enqueue(%#v) error = nil, want invalid task error", item)
		}
	}
}

func TestRedisDelayTaskBackendRejectsInvalidLease(t *testing.T) {
	backend := taskredis.NewBackend(scriptedTaskRedis{result: "ok"}, taskredis.Options{})
	for _, lease := range []task.DelayTaskLease{
		{},
		{Task: task.DelayTask{Type: "role", ID: "1", Action: "save"}},
		{Task: task.DelayTask{Type: "role", ID: "1", Action: "save"}, Token: "lease", Attempts: 0},
	} {
		if err := backend.Complete(context.Background(), lease); !errors.Is(err, task.ErrInvalidDelayTaskLease) {
			t.Fatalf("Complete(%#v) error = %v, want ErrInvalidDelayTaskLease", lease, err)
		}
		if err := backend.Retry(context.Background(), lease, time.Now(), context.Canceled); !errors.Is(err, task.ErrInvalidDelayTaskLease) {
			t.Fatalf("Retry(%#v) error = %v, want ErrInvalidDelayTaskLease", lease, err)
		}
	}
}

func TestRedisDelayTaskBackendClaimDueRejectsIncompleteTaskPayload(t *testing.T) {
	backend := taskredis.NewBackend(scriptedTaskRedis{result: `[{"body":"{}","token":"lease","attempts":1}]`}, taskredis.Options{})
	if _, err := backend.ClaimDue(context.Background(), time.Now(), 1); err == nil {
		t.Fatal("ClaimDue error = nil, want invalid task error")
	}
}

func TestRedisDelayTaskBackendClaimDueKeepsTaskWithMissingPayload(t *testing.T) {
	ctx := context.Background()
	client := newFakeTaskRedis()
	backend := taskredis.NewBackend(client, taskredis.Options{Prefix: "tx:test", BucketCount: 1})
	now := time.Unix(100, 0)
	items := []task.DelayTask{
		{Type: "role", ID: "1", Action: "save", DueAt: now},
		{Type: "role", ID: "2", Action: "save", DueAt: now},
	}

	for _, item := range items {
		if err := backend.Enqueue(ctx, item); err != nil {
			t.Fatalf("Enqueue returned error: %v", err)
		}
	}
	queue := client.queues["tx:test:{0}:due"]
	missingKey := items[1].IdempotencyKey()
	body := queue.payload[missingKey]
	delete(queue.payload, missingKey)

	if _, err := backend.ClaimDue(ctx, now, 2); err == nil || !strings.Contains(err.Error(), "missing payload") {
		t.Fatalf("ClaimDue error = %v, want missing payload error", err)
	}
	if len(queue.due) != 2 || len(queue.busy) != 0 || len(queue.token) != 0 || len(queue.attempts) != 0 {
		t.Fatalf("failed batch mutated queue: due=%#v busy=%#v token=%#v attempts=%#v", queue.due, queue.busy, queue.token, queue.attempts)
	}

	queue.payload[missingKey] = body
	leases, err := backend.ClaimDue(ctx, now, 2)
	if err != nil || len(leases) != 2 || leases[0].Attempts != 1 || leases[1].Attempts != 1 {
		t.Fatalf("ClaimDue after restoring payload leases = %#v, error = %v, want two first attempts", leases, err)
	}
}

func TestRedisDelayTaskBackendClaimDueRejectsIncompleteLeaseRows(t *testing.T) {
	for _, result := range []string{
		`[{"body":"{}","token":"","attempts":1}]`,
		`[{"body":"{}","token":"lease","attempts":0}]`,
	} {
		t.Run(result, func(t *testing.T) {
			backend := taskredis.NewBackend(scriptedTaskRedis{result: result}, taskredis.Options{})
			_, err := backend.ClaimDue(context.Background(), time.Now(), 1)
			if err == nil || !strings.Contains(err.Error(), "invalid task lease") {
				t.Fatalf("ClaimDue error = %v, want invalid task lease error", err)
			}
		})
	}
}

func TestRedisDelayTaskBackendClaimsAndCompletesDueTasks(t *testing.T) {
	ctx := context.Background()
	client := newFakeTaskRedis()
	backend := taskredis.NewBackend(client, taskredis.Options{Prefix: "tx:test", LeaseTimeout: time.Minute})
	now := time.Unix(100, 0)

	items := []task.DelayTask{
		{Type: "role", ID: "2", ShardKey: "b", Action: "save", DueAt: now.Add(time.Second)},
		{Type: "role", ID: "1", ShardKey: "a", Action: "save", DueAt: now.Add(-time.Second)},
	}
	for _, item := range items {
		if err := backend.Enqueue(ctx, item); err != nil {
			t.Fatalf("Enqueue returned error: %v", err)
		}
	}

	leases, err := backend.ClaimDue(ctx, now, 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 || leases[0].Task.ID != "1" || leases[0].Attempts != 1 || leases[0].Token == "" {
		t.Fatalf("leases = %#v", leases)
	}
	if err := backend.Complete(ctx, leases[0]); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	leases, err = backend.ClaimDue(ctx, now.Add(2*time.Second), 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 || leases[0].Task.ID != "2" {
		t.Fatalf("leases after complete = %#v", leases)
	}
}

func TestRedisDelayTaskBackendRetryReschedulesLease(t *testing.T) {
	ctx := context.Background()
	client := newFakeTaskRedis()
	backend := taskredis.NewBackend(client, taskredis.Options{Prefix: "tx:test", LeaseTimeout: time.Minute})
	now := time.Unix(100, 0)

	if err := backend.Enqueue(ctx, task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	leases, err := backend.ClaimDue(ctx, now, 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %#v", leases)
	}
	if err := backend.Retry(ctx, leases[0], now.Add(time.Minute), context.Canceled); err != nil {
		t.Fatalf("Retry returned error: %v", err)
	}

	leases, err = backend.ClaimDue(ctx, now.Add(30*time.Second), 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases before retry due = %#v", leases)
	}
	leases, err = backend.ClaimDue(ctx, now.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 || leases[0].Attempts != 2 {
		t.Fatalf("leases after retry due = %#v", leases)
	}
}

func TestRedisDelayTaskBackendRequeuesEnqueueDuringLease(t *testing.T) {
	ctx := context.Background()
	client := newFakeTaskRedis()
	backend := taskredis.NewBackend(client, taskredis.Options{Prefix: "tx:test", LeaseTimeout: time.Minute})
	now := time.Unix(100, 0)
	item := task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}

	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	leases, err := backend.ClaimDue(ctx, now, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("ClaimDue leases = %#v, error = %v, want one", leases, err)
	}

	item.DueAt = now.Add(time.Minute)
	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue during lease returned error: %v", err)
	}
	if err := backend.Complete(ctx, leases[0]); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	if leases, err = backend.ClaimDue(ctx, now.Add(30*time.Second), 1); err != nil || len(leases) != 0 {
		t.Fatalf("ClaimDue before next due leases = %#v, error = %v, want none", leases, err)
	}
	if leases, err = backend.ClaimDue(ctx, now.Add(time.Minute), 1); err != nil || len(leases) != 1 || leases[0].Attempts != 1 {
		t.Fatalf("ClaimDue after next due leases = %#v, error = %v, want one", leases, err)
	}
}

func TestRedisDelayTaskBackendMergesEnqueueIntoRetryDuringLease(t *testing.T) {
	ctx := context.Background()
	client := newFakeTaskRedis()
	backend := taskredis.NewBackend(client, taskredis.Options{Prefix: "tx:test", LeaseTimeout: time.Minute})
	now := time.Unix(100, 0)
	item := task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}

	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	leases, err := backend.ClaimDue(ctx, now, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("ClaimDue leases = %#v, error = %v, want one", leases, err)
	}

	item.DueAt = now.Add(time.Minute)
	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue during lease returned error: %v", err)
	}
	retryAt := now.Add(2 * time.Minute)
	if err := backend.Retry(ctx, leases[0], retryAt, context.Canceled); err != nil {
		t.Fatalf("Retry returned error: %v", err)
	}

	if leases, err = backend.ClaimDue(ctx, now.Add(time.Minute), 1); err != nil || len(leases) != 0 {
		t.Fatalf("ClaimDue before retry due leases = %#v, error = %v, want none", leases, err)
	}
	if leases, err = backend.ClaimDue(ctx, retryAt, 1); err != nil || len(leases) != 1 || leases[0].Attempts != 2 {
		t.Fatalf("ClaimDue after retry due leases = %#v, error = %v, want attempt two", leases, err)
	}
}

func TestRedisDelayTaskBackendRecoversExpiredLeases(t *testing.T) {
	ctx := context.Background()
	client := newFakeTaskRedis()
	backend := taskredis.NewBackend(client, taskredis.Options{Prefix: "tx:test", LeaseTimeout: time.Second})
	now := time.Unix(100, 0)

	if err := backend.Enqueue(ctx, task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	leases, err := backend.ClaimDue(ctx, now, 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %#v", leases)
	}

	leases, err = backend.ClaimDue(ctx, now.Add(500*time.Millisecond), 10)
	if err != nil {
		t.Fatalf("ClaimDue before lease timeout returned error: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases before timeout = %#v", leases)
	}

	leases, err = backend.ClaimDue(ctx, now.Add(2*time.Second), 10)
	if err != nil {
		t.Fatalf("ClaimDue after lease timeout returned error: %v", err)
	}
	if len(leases) != 1 || leases[0].Task.ID != "1" || leases[0].Attempts != 2 {
		t.Fatalf("leases after timeout recovery = %#v", leases)
	}
}

func TestRedisDelayTaskBackendRecoversLatestEnqueueAfterLeaseExpires(t *testing.T) {
	ctx := context.Background()
	client := newFakeTaskRedis()
	backend := taskredis.NewBackend(client, taskredis.Options{Prefix: "tx:test", LeaseTimeout: time.Second})
	now := time.Unix(100, 0)
	item := task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}

	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	leases, err := backend.ClaimDue(ctx, now, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("ClaimDue leases = %#v, error = %v, want one", leases, err)
	}

	item.DueAt = now.Add(time.Minute)
	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue during lease returned error: %v", err)
	}
	if leases, err = backend.ClaimDue(ctx, now.Add(2*time.Second), 1); err != nil || len(leases) != 1 || leases[0].Attempts != 2 {
		t.Fatalf("ClaimDue after lease timeout leases = %#v, error = %v, want latest attempt two", leases, err)
	}
	if err := backend.Complete(ctx, leases[0]); err != nil {
		t.Fatalf("Complete recovered lease returned error: %v", err)
	}
	if leases, err = backend.ClaimDue(ctx, now.Add(2*time.Minute), 1); err != nil || len(leases) != 0 {
		t.Fatalf("ClaimDue after recovered complete leases = %#v, error = %v, want none", leases, err)
	}
}

func TestRedisDelayTaskBackendUsesBucketHashTagKeys(t *testing.T) {
	ctx := context.Background()
	client := newFakeTaskRedis()
	backend := taskredis.NewBackend(client, taskredis.Options{Prefix: "tx:test", BucketCount: 8, LeaseTimeout: time.Minute})
	now := time.Unix(100, 0)
	item := task.DelayTask{Type: "role", ID: "1", Action: "save", DueAt: now}

	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	bucket := testBucketFor(item.IdempotencyKey(), 8)
	prefix := fmt.Sprintf("tx:test:{%d}", bucket)
	want := []string{
		prefix + ":due",
		prefix + ":payload",
		prefix + ":busy",
		prefix + ":token",
		prefix + ":attempts",
		prefix + ":pending",
	}
	if fmt.Sprint(client.lastKeys) != fmt.Sprint(want) {
		t.Fatalf("keys = %#v, want %#v", client.lastKeys, want)
	}
}

func TestRedisDelayTaskBackendPreservesNumericRecordKey(t *testing.T) {
	ctx := context.Background()
	client := newFakeTaskRedis()
	backend := taskredis.NewBackend(client, taskredis.Options{Prefix: "tx:test", BucketCount: 8, LeaseTimeout: time.Minute})
	now := time.Unix(100, 0)

	if err := backend.Enqueue(ctx, task.DelayTask{Type: "role", ID: int64(1001), ShardKey: int64(7), Action: "save", DueAt: now}); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	leases, err := backend.ClaimDue(ctx, now, 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %#v, want one", leases)
	}
	if leases[0].Task.ID != int64(1001) || leases[0].Task.ShardKey != int64(7) {
		t.Fatalf("task key = %#v/%#v, want int64 values", leases[0].Task.ID, leases[0].Task.ShardKey)
	}
}

func TestRedisDelayTaskBackendPreservesUint64RecordKey(t *testing.T) {
	ctx := context.Background()
	client := newFakeTaskRedis()
	backend := taskredis.NewBackend(client, taskredis.Options{Prefix: "tx:test", BucketCount: 8, LeaseTimeout: time.Minute})
	now := time.Unix(100, 0)
	id := uint64(9223372036854775809)
	shardKey := uint64(9223372036854775811)

	if err := backend.Enqueue(ctx, task.DelayTask{Type: "role", ID: id, ShardKey: shardKey, Action: "save", DueAt: now}); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	leases, err := backend.ClaimDue(ctx, now, 10)
	if err != nil {
		t.Fatalf("ClaimDue returned error: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %#v, want one", leases)
	}
	if leases[0].Task.ID != id || leases[0].Task.ShardKey != shardKey {
		t.Fatalf("task key = %#v/%#v, want uint64 values %d/%d", leases[0].Task.ID, leases[0].Task.ShardKey, id, shardKey)
	}
	if err := backend.Complete(ctx, leases[0]); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if leases, err = backend.ClaimDue(ctx, now.Add(2*time.Minute), 10); err != nil || len(leases) != 0 {
		t.Fatalf("leases after Complete = %#v, error = %v, want empty", leases, err)
	}
}

func TestRedisDelayTaskBackendRequeuesEnqueueDuringLeaseIntegration(t *testing.T) {
	endpoint := os.Getenv("TX_REDIS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_REDIS_ENDPOINT to run Redis delay task integration test")
	}
	ctx := context.Background()
	raw := redisclient.NewClient(&redisclient.Options{Addr: endpoint})
	defer raw.Close()
	if err := raw.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis %q: %v", endpoint, err)
	}

	prefix := fmt.Sprintf("tx:test:task-followup:%d", time.Now().UnixNano())
	queuePrefix := prefix + ":{0}"
	keys := []string{
		queuePrefix + ":due",
		queuePrefix + ":payload",
		queuePrefix + ":busy",
		queuePrefix + ":token",
		queuePrefix + ":attempts",
		queuePrefix + ":pending",
	}
	t.Cleanup(func() { _ = raw.Del(context.Background(), keys...).Err() })
	backend := taskredis.NewBackend(txgoredis.NewClient(raw), taskredis.Options{
		Prefix:       prefix,
		BucketCount:  1,
		LeaseTimeout: time.Minute,
	})
	now := time.Unix(100, 0)
	item := task.DelayTask{Type: "role", ID: int64(1001), Action: "save", DueAt: now}

	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	leases, err := backend.ClaimDue(ctx, now, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("ClaimDue leases = %#v, error = %v, want one", leases, err)
	}
	item.DueAt = now.Add(time.Minute)
	if err := backend.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue during lease returned error: %v", err)
	}
	if err := backend.Complete(ctx, leases[0]); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if leases, err = backend.ClaimDue(ctx, now.Add(30*time.Second), 1); err != nil || len(leases) != 0 {
		t.Fatalf("ClaimDue before next due leases = %#v, error = %v, want none", leases, err)
	}
	if leases, err = backend.ClaimDue(ctx, now.Add(time.Minute), 1); err != nil || len(leases) != 1 || leases[0].Attempts != 1 {
		t.Fatalf("ClaimDue after next due leases = %#v, error = %v, want one", leases, err)
	}
}

func TestRedisDelayTaskBackendKeepsMissingPayloadTaskIntegration(t *testing.T) {
	endpoint := os.Getenv("TX_REDIS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_REDIS_ENDPOINT to run Redis delay task integration test")
	}
	ctx := context.Background()
	raw := redisclient.NewClient(&redisclient.Options{Addr: endpoint})
	defer raw.Close()
	if err := raw.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis %q: %v", endpoint, err)
	}

	prefix := fmt.Sprintf("tx:test:task-missing-payload:%d", time.Now().UnixNano())
	queuePrefix := prefix + ":{0}"
	keys := []string{
		queuePrefix + ":due",
		queuePrefix + ":payload",
		queuePrefix + ":busy",
		queuePrefix + ":token",
		queuePrefix + ":attempts",
		queuePrefix + ":pending",
	}
	t.Cleanup(func() { _ = raw.Del(context.Background(), keys...).Err() })
	backend := taskredis.NewBackend(txgoredis.NewClient(raw), taskredis.Options{Prefix: prefix, BucketCount: 1})
	now := time.Unix(100, 0)
	item := task.DelayTask{Type: "role", ID: int64(1001), Action: "save", DueAt: now}
	if err := raw.ZAdd(ctx, keys[0], redisclient.Z{Score: float64(now.UnixMilli()), Member: item.IdempotencyKey()}).Err(); err != nil {
		t.Fatalf("seed due task without payload: %v", err)
	}

	if _, err := backend.ClaimDue(ctx, now, 1); err == nil || !strings.Contains(err.Error(), "missing payload") {
		t.Fatalf("ClaimDue error = %v, want missing payload error", err)
	}
	if score, err := raw.ZScore(ctx, keys[0], item.IdempotencyKey()).Result(); err != nil || int64(score) != now.UnixMilli() {
		t.Fatalf("due task after failed claim score = %v, error = %v, want preserved", score, err)
	}
}

func TestRedisDelayTaskBackendReturnsCrossBucketPartialLeasesIntegration(t *testing.T) {
	endpoint := os.Getenv("TX_REDIS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_REDIS_ENDPOINT to run Redis delay task integration test")
	}
	ctx := context.Background()
	raw := redisclient.NewClient(&redisclient.Options{Addr: endpoint})
	defer raw.Close()
	if err := raw.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis %q: %v", endpoint, err)
	}

	prefix := fmt.Sprintf("tx:test:task-partial-claim:%d", time.Now().UnixNano())
	keys := make([]string, 0, 12)
	for bucket := 0; bucket < 2; bucket++ {
		queuePrefix := fmt.Sprintf("%s:{%d}", prefix, bucket)
		keys = append(keys,
			queuePrefix+":due",
			queuePrefix+":payload",
			queuePrefix+":busy",
			queuePrefix+":token",
			queuePrefix+":attempts",
			queuePrefix+":pending",
		)
	}
	t.Cleanup(func() { _ = raw.Del(context.Background(), keys...).Err() })
	now := time.Unix(100, 0)
	valid := delayTaskForBucket(t, 1, 2, now)
	missing := delayTaskForBucket(t, 0, 2, now)
	validBody, err := json.Marshal(valid)
	if err != nil {
		t.Fatalf("marshal valid task: %v", err)
	}
	if err := raw.ZAdd(ctx, keys[6], redisclient.Z{Score: float64(now.UnixMilli()), Member: valid.IdempotencyKey()}).Err(); err != nil {
		t.Fatalf("seed valid due task: %v", err)
	}
	if err := raw.HSet(ctx, keys[7], valid.IdempotencyKey(), string(validBody)).Err(); err != nil {
		t.Fatalf("seed valid payload: %v", err)
	}
	if err := raw.ZAdd(ctx, keys[0], redisclient.Z{Score: float64(now.UnixMilli()), Member: missing.IdempotencyKey()}).Err(); err != nil {
		t.Fatalf("seed missing-payload due task: %v", err)
	}

	backend := taskredis.NewBackend(txgoredis.NewClient(raw), taskredis.Options{Prefix: prefix, BucketCount: 2})
	leases, err := backend.ClaimDue(ctx, now, 2)
	if err == nil || !strings.Contains(err.Error(), "missing payload") {
		t.Fatalf("ClaimDue error = %v, want later bucket missing payload", err)
	}
	if len(leases) != 1 || fmt.Sprint(leases[0].Task.ID) != fmt.Sprint(valid.ID) {
		t.Fatalf("ClaimDue leases = %#v, want valid first bucket lease", leases)
	}
}

func delayTaskForBucket(t *testing.T, bucket int, bucketCount int, dueAt time.Time) task.DelayTask {
	t.Helper()
	for id := 1; id <= 1000; id++ {
		item := task.DelayTask{Type: "role", ID: id, Action: "save", DueAt: dueAt}
		if testBucketFor(item.IdempotencyKey(), bucketCount) == bucket {
			return item
		}
	}
	t.Fatalf("no task id found for bucket %d", bucket)
	return task.DelayTask{}
}

type fakeTaskRedis struct {
	queues   map[string]*fakeTaskRedisQueue
	seq      int
	lastKeys []string
}

type scriptedTaskRedis struct {
	result string
	err    error
}

func (r scriptedTaskRedis) Eval(context.Context, string, []string, ...any) (string, error) {
	return r.result, r.err
}

type sequenceTaskRedis struct {
	results []scriptedTaskRedis
	next    int
}

func (r *sequenceTaskRedis) Eval(context.Context, string, []string, ...any) (string, error) {
	if r.next >= len(r.results) {
		return "", fmt.Errorf("unexpected eval call %d", r.next+1)
	}
	result := r.results[r.next]
	r.next++
	return result.result, result.err
}

type fakeTaskRedisQueue struct {
	payload  map[string]string
	due      map[string]int64
	busy     map[string]int64
	token    map[string]string
	attempts map[string]int
	pending  map[string]int64
}

func newFakeTaskRedis() *fakeTaskRedis {
	return &fakeTaskRedis{
		queues: map[string]*fakeTaskRedisQueue{},
	}
}

func (r *fakeTaskRedis) Eval(_ context.Context, script string, keys []string, args ...any) (string, error) {
	r.lastKeys = append([]string(nil), keys...)
	queue := r.queue(keys)
	switch {
	case strings.Contains(script, "tx_task_enqueue"):
		return r.enqueue(queue, args...)
	case strings.Contains(script, "tx_task_claim"):
		return r.claim(queue, args...)
	case strings.Contains(script, "tx_task_complete"):
		return r.complete(queue, args...)
	case strings.Contains(script, "tx_task_retry"):
		return r.retry(queue, args...)
	default:
		return "", fmt.Errorf("unknown script")
	}
}

func (r *fakeTaskRedis) queue(keys []string) *fakeTaskRedisQueue {
	key := keys[0]
	queue := r.queues[key]
	if queue == nil {
		queue = &fakeTaskRedisQueue{
			payload:  map[string]string{},
			due:      map[string]int64{},
			busy:     map[string]int64{},
			token:    map[string]string{},
			attempts: map[string]int{},
			pending:  map[string]int64{},
		}
		r.queues[key] = queue
	}
	return queue
}

func (r *fakeTaskRedis) enqueue(queue *fakeTaskRedisQueue, args ...any) (string, error) {
	key := fmt.Sprint(args[0])
	score := mustInt64(args[1])
	body := fmt.Sprint(args[2])
	queue.payload[key] = body
	if _, ok := queue.busy[key]; ok {
		queue.pending[key] = score
		return "leased", nil
	}
	queue.due[key] = score
	return "ok", nil
}

func (r *fakeTaskRedis) claim(queue *fakeTaskRedisQueue, args ...any) (string, error) {
	now := mustInt64(args[0])
	limit := int(mustInt64(args[1]))
	leaseUntil := mustInt64(args[2])
	tokenPrefix := fmt.Sprint(args[3])

	for key, until := range queue.busy {
		if until <= now {
			delete(queue.busy, key)
			delete(queue.token, key)
			delete(queue.pending, key)
			queue.due[key] = now
		}
	}

	keys := make([]string, 0, len(queue.due))
	for key, due := range queue.due {
		if due <= now {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if queue.due[keys[i]] == queue.due[keys[j]] {
			return keys[i] < keys[j]
		}
		return queue.due[keys[i]] < queue.due[keys[j]]
	})
	if len(keys) > limit {
		keys = keys[:limit]
	}
	for _, key := range keys {
		if _, ok := queue.payload[key]; !ok {
			encoded, err := json.Marshal(map[string]string{"error": "missing_payload", "taskKey": key})
			return string(encoded), err
		}
	}

	type leaseJSON struct {
		Body     string `json:"body"`
		Token    string `json:"token"`
		Attempts int    `json:"attempts"`
	}
	out := make([]leaseJSON, 0, len(keys))
	for _, key := range keys {
		body := queue.payload[key]
		r.seq++
		token := fmt.Sprintf("%s-%d", tokenPrefix, r.seq)
		queue.attempts[key]++
		delete(queue.due, key)
		queue.busy[key] = leaseUntil
		queue.token[key] = token
		out = append(out, leaseJSON{Body: body, Token: token, Attempts: queue.attempts[key]})
	}
	encoded, err := json.Marshal(out)
	return string(encoded), err
}

func (r *fakeTaskRedis) complete(queue *fakeTaskRedisQueue, args ...any) (string, error) {
	key := fmt.Sprint(args[0])
	token := fmt.Sprint(args[1])
	if queue.token[key] != token {
		return "stale", nil
	}
	delete(queue.busy, key)
	delete(queue.token, key)
	if due, ok := queue.pending[key]; ok {
		delete(queue.pending, key)
		delete(queue.attempts, key)
		queue.due[key] = due
		return "ok", nil
	}
	delete(queue.payload, key)
	delete(queue.due, key)
	delete(queue.attempts, key)
	return "ok", nil
}

func (r *fakeTaskRedis) retry(queue *fakeTaskRedisQueue, args ...any) (string, error) {
	key := fmt.Sprint(args[0])
	token := fmt.Sprint(args[1])
	next := mustInt64(args[2])
	if queue.token[key] != token {
		return "stale", nil
	}
	delete(queue.busy, key)
	delete(queue.token, key)
	delete(queue.pending, key)
	queue.due[key] = next
	return "ok", nil
}

func mustInt64(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		if err != nil {
			panic(err)
		}
		return parsed
	default:
		panic(fmt.Sprintf("unexpected int64 value %T", value))
	}
}

func testBucketFor(key string, bucketCount int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(bucketCount))
}
