package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ngnl47/tx/storage/task"
	taskredis "github.com/ngnl47/tx/storage/task/redis"
)

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

type fakeTaskRedis struct {
	queues   map[string]*fakeTaskRedisQueue
	seq      int
	lastKeys []string
}

type fakeTaskRedisQueue struct {
	payload  map[string]string
	due      map[string]int64
	busy     map[string]int64
	token    map[string]string
	attempts map[string]int
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
	if _, ok := queue.busy[key]; !ok {
		queue.due[key] = score
	}
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

	type leaseJSON struct {
		Body     string `json:"body"`
		Token    string `json:"token"`
		Attempts int    `json:"attempts"`
	}
	out := make([]leaseJSON, 0, len(keys))
	for _, key := range keys {
		body := queue.payload[key]
		if body == "" {
			delete(queue.due, key)
			continue
		}
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
	delete(queue.payload, key)
	delete(queue.due, key)
	delete(queue.busy, key)
	delete(queue.token, key)
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
