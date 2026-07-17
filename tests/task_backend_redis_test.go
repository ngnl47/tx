package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ngnl47/tx/storage/task"
	taskredis "github.com/ngnl47/tx/storage/task/redis"
)

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

type fakeTaskRedis struct {
	payload  map[string]string
	due      map[string]int64
	busy     map[string]int64
	token    map[string]string
	attempts map[string]int
	seq      int
}

func newFakeTaskRedis() *fakeTaskRedis {
	return &fakeTaskRedis{
		payload:  map[string]string{},
		due:      map[string]int64{},
		busy:     map[string]int64{},
		token:    map[string]string{},
		attempts: map[string]int{},
	}
}

func (r *fakeTaskRedis) Eval(_ context.Context, script string, _ []string, args ...any) (string, error) {
	switch {
	case strings.Contains(script, "tx_task_enqueue"):
		return r.enqueue(args...)
	case strings.Contains(script, "tx_task_claim"):
		return r.claim(args...)
	case strings.Contains(script, "tx_task_complete"):
		return r.complete(args...)
	case strings.Contains(script, "tx_task_retry"):
		return r.retry(args...)
	default:
		return "", fmt.Errorf("unknown script")
	}
}

func (r *fakeTaskRedis) enqueue(args ...any) (string, error) {
	key := fmt.Sprint(args[0])
	score := mustInt64(args[1])
	body := fmt.Sprint(args[2])
	r.payload[key] = body
	if _, ok := r.busy[key]; !ok {
		r.due[key] = score
	}
	return "ok", nil
}

func (r *fakeTaskRedis) claim(args ...any) (string, error) {
	now := mustInt64(args[0])
	limit := int(mustInt64(args[1]))
	leaseUntil := mustInt64(args[2])
	tokenPrefix := fmt.Sprint(args[3])

	for key, until := range r.busy {
		if until <= now {
			delete(r.busy, key)
			delete(r.token, key)
			r.due[key] = now
		}
	}

	keys := make([]string, 0, len(r.due))
	for key, due := range r.due {
		if due <= now {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if r.due[keys[i]] == r.due[keys[j]] {
			return keys[i] < keys[j]
		}
		return r.due[keys[i]] < r.due[keys[j]]
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
		body := r.payload[key]
		if body == "" {
			delete(r.due, key)
			continue
		}
		r.seq++
		token := fmt.Sprintf("%s-%d", tokenPrefix, r.seq)
		r.attempts[key]++
		delete(r.due, key)
		r.busy[key] = leaseUntil
		r.token[key] = token
		out = append(out, leaseJSON{Body: body, Token: token, Attempts: r.attempts[key]})
	}
	encoded, err := json.Marshal(out)
	return string(encoded), err
}

func (r *fakeTaskRedis) complete(args ...any) (string, error) {
	key := fmt.Sprint(args[0])
	token := fmt.Sprint(args[1])
	if r.token[key] != token {
		return "stale", nil
	}
	delete(r.payload, key)
	delete(r.due, key)
	delete(r.busy, key)
	delete(r.token, key)
	delete(r.attempts, key)
	return "ok", nil
}

func (r *fakeTaskRedis) retry(args ...any) (string, error) {
	key := fmt.Sprint(args[0])
	token := fmt.Sprint(args[1])
	next := mustInt64(args[2])
	if r.token[key] != token {
		return "stale", nil
	}
	delete(r.busy, key)
	delete(r.token, key)
	r.due[key] = next
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
