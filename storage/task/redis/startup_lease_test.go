package redis

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	txgoredis "github.com/ngnl47/tx/storage/redis/goredis"
	"github.com/ngnl47/tx/storage/task"
	goredis "github.com/redis/go-redis/v9"
)

type startupLeaseClient struct {
	results []string
	keys    [][]string
	args    [][]any
}

func (client *startupLeaseClient) Eval(_ context.Context, _ string, keys []string, args ...any) (string, error) {
	client.keys = append(client.keys, append([]string(nil), keys...))
	client.args = append(client.args, append([]any(nil), args...))
	result := client.results[0]
	client.results = client.results[1:]
	return result, nil
}

func TestStartupLeaseMapsProtocolAndUsesHiddenSameSlotKey(t *testing.T) {
	client := &startupLeaseClient{results: []string{"acquired", "running", "complete", "ok", "stale", "ok"}}
	lease := NewBackend(client, Options{Prefix: "txgame:task"}).StartupLease()
	for index, want := range []task.StartupLeaseStatus{
		task.StartupLeaseStatusAcquired,
		task.StartupLeaseStatusRunning,
		task.StartupLeaseStatusComplete,
	} {
		got, err := lease.Acquire(context.Background(), "owner-a", time.Second)
		if err != nil || got != want {
			t.Fatalf("Acquire %d = %v, %v, want %v", index, got, err, want)
		}
	}
	if ok, err := lease.Renew(context.Background(), "owner-a", time.Second); err != nil || !ok {
		t.Fatalf("Renew = %v, %v", ok, err)
	}
	if ok, err := lease.Complete(context.Background(), "owner-b", time.Second); err != nil || ok {
		t.Fatalf("stale Complete = %v, %v", ok, err)
	}
	if ok, err := lease.Release(context.Background(), "owner-a"); err != nil || !ok {
		t.Fatalf("Release = %v, %v", ok, err)
	}
	for _, keys := range client.keys {
		if len(keys) != 1 || keys[0] != "txgame:task:{startup}:handoff-recovery" {
			t.Fatalf("startup lease keys = %#v", keys)
		}
	}
}

func TestStartupLeaseRejectsUnknownProtocolResult(t *testing.T) {
	lease := NewBackend(&startupLeaseClient{results: []string{"mystery"}}, Options{}).StartupLease()
	if _, err := lease.Acquire(context.Background(), "owner-a", time.Second); err == nil || !strings.Contains(err.Error(), "unexpected") {
		t.Fatalf("Acquire error = %v, want unexpected result", err)
	}
}

func TestStartupLeaseRejectsInvalidInput(t *testing.T) {
	lease := NewBackend(&startupLeaseClient{}, Options{}).StartupLease()
	if _, err := lease.Acquire(context.Background(), "", time.Second); err == nil {
		t.Fatal("empty token error = nil")
	}
	if _, err := lease.Acquire(context.Background(), "owner-a", time.Nanosecond); err == nil {
		t.Fatal("short TTL error = nil")
	}
}

func TestStartupLeaseTokenFencingIntegration(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv("TX_REDIS_ENDPOINT"))
	if endpoint == "" {
		t.Skip("TX_REDIS_ENDPOINT is not configured")
	}
	options, err := goredis.ParseURL(endpoint)
	if err != nil {
		t.Fatalf("parse Redis endpoint: %v", err)
	}
	client := goredis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}
	prefix := "tx:test:startup-lease:" + strings.ReplaceAll(t.Name(), "/", ":")
	key := prefix + startupLeaseSuffix
	t.Cleanup(func() { _ = client.Del(context.Background(), key).Err() })
	lease := NewBackend(txgoredis.NewClient(client), Options{Prefix: prefix}).StartupLease()

	status, err := lease.Acquire(ctx, "owner-a", 50*time.Millisecond)
	if err != nil || status != task.StartupLeaseStatusAcquired {
		t.Fatalf("owner-a acquire = %v, %v", status, err)
	}
	if ok, err := lease.Complete(ctx, "owner-b", time.Second); err != nil || ok {
		t.Fatalf("stale owner complete = %v, %v", ok, err)
	}
	time.Sleep(80 * time.Millisecond)
	status, err = lease.Acquire(ctx, "owner-b", time.Second)
	if err != nil || status != task.StartupLeaseStatusAcquired {
		t.Fatalf("owner-b takeover = %v, %v", status, err)
	}
	if ok, err := lease.Release(ctx, "owner-a"); err != nil || ok {
		t.Fatalf("stale owner release = %v, %v", ok, err)
	}
	if ok, err := lease.Complete(ctx, "owner-b", time.Second); err != nil || !ok {
		t.Fatalf("owner-b complete = %v, %v", ok, err)
	}
	status, err = lease.Acquire(ctx, "owner-c", time.Second)
	if err != nil || status != task.StartupLeaseStatusComplete {
		t.Fatalf("observer acquire = %v, %v", status, err)
	}
}
