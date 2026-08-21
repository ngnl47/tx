package goredis

import (
	"context"
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"

	redis "github.com/redis/go-redis/v9"

	"github.com/ngnl47/tx/storage"
)

type testRedisHook struct {
	process func(redis.Cmder) error
}

func (hook testRedisHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (hook testRedisHook) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return func(_ context.Context, cmd redis.Cmder) error {
		return hook.process(cmd)
	}
}

func (hook testRedisHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestClientClassifiesDriverErrorsButNotResultDecoding(t *testing.T) {
	transportErr := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	raw := redis.NewClient(&redis.Options{Addr: "unused"})
	raw.AddHook(testRedisHook{process: func(redis.Cmder) error { return transportErr }})

	_, err := NewClient(raw).Eval(context.Background(), "return 'ok'", nil)
	if !errors.Is(err, transportErr) {
		t.Fatalf("Eval error = %v, want transport error", err)
	}
	if backend, ok := storage.BackendFromError(err); !ok || backend != storage.BackendRedis {
		t.Fatalf("BackendFromError = %v, %t, want redis, true", backend, ok)
	}

	raw = redis.NewClient(&redis.Options{Addr: "unused"})
	raw.AddHook(testRedisHook{process: func(redis.Cmder) error { return redis.ErrCrossSlot }})
	_, err = NewClient(raw).Eval(context.Background(), "return 'ok'", nil)
	if !errors.Is(err, redis.ErrCrossSlot) {
		t.Fatalf("Eval error = %v, want Redis semantic error", err)
	}
	if _, ok := storage.BackendFromError(err); ok {
		t.Fatalf("BackendFromError classified Redis semantic error: %v", err)
	}

	raw = redis.NewClient(&redis.Options{Addr: "unused"})
	raw.AddHook(testRedisHook{process: func(cmd redis.Cmder) error {
		cmd.(*redis.Cmd).SetVal(true)
		return nil
	}})
	_, err = NewClient(raw).Eval(context.Background(), "return true", nil)
	if err == nil || !strings.Contains(err.Error(), "returned bool") {
		t.Fatalf("Eval error = %v, want result decoding error", err)
	}
	if _, ok := storage.BackendFromError(err); ok {
		t.Fatalf("BackendFromError classified result decoding error: %v", err)
	}
}

func TestClientReturnsErrorWhenRedisClientMissing(t *testing.T) {
	client := NewClient(nil)

	if _, _, err := client.Get(context.Background(), "role:{1001}"); err == nil || !strings.Contains(err.Error(), "redis client is not configured") {
		t.Fatalf("Get error = %v, want missing client error", err)
	}
	if err := client.Set(context.Background(), "role:{1001}", "{}"); err == nil || !strings.Contains(err.Error(), "redis client is not configured") {
		t.Fatalf("Set error = %v, want missing client error", err)
	}
	if _, err := client.Eval(context.Background(), "return 'ok'", nil); err == nil || !strings.Contains(err.Error(), "redis client is not configured") {
		t.Fatalf("Eval error = %v, want missing client error", err)
	}
	if _, err := client.MGet(context.Background(), "role:{1001}"); err == nil || !strings.Contains(err.Error(), "redis client is not configured") {
		t.Fatalf("MGet error = %v, want missing client error", err)
	}
}
