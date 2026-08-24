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
	txredis "github.com/ngnl47/tx/storage/redis"
)

type testRedisHook struct {
	process  func(redis.Cmder) error
	pipeline func([]redis.Cmder) error
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
	if hook.pipeline != nil {
		return func(_ context.Context, commands []redis.Cmder) error {
			return hook.pipeline(commands)
		}
	}
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

func TestClientDoesNotClassifyCallerContextError(t *testing.T) {
	raw := redis.NewClient(&redis.Options{Addr: "unused"})
	raw.AddHook(testRedisHook{process: func(redis.Cmder) error { return context.DeadlineExceeded }})

	_, err := NewClient(raw).Eval(context.Background(), "return 'ok'", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Eval error = %v, want context.DeadlineExceeded", err)
	}
	if backend, ok := storage.BackendFromError(err); ok {
		t.Fatalf("BackendFromError = %v, true, want unclassified caller context error", backend)
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
	if _, err := client.EvalBatch(context.Background(), []txredis.ScriptCall{{Script: "return 'ok'"}}); err == nil || !strings.Contains(err.Error(), "redis client is not configured") {
		t.Fatalf("EvalBatch error = %v, want missing client error", err)
	}
	if _, err := client.MGet(context.Background(), "role:{1001}"); err == nil || !strings.Contains(err.Error(), "redis client is not configured") {
		t.Fatalf("MGet error = %v, want missing client error", err)
	}
}

func TestClientEvalBatchUsesOnePipelineAndDecodesEveryResult(t *testing.T) {
	raw := redis.NewClient(&redis.Options{Addr: "unused"})
	pipelines := 0
	raw.AddHook(testRedisHook{pipeline: func(commands []redis.Cmder) error {
		pipelines++
		for index, command := range commands {
			command.(*redis.Cmd).SetVal([]any{"one", int64(2)}[index])
		}
		return nil
	}})

	results, err := NewClient(raw).EvalBatch(context.Background(), []txredis.ScriptCall{
		{Script: "return 'one'", Keys: []string{"slot:{1}"}},
		{Script: "return 2", Keys: []string{"slot:{2}"}},
	})
	if err != nil {
		t.Fatalf("EvalBatch returned error: %v", err)
	}
	if pipelines != 1 {
		t.Fatalf("pipeline calls = %d, want 1", pipelines)
	}
	if len(results) != 2 || results[0] != "one" || results[1] != "2" {
		t.Fatalf("EvalBatch results = %#v, want [one 2]", results)
	}
}
