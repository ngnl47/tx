package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ngnl47/tx/storage/task"
)

const startupLeaseSuffix = ":{startup}:handoff-recovery"

// StartupLease 使用 Redis Lua 和 token fencing 协调启动恢复。
type StartupLease struct {
	client     ScriptClient
	key        string
	optionsErr error
}

// StartupLease 返回与当前延迟队列前缀绑定的启动恢复租约。
func (b *Backend) StartupLease() *StartupLease {
	if b == nil {
		return &StartupLease{}
	}
	return &StartupLease{
		client:     b.client,
		key:        b.prefix + startupLeaseSuffix,
		optionsErr: b.optionsErr,
	}
}

func (lease *StartupLease) Acquire(ctx context.Context, token string, ttl time.Duration) (task.StartupLeaseStatus, error) {
	result, err := lease.eval(ctx, "acquire", token, ttl)
	if err != nil {
		return task.StartupLeaseStatusMissing, err
	}
	switch result {
	case "acquired":
		return task.StartupLeaseStatusAcquired, nil
	case "running":
		return task.StartupLeaseStatusRunning, nil
	case "complete":
		return task.StartupLeaseStatusComplete, nil
	default:
		return task.StartupLeaseStatusMissing, fmt.Errorf("redis task: acquire startup lease: unexpected script result %q", result)
	}
}

func (lease *StartupLease) Renew(ctx context.Context, token string, ttl time.Duration) (bool, error) {
	return lease.compareAndSet(ctx, "renew", token, ttl)
}

func (lease *StartupLease) Complete(ctx context.Context, token string, ttl time.Duration) (bool, error) {
	return lease.compareAndSet(ctx, "complete", token, ttl)
}

func (lease *StartupLease) Release(ctx context.Context, token string) (bool, error) {
	return lease.compareAndSet(ctx, "release", token, time.Millisecond)
}

func (lease *StartupLease) compareAndSet(ctx context.Context, operation, token string, ttl time.Duration) (bool, error) {
	result, err := lease.eval(ctx, operation, token, ttl)
	if err != nil {
		return false, err
	}
	switch result {
	case "ok":
		return true, nil
	case "stale":
		return false, nil
	default:
		return false, fmt.Errorf("redis task: %s startup lease: unexpected script result %q", operation, result)
	}
}

func (lease *StartupLease) eval(ctx context.Context, operation, token string, ttl time.Duration) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if lease == nil || isNilScriptClient(lease.client) {
		return "", ErrClientNotConfigured
	}
	if lease.optionsErr != nil {
		return "", lease.optionsErr
	}
	if strings.TrimSpace(token) == "" || token != strings.TrimSpace(token) {
		return "", fmt.Errorf("%w: startup lease token is empty or contains surrounding whitespace", ErrInvalidOptions)
	}
	if ttl < time.Millisecond {
		return "", fmt.Errorf("%w: startup lease TTL must be at least 1ms", ErrInvalidOptions)
	}
	result, err := lease.client.Eval(ctx, startupLeaseScript, []string{lease.key}, operation, token, ttl.Milliseconds())
	if err != nil {
		return "", fmt.Errorf("redis task: %s startup lease: %w", operation, err)
	}
	return result, nil
}

var _ task.StartupLease = (*StartupLease)(nil)
