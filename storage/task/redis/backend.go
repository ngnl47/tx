package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ngnl47/tx/storage/task"
)

const (
	defaultPrefix       = "tx:task"
	defaultLeaseTimeout = 30 * time.Second
)

type ScriptClient interface {
	Eval(context.Context, string, []string, ...any) (string, error)
}

type Options struct {
	Prefix       string
	LeaseTimeout time.Duration
}

type Backend struct {
	client       ScriptClient
	prefix       string
	leaseTimeout time.Duration
	seq          atomic.Uint64
}

func NewBackend(client ScriptClient, options Options) *Backend {
	prefix := options.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}
	leaseTimeout := options.LeaseTimeout
	if leaseTimeout <= 0 {
		leaseTimeout = defaultLeaseTimeout
	}
	return &Backend{
		client:       client,
		prefix:       prefix,
		leaseTimeout: leaseTimeout,
	}
}

func (b *Backend) Enqueue(ctx context.Context, item task.DelayTask) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.client == nil {
		return nil
	}
	body, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("redis task: encode %s: %w", item.IdempotencyKey(), err)
	}
	_, err = b.client.Eval(ctx, enqueueScript, b.keys(), item.IdempotencyKey(), unixMillis(item.DueAt), string(body))
	if err != nil {
		return fmt.Errorf("redis task: enqueue %s: %w", item.IdempotencyKey(), err)
	}
	return nil
}

func (b *Backend) ClaimDue(ctx context.Context, now time.Time, limit int) ([]task.DelayTaskLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.client == nil || limit <= 0 {
		return nil, nil
	}
	tokenPrefix := strconv.FormatInt(now.UnixNano(), 10) + "-" + strconv.FormatUint(b.seq.Add(1), 10)
	body, err := b.client.Eval(
		ctx,
		claimScript,
		b.keys(),
		unixMillis(now),
		limit,
		unixMillis(now.Add(b.leaseTimeout)),
		tokenPrefix,
	)
	if err != nil {
		return nil, fmt.Errorf("redis task: claim due: %w", err)
	}
	if body == "" {
		return nil, nil
	}
	var rows []leaseRow
	if err := json.Unmarshal([]byte(body), &rows); err != nil {
		return nil, fmt.Errorf("redis task: decode leases: %w", err)
	}
	leases := make([]task.DelayTaskLease, 0, len(rows))
	for _, row := range rows {
		var item task.DelayTask
		if err := json.Unmarshal([]byte(row.Body), &item); err != nil {
			return nil, fmt.Errorf("redis task: decode task lease: %w", err)
		}
		leases = append(leases, task.DelayTaskLease{
			Task:     item,
			Token:    row.Token,
			Attempts: row.Attempts,
		})
	}
	return leases, nil
}

func (b *Backend) Complete(ctx context.Context, lease task.DelayTaskLease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.client == nil {
		return nil
	}
	if _, err := b.client.Eval(ctx, completeScript, b.keys(), lease.Task.IdempotencyKey(), lease.Token); err != nil {
		return fmt.Errorf("redis task: complete %s: %w", lease.Task.IdempotencyKey(), err)
	}
	return nil
}

func (b *Backend) Retry(ctx context.Context, lease task.DelayTaskLease, next time.Time, reason error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.client == nil {
		return nil
	}
	_ = reason
	if _, err := b.client.Eval(ctx, retryScript, b.keys(), lease.Task.IdempotencyKey(), lease.Token, unixMillis(next)); err != nil {
		return fmt.Errorf("redis task: retry %s: %w", lease.Task.IdempotencyKey(), err)
	}
	return nil
}

func (b *Backend) keys() []string {
	return []string{
		b.prefix + ":due",
		b.prefix + ":payload",
		b.prefix + ":busy",
		b.prefix + ":token",
		b.prefix + ":attempts",
	}
}

type leaseRow struct {
	Body     string `json:"body"`
	Token    string `json:"token"`
	Attempts int    `json:"attempts"`
}

func unixMillis(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}
