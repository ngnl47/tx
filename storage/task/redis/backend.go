package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ngnl47/tx/storage/task"
)

const (
	defaultPrefix       = "tx:task"
	defaultBucketCount  = 64
	defaultLeaseTimeout = 30 * time.Second
)

type ScriptClient interface {
	Eval(context.Context, string, []string, ...any) (string, error)
}

type Options struct {
	Prefix       string
	BucketCount  int
	LeaseTimeout time.Duration
}

type Backend struct {
	client       ScriptClient
	prefix       string
	bucketCount  int
	leaseTimeout time.Duration
	seq          atomic.Uint64
	claimCursor  atomic.Uint64
}

func NewBackend(client ScriptClient, options Options) *Backend {
	prefix := options.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}
	bucketCount := options.BucketCount
	if bucketCount <= 0 {
		bucketCount = defaultBucketCount
	}
	leaseTimeout := options.LeaseTimeout
	if leaseTimeout <= 0 {
		leaseTimeout = defaultLeaseTimeout
	}
	return &Backend{
		client:       client,
		prefix:       prefix,
		bucketCount:  bucketCount,
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
	_, err = b.client.Eval(ctx, enqueueScript, b.keysForTask(item), item.IdempotencyKey(), unixMillis(item.DueAt), string(body))
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
	leases := make([]task.DelayTaskLease, 0, limit)
	start := int(b.claimCursor.Add(1) % uint64(b.bucketCount))
	for offset := 0; offset < b.bucketCount && len(leases) < limit; offset++ {
		bucket := (start + offset) % b.bucketCount
		tokenPrefix := strconv.FormatInt(now.UnixNano(), 10) + "-" + strconv.FormatUint(b.seq.Add(1), 10)
		body, err := b.client.Eval(
			ctx,
			claimScript,
			b.keysForBucket(bucket),
			unixMillis(now),
			limit-len(leases),
			unixMillis(now.Add(b.leaseTimeout)),
			tokenPrefix,
		)
		if err != nil {
			return nil, fmt.Errorf("redis task: claim due bucket %d: %w", bucket, err)
		}
		if body == "" {
			continue
		}
		var rows []leaseRow
		if err := json.Unmarshal([]byte(body), &rows); err != nil {
			return nil, fmt.Errorf("redis task: decode leases: %w", err)
		}
		for _, row := range rows {
			item, err := decodeDelayTask(row.Body)
			if err != nil {
				return nil, fmt.Errorf("redis task: decode task lease: %w", err)
			}
			leases = append(leases, task.DelayTaskLease{
				Task:     item,
				Token:    row.Token,
				Attempts: row.Attempts,
			})
		}
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
	if _, err := b.client.Eval(ctx, completeScript, b.keysForTask(lease.Task), lease.Task.IdempotencyKey(), lease.Token); err != nil {
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
	if _, err := b.client.Eval(ctx, retryScript, b.keysForTask(lease.Task), lease.Task.IdempotencyKey(), lease.Token, unixMillis(next)); err != nil {
		return fmt.Errorf("redis task: retry %s: %w", lease.Task.IdempotencyKey(), err)
	}
	return nil
}

func (b *Backend) keysForTask(item task.DelayTask) []string {
	return b.keysForBucket(b.bucketFor(item.IdempotencyKey()))
}

func (b *Backend) keysForBucket(bucket int) []string {
	prefix := b.prefix + ":{" + strconv.Itoa(bucket) + "}"
	return []string{
		prefix + ":due",
		prefix + ":payload",
		prefix + ":busy",
		prefix + ":token",
		prefix + ":attempts",
	}
}

func (b *Backend) bucketFor(key string) int {
	return int(stringHashCode(key) % uint32(b.bucketCount))
}

type leaseRow struct {
	Body     string `json:"body"`
	Token    string `json:"token"`
	Attempts int    `json:"attempts"`
}

func unixMillis(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func decodeDelayTask(body string) (task.DelayTask, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(body))
	decoder.UseNumber()
	var item task.DelayTask
	if err := decoder.Decode(&item); err != nil {
		return task.DelayTask{}, err
	}
	item.ID = normalizeJSONNumber(item.ID)
	item.ShardKey = normalizeJSONNumber(item.ShardKey)
	return item, nil
}

func normalizeJSONNumber(value any) any {
	number, ok := value.(json.Number)
	if !ok {
		return value
	}
	if i, err := number.Int64(); err == nil {
		return i
	}
	if f, err := number.Float64(); err == nil {
		return f
	}
	return string(number)
}

func stringHashCode(value string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(value))
	return h.Sum32()
}
