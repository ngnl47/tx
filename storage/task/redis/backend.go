package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ngnl47/tx/storage/task"
)

var ErrClientNotConfigured = errors.New("redis task: client is not configured")

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
	if err := task.ValidateDelayTask(item); err != nil {
		return err
	}
	if b == nil || isNilScriptClient(b.client) {
		return ErrClientNotConfigured
	}
	body, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("redis task: encode %s: %w", item.IdempotencyKey(), err)
	}
	result, err := b.client.Eval(ctx, enqueueScript, b.keysForTask(item), item.IdempotencyKey(), unixMillis(item.DueAt), string(body))
	if err != nil {
		return fmt.Errorf("redis task: enqueue %s: %w", item.IdempotencyKey(), err)
	}
	if result == scriptResultLeased || result == scriptResultOK {
		return nil
	}
	return fmt.Errorf("redis task: enqueue %s: unexpected script result %q", item.IdempotencyKey(), result)
}

func (b *Backend) ClaimDue(ctx context.Context, now time.Time, limit int) ([]task.DelayTaskLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b == nil || isNilScriptClient(b.client) {
		return nil, ErrClientNotConfigured
	}
	if limit <= 0 {
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
			return leases, fmt.Errorf("redis task: claim due bucket %d: %w", bucket, err)
		}
		rows, err := decodeLeaseRows(body)
		if err != nil {
			return leases, fmt.Errorf("redis task: decode leases from bucket %d: %w", bucket, err)
		}
		var rowErrors []error
		for index, row := range rows {
			if row.Token == "" || row.Attempts <= 0 {
				rowErrors = append(rowErrors, fmt.Errorf("row %d: invalid task lease", index))
				continue
			}
			item, err := decodeDelayTask(row.Body)
			if err != nil {
				rowErrors = append(rowErrors, fmt.Errorf("row %d: decode task lease: %w", index, err))
				continue
			}
			leases = append(leases, task.DelayTaskLease{
				Task:     item,
				Token:    row.Token,
				Attempts: row.Attempts,
			})
		}
		if err := errors.Join(rowErrors...); err != nil {
			return leases, fmt.Errorf("redis task: decode leases from bucket %d: %w", bucket, err)
		}
	}
	return leases, nil
}

func (b *Backend) Complete(ctx context.Context, lease task.DelayTaskLease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b == nil || isNilScriptClient(b.client) {
		return ErrClientNotConfigured
	}
	if err := task.ValidateDelayTaskLease(lease); err != nil {
		return err
	}
	result, err := b.client.Eval(ctx, completeScript, b.keysForTask(lease.Task), lease.Task.IdempotencyKey(), lease.Token)
	if err != nil {
		return fmt.Errorf("redis task: complete %s: %w", lease.Task.IdempotencyKey(), err)
	}
	if result == scriptResultStale || result == scriptResultOK {
		return nil
	}
	return fmt.Errorf("redis task: complete %s: unexpected script result %q", lease.Task.IdempotencyKey(), result)
}

func (b *Backend) Retry(ctx context.Context, lease task.DelayTaskLease, next time.Time, reason error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b == nil || isNilScriptClient(b.client) {
		return ErrClientNotConfigured
	}
	if err := task.ValidateDelayTaskLease(lease); err != nil {
		return err
	}
	_ = reason
	result, err := b.client.Eval(ctx, retryScript, b.keysForTask(lease.Task), lease.Task.IdempotencyKey(), lease.Token, unixMillis(next))
	if err != nil {
		return fmt.Errorf("redis task: retry %s: %w", lease.Task.IdempotencyKey(), err)
	}
	if result == scriptResultStale || result == scriptResultOK {
		return nil
	}
	return fmt.Errorf("redis task: retry %s: unexpected script result %q", lease.Task.IdempotencyKey(), result)
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
		prefix + ":pending",
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

type claimFailure struct {
	Error   string `json:"error"`
	TaskKey string `json:"taskKey"`
}

func decodeLeaseRows(body string) ([]leaseRow, error) {
	encoded := bytes.TrimSpace([]byte(body))
	if len(encoded) == 0 {
		return nil, fmt.Errorf("expected JSON array")
	}
	if encoded[0] == '{' {
		var failure claimFailure
		if err := json.Unmarshal(encoded, &failure); err != nil {
			return nil, fmt.Errorf("decode claim failure: %w", err)
		}
		if failure.Error == "missing_payload" && failure.TaskKey != "" {
			return nil, fmt.Errorf("missing payload for task %q", failure.TaskKey)
		}
		return nil, fmt.Errorf("unexpected claim failure %q for task %q", failure.Error, failure.TaskKey)
	}
	if encoded[0] != '[' {
		return nil, fmt.Errorf("expected JSON array")
	}
	var rows []leaseRow
	if err := json.Unmarshal(encoded, &rows); err != nil {
		return nil, err
	}
	return rows, nil
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
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return task.DelayTask{}, fmt.Errorf("multiple JSON values")
		}
		return task.DelayTask{}, fmt.Errorf("invalid trailing JSON: %w", err)
	}
	item.ID = normalizeJSONNumber(item.ID)
	item.ShardKey = normalizeJSONNumber(item.ShardKey)
	if err := task.ValidateDelayTask(item); err != nil {
		return task.DelayTask{}, err
	}
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
	if u, err := strconv.ParseUint(string(number), 10, 64); err == nil {
		return u
	}
	if f, err := number.Float64(); err == nil {
		return f
	}
	return string(number)
}

func isNilScriptClient(client ScriptClient) bool {
	if client == nil {
		return true
	}
	value := reflect.ValueOf(client)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func stringHashCode(value string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(value))
	return h.Sum32()
}
