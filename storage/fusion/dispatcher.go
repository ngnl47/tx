package fusion

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/task"
)

// DelayTaskSubmitter 登记一次根记录的延迟保存需求。登记只写进程内状态，
// 不执行外部 I/O，因此 Redis 热写成功后可以立即结束业务保存。
type DelayTaskSubmitter interface {
	Submit(meta.TypeSpec, storage.RecordKey)
}

// DelayTaskDispatcherOptions 仅用于测试和高级装配覆盖默认退避参数。
// 标准 server 运行时使用框架默认值，不要求业务配置这些细节。
type DelayTaskDispatcherOptions struct {
	RetryInitialDelay time.Duration
	RetryMultiplier   float64
	RetryMaxDelay     time.Duration
	RetryJitter       float64
	Random            func() float64
}

type pendingDelayedWrite struct {
	id   string
	seq  uint64
	spec meta.TypeSpec
	key  storage.RecordKey
}

// DelayTaskDispatcher 把 Redis 热写后的根记录登记异步发布到 Fusion 延迟任务队列。
// 相同根记录只保留最新登记；发布失败会记录错误并持续重试。
type DelayTaskDispatcher struct {
	queue             DelayQueue
	retryInitialDelay time.Duration
	retryMultiplier   float64
	retryMaxDelay     time.Duration
	retryJitter       float64
	random            func() float64

	mu      sync.Mutex
	nextSeq uint64
	pending map[string]pendingDelayedWrite
	wake    chan struct{}
}

func NewDelayTaskDispatcher(queue DelayQueue) *DelayTaskDispatcher {
	return NewDelayTaskDispatcherWithOptions(queue, DelayTaskDispatcherOptions{})
}

func NewDelayTaskDispatcherWithOptions(queue DelayQueue, options DelayTaskDispatcherOptions) *DelayTaskDispatcher {
	retryInitialDelay := options.RetryInitialDelay
	if retryInitialDelay <= 0 {
		retryInitialDelay = defaultRetryInitialDelay
	}
	retryMultiplier := options.RetryMultiplier
	if retryMultiplier <= 1 {
		retryMultiplier = defaultRetryMultiplier
	}
	retryMaxDelay := options.RetryMaxDelay
	if retryMaxDelay <= 0 {
		retryMaxDelay = defaultRetryMaxDelay
	}
	if retryMaxDelay < retryInitialDelay {
		retryMaxDelay = retryInitialDelay
	}
	retryJitter := options.RetryJitter
	if retryJitter < 0 || retryJitter > 1 {
		retryJitter = defaultRetryJitter
	}
	random := options.Random
	if random == nil {
		random = rand.Float64
	}
	return &DelayTaskDispatcher{
		queue:             queue,
		retryInitialDelay: retryInitialDelay,
		retryMultiplier:   retryMultiplier,
		retryMaxDelay:     retryMaxDelay,
		retryJitter:       retryJitter,
		random:            random,
		pending:           make(map[string]pendingDelayedWrite),
		wake:              make(chan struct{}, 1),
	}
}

// Submit 登记根记录。重复登记会覆盖旧代次，但不会让 pending 数量增长。
func (d *DelayTaskDispatcher) Submit(spec meta.TypeSpec, key storage.RecordKey) {
	id := delayedWriteIdentity(spec, key)
	d.mu.Lock()
	d.nextSeq++
	d.pending[id] = pendingDelayedWrite{id: id, seq: d.nextSeq, spec: spec, key: key}
	d.mu.Unlock()
	d.notify()
}

// Start 使用服务根生命周期启动发布循环。
func (d *DelayTaskDispatcher) Start(ctx waitGroupContext) error {
	if d == nil || isNilFusionDependency(d.queue) {
		return ErrDelayQueueNotConfigured
	}
	if isNilFusionDependency(ctx) {
		return ErrSchedulerContextNotConfigured
	}
	return ctx.Go(func(context.Context) {
		d.run(ctx)
	})
}

func (d *DelayTaskDispatcher) run(ctx context.Context) {
	failures := 0
	for {
		if !d.hasPending() {
			failures = 0
			select {
			case <-ctx.Done():
				return
			case <-d.wake:
			}
		}

		_, err := d.runPendingOnce(ctx)
		if err == nil {
			failures = 0
			continue
		}
		if ctx.Err() != nil {
			return
		}
		failures++
		retryAfter := d.retryDelay(failures)
		txlog.ErrorContext(ctx, "fusion delay task publish failed",
			"pending", d.pendingCount(),
			"retryAfter", retryAfter,
			"err", err,
		)
		timer := time.NewTimer(retryAfter)
		select {
		case <-ctx.Done():
			stopDelayTimer(timer)
			return
		case <-d.wake:
			stopDelayTimer(timer)
		case <-timer.C:
		}
	}
}

func stopDelayTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func (d *DelayTaskDispatcher) runPendingOnce(ctx context.Context) (int, error) {
	entries := d.pendingSnapshot()
	var runErr error
	completed := 0
	for _, entry := range entries {
		if err := enqueueDelayedWrites(ctx, d.queue, entry.spec, entry.key); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf(
				"publish delayed task type=%s id=%v: %w",
				delayedTaskType(entry.spec.Type.Ref),
				entry.key.ID,
				err,
			))
			continue
		}
		d.removePending(entry)
		completed++
	}
	return completed, runErr
}

func (d *DelayTaskDispatcher) pendingSnapshot() []pendingDelayedWrite {
	d.mu.Lock()
	defer d.mu.Unlock()
	entries := make([]pendingDelayedWrite, 0, len(d.pending))
	for _, entry := range d.pending {
		entries = append(entries, entry)
	}
	return entries
}

func (d *DelayTaskDispatcher) removePending(entry pendingDelayedWrite) {
	d.mu.Lock()
	defer d.mu.Unlock()
	current, exists := d.pending[entry.id]
	if exists && current.seq == entry.seq {
		delete(d.pending, entry.id)
	}
}

func (d *DelayTaskDispatcher) hasPending() bool {
	return d.pendingCount() > 0
}

func (d *DelayTaskDispatcher) pendingCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.pending)
}

func (d *DelayTaskDispatcher) notify() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *DelayTaskDispatcher) retryDelay(attempts int) time.Duration {
	delay := d.retryInitialDelay
	if attempts > 1 && delay < d.retryMaxDelay {
		multiplier := math.Pow(d.retryMultiplier, float64(attempts-1))
		if multiplier >= float64(d.retryMaxDelay)/float64(delay) {
			delay = d.retryMaxDelay
		} else {
			delay = time.Duration(float64(delay) * multiplier)
		}
	}
	if d.retryJitter == 0 {
		return delay
	}
	jitter := time.Duration((d.random()*2 - 1) * d.retryJitter * float64(delay))
	if delay+jitter <= 0 {
		return time.Nanosecond
	}
	if delay+jitter > d.retryMaxDelay {
		return d.retryMaxDelay
	}
	return delay + jitter
}

func delayedWriteIdentity(spec meta.TypeSpec, key storage.RecordKey) string {
	item := task.DelayTask{
		Type:     delayedOperationType(DelayedOperation{Kind: DelayedSave, Type: spec.Type.Ref, Key: key}),
		ID:       key.ID,
		ShardKey: key.ShardKey,
		Action:   "root",
	}
	return item.IdempotencyKey()
}
