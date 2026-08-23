package fusion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"strconv"
	"time"

	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	txredis "github.com/ngnl47/tx/storage/redis"
	"github.com/ngnl47/tx/storage/task"
)

const (
	defaultHandoffSlotsPerRun    = 256
	defaultHandoffEntriesPerHash = 64
)

// HandoffStore 是 Fusion 交接恢复使用的 Redis 高级边界。
// 普通业务保存不应直接调用这些方法。
type HandoffStore interface {
	ReadHandoffs(context.Context, uint64, int, int) ([]txredis.HandoffEntry, uint64, error)
	storage.RecordRevisionReader
	ConfirmHandoff(context.Context, meta.TypeSpec, storage.RecordKey, string, txredis.HandoffReceipt) (bool, error)
}

// HandoffRecovererOptions 控制固定 slot 轮询批量和循环级故障退避。
type HandoffRecovererOptions struct {
	SlotsPerRun       int
	EntriesPerSlot    int
	PollInterval      time.Duration
	RetryInitialDelay time.Duration
	RetryMultiplier   float64
	RetryMaxDelay     time.Duration
	RetryJitter       float64
	Random            func() float64
}

// HandoffRecoverer 把 Redis 同槽交接标记幂等重建为现有 Fusion 延迟任务。
// 它只访问固定物理 slot 的已知 Hash，不扫描 Redis keyspace。
type HandoffRecoverer struct {
	types           TypeRegistry
	store           HandoffStore
	queue           DelayQueue
	cursor          uint64
	slotsPerRun     int
	entriesPerSlot  int
	pollInterval    time.Duration
	retryInitial    time.Duration
	retryMultiplier float64
	retryMax        time.Duration
	retryJitter     float64
	random          func() float64
}

func NewHandoffRecoverer(types TypeRegistry, store HandoffStore, queue DelayQueue) *HandoffRecoverer {
	return NewHandoffRecovererWithOptions(types, store, queue, HandoffRecovererOptions{})
}

func NewHandoffRecovererWithOptions(types TypeRegistry, store HandoffStore, queue DelayQueue, options HandoffRecovererOptions) *HandoffRecoverer {
	slotsPerRun := options.SlotsPerRun
	if slotsPerRun <= 0 {
		slotsPerRun = defaultHandoffSlotsPerRun
	}
	entriesPerSlot := options.EntriesPerSlot
	if entriesPerSlot <= 0 {
		entriesPerSlot = defaultHandoffEntriesPerHash
	}
	pollInterval := options.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultSchedulerPollInterval
	}
	retryInitial := options.RetryInitialDelay
	if retryInitial <= 0 {
		retryInitial = defaultRetryInitialDelay
	}
	retryMultiplier := options.RetryMultiplier
	if retryMultiplier <= 1 {
		retryMultiplier = defaultRetryMultiplier
	}
	retryMax := options.RetryMaxDelay
	if retryMax <= 0 {
		retryMax = defaultRetryMaxDelay
	}
	if retryMax < retryInitial {
		retryMax = retryInitial
	}
	retryJitter := options.RetryJitter
	if retryJitter < 0 || retryJitter > 1 {
		retryJitter = defaultRetryJitter
	}
	random := options.Random
	if random == nil {
		random = rand.Float64
	}
	return &HandoffRecoverer{
		types:           types,
		store:           store,
		queue:           queue,
		slotsPerRun:     slotsPerRun,
		entriesPerSlot:  entriesPerSlot,
		pollInterval:    pollInterval,
		retryInitial:    retryInitial,
		retryMultiplier: retryMultiplier,
		retryMax:        retryMax,
		retryJitter:     retryJitter,
		random:          random,
	}
}

func (r *HandoffRecoverer) Start(ctx waitGroupContext) error {
	if err := r.validate(); err != nil {
		return err
	}
	if isNilFusionDependency(ctx) {
		return ErrSchedulerContextNotConfigured
	}
	return ctx.Go(func(context.Context) {
		r.run(ctx)
	})
}

func (r *HandoffRecoverer) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	entries, next, runErr := r.store.ReadHandoffs(ctx, r.cursor, r.slotsPerRun, r.entriesPerSlot)
	r.cursor = next
	processed := 0
	for _, entry := range entries {
		if err := r.recoverEntry(ctx, entry); err != nil {
			runErr = errors.Join(runErr, err)
			continue
		}
		processed++
	}
	return processed, runErr
}

func (r *HandoffRecoverer) validate() error {
	if r == nil || isNilFusionDependency(r.store) {
		return ErrHandoffStoreNotConfigured
	}
	if isNilFusionDependency(r.types) {
		return ErrHandoffTypesNotConfigured
	}
	if isNilFusionDependency(r.queue) {
		return ErrMongoQueueNotConfigured
	}
	return nil
}

func (r *HandoffRecoverer) recoverEntry(ctx context.Context, entry txredis.HandoffEntry) error {
	marker, err := decodeHandoffMarker(entry.Marker)
	if err != nil {
		return err
	}
	spec, ok := r.types.Lookup(meta.TypeRef{FullName: marker.Type})
	if !ok {
		return fmt.Errorf("fusion: unknown handoff type %q", marker.Type)
	}
	if spec.Storage.Mode != meta.StorageModeFusion {
		return fmt.Errorf("fusion: handoff type %q uses storage mode %q", marker.Type, spec.Storage.Mode)
	}
	key := storage.RecordKey{TypeName: marker.Type, ID: marker.ID, ShardKey: marker.ShardKey}
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return err
	}
	revision, err := r.store.ReadRecordRevision(ctx, spec, key)
	if err != nil {
		return err
	}
	if err := enqueueDelayedWrites(ctx, r.queue, spec, key); err != nil {
		return err
	}
	_, err = r.store.ConfirmHandoff(ctx, spec, key, entry.Marker, txredis.HandoffReceipt{Revision: revision, Token: entry.Token})
	return err
}

func decodeHandoffMarker(encoded string) (handoffMarker, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(encoded))
	decoder.UseNumber()
	var marker handoffMarker
	if err := decoder.Decode(&marker); err != nil {
		return handoffMarker{}, fmt.Errorf("fusion: decode handoff marker: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return handoffMarker{}, fmt.Errorf("fusion: decode handoff marker: multiple JSON values")
		}
		return handoffMarker{}, fmt.Errorf("fusion: decode handoff marker trailing data: %w", err)
	}
	marker.ID = normalizeHandoffNumber(marker.ID)
	marker.ShardKey = normalizeHandoffNumber(marker.ShardKey)
	if err := task.ValidateDelayTask(task.DelayTask{Type: marker.Type, ID: marker.ID, ShardKey: marker.ShardKey, Action: string(DelayedSave)}); err != nil {
		return handoffMarker{}, err
	}
	return marker, nil
}

func normalizeHandoffNumber(value any) any {
	number, ok := value.(json.Number)
	if !ok {
		return value
	}
	if integer, err := number.Int64(); err == nil {
		return integer
	}
	if unsigned, err := strconv.ParseUint(string(number), 10, 64); err == nil {
		return unsigned
	}
	if decimal, err := number.Float64(); err == nil {
		return decimal
	}
	return string(number)
}

func (r *HandoffRecoverer) run(ctx context.Context) {
	failures := 0
	for {
		_, err := r.RunOnce(ctx)
		wait := r.pollInterval
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failures++
			wait = handoffRetryDelay(r.retryInitial, r.retryMultiplier, r.retryMax, r.retryJitter, r.random, failures)
			txlog.ErrorContext(ctx, "fusion handoff recovery failed", "retryAfter", wait, "err", err)
		} else {
			failures = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func handoffRetryDelay(initial time.Duration, multiplier float64, maximum time.Duration, jitter float64, random func() float64, attempts int) time.Duration {
	delay := initial
	if attempts > 1 && delay < maximum {
		factor := math.Pow(multiplier, float64(attempts-1))
		if factor >= float64(maximum)/float64(delay) {
			delay = maximum
		} else {
			delay = time.Duration(float64(delay) * factor)
		}
	}
	if jitter == 0 {
		return delay
	}
	shift := time.Duration((random()*2 - 1) * jitter * float64(delay))
	if delay+shift <= 0 {
		return time.Nanosecond
	}
	if delay+shift > maximum {
		return maximum
	}
	return delay + shift
}
