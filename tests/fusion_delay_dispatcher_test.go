package tests

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
)

type retryingDelayQueue struct {
	mu         sync.Mutex
	failures   int
	attempts   int
	operations []fusion.DelayedOperation
	succeeded  chan struct{}
}

type blockingDelayQueue struct {
	mu         sync.Mutex
	once       sync.Once
	attempts   int
	operations []fusion.DelayedOperation
	started    chan struct{}
	release    chan struct{}
}

func (q *blockingDelayQueue) Enqueue(ctx context.Context, operation fusion.DelayedOperation) error {
	q.mu.Lock()
	q.attempts++
	q.mu.Unlock()
	q.once.Do(func() { close(q.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.release:
	}
	q.mu.Lock()
	q.operations = append(q.operations, operation)
	q.mu.Unlock()
	return nil
}

func (q *blockingDelayQueue) snapshot() (int, []fusion.DelayedOperation) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.attempts, append([]fusion.DelayedOperation(nil), q.operations...)
}

func (q *retryingDelayQueue) Enqueue(_ context.Context, operation fusion.DelayedOperation) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.attempts++
	if q.failures > 0 {
		q.failures--
		return errors.New("delay task backend unavailable")
	}
	q.operations = append(q.operations, operation)
	if q.succeeded != nil {
		select {
		case q.succeeded <- struct{}{}:
		default:
		}
	}
	return nil
}

func (q *retryingDelayQueue) snapshot() (int, []fusion.DelayedOperation) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.attempts, append([]fusion.DelayedOperation(nil), q.operations...)
}

func (q *retryingDelayQueue) setFailures(failures int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.failures = failures
}

func TestDelayTaskDispatcherRetriesFailedPublishAndLogsError(t *testing.T) {
	queue := &retryingDelayQueue{failures: 1, succeeded: make(chan struct{}, 1)}
	handler := &recordingSlogHandler{}
	txlog.SetDefault(slog.New(handler))
	defer txlog.SetDefault(nil)

	dispatcher := fusion.NewDelayTaskDispatcherWithOptions(queue, fusion.DelayTaskDispatcherOptions{
		RetryInitialDelay: time.Millisecond,
		RetryMaxDelay:     time.Millisecond,
		RetryJitter:       0,
	})
	root := lifecycle.NewWaitGroupContext(context.Background())
	if err := dispatcher.Start(root); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	dispatcher.Submit(
		meta.TypeSpec{Type: fusionRoleType(), Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Minute}}},
		storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001)},
	)

	select {
	case <-queue.succeeded:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not retry the failed delayed task publication")
	}
	root.Cancel()
	root.WaitDoneAndWaitGroupWait()

	attempts, operations := queue.snapshot()
	if attempts != 2 || len(operations) != 1 || operations[0].Kind != fusion.DelayedSave {
		t.Fatalf("attempts=%d operations=%#v, want one failed and one successful save publication", attempts, operations)
	}
	records := handler.Records()
	if len(records) != 1 {
		t.Fatalf("log records = %#v, want one publication failure", records)
	}
	record := records[0]
	if record.Level != slog.LevelError || record.Message != "fusion delay task publish failed" {
		t.Fatalf("log record = %#v", record)
	}
	if !recordAttrContains(record, "err", "delay task backend unavailable") {
		t.Fatalf("log attrs = %#v, want backend error", recordAttrs(record))
	}
}

func recordAttrContains(record slog.Record, key string, fragment string) bool {
	found := false
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == key && strings.Contains(attr.Value.String(), fragment) {
			found = true
			return false
		}
		return true
	})
	return found
}

func TestDelayTaskDispatcherDeduplicatesPendingRoot(t *testing.T) {
	queue := &retryingDelayQueue{succeeded: make(chan struct{}, 1)}
	dispatcher := fusion.NewDelayTaskDispatcher(queue)
	spec := meta.TypeSpec{Type: fusionRoleType(), Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Minute}}}
	key := storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001), ShardKey: int64(7)}

	dispatcher.Submit(spec, key)
	dispatcher.Submit(spec, key)
	root := lifecycle.NewWaitGroupContext(context.Background())
	if err := dispatcher.Start(root); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	select {
	case <-queue.succeeded:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not publish the pending delayed task")
	}
	time.Sleep(10 * time.Millisecond)
	root.Cancel()
	root.WaitDoneAndWaitGroupWait()

	attempts, operations := queue.snapshot()
	if attempts != 1 || len(operations) != 1 {
		t.Fatalf("attempts=%d operations=%#v, want one deduplicated publication", attempts, operations)
	}
}

func TestDelayTaskDispatcherPublishesOnlyDelayedSave(t *testing.T) {
	queue := &retryingDelayQueue{succeeded: make(chan struct{}, 1)}
	dispatcher := fusion.NewDelayTaskDispatcher(queue)
	spec := meta.TypeSpec{
		Type: fusionRoleType(),
		Storage: meta.StorageSpec{
			DelaySave:   meta.Duration{Duration: 10 * time.Minute},
			DelayDelete: meta.Duration{Duration: 24 * time.Hour},
		},
	}
	key := storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001), ShardKey: int64(7)}
	dispatcher.Submit(spec, key)

	root := lifecycle.NewWaitGroupContext(context.Background())
	if err := dispatcher.Start(root); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	select {
	case <-queue.succeeded:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not publish delayed save")
	}
	root.Cancel()
	root.WaitDoneAndWaitGroupWait()

	_, operations := queue.snapshot()
	if len(operations) != 1 {
		t.Fatalf("operations = %#v, want one save", operations)
	}
	if operations[0].Kind != fusion.DelayedSave || operations[0].RunAfter != 10*time.Minute {
		t.Fatalf("save operation = %#v", operations[0])
	}
}

func TestDelayTaskDispatcherDrainRetriesUntilPendingPublished(t *testing.T) {
	queue := &retryingDelayQueue{failures: 1}
	dispatcher := fusion.NewDelayTaskDispatcherWithOptions(queue, fusion.DelayTaskDispatcherOptions{
		RetryInitialDelay: time.Millisecond,
		RetryMaxDelay:     time.Millisecond,
		RetryJitter:       0,
	})
	dispatcher.Submit(
		meta.TypeSpec{Type: fusionRoleType(), Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Minute}}},
		storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001)},
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := dispatcher.Drain(ctx); err != nil {
		t.Fatalf("Drain returned error: %v", err)
	}

	attempts, operations := queue.snapshot()
	if attempts != 2 || len(operations) != 1 || operations[0].Kind != fusion.DelayedSave {
		t.Fatalf("attempts=%d operations=%#v, want one failed and one successful save publication", attempts, operations)
	}
}

func TestDelayTaskDispatcherDrainTimeoutKeepsPendingForNextDrain(t *testing.T) {
	queue := &retryingDelayQueue{failures: 1000}
	dispatcher := fusion.NewDelayTaskDispatcherWithOptions(queue, fusion.DelayTaskDispatcherOptions{
		RetryInitialDelay: time.Millisecond,
		RetryMaxDelay:     time.Millisecond,
		RetryJitter:       0,
	})
	dispatcher.Submit(
		meta.TypeSpec{Type: fusionRoleType(), Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Minute}}},
		storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001)},
	)

	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	err := dispatcher.Drain(timeoutCtx)
	timeoutCancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain error = %v, want context deadline exceeded", err)
	}

	queue.setFailures(0)
	retryCtx, retryCancel := context.WithTimeout(context.Background(), time.Second)
	defer retryCancel()
	if err := dispatcher.Drain(retryCtx); err != nil {
		t.Fatalf("second Drain returned error: %v", err)
	}
	_, operations := queue.snapshot()
	if len(operations) != 1 || operations[0].Kind != fusion.DelayedSave {
		t.Fatalf("operations=%#v, want retained pending save to be published", operations)
	}
}

func TestDelayTaskDispatcherDrainDoesNotDuplicateBackgroundPublish(t *testing.T) {
	queue := &blockingDelayQueue{started: make(chan struct{}), release: make(chan struct{})}
	dispatcher := fusion.NewDelayTaskDispatcher(queue)
	dispatcher.Submit(
		meta.TypeSpec{Type: fusionRoleType(), Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Minute}}},
		storage.RecordKey{TypeName: "data.GDRole", ID: int64(1001)},
	)
	root := lifecycle.NewWaitGroupContext(context.Background())
	if err := dispatcher.Start(root); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	select {
	case <-queue.started:
	case <-time.After(time.Second):
		t.Fatal("background publisher did not start")
	}

	drained := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		drained <- dispatcher.Drain(ctx)
	}()
	close(queue.release)
	if err := <-drained; err != nil {
		t.Fatalf("Drain returned error: %v", err)
	}
	root.Cancel()
	root.WaitDoneAndWaitGroupWait()

	attempts, operations := queue.snapshot()
	if attempts != 1 || len(operations) != 1 {
		t.Fatalf("attempts=%d operations=%#v, want one serialized publication", attempts, operations)
	}
}
