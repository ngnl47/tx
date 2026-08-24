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

func TestDelayTaskDispatcherPublishesSaveAndDeleteWindows(t *testing.T) {
	queue := &retryingDelayQueue{succeeded: make(chan struct{}, 2)}
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
	for range 2 {
		select {
		case <-queue.succeeded:
		case <-time.After(time.Second):
			t.Fatal("dispatcher did not publish both delayed tasks")
		}
	}
	root.Cancel()
	root.WaitDoneAndWaitGroupWait()

	_, operations := queue.snapshot()
	if len(operations) != 2 {
		t.Fatalf("operations = %#v, want save and delete", operations)
	}
	if operations[0].Kind != fusion.DelayedSave || operations[0].RunAfter != 10*time.Minute {
		t.Fatalf("save operation = %#v", operations[0])
	}
	if operations[1].Kind != fusion.DelayedDelete || operations[1].RunAfter != 24*time.Hour+10*time.Minute {
		t.Fatalf("delete operation = %#v", operations[1])
	}
}
