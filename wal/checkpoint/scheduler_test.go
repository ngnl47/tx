package checkpoint_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/wal"
	walcheckpoint "github.com/ngnl47/tx/wal/checkpoint"
)

func TestSchedulerSerializesSameStreamAndKeepsLatestSequence(t *testing.T) {
	started := make(chan walcheckpoint.Request, 2)
	release := make(chan struct{}, 2)
	scheduler := newStartedScheduler(t, 2, walcheckpoint.HandlerFunc(func(_ context.Context, request walcheckpoint.Request) error {
		started <- request
		<-release
		return nil
	}))
	stream := testStream("world-1")
	if err := scheduler.Submit(walcheckpoint.Request{Stream: stream, ThroughSeq: 1}); err != nil {
		t.Fatalf("Submit sequence 1: %v", err)
	}
	first := waitRequest(t, started)
	if first.ThroughSeq != 1 {
		t.Fatalf("first sequence = %d, want 1", first.ThroughSeq)
	}
	if err := scheduler.Submit(walcheckpoint.Request{Stream: stream, ThroughSeq: 3}); err != nil {
		t.Fatalf("Submit sequence 3: %v", err)
	}
	select {
	case request := <-started:
		t.Fatalf("same stream ran concurrently: %#v", request)
	case <-time.After(20 * time.Millisecond):
	}
	release <- struct{}{}
	second := waitRequest(t, started)
	if second.ThroughSeq != 3 {
		t.Fatalf("second sequence = %d, want 3", second.ThroughSeq)
	}
	release <- struct{}{}
	waitIdle(t, scheduler)
}

func TestSchedulerRunsDifferentStreamsInParallel(t *testing.T) {
	started := make(chan walcheckpoint.Request, 2)
	release := make(chan struct{})
	scheduler := newStartedScheduler(t, 2, walcheckpoint.HandlerFunc(func(_ context.Context, request walcheckpoint.Request) error {
		started <- request
		<-release
		return nil
	}))
	if err := scheduler.Submit(walcheckpoint.Request{Stream: testStream("world-1"), ThroughSeq: 1}); err != nil {
		t.Fatalf("Submit world-1: %v", err)
	}
	if err := scheduler.Submit(walcheckpoint.Request{Stream: testStream("world-2"), ThroughSeq: 1}); err != nil {
		t.Fatalf("Submit world-2: %v", err)
	}
	first := waitRequest(t, started)
	second := waitRequest(t, started)
	if first.Stream == second.Stream {
		t.Fatalf("parallel requests use the same stream: %#v %#v", first, second)
	}
	close(release)
	waitIdle(t, scheduler)
}

func TestSchedulerRetainsFailedStreamAndRetries(t *testing.T) {
	var attempts atomic.Int32
	scheduler := newStartedScheduler(t, 1, walcheckpoint.HandlerFunc(func(context.Context, walcheckpoint.Request) error {
		if attempts.Add(1) == 1 {
			return errors.New("mongo unavailable")
		}
		return nil
	}))
	if err := scheduler.Submit(walcheckpoint.Request{Stream: testStream("world-1"), ThroughSeq: 5}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitIdle(t, scheduler)
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
}

func BenchmarkSchedulerSubmitCoalescedStream(b *testing.B) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	scheduler, err := walcheckpoint.New(walcheckpoint.HandlerFunc(func(context.Context, walcheckpoint.Request) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	}), walcheckpoint.Config{Workers: 1})
	if err != nil {
		b.Fatalf("New scheduler: %v", err)
	}
	tasks := lifecycle.NewWaitGroupContext(context.Background())
	if err := scheduler.Start(tasks); err != nil {
		b.Fatalf("Start scheduler: %v", err)
	}
	stream := testStream("world-1")
	if err := scheduler.Submit(walcheckpoint.Request{Stream: stream, ThroughSeq: 1}); err != nil {
		b.Fatalf("Submit first request: %v", err)
	}
	<-started
	b.Cleanup(func() {
		close(release)
		tasks.Cancel()
		tasks.Wait()
	})

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := scheduler.Submit(walcheckpoint.Request{Stream: stream, ThroughSeq: uint64(index + 2)}); err != nil {
			b.Fatalf("Submit: %v", err)
		}
	}
}

func newStartedScheduler(t *testing.T, workers int, handler walcheckpoint.Handler) *walcheckpoint.Scheduler {
	t.Helper()
	scheduler, err := walcheckpoint.New(handler, walcheckpoint.Config{
		Workers:         workers,
		RetryInitial:    time.Millisecond,
		RetryMax:        5 * time.Millisecond,
		RetryMultiplier: 2,
	})
	if err != nil {
		t.Fatalf("New scheduler: %v", err)
	}
	tasks := lifecycle.NewWaitGroupContext(context.Background())
	if err := scheduler.Start(tasks); err != nil {
		t.Fatalf("Start scheduler: %v", err)
	}
	t.Cleanup(func() {
		tasks.Cancel()
		tasks.Wait()
	})
	return scheduler
}

func waitRequest(t *testing.T, started <-chan walcheckpoint.Request) walcheckpoint.Request {
	t.Helper()
	select {
	case request := <-started:
		return request
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for checkpoint request")
		return walcheckpoint.Request{}
	}
}

func waitIdle(t *testing.T, scheduler *walcheckpoint.Scheduler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := scheduler.WaitIdle(ctx); err != nil {
		t.Fatalf("WaitIdle returned error: %v", err)
	}
}

func testStream(key string) wal.Stream {
	return wal.Stream{Namespace: "slgworld", StreamKey: key, Generation: 1}
}
