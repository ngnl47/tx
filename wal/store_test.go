package wal

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

type storeTestBackend struct{}

func (storeTestBackend) Append(context.Context, Record) error { return nil }
func (storeTestBackend) Read(context.Context, Stream, uint64, int) ([]Record, error) {
	return nil, nil
}
func (storeTestBackend) Trim(context.Context, Stream, uint64) error { return nil }

func TestCheckpointRegistryReleasesIdleStream(t *testing.T) {
	created, err := NewStore(storeTestBackend{})
	if err != nil {
		t.Fatalf("NewStore returned error: %v", err)
	}
	current := created.(*store)
	stream := Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}
	if err := current.Checkpoint(context.Background(), stream, 1, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}
	current.checkpoints.mu.Lock()
	count := len(current.checkpoints.entries)
	current.checkpoints.mu.Unlock()
	if count != 0 {
		t.Fatalf("idle checkpoint entries = %d, want 0", count)
	}
}

func TestCheckpointWaitRespectsContextCancellation(t *testing.T) {
	created, err := NewStore(storeTestBackend{})
	if err != nil {
		t.Fatalf("NewStore returned error: %v", err)
	}
	stream := Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}
	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- created.Checkpoint(context.Background(), stream, 2, func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- created.Checkpoint(waitCtx, stream, 1, func(context.Context) error { return nil })
	}()
	select {
	case err := <-waitDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting Checkpoint error = %v, want context.Canceled", err)
		}
	case <-time.After(100 * time.Millisecond):
		close(release)
		<-firstDone
		t.Fatal("waiting Checkpoint ignored context cancellation")
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Checkpoint returned error: %v", err)
	}
}

func TestCheckpointRejectsRegressionFromOverlappingCall(t *testing.T) {
	created, err := NewStore(storeTestBackend{})
	if err != nil {
		t.Fatalf("NewStore returned error: %v", err)
	}
	current := created.(*store)
	stream := Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}
	entered := make(chan struct{})
	release := make(chan struct{})
	highDone := make(chan error, 1)
	go func() {
		highDone <- current.Checkpoint(context.Background(), stream, 200, func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	var lowCallbackCalled atomic.Bool
	lowDone := make(chan error, 1)
	go func() {
		lowDone <- current.Checkpoint(context.Background(), stream, 100, func(context.Context) error {
			lowCallbackCalled.Store(true)
			return nil
		})
	}()
	waitCheckpointReferences(t, current, stream, 2)
	close(release)
	if err := <-highDone; err != nil {
		t.Fatalf("higher Checkpoint returned error: %v", err)
	}
	if err := <-lowDone; !errors.Is(err, ErrCheckpointRegression) {
		t.Fatalf("lower Checkpoint error = %v, want ErrCheckpointRegression", err)
	}
	if lowCallbackCalled.Load() {
		t.Fatal("regressing checkpoint callback was called")
	}
}

func waitCheckpointReferences(t *testing.T, current *store, stream Stream, want int) {
	t.Helper()
	id := streamID{namespace: stream.Namespace, streamKey: stream.StreamKey, generation: stream.Generation}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		current.checkpoints.mu.Lock()
		entry := current.checkpoints.entries[id]
		got := 0
		if entry != nil {
			got = entry.references
		}
		current.checkpoints.mu.Unlock()
		if got == want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("checkpoint references did not reach %d", want)
}
