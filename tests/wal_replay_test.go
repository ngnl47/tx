package tests

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ngnl47/tx/wal"
)

type replayBackend struct {
	records   []wal.Record
	pageSize  int
	readCalls int
}

func (*replayBackend) Append(context.Context, wal.Record) error { return nil }

func (backend *replayBackend) Read(_ context.Context, _ wal.Stream, afterSeq uint64, limit int) ([]wal.Record, error) {
	backend.readCalls++
	pageSize := limit
	if backend.pageSize > 0 && backend.pageSize < pageSize {
		pageSize = backend.pageSize
	}
	records := make([]wal.Record, 0, pageSize)
	for _, record := range backend.records {
		if record.Seq <= afterSeq {
			continue
		}
		records = append(records, record)
		if len(records) == pageSize {
			break
		}
	}
	return records, nil
}

func (*replayBackend) Trim(context.Context, wal.Stream, uint64) error { return nil }

func TestWALReplayReadsShortPagesUntilEmpty(t *testing.T) {
	stream := wal.Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}
	backend := &replayBackend{
		records:  newReplayRecords(t, stream, 11, "frame-11", "frame-12", "frame-13", "frame-14", "frame-15"),
		pageSize: 2,
	}
	store, err := wal.NewStore(backend)
	if err != nil {
		t.Fatalf("NewStore returned error: %v", err)
	}

	var applied []uint64
	lastSeq, err := store.Replay(context.Background(), stream, 10, func(_ context.Context, record wal.Record) error {
		applied = append(applied, record.Seq)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if lastSeq != 15 {
		t.Fatalf("Replay last sequence = %d, want 15", lastSeq)
	}
	if want := []uint64{11, 12, 13, 14, 15}; !reflect.DeepEqual(applied, want) {
		t.Fatalf("applied sequences = %v, want %v", applied, want)
	}
	if backend.readCalls != 4 {
		t.Fatalf("backend read calls = %d, want 4", backend.readCalls)
	}
}

func TestWALReplayStopsAtApplyFailure(t *testing.T) {
	stream := wal.Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}
	backend := &replayBackend{
		records:  newReplayRecords(t, stream, 1, "frame-1", "frame-2", "frame-3"),
		pageSize: 2,
	}
	store, err := wal.NewStore(backend)
	if err != nil {
		t.Fatalf("NewStore returned error: %v", err)
	}
	applyErr := errors.New("apply failed")

	lastSeq, err := store.Replay(context.Background(), stream, 0, func(_ context.Context, record wal.Record) error {
		if record.Seq == 2 {
			return applyErr
		}
		return nil
	})
	if !errors.Is(err, applyErr) {
		t.Fatalf("Replay error = %v, want apply failure", err)
	}
	if lastSeq != 1 {
		t.Fatalf("Replay last sequence = %d, want 1", lastSeq)
	}
	if backend.readCalls != 1 {
		t.Fatalf("backend read calls = %d, want 1", backend.readCalls)
	}
}

func TestWALReplayStopsBeforeNextRecordWhenContextIsCanceled(t *testing.T) {
	stream := wal.Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}
	backend := &replayBackend{records: newReplayRecords(t, stream, 1, "frame-1", "frame-2")}
	store, err := wal.NewStore(backend)
	if err != nil {
		t.Fatalf("NewStore returned error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var applied []uint64

	lastSeq, err := store.Replay(ctx, stream, 0, func(_ context.Context, record wal.Record) error {
		applied = append(applied, record.Seq)
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Replay error = %v, want context.Canceled", err)
	}
	if lastSeq != 1 {
		t.Fatalf("Replay last sequence = %d, want 1", lastSeq)
	}
	if want := []uint64{1}; !reflect.DeepEqual(applied, want) {
		t.Fatalf("applied sequences = %v, want %v", applied, want)
	}
}

func newReplayRecords(t *testing.T, stream wal.Stream, firstSeq uint64, payloads ...string) []wal.Record {
	t.Helper()
	records := make([]wal.Record, len(payloads))
	for index, payload := range payloads {
		record, err := wal.NewRecord(stream, firstSeq+uint64(index), 1, []byte(payload))
		if err != nil {
			t.Fatalf("NewRecord sequence %d: %v", firstSeq+uint64(index), err)
		}
		records[index] = record
	}
	return records
}
