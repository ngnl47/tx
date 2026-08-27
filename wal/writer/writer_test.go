package writer_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/wal"
	walcheckpoint "github.com/ngnl47/tx/wal/checkpoint"
	walwriter "github.com/ngnl47/tx/wal/writer"
)

func TestSyncWriterAppendsBeforePersistAndCheckpointsAfterSuccess(t *testing.T) {
	log := &recordingStore{}
	writer, err := walwriter.NewSync(log)
	if err != nil {
		t.Fatalf("NewSync returned error: %v", err)
	}
	record := newRecord(t, 3)
	if err := writer.Write(context.Background(), record, func(context.Context) error {
		log.events = append(log.events, "persist")
		return nil
	}); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	want := []string{"append", "checkpoint", "persist"}
	if !equalStrings(log.events, want) {
		t.Fatalf("events = %v, want %v", log.events, want)
	}
}

func TestSyncWriterDoesNotCheckpointWhenAppendFails(t *testing.T) {
	wantErr := errors.New("append failed")
	log := &recordingStore{appendErr: wantErr}
	writer, err := walwriter.NewSync(log)
	if err != nil {
		t.Fatalf("NewSync returned error: %v", err)
	}
	persisted := false
	err = writer.Write(context.Background(), newRecord(t, 1), func(context.Context) error {
		persisted = true
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Write error = %v, want %v", err, wantErr)
	}
	if persisted {
		t.Fatal("persist callback ran after append failure")
	}
}

func TestAsyncWriterSubmitsCheckpointAfterAppend(t *testing.T) {
	log := &recordingStore{}
	submitter := &recordingSubmitter{}
	writer, err := walwriter.NewAsync(log, submitter)
	if err != nil {
		t.Fatalf("NewAsync returned error: %v", err)
	}
	record := newRecord(t, 7)
	if err := writer.Write(context.Background(), record); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	if len(log.events) != 1 || log.events[0] != "append" {
		t.Fatalf("events = %v, want append", log.events)
	}
	if submitter.request.Stream != record.Stream || submitter.request.ThroughSeq != record.Seq {
		t.Fatalf("submitted request = %#v, want stream %#v through %d", submitter.request, record.Stream, record.Seq)
	}
}

func newRecord(t *testing.T, seq uint64) wal.Record {
	t.Helper()
	record, err := wal.NewRecord(wal.Stream{Namespace: "world", StreamKey: "1001/7", Generation: 1}, seq, 1, []byte("delta"))
	if err != nil {
		t.Fatalf("NewRecord returned error: %v", err)
	}
	return record
}

type recordingStore struct {
	events    []string
	appendErr error
}

func (store *recordingStore) Append(context.Context, wal.Record) error {
	store.events = append(store.events, "append")
	return store.appendErr
}

func (*recordingStore) Read(context.Context, wal.Stream, uint64, int) ([]wal.Record, error) {
	return nil, nil
}

func (*recordingStore) Replay(context.Context, wal.Stream, uint64, func(context.Context, wal.Record) error) (uint64, error) {
	return 0, nil
}

func (store *recordingStore) Checkpoint(ctx context.Context, _ wal.Stream, _ uint64, persist func(context.Context) error) error {
	store.events = append(store.events, "checkpoint")
	return persist(ctx)
}

type recordingSubmitter struct {
	request walcheckpoint.Request
}

func (submitter *recordingSubmitter) Submit(request walcheckpoint.Request) error {
	submitter.request = request
	return nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
