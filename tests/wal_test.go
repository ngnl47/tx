package tests

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/wal"
)

type checkpointBackend struct {
	trimCalls atomic.Int64
}

func (*checkpointBackend) Append(context.Context, wal.Record) error { return nil }
func (*checkpointBackend) Read(context.Context, wal.Stream, uint64, int) ([]wal.Record, error) {
	return nil, nil
}
func (backend *checkpointBackend) Trim(context.Context, wal.Stream, uint64) error {
	backend.trimCalls.Add(1)
	return nil
}

func TestWALNewRecordBuildsValidatedImmutableIdentity(t *testing.T) {
	stream := wal.Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 3}
	payload := []byte("frame-101")

	record, err := wal.NewRecord(stream, 101, 1, payload)
	if err != nil {
		t.Fatalf("NewRecord returned error: %v", err)
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("Record.Validate returned error: %v", err)
	}
	if record.CreatedAt.IsZero() {
		t.Fatal("CreatedAt is zero")
	}
	if !bytes.Equal(record.Payload, payload) {
		t.Fatalf("Payload = %q, want %q", record.Payload, payload)
	}
	if len(record.Payload) > 0 && &record.Payload[0] != &payload[0] {
		t.Fatal("NewRecord copied payload before the synchronous append boundary")
	}
}

func TestWALRecordRejectsInvalidIdentityAndPayload(t *testing.T) {
	valid, err := wal.NewRecord(wal.Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}, 1, 1, []byte("frame"))
	if err != nil {
		t.Fatalf("NewRecord returned error: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*wal.Record)
	}{
		{name: "blank namespace", mutate: func(record *wal.Record) { record.Stream.Namespace = " " }},
		{name: "blank stream key", mutate: func(record *wal.Record) { record.Stream.StreamKey = "" }},
		{name: "zero generation", mutate: func(record *wal.Record) { record.Stream.Generation = 0 }},
		{name: "generation overflow", mutate: func(record *wal.Record) { record.Stream.Generation = math.MaxUint64 }},
		{name: "zero sequence", mutate: func(record *wal.Record) { record.Seq = 0 }},
		{name: "sequence overflow", mutate: func(record *wal.Record) { record.Seq = math.MaxUint64 }},
		{name: "zero payload version", mutate: func(record *wal.Record) { record.PayloadVersion = 0 }},
		{name: "empty payload", mutate: func(record *wal.Record) { record.Payload = nil }},
		{name: "payload too large", mutate: func(record *wal.Record) { record.Payload = make([]byte, wal.MaxPayloadBytes+1) }},
		{name: "changed payload", mutate: func(record *wal.Record) { record.Payload[0] ^= 0xff }},
		{name: "zero created time", mutate: func(record *wal.Record) { record.CreatedAt = time.Time{} }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := valid
			record.Payload = append([]byte(nil), valid.Payload...)
			tt.mutate(&record)
			if err := record.Validate(); !errors.Is(err, wal.ErrInvalidRecord) {
				t.Fatalf("Validate error = %v, want ErrInvalidRecord", err)
			}
		})
	}
}

func TestWALValidateSequenceRejectsRecoveryGaps(t *testing.T) {
	stream := wal.Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}
	first, err := wal.NewRecord(stream, 41, 1, []byte("first"))
	if err != nil {
		t.Fatalf("NewRecord first: %v", err)
	}
	third, err := wal.NewRecord(stream, 43, 1, []byte("third"))
	if err != nil {
		t.Fatalf("NewRecord third: %v", err)
	}

	if err := wal.ValidateSequence(stream, 40, []wal.Record{first, third}); !errors.Is(err, wal.ErrSequenceGap) {
		t.Fatalf("ValidateSequence error = %v, want ErrSequenceGap", err)
	}
}

func TestWALCheckpointOnlyTrimsAfterCheckpointSucceeds(t *testing.T) {
	backend := &checkpointBackend{}
	store, err := wal.NewStore(backend)
	if err != nil {
		t.Fatalf("NewStore returned error: %v", err)
	}
	stream := wal.Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}
	checkpointErr := errors.New("checkpoint failed")
	if err := store.Checkpoint(context.Background(), stream, 10, func(context.Context) error {
		return checkpointErr
	}); !errors.Is(err, checkpointErr) {
		t.Fatalf("Checkpoint error = %v, want checkpoint failure", err)
	}
	if backend.trimCalls.Load() != 0 {
		t.Fatalf("trim calls after checkpoint failure = %d, want 0", backend.trimCalls.Load())
	}
	if err := store.Checkpoint(context.Background(), stream, 10, func(context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}
	if backend.trimCalls.Load() != 1 {
		t.Fatalf("trim calls after checkpoint success = %d, want 1", backend.trimCalls.Load())
	}
}

func TestWALReadRejectsOversizedBatch(t *testing.T) {
	backend := &checkpointBackend{}
	store, err := wal.NewStore(backend)
	if err != nil {
		t.Fatalf("NewStore returned error: %v", err)
	}
	_, err = store.Read(context.Background(), wal.Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}, 0, wal.MaxReadBatch+1)
	if !errors.Is(err, wal.ErrInvalidRecord) {
		t.Fatalf("Read error = %v, want ErrInvalidRecord", err)
	}
}

func TestWALValidateSequenceRejectsOversizedPayloadBatch(t *testing.T) {
	stream := wal.Stream{Namespace: "slgworld", StreamKey: "world:47", Generation: 1}
	payload := make([]byte, wal.MaxPayloadBytes)
	record, err := wal.NewRecord(stream, 1, 1, payload)
	if err != nil {
		t.Fatalf("NewRecord returned error: %v", err)
	}
	recordCount := wal.MaxReadBatchBytes/wal.MaxPayloadBytes + 1
	records := make([]wal.Record, recordCount)
	for index := range records {
		records[index] = record
		records[index].Seq = uint64(index + 1)
	}
	if err := wal.ValidateSequence(stream, 0, records); !errors.Is(err, wal.ErrInvalidRecord) {
		t.Fatalf("ValidateSequence error = %v, want ErrInvalidRecord", err)
	}
}
