package tests

import (
	"context"
	"testing"

	"github.com/ngnl47/gobricks/storage"
	"github.com/ngnl47/gobricks/storage/fusion"
)

func TestFusionAdapterWritesPrimaryThenSecondary(t *testing.T) {
	ctx := context.Background()
	redis := &recordingAdapter{}
	mongo := &recordingAdapter{}
	adapter := fusion.NewAdapter(redis, mongo)

	err := adapter.Save(ctx, storage.Record{
		TypeName: "example.com/project/data.Role",
		Values:   map[string]any{"roleId": int64(1)},
	})
	if err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	if redis.saved != 1 || mongo.saved != 1 {
		t.Fatalf("saved counts redis=%d mongo=%d", redis.saved, mongo.saved)
	}
}

func TestAdapterFuncImplementsStoreAdapter(t *testing.T) {
	ctx := context.Background()
	var saved storage.Record
	adapter := storage.AdapterFuncs{
		SaveFunc: func(ctx context.Context, record storage.Record) error {
			saved = record
			return nil
		},
		LoadFunc: func(ctx context.Context, key storage.RecordKey) (storage.Record, error) {
			return storage.Record{TypeName: key.TypeName, Values: map[string]any{"id": key.ID}}, nil
		},
		DeleteFunc: func(ctx context.Context, key storage.RecordKey) error {
			return nil
		},
	}

	err := adapter.Save(ctx, storage.Record{TypeName: "role"})
	if err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	if saved.TypeName != "role" {
		t.Fatalf("saved = %#v", saved)
	}
	record, err := adapter.Load(ctx, storage.RecordKey{TypeName: "role", ID: "1"})
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if record.Values["id"] != "1" {
		t.Fatalf("record = %#v", record)
	}
}

type recordingAdapter struct {
	saved int
}

func (a *recordingAdapter) Save(ctx context.Context, record storage.Record) error {
	a.saved++
	return nil
}

func (a *recordingAdapter) Load(ctx context.Context, key storage.RecordKey) (storage.Record, error) {
	return storage.Record{TypeName: key.TypeName}, nil
}

func (a *recordingAdapter) Delete(ctx context.Context, key storage.RecordKey) error {
	return nil
}
