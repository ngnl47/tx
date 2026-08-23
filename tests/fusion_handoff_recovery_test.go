package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
	txredis "github.com/ngnl47/tx/storage/redis"
)

type recordingHandoffStore struct {
	entries      []txredis.HandoffEntry
	next         uint64
	readErr      error
	revision     string
	revisionErr  error
	confirmed    bool
	confirmErr   error
	readCursor   uint64
	readSlots    int
	readEntries  int
	revisionKeys []storage.RecordKey
	confirmKeys  []storage.RecordKey
	receipts     []txredis.HandoffReceipt
}

func (s *recordingHandoffStore) ReadHandoffs(_ context.Context, cursor uint64, slots int, entries int) ([]txredis.HandoffEntry, uint64, error) {
	s.readCursor = cursor
	s.readSlots = slots
	s.readEntries = entries
	return append([]txredis.HandoffEntry(nil), s.entries...), s.next, s.readErr
}

func (s *recordingHandoffStore) ReadRecordRevision(_ context.Context, _ meta.TypeSpec, key storage.RecordKey) (string, error) {
	s.revisionKeys = append(s.revisionKeys, key)
	return s.revision, s.revisionErr
}

func (s *recordingHandoffStore) ConfirmHandoff(_ context.Context, _ meta.TypeSpec, key storage.RecordKey, _ string, receipt txredis.HandoffReceipt) (bool, error) {
	s.confirmKeys = append(s.confirmKeys, key)
	s.receipts = append(s.receipts, receipt)
	return s.confirmed, s.confirmErr
}

func handoffTypeRegistry(t *testing.T) *meta.Registry {
	t.Helper()
	registry := meta.NewRegistry()
	err := registry.RegisterType(meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole", FullName: "data.GDRole"}},
		Storage: meta.StorageSpec{
			Mode:        meta.StorageModeFusion,
			DelaySave:   meta.Duration{Duration: time.Minute},
			DelayDelete: meta.Duration{Duration: time.Hour},
		},
	})
	if err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	registry.Freeze()
	return registry
}

func TestHandoffRecovererRebuildsCurrentDelayedTasksAndConfirmsRevision(t *testing.T) {
	store := &recordingHandoffStore{
		entries:   []txredis.HandoffEntry{{Marker: `{"type":"data.GDRole","id":1001,"shardKey":7}`, Token: "token-18"}},
		next:      64,
		revision:  "18",
		confirmed: true,
	}
	queue := &recordingQueue{}
	recoverer := fusion.NewHandoffRecoverer(handoffTypeRegistry(t), store, queue)

	count, err := recoverer.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if count != 1 || len(queue.ops) != 2 {
		t.Fatalf("count=%d queue=%#v, want one marker and save/delete tasks", count, queue.ops)
	}
	if queue.ops[0].Kind != fusion.DelayedSave || queue.ops[0].RunAfter != time.Minute {
		t.Fatalf("save operation = %#v", queue.ops[0])
	}
	if queue.ops[1].Kind != fusion.DelayedDelete || queue.ops[1].RunAfter != time.Hour+time.Minute {
		t.Fatalf("delete operation = %#v", queue.ops[1])
	}
	if len(store.confirmKeys) != 1 || store.confirmKeys[0].ID != int64(1001) || store.confirmKeys[0].ShardKey != int64(7) {
		t.Fatalf("confirm keys = %#v", store.confirmKeys)
	}
	if len(store.receipts) != 1 || store.receipts[0] != (txredis.HandoffReceipt{Revision: "18", Token: "token-18"}) {
		t.Fatalf("confirm receipts = %#v", store.receipts)
	}
	if store.readCursor != 0 || store.readSlots <= 0 || store.readEntries <= 0 {
		t.Fatalf("read cursor=%d slots=%d entries=%d", store.readCursor, store.readSlots, store.readEntries)
	}
}

func TestHandoffRecovererKeepsMarkerWhenQueueEnqueueFails(t *testing.T) {
	queueErr := errors.New("queue unavailable")
	store := &recordingHandoffStore{
		entries:  []txredis.HandoffEntry{{Marker: `{"type":"data.GDRole","id":1001}`, Token: "token-18"}},
		revision: "18",
	}
	recoverer := fusion.NewHandoffRecoverer(handoffTypeRegistry(t), store, &recordingQueue{err: queueErr})

	count, err := recoverer.RunOnce(context.Background())
	if count != 0 || !errors.Is(err, queueErr) {
		t.Fatalf("RunOnce count=%d error=%v, want queue error", count, err)
	}
	if len(store.confirmKeys) != 0 {
		t.Fatalf("confirm keys = %#v, want marker retained", store.confirmKeys)
	}
}

func TestHandoffRecovererDoesNotTreatStaleConfirmationAsFailure(t *testing.T) {
	store := &recordingHandoffStore{
		entries:  []txredis.HandoffEntry{{Marker: `{"type":"data.GDRole","id":1001}`, Token: "token-18"}},
		revision: "18",
	}
	recoverer := fusion.NewHandoffRecoverer(handoffTypeRegistry(t), store, &recordingQueue{})

	count, err := recoverer.RunOnce(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("RunOnce count=%d error=%v, want stale marker retained without failure", count, err)
	}
}
