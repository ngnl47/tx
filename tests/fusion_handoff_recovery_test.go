package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
	txredis "github.com/ngnl47/tx/storage/redis"
	txgoredis "github.com/ngnl47/tx/storage/redis/goredis"
	redisclient "github.com/redis/go-redis/v9"
)

type recordingHandoffStore struct {
	entries      []txredis.HandoffEntry
	next         uint64
	readErr      error
	readResults  []handoffReadResult
	readCalls    int
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

type handoffReadResult struct {
	entries []txredis.HandoffEntry
	next    uint64
	err     error
}

type notifyingHandoffStore struct {
	readCalls atomic.Int32
	confirmed chan txredis.HandoffReceipt
}

func (s *notifyingHandoffStore) ReadHandoffs(context.Context, uint64, int, int) ([]txredis.HandoffEntry, uint64, error) {
	s.readCalls.Add(1)
	return nil, 0, nil
}

func (*notifyingHandoffStore) ReadRecordRevision(context.Context, meta.TypeSpec, storage.RecordKey) (string, error) {
	return "18", nil
}

func (s *notifyingHandoffStore) ConfirmHandoff(_ context.Context, _ meta.TypeSpec, _ storage.RecordKey, _ string, receipt txredis.HandoffReceipt) (bool, error) {
	s.confirmed <- receipt
	return true, nil
}

type notifyingDelayQueue struct {
	operations chan fusion.DelayedOperation
}

func (q notifyingDelayQueue) Enqueue(_ context.Context, operation fusion.DelayedOperation) error {
	q.operations <- operation
	return nil
}

func (s *recordingHandoffStore) ReadHandoffs(_ context.Context, cursor uint64, slots int, entries int) ([]txredis.HandoffEntry, uint64, error) {
	s.readCalls++
	s.readCursor = cursor
	s.readSlots = slots
	s.readEntries = entries
	if len(s.readResults) > 0 {
		result := s.readResults[0]
		s.readResults = s.readResults[1:]
		return append([]txredis.HandoffEntry(nil), result.entries...), result.next, result.err
	}
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
		next:      0,
		revision:  "18",
		confirmed: true,
	}
	queue := &recordingQueue{}
	recoverer := fusion.NewHandoffRecoverer(handoffTypeRegistry(t), store, queue)

	count, err := recoverer.RecoverStartup(context.Background())
	if err != nil {
		t.Fatalf("RecoverStartup returned error: %v", err)
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

	count, err := recoverer.RecoverStartup(context.Background())
	if count != 0 || !errors.Is(err, queueErr) {
		t.Fatalf("RecoverStartup count=%d error=%v, want queue error", count, err)
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

	count, err := recoverer.RecoverStartup(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("RecoverStartup count=%d error=%v, want stale marker retained without failure", count, err)
	}
}

type recordingWorkerContext struct {
	context.Context
	goCalls int
}

func (c *recordingWorkerContext) Go(func(context.Context)) error {
	c.goCalls++
	return nil
}

func TestHandoffRecovererStartCompletesStartupRecoveryBeforeRegisteringWorker(t *testing.T) {
	store := &recordingHandoffStore{
		readResults: []handoffReadResult{
			{entries: []txredis.HandoffEntry{{Marker: `{"type":"data.GDRole","id":1001}`, Token: "token-18"}}, next: 256},
			{next: 0},
		},
		revision:  "18",
		confirmed: true,
	}
	queue := &recordingQueue{}
	recoverer := fusion.NewHandoffRecoverer(handoffTypeRegistry(t), store, queue)
	ctx := &recordingWorkerContext{Context: context.Background()}

	if err := recoverer.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if store.readCalls != 2 || len(queue.ops) != 2 || len(store.receipts) != 1 {
		t.Fatalf("reads=%d queue=%#v receipts=%#v, want complete startup recovery", store.readCalls, queue.ops, store.receipts)
	}
	if ctx.goCalls != 1 {
		t.Fatalf("Go calls = %d, want worker registered after recovery", ctx.goCalls)
	}
}

func TestHandoffRecovererStartFailsBeforeWorkerWhenStartupRecoveryFails(t *testing.T) {
	recoveryErr := errors.New("handoff recovery unavailable")
	store := &recordingHandoffStore{readErr: recoveryErr}
	recoverer := fusion.NewHandoffRecoverer(handoffTypeRegistry(t), store, &recordingQueue{})
	ctx := &recordingWorkerContext{Context: context.Background()}

	err := recoverer.Start(ctx)
	if !errors.Is(err, recoveryErr) {
		t.Fatalf("Start error = %v, want startup recovery error", err)
	}
	if ctx.goCalls != 0 {
		t.Fatalf("Go calls = %d, want worker not registered", ctx.goCalls)
	}
}

func TestHandoffRecovererRetriesOnlyQueuedMarkers(t *testing.T) {
	store := &recordingHandoffStore{revision: "18", confirmed: true}
	queue := &recordingQueue{}
	recoverer := fusion.NewHandoffRecoverer(handoffTypeRegistry(t), store, queue)
	entry := txredis.HandoffEntry{Marker: `{"type":"data.GDRole","id":1001}`, Token: "token-18"}

	if err := recoverer.QueueHandoffRetry(entry); err != nil {
		t.Fatalf("QueueHandoffRetry returned error: %v", err)
	}
	count, err := recoverer.RunPendingOnce(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("RunPendingOnce count=%d error=%v, want one targeted retry", count, err)
	}
	if store.readCalls != 0 {
		t.Fatalf("handoff slot reads = %d, want no runtime traversal", store.readCalls)
	}
	if len(queue.ops) != 2 || len(store.receipts) != 1 || store.receipts[0].Token != "token-18" {
		t.Fatalf("queue=%#v receipts=%#v, want targeted save/delete recovery", queue.ops, store.receipts)
	}
	if count, err = recoverer.RunPendingOnce(context.Background()); err != nil || count != 0 {
		t.Fatalf("second RunPendingOnce count=%d error=%v, want empty", count, err)
	}
}

func TestHandoffRecovererKeepsLatestQueuedToken(t *testing.T) {
	store := &recordingHandoffStore{revision: "19", confirmed: true}
	recoverer := fusion.NewHandoffRecoverer(handoffTypeRegistry(t), store, &recordingQueue{})
	marker := `{"type":"data.GDRole","id":1001}`
	if err := recoverer.QueueHandoffRetry(txredis.HandoffEntry{Marker: marker, Token: "token-18"}); err != nil {
		t.Fatalf("Queue old handoff returned error: %v", err)
	}
	if err := recoverer.QueueHandoffRetry(txredis.HandoffEntry{Marker: marker, Token: "token-19"}); err != nil {
		t.Fatalf("Queue new handoff returned error: %v", err)
	}

	count, err := recoverer.RunPendingOnce(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("RunPendingOnce count=%d error=%v, want latest marker", count, err)
	}
	if len(store.receipts) != 1 || store.receipts[0].Token != "token-19" {
		t.Fatalf("receipts = %#v, want only latest token", store.receipts)
	}
}

func TestHandoffRecovererRetainsFailedQueuedMarker(t *testing.T) {
	queueErr := errors.New("queue unavailable")
	store := &recordingHandoffStore{revision: "18", confirmed: true}
	queue := &recordingQueue{err: queueErr}
	recoverer := fusion.NewHandoffRecoverer(handoffTypeRegistry(t), store, queue)
	entry := txredis.HandoffEntry{Marker: `{"type":"data.GDRole","id":1001}`, Token: "token-18"}
	if err := recoverer.QueueHandoffRetry(entry); err != nil {
		t.Fatalf("QueueHandoffRetry returned error: %v", err)
	}

	if count, err := recoverer.RunPendingOnce(context.Background()); count != 0 || !errors.Is(err, queueErr) {
		t.Fatalf("first RunPendingOnce count=%d error=%v, want retained failure", count, err)
	}
	queue.err = nil
	if count, err := recoverer.RunPendingOnce(context.Background()); count != 1 || err != nil {
		t.Fatalf("second RunPendingOnce count=%d error=%v, want successful retry", count, err)
	}
}

func TestHandoffRecovererWorkerConsumesQueuedMarkerWithoutRuntimeTraversal(t *testing.T) {
	store := &notifyingHandoffStore{confirmed: make(chan txredis.HandoffReceipt, 1)}
	queue := notifyingDelayQueue{operations: make(chan fusion.DelayedOperation, 2)}
	recoverer := fusion.NewHandoffRecoverer(handoffTypeRegistry(t), store, queue)
	root := lifecycle.NewWaitGroupContext(context.Background())
	if err := recoverer.Start(root); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	t.Cleanup(func() {
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	})
	entry := txredis.HandoffEntry{Marker: `{"type":"data.GDRole","id":1001}`, Token: "token-18"}
	if err := recoverer.QueueHandoffRetry(entry); err != nil {
		t.Fatalf("QueueHandoffRetry returned error: %v", err)
	}

	select {
	case receipt := <-store.confirmed:
		if receipt.Token != entry.Token || receipt.Revision != "18" {
			t.Fatalf("confirmed receipt = %#v, want revision 18 and token-18", receipt)
		}
	case <-time.After(time.Second):
		t.Fatal("targeted handoff worker did not confirm queued marker")
	}
	if len(queue.operations) != 2 {
		t.Fatalf("queued operations = %d, want save and delete", len(queue.operations))
	}
	if store.readCalls.Load() != 1 {
		t.Fatalf("handoff slot reads = %d, want startup traversal only", store.readCalls.Load())
	}
}

func TestHandoffRecovererStartupRecoveryIntegration(t *testing.T) {
	endpoint := os.Getenv("TX_REDIS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_REDIS_ENDPOINT to run Fusion handoff recovery integration test")
	}
	ctx := context.Background()
	raw := redisclient.NewClient(&redisclient.Options{Addr: endpoint, DB: 14})
	defer raw.Close()
	if err := raw.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis %q: %v", endpoint, err)
	}
	adapter := mustRedisAdapter(t, txgoredis.NewClient(raw))
	spec := meta.TypeSpec{
		Type: meta.Type{
			Ref:    meta.TypeRef{Package: "data", Name: "GDRole", FullName: "data.GDRole"},
			Fields: []meta.Field{{Name: "Name", JSONName: "name"}},
		},
		Storage: meta.StorageSpec{
			Mode:        meta.StorageModeFusion,
			DelaySave:   meta.Duration{Duration: time.Minute},
			DelayDelete: meta.Duration{Duration: time.Hour},
		},
		Redis: &meta.RedisSpec{Key: fmt.Sprintf("tx:test:handoff:%d", time.Now().UnixNano())},
	}
	key := storage.RecordKey{TypeName: spec.Type.Ref.FullName, ID: int64(1001)}
	marker, err := txredis.EncodeHandoffMarker(spec.Type.Ref, key)
	if err != nil {
		t.Fatalf("EncodeHandoffMarker returned error: %v", err)
	}
	changes := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	receipt, err := adapter.ApplyChangesWithHandoff(
		ctx,
		spec,
		key,
		changes,
		testResolverForKind(change.FieldNameJSON, changeTestJSONNames()),
		testChangeValues(map[string]any{"Name": "alice"}),
		storage.ApplyOptions{Upsert: true, TTLSeconds: -1},
		txredis.HandoffOptions{Marker: marker, MaxPendingPerSlot: 1024},
	)
	if err != nil {
		t.Fatalf("ApplyChangesWithHandoff returned error: %v", err)
	}
	t.Cleanup(func() {
		_, _, _ = adapter.ApplyDeleteWithRevision(context.Background(), spec, key, receipt.Revision)
	})

	registry := meta.NewRegistry()
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	registry.Freeze()
	queue := &recordingQueue{}
	recoverer := fusion.NewHandoffRecovererWithOptions(registry, adapter, queue, fusion.HandoffRecovererOptions{
		SlotsPerRun:    256,
		EntriesPerSlot: 1024,
	})
	count, err := recoverer.RecoverStartup(ctx)
	if err != nil {
		t.Fatalf("RecoverStartup returned error: %v", err)
	}
	if count != 1 || len(queue.ops) != 2 {
		t.Fatalf("recovered=%d queue=%#v, want one marker and save/delete tasks", count, queue.ops)
	}
	confirmed, err := adapter.ConfirmHandoff(ctx, spec, key, marker, receipt)
	if err != nil {
		t.Fatalf("ConfirmHandoff after recovery returned error: %v", err)
	}
	if confirmed {
		t.Fatal("ConfirmHandoff after recovery = true, want marker already removed")
	}
}
