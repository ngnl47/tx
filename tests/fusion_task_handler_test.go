package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
	"github.com/ngnl47/tx/storage/task"
)

type recordingDocumentReplacer struct {
	calls int
	spec  meta.TypeSpec
	key   storage.RecordKey
	doc   map[string]any
}

type recordingRevisionReader struct {
	calls    int
	revision string
	spec     meta.TypeSpec
	key      storage.RecordKey
}

type recordingRevisionDeleteApplier struct {
	calls   int
	deleted bool
	current string
	spec    meta.TypeSpec
	key     storage.RecordKey
	expect  string
}

func (r *recordingDocumentReplacer) ReplaceDocument(_ context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any) error {
	r.calls++
	r.spec = spec
	r.key = key
	r.doc = doc
	return nil
}

func (r *recordingRevisionReader) ReadRecordRevision(_ context.Context, spec meta.TypeSpec, key storage.RecordKey) (string, error) {
	r.calls++
	r.spec = spec
	r.key = key
	return r.revision, nil
}

func (a *recordingRevisionDeleteApplier) ApplyDeleteWithRevision(_ context.Context, spec meta.TypeSpec, key storage.RecordKey, expectedRevision string) (bool, string, error) {
	a.calls++
	a.spec = spec
	a.key = key
	a.expect = expectedRevision
	return a.deleted, a.current, nil
}

func TestFusionDelayTaskHandlerSavesRedisDocumentToMongo(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{
		Type:  meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}},
		Mongo: &meta.MongoSpec{Collection: "roles"},
	}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	redis := &recordingDocumentLoader{
		doc: map[string]any{
			"roleId":  int64(1001),
			"name":    "alice",
			"headImg": nil,
			"items": map[string]any{
				"1001": map[string]any{
					"quantity": int64(3),
					"quality":  nil,
				},
			},
		},
		exists: true,
	}
	mongo := &recordingDocumentReplacer{}
	handler := fusion.NewDelayTaskHandler(registry, redis, mongo)

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:     "data.GDRole",
		ID:       int64(1001),
		ShardKey: int64(7),
		Action:   string(fusion.DelayedSave),
	})
	if err != nil {
		t.Fatalf("HandleDelayTask returned error: %v", err)
	}
	if redis.calls != 1 || redis.key.ID != int64(1001) || redis.key.ShardKey != int64(7) {
		t.Fatalf("redis call key = %#v calls=%d", redis.key, redis.calls)
	}
	if mongo.calls != 1 || mongo.key.ID != int64(1001) || mongo.key.ShardKey != int64(7) {
		t.Fatalf("mongo call key = %#v calls=%d", mongo.key, mongo.calls)
	}
	if mongo.spec.Mongo.Collection != "roles" || mongo.doc["name"] != "alice" {
		t.Fatalf("mongo spec/doc = %#v %#v", mongo.spec, mongo.doc)
	}
	if _, exists := mongo.doc["headImg"]; exists {
		t.Fatalf("mongo doc kept nil headImg: %#v", mongo.doc)
	}
	items := mongo.doc["items"].(map[string]any)
	item1001 := items["1001"].(map[string]any)
	if _, exists := item1001["quality"]; exists {
		t.Fatalf("mongo doc kept nil nested quality: %#v", mongo.doc)
	}
}

func TestFusionDelayTaskHandlerSkipsMissingRedisDocument(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}}}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	redis := &recordingDocumentLoader{}
	mongo := &recordingDocumentReplacer{}
	handler := fusion.NewDelayTaskHandler(registry, redis, mongo)

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDRole",
		ID:     int64(1001),
		Action: string(fusion.DelayedSave),
	})
	if err != nil {
		t.Fatalf("HandleDelayTask returned error: %v", err)
	}
	if mongo.calls != 0 {
		t.Fatalf("mongo calls = %d, want 0", mongo.calls)
	}
}

func TestFusionDelayTaskHandlerDeletesRedisAfterSavingCurrentDocument(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}},
		Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Minute}, DelayDelete: meta.Duration{Duration: time.Hour}},
	}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	redis := &recordingDocumentLoader{doc: map[string]any{"roleId": int64(1001), "name": "alice"}, exists: true}
	mongo := &recordingDocumentReplacer{}
	revisions := &recordingRevisionReader{revision: "5"}
	deleter := &recordingRevisionDeleteApplier{deleted: true}
	locker := &recordingRecordLocker{}
	queue := &recordingQueue{}
	handler := fusion.NewDelayTaskHandlerWithOptions(registry, redis, mongo, fusion.DelayTaskHandlerOptions{
		RedisRevisionReader: revisions,
		RedisRevisionDelete: deleter,
		RedisLocker:         locker,
		DelayQueue:          queue,
	})

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:     "data.GDRole",
		ID:       int64(1001),
		ShardKey: int64(7),
		Action:   string(fusion.DelayedDelete),
	})
	if err != nil {
		t.Fatalf("HandleDelayTask returned error: %v", err)
	}
	if revisions.calls != 1 || revisions.key.ID != int64(1001) {
		t.Fatalf("revision calls/key = %d/%#v, want one read for role", revisions.calls, revisions.key)
	}
	if locker.calls != 1 || locker.key.ID != int64(1001) {
		t.Fatalf("lock calls/key = %d/%#v, want one record lock", locker.calls, locker.key)
	}
	if mongo.calls != 1 || mongo.doc["name"] != "alice" {
		t.Fatalf("mongo calls/doc = %d/%#v, want save current Redis document", mongo.calls, mongo.doc)
	}
	if deleter.calls != 1 || deleter.expect != "5" {
		t.Fatalf("delete calls/expect = %d/%q, want revision 5", deleter.calls, deleter.expect)
	}
	if len(queue.ops) != 0 {
		t.Fatalf("queued ops = %#v, want no requeue on successful delete", queue.ops)
	}
}

func TestFusionDelayTaskHandlerRequeuesDelayedDeleteWhenRevisionChanged(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}},
		Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Minute}, DelayDelete: meta.Duration{Duration: time.Hour}},
	}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	redis := &recordingDocumentLoader{doc: map[string]any{"roleId": int64(1001), "name": "alice"}, exists: true}
	mongo := &recordingDocumentReplacer{}
	revisions := &recordingRevisionReader{revision: "5"}
	deleter := &recordingRevisionDeleteApplier{deleted: false, current: "6"}
	locker := &recordingRecordLocker{}
	queue := &recordingQueue{}
	handler := fusion.NewDelayTaskHandlerWithOptions(registry, redis, mongo, fusion.DelayTaskHandlerOptions{
		RedisRevisionReader: revisions,
		RedisRevisionDelete: deleter,
		RedisLocker:         locker,
		DelayQueue:          queue,
	})

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:     "data.GDRole",
		ID:       int64(1001),
		ShardKey: int64(7),
		Action:   string(fusion.DelayedDelete),
	})
	if err != nil {
		t.Fatalf("HandleDelayTask returned error: %v", err)
	}
	if deleter.calls != 1 || deleter.expect != "5" {
		t.Fatalf("delete calls/expect = %d/%q, want revision 5", deleter.calls, deleter.expect)
	}
	if len(queue.ops) != 1 {
		t.Fatalf("queued ops = %#v, want one delayed delete requeue", queue.ops)
	}
	got := queue.ops[0]
	if got.Kind != fusion.DelayedDelete || got.Key.ID != int64(1001) || got.Key.ShardKey != int64(7) {
		t.Fatalf("queued op = %#v, want delayed delete for same record", got)
	}
	if got.RunAfter != time.Hour+time.Minute {
		t.Fatalf("queued delay = %s, want delaySave + delayDel", got.RunAfter)
	}
}

func TestFusionDelayTaskHandlerRejectsMissingRecordID(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}}}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	handler := fusion.NewDelayTaskHandler(registry, nil, nil)

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDRole",
		Action: string(fusion.DelayedSave),
	})
	if err == nil {
		t.Fatal("HandleDelayTask returned nil error, want missing record id error")
	}
}
