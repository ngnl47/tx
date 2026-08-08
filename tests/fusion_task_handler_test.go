package tests

import (
	"context"
	"errors"
	"strings"
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
	lock := &recordingRecordLock{}
	locker := &recordingRecordLocker{lock: lock}
	handler := fusion.NewDelayTaskHandler(registry, redis, mongo, locker)

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
	if locker.calls != 1 {
		t.Fatalf("redis lock calls = %d, want 1", locker.calls)
	}
	if lock.unlocks != 1 {
		t.Fatalf("redis lock unlocks = %d, want 1", lock.unlocks)
	}
	if mongo.spec.Mongo.Collection != "roles" || mongo.doc["name"] != "alice" {
		t.Fatalf("mongo spec/doc = %#v %#v", mongo.spec, mongo.doc)
	}
	if value, exists := mongo.doc["headImg"]; !exists || value != nil {
		t.Fatalf("mongo doc headImg = %#v, want explicit nil", mongo.doc)
	}
	items := mongo.doc["items"].(map[string]any)
	item1001 := items["1001"].(map[string]any)
	if value, exists := item1001["quality"]; !exists || value != nil {
		t.Fatalf("mongo doc nested quality = %#v, want explicit nil", mongo.doc)
	}
}

func TestFusionDelayTaskHandlerRejectsSaveWithoutRecordLockBeforeRead(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}}}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	redis := &recordingDocumentLoader{doc: map[string]any{"name": "alice"}, exists: true}
	mongo := &recordingDocumentReplacer{}
	handler := fusion.NewDelayTaskHandler(registry, redis, mongo, nil)

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDRole",
		ID:     int64(1001),
		Action: string(fusion.DelayedSave),
	})
	if !errors.Is(err, fusion.ErrDelayTaskDependenciesNotConfigured) {
		t.Fatalf("HandleDelayTask error = %v, want missing record lock error", err)
	}
	if redis.calls != 0 || mongo.calls != 0 {
		t.Fatalf("redis/mongo calls = %d/%d, want 0/0", redis.calls, mongo.calls)
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
	handler := fusion.NewDelayTaskHandler(registry, redis, mongo, &recordingRecordLocker{})

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

func TestFusionDelayTaskHandlerRejectsInconsistentRedisDocument(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}}}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	redis := &recordingDocumentLoader{doc: map[string]any{"name": "unexpected"}}
	mongo := &recordingDocumentReplacer{}
	handler := fusion.NewDelayTaskHandler(registry, redis, mongo, &recordingRecordLocker{})

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDRole",
		ID:     int64(1001),
		Action: string(fusion.DelayedSave),
	})
	if err == nil || !strings.Contains(err.Error(), "missing document with data") {
		t.Fatalf("HandleDelayTask error = %v, want inconsistent document error", err)
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

func TestFusionDelayTaskHandlerReturnsRecordUnlockError(t *testing.T) {
	unlockErr := errors.New("unlock failed")
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}},
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	handler := fusion.NewDelayTaskHandlerWithOptions(
		registry,
		&recordingDocumentLoader{doc: map[string]any{"roleId": int64(1001)}, exists: true},
		&recordingDocumentReplacer{},
		fusion.DelayTaskHandlerOptions{
			RedisRevisionReader: &recordingRevisionReader{revision: "5"},
			RedisRevisionDelete: &recordingRevisionDeleteApplier{deleted: true},
			RedisLocker:         &recordingRecordLocker{lock: &recordingRecordLock{unlockErr: unlockErr}},
		},
	)

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDRole",
		ID:     int64(1001),
		Action: string(fusion.DelayedDelete),
	})
	if !errors.Is(err, unlockErr) {
		t.Fatalf("HandleDelayTask error = %v, want unlock error", err)
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

func TestFusionDelayTaskHandlerRejectsMissingQueueWhenDeleteMustRequeue(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}},
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: time.Hour}},
	}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	handler := fusion.NewDelayTaskHandlerWithOptions(
		registry,
		&recordingDocumentLoader{doc: map[string]any{"roleId": int64(1001)}, exists: true},
		&recordingDocumentReplacer{},
		fusion.DelayTaskHandlerOptions{
			RedisRevisionReader: &recordingRevisionReader{revision: "5"},
			RedisRevisionDelete: &recordingRevisionDeleteApplier{deleted: false, current: "6"},
			RedisLocker:         &recordingRecordLocker{},
		},
	)

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDRole",
		ID:     int64(1001),
		Action: string(fusion.DelayedDelete),
	})
	if !errors.Is(err, fusion.ErrDelayTaskDependenciesNotConfigured) {
		t.Fatalf("HandleDelayTask error = %v, want missing delay queue error", err)
	}
}

func TestFusionDelayTaskHandlerRejectsMissingRecordID(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}}}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	handler := fusion.NewDelayTaskHandler(registry, nil, nil, nil)

	var typedNil *int64
	for _, item := range []task.DelayTask{
		{Type: "data.GDRole", Action: string(fusion.DelayedSave)},
		{Type: "data.GDRole", ID: typedNil, Action: string(fusion.DelayedSave)},
		{Type: "data.GDRole", ID: "   ", Action: string(fusion.DelayedSave)},
		{ID: int64(1001), Action: string(fusion.DelayedSave)},
		{Type: "data.GDRole", ID: int64(1001)},
	} {
		err := handler.HandleDelayTask(context.Background(), item)
		if !errors.Is(err, task.ErrInvalidDelayTask) {
			t.Fatalf("HandleDelayTask(%#v) error = %v, want ErrInvalidDelayTask", item, err)
		}
	}
}

func TestFusionDelayTaskHandlerRejectsMissingSaveDependencies(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}}}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	handler := fusion.NewDelayTaskHandler(registry, nil, nil, nil)

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDRole",
		ID:     int64(1001),
		Action: string(fusion.DelayedSave),
	})
	if !errors.Is(err, fusion.ErrDelayTaskDependenciesNotConfigured) {
		t.Fatalf("HandleDelayTask error = %v, want ErrDelayTaskDependenciesNotConfigured", err)
	}
}

func TestFusionDelayTaskHandlerRejectsTypedNilDependencies(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}}}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	var redis *recordingDocumentLoader
	var mongo *recordingDocumentReplacer
	handler := fusion.NewDelayTaskHandler(registry, redis, mongo, &recordingRecordLocker{})

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDRole",
		ID:     int64(1001),
		Action: string(fusion.DelayedSave),
	})
	if !errors.Is(err, fusion.ErrDelayTaskDependenciesNotConfigured) {
		t.Fatalf("typed nil dependencies error = %v, want ErrDelayTaskDependenciesNotConfigured", err)
	}
}

func TestFusionDelayTaskHandlerRejectsTypedNilRegistry(t *testing.T) {
	var registry *meta.Registry
	handler := fusion.NewDelayTaskHandler(registry, &recordingDocumentLoader{}, &recordingDocumentReplacer{}, &recordingRecordLocker{})

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDRole",
		ID:     int64(1001),
		Action: string(fusion.DelayedSave),
	})
	if !errors.Is(err, fusion.ErrDelayTaskDependenciesNotConfigured) {
		t.Fatalf("typed nil registry error = %v, want ErrDelayTaskDependenciesNotConfigured", err)
	}
}
