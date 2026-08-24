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

type recordingRevisionTTLApplier struct {
	calls   int
	applied bool
	current string
	spec    meta.TypeSpec
	key     storage.RecordKey
	expect  string
	ttl     time.Duration
}

type cancelingDocumentLoader struct {
	cancel context.CancelFunc
}

type contextRecordingRecordLock struct {
	unlockContextErr error
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

func (a *recordingRevisionTTLApplier) ApplyTTLWithRevision(_ context.Context, spec meta.TypeSpec, key storage.RecordKey, expectedRevision string, ttl time.Duration) (bool, string, error) {
	a.calls++
	a.spec = spec
	a.key = key
	a.expect = expectedRevision
	a.ttl = ttl
	return a.applied, a.current, nil
}

func (l *cancelingDocumentLoader) LoadDocument(context.Context, meta.TypeSpec, storage.RecordKey, ...string) (map[string]any, bool, error) {
	l.cancel()
	return nil, false, context.Canceled
}

func (l *contextRecordingRecordLock) Unlock(ctx context.Context) error {
	l.unlockContextErr = ctx.Err()
	return l.unlockContextErr
}

func newRecordingDelayTaskHandler(types fusion.TypeRegistry, redis storage.DocumentLoader, mongo storage.DocumentReplacer, locker storage.RecordLocker) *fusion.StorageTaskHandler {
	return fusion.NewDelayTaskHandlerWithOptions(types, redis, mongo, fusion.DelayTaskHandlerOptions{
		RedisRevisionReader: &recordingRevisionReader{revision: "1"},
		RedisRevisionTTL:    &recordingRevisionTTLApplier{applied: true},
		RedisLocker:         locker,
	})
}

func TestFusionDelayTaskHandlerSavesRedisDocumentToMongo(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}},
		Storage: meta.StorageSpec{DelayDelete: meta.Duration{Duration: 24 * time.Hour}},
		Mongo:   &meta.MongoSpec{Collection: "roles"},
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
	revision := &recordingRevisionReader{revision: "5"}
	ttl := &recordingRevisionTTLApplier{applied: true}
	handler := fusion.NewDelayTaskHandlerWithOptions(registry, redis, mongo, fusion.DelayTaskHandlerOptions{
		RedisRevisionReader: revision,
		RedisRevisionTTL:    ttl,
		RedisLocker:         locker,
	})

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:     "data.GDRole",
		ID:       int64(1001),
		ShardKey: int64(7),
		Action:   "save",
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
	if revision.calls != 1 || ttl.calls != 1 || ttl.expect != "5" || ttl.ttl != 24*time.Hour {
		t.Fatalf("revision/TTL calls = %d/%d expected=%q ttl=%s", revision.calls, ttl.calls, ttl.expect, ttl.ttl)
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

func TestFusionDelayTaskHandlerReleasesRecordLockAfterRequestCancellation(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{
		Type:  meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}},
		Mongo: &meta.MongoSpec{Collection: "roles"},
	}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	lock := &contextRecordingRecordLock{}
	handler := fusion.NewDelayTaskHandler(
		registry,
		&cancelingDocumentLoader{cancel: cancel},
		&recordingDocumentReplacer{},
		&recordingRecordLocker{lock: lock},
	)

	err := handler.HandleDelayTask(ctx, task.DelayTask{
		Type:   "data.GDRole",
		ID:     int64(1001),
		Action: "save",
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("HandleDelayTask error = %v, want context.Canceled", err)
	}
	if lock.unlockContextErr != nil {
		t.Fatalf("unlock context error = %v, want cleanup context unaffected by request cancellation", lock.unlockContextErr)
	}
}

func TestFusionDelayTaskHandlerRequeuesSaveWhenRevisionChangesBeforeTTLConverges(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}},
		Storage: meta.StorageSpec{
			DelaySave:   meta.Duration{Duration: 10 * time.Minute},
			DelayDelete: meta.Duration{Duration: 24 * time.Hour},
		},
	}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	queue := &recordingQueue{}
	handler := fusion.NewDelayTaskHandlerWithOptions(
		registry,
		&recordingDocumentLoader{doc: map[string]any{"name": "alice"}, exists: true},
		&recordingDocumentReplacer{},
		fusion.DelayTaskHandlerOptions{
			RedisRevisionReader: &recordingRevisionReader{revision: "5"},
			RedisRevisionTTL:    &recordingRevisionTTLApplier{applied: false, current: "6"},
			RedisLocker:         &recordingRecordLocker{},
			DelayedSaveQueue:    queue,
		},
	)

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDRole",
		ID:     int64(1001),
		Action: "save",
	})
	if err != nil {
		t.Fatalf("HandleDelayTask returned error: %v", err)
	}
	if len(queue.ops) != 1 || queue.ops[0].RunAfter != 10*time.Minute {
		t.Fatalf("requeued operations = %#v, want one delayed save", queue.ops)
	}
}

func TestFusionDelayTaskHandlerKeepsDirtyTTLWhenDelayDeleteIsNotConfigured(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDGlobalConfig"}}}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	ttl := &recordingRevisionTTLApplier{applied: true}
	revision := &recordingRevisionReader{revision: "5"}
	handler := fusion.NewDelayTaskHandlerWithOptions(
		registry,
		&recordingDocumentLoader{doc: map[string]any{"name": "global"}, exists: true},
		&recordingDocumentReplacer{},
		fusion.DelayTaskHandlerOptions{
			RedisRevisionReader: revision,
			RedisRevisionTTL:    ttl,
			RedisLocker:         &recordingRecordLocker{},
		},
	)

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDGlobalConfig",
		ID:     int64(1),
		Action: "save",
	})
	if err != nil {
		t.Fatalf("HandleDelayTask returned error: %v", err)
	}
	if revision.calls != 0 || ttl.calls != 0 {
		t.Fatalf("revision/TTL convergence calls = %d/%d, want dirty TTL unchanged without extra Redis I/O", revision.calls, ttl.calls)
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
		Action: "save",
	})
	if !errors.Is(err, fusion.ErrDelayTaskDependenciesNotConfigured) {
		t.Fatalf("HandleDelayTask error = %v, want missing record lock error", err)
	}
	if redis.calls != 0 || mongo.calls != 0 {
		t.Fatalf("redis/mongo calls = %d/%d, want 0/0", redis.calls, mongo.calls)
	}
}

func TestFusionDelayTaskHandlerRejectsMissingRedisDocument(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}}}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	redis := &recordingDocumentLoader{}
	mongo := &recordingDocumentReplacer{}
	handler := newRecordingDelayTaskHandler(registry, redis, mongo, &recordingRecordLocker{})

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDRole",
		ID:     int64(1001),
		Action: "save",
	})
	if !errors.Is(err, storage.ErrNotExists) {
		t.Fatalf("HandleDelayTask error = %v, want storage.ErrNotExists", err)
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
	handler := newRecordingDelayTaskHandler(registry, redis, mongo, &recordingRecordLocker{})

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDRole",
		ID:     int64(1001),
		Action: "save",
	})
	if err == nil || !strings.Contains(err.Error(), "missing document with data") {
		t.Fatalf("HandleDelayTask error = %v, want inconsistent document error", err)
	}
	if mongo.calls != 0 {
		t.Fatalf("mongo calls = %d, want 0", mongo.calls)
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
			RedisRevisionTTL:    &recordingRevisionTTLApplier{applied: true},
			RedisLocker:         &recordingRecordLocker{lock: &recordingRecordLock{unlockErr: unlockErr}},
		},
	)

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDRole",
		ID:     int64(1001),
		Action: "save",
	})
	if !errors.Is(err, unlockErr) {
		t.Fatalf("HandleDelayTask error = %v, want unlock error", err)
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
		{Type: "data.GDRole", Action: "save"},
		{Type: "data.GDRole", ID: typedNil, Action: "save"},
		{Type: "data.GDRole", ID: "   ", Action: "save"},
		{ID: int64(1001), Action: "save"},
		{Type: "data.GDRole", ID: int64(1001)},
	} {
		err := handler.HandleDelayTask(context.Background(), item)
		if !errors.Is(err, task.ErrInvalidDelayTask) {
			t.Fatalf("HandleDelayTask(%#v) error = %v, want ErrInvalidDelayTask", item, err)
		}
	}
}

func TestFusionDelayTaskHandlerRejectsUnknownActionBeforeStorage(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}}}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	redis := &recordingDocumentLoader{doc: map[string]any{"name": "alice"}, exists: true}
	mongo := &recordingDocumentReplacer{}
	handler := newRecordingDelayTaskHandler(registry, redis, mongo, &recordingRecordLocker{})

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:   "data.GDRole",
		ID:     int64(1001),
		Action: "delete",
	})
	if err == nil || !strings.Contains(err.Error(), "unknown delayed task action") {
		t.Fatalf("HandleDelayTask error = %v, want unknown action error", err)
	}
	if redis.calls != 0 || mongo.calls != 0 {
		t.Fatalf("redis/mongo calls = %d/%d, want 0/0", redis.calls, mongo.calls)
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
		Action: "save",
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
		Action: "save",
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
		Action: "save",
	})
	if !errors.Is(err, fusion.ErrDelayTaskDependenciesNotConfigured) {
		t.Fatalf("typed nil registry error = %v, want ErrDelayTaskDependenciesNotConfigured", err)
	}
}
