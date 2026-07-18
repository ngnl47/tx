package tests

import (
	"context"
	"testing"

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

func (r *recordingDocumentReplacer) ReplaceDocument(_ context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any) error {
	r.calls++
	r.spec = spec
	r.key = key
	r.doc = doc
	return nil
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
	redis := &recordingDocumentLoader{doc: map[string]any{"roleId": int64(1001), "name": "alice"}, exists: true}
	mongo := &recordingDocumentReplacer{}
	handler := fusion.NewDelayTaskHandler(registry, redis, mongo, nil)

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
}

func TestFusionDelayTaskHandlerSkipsMissingRedisDocument(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}}}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	redis := &recordingDocumentLoader{}
	mongo := &recordingDocumentReplacer{}
	handler := fusion.NewDelayTaskHandler(registry, redis, mongo, nil)

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

func TestFusionDelayTaskHandlerDeletesMongoDocument(t *testing.T) {
	registry := meta.NewRegistry()
	spec := meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{Package: "data", Name: "GDRole"}}}
	if err := registry.RegisterType(spec); err != nil {
		t.Fatalf("RegisterType returned error: %v", err)
	}
	mongoDelete := &recordingDeleteApplier{}
	handler := fusion.NewDelayTaskHandler(registry, nil, nil, mongoDelete)

	err := handler.HandleDelayTask(context.Background(), task.DelayTask{
		Type:     "data.GDRole",
		ID:       int64(1001),
		ShardKey: int64(7),
		Action:   string(fusion.DelayedDelete),
	})
	if err != nil {
		t.Fatalf("HandleDelayTask returned error: %v", err)
	}
	if mongoDelete.calls != 1 || mongoDelete.key.ID != int64(1001) || mongoDelete.key.ShardKey != int64(7) {
		t.Fatalf("delete key = %#v calls=%d", mongoDelete.key, mongoDelete.calls)
	}
}
