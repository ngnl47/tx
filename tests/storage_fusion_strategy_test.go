package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	"github.com/ngnl47/tx/storage/fusion"
)

type recordingQueue struct {
	ops []fusion.DelayedOperation
}

func (q *recordingQueue) Enqueue(_ context.Context, op fusion.DelayedOperation) error {
	q.ops = append(q.ops, op)
	return nil
}

type recordingApplier struct {
	calls int
	spec  meta.TypeSpec
	key   storage.RecordKey
	set   change.ChangeSet
}

func (a *recordingApplier) ApplyChanges(_ context.Context, spec meta.TypeSpec, key storage.RecordKey, set change.ChangeSet) error {
	a.calls++
	a.spec = spec
	a.key = key
	a.set = set
	return nil
}

func TestFusionStrategyEnqueuesMongoSave(t *testing.T) {
	queue := &recordingQueue{}
	strategy := fusion.Strategy{MongoQueue: queue}
	spec := meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{
			DelaySave: meta.Duration{Duration: 600 * time.Second},
		},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	if err := strategy.ApplyChanges(context.Background(), spec, int64(1001), set); err != nil {
		t.Fatalf("ApplyChanges returned error: %v", err)
	}
	if len(queue.ops) != 1 {
		t.Fatalf("queued ops = %#v", queue.ops)
	}
	if queue.ops[0].RunAfter != 600*time.Second {
		t.Fatalf("RunAfter = %s", queue.ops[0].RunAfter)
	}
}

func TestFusionStrategyAppliesRedisBeforeQueue(t *testing.T) {
	queue := &recordingQueue{}
	redis := &recordingApplier{}
	strategy := fusion.Strategy{Redis: redis, MongoQueue: queue}
	spec := meta.TypeSpec{
		Type:    meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{DelaySave: meta.Duration{Duration: time.Second}},
	}
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	if err := strategy.ApplyChanges(context.Background(), spec, int64(1001), set); err != nil {
		t.Fatalf("ApplyChanges returned error: %v", err)
	}
	if redis.calls != 1 {
		t.Fatalf("redis calls = %d, want 1", redis.calls)
	}
	if redis.key.ID != int64(1001) {
		t.Fatalf("redis key = %#v", redis.key)
	}
	if len(queue.ops) != 1 {
		t.Fatalf("queued ops = %#v", queue.ops)
	}
}

func TestFusionStrategySkipsEmptyChangeSet(t *testing.T) {
	queue := &recordingQueue{}
	strategy := fusion.Strategy{MongoQueue: queue}

	if err := strategy.ApplyChanges(context.Background(), meta.TypeSpec{}, int64(1001), change.ChangeSet{}); err != nil {
		t.Fatalf("ApplyChanges returned error: %v", err)
	}
	if len(queue.ops) != 0 {
		t.Fatalf("queued ops = %#v, want empty", queue.ops)
	}
}

func TestFusionStrategyEnqueuesMongoDelete(t *testing.T) {
	queue := &recordingQueue{}
	strategy := fusion.Strategy{MongoQueue: queue}
	spec := meta.TypeSpec{
		Type: meta.Type{Ref: meta.TypeRef{Name: "GDRole"}},
		Storage: meta.StorageSpec{
			DelayDelete: meta.Duration{Duration: 24 * time.Hour},
		},
	}

	if err := strategy.ApplyDelete(context.Background(), spec, storage.RecordKey{ID: int64(1001)}); err != nil {
		t.Fatalf("ApplyDelete returned error: %v", err)
	}
	if len(queue.ops) != 1 {
		t.Fatalf("queued ops = %#v", queue.ops)
	}
	if queue.ops[0].Kind != fusion.DelayedDelete || queue.ops[0].RunAfter != 24*time.Hour {
		t.Fatalf("queued op = %#v", queue.ops[0])
	}
}
