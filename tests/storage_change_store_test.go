package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

type recordingChangeStoreApplier struct {
	spec       meta.TypeSpec
	key        storage.RecordKey
	changes    recordedChanges
	options    storage.ApplyOptions
	hasOptions bool
	err        error
}

func (a *recordingChangeStoreApplier) ApplyChangesWithOptions(_ context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, _ change.FieldNameResolver, _ change.ValueResolver, options storage.ApplyOptions) error {
	a.spec = spec
	a.key = key
	changes, err := collectRecordedChanges(source)
	if err != nil {
		return err
	}
	a.changes = changes
	a.options = options
	a.hasOptions = true
	return a.err
}

type testChangeStore struct {
	spec       meta.TypeSpec
	key        storage.RecordKey
	changes    recordedChanges
	defaults   storage.ApplyOptions
	allTouched int
	cleared    int
}

func (s *testChangeStore) TypeSpec() meta.TypeSpec      { return s.spec }
func (s *testChangeStore) RecordKey() storage.RecordKey { return s.key }
func (s *testChangeStore) VisitChanges(visitor change.Visitor) error {
	for _, item := range s.changes {
		if err := visitor.VisitChange(item.Path, item.Op); err != nil {
			return err
		}
	}
	return nil
}
func (s *testChangeStore) HasChanges() bool { return len(s.changes) > 0 }
func (s *testChangeStore) FieldNameResolver() change.FieldNameResolver {
	return change.FieldNameResolverFunc(func(field change.FieldID, kind change.FieldNameKind) string { return "" })
}
func (s *testChangeStore) ResolveValue(change.Path) (any, bool)      { return nil, false }
func (s *testChangeStore) DefaultApplyOptions() storage.ApplyOptions { return s.defaults }
func (s *testChangeStore) AllTouch() {
	s.allTouched++
	s.changes = recordedChanges{{Path: testStoragePath(1), Op: change.OpSet}}
}
func (s *testChangeStore) ClearChanges() {
	s.cleared++
	s.changes = nil
}

func TestStorageCreateAppliesWholeStoreAndForcesUpsert(t *testing.T) {
	store := &testChangeStore{
		spec:     meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{FullName: "GDRole"}}},
		key:      storage.RecordKey{ID: int64(1001)},
		defaults: storage.ApplyOptions{TTL: 10 * time.Minute},
	}
	applier := &recordingChangeStoreApplier{}

	if err := storage.Create(context.Background(), applier, store); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if store.allTouched != 1 {
		t.Fatalf("AllTouch calls = %d, want 1", store.allTouched)
	}
	if store.cleared != 1 {
		t.Fatalf("ClearChanges calls = %d, want 1", store.cleared)
	}
	if !applier.hasOptions || !applier.options.Upsert || applier.options.TTL != 10*time.Minute {
		t.Fatalf("options = %#v hasOptions=%v, want upsert with default TTL", applier.options, applier.hasOptions)
	}
	if len(applier.changes) != 1 {
		t.Fatalf("changes len = %d, want whole-store change", len(applier.changes))
	}
}

func TestStorageSaveAppliesDirtyChangesAndForcesNonUpsert(t *testing.T) {
	item := recordedChange{Path: testStoragePath(2), Op: change.OpSet}
	store := &testChangeStore{
		spec:     meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{FullName: "GDRole"}}},
		key:      storage.RecordKey{ID: int64(1001)},
		changes:  recordedChanges{item},
		defaults: storage.ApplyOptions{TTL: 10 * time.Minute},
	}
	applier := &recordingChangeStoreApplier{}

	if err := storage.SaveWithOptions(context.Background(), applier, store, storage.ApplyOptions{Upsert: true, TTL: 2 * time.Minute}); err != nil {
		t.Fatalf("SaveWithOptions returned error: %v", err)
	}
	if store.allTouched != 0 {
		t.Fatalf("AllTouch calls = %d, want 0", store.allTouched)
	}
	if store.cleared != 1 {
		t.Fatalf("ClearChanges calls = %d, want 1", store.cleared)
	}
	if !applier.hasOptions || applier.options.Upsert || applier.options.TTL != 2*time.Minute {
		t.Fatalf("options = %#v hasOptions=%v, want non-upsert with explicit TTL", applier.options, applier.hasOptions)
	}
	if len(applier.changes) != 1 || applier.changes[0].Path != item.Path {
		t.Fatalf("changes = %#v, want original projected change", applier.changes)
	}
}

func TestStorageSaveFailureKeepsChanges(t *testing.T) {
	saveErr := errors.New("boom")
	store := &testChangeStore{
		key:     storage.RecordKey{ID: int64(1001)},
		changes: recordedChanges{{Path: testStoragePath(2), Op: change.OpSet}},
	}
	applier := &recordingChangeStoreApplier{err: saveErr}

	if err := storage.Save(context.Background(), applier, store); !errors.Is(err, saveErr) {
		t.Fatalf("Save err = %v, want boom", err)
	}
	if store.cleared != 0 {
		t.Fatalf("ClearChanges calls after failure = %d, want 0", store.cleared)
	}
}

func TestStorageSaveRejectsInvalidRecordKeyAndKeepsChanges(t *testing.T) {
	store := &testChangeStore{
		changes: recordedChanges{{Path: testStoragePath(2), Op: change.OpSet}},
	}
	applier := &recordingChangeStoreApplier{}

	err := storage.Save(context.Background(), applier, store)
	if !errors.Is(err, storage.ErrInvalidRecordKey) {
		t.Fatalf("Save error = %v, want ErrInvalidRecordKey", err)
	}
	if applier.hasOptions || store.cleared != 0 {
		t.Fatalf("invalid key reached persistence path: applier=%#v store=%#v", applier, store)
	}
}

func TestStorageRejectsTypedNilChangeApplier(t *testing.T) {
	var applier *recordingChangeStoreApplier
	store := &testChangeStore{changes: recordedChanges{{Path: testStoragePath(2), Op: change.OpSet}}}

	if err := storage.Save(context.Background(), applier, store); !errors.Is(err, storage.ErrNilChangeApplier) {
		t.Fatalf("Save error = %v, want ErrNilChangeApplier", err)
	}
	if store.cleared != 0 {
		t.Fatalf("ClearChanges calls = %d, want 0", store.cleared)
	}
	if err := storage.Create(context.Background(), applier, store); !errors.Is(err, storage.ErrNilChangeApplier) {
		t.Fatalf("Create error = %v, want ErrNilChangeApplier", err)
	}
	if store.allTouched != 0 {
		t.Fatalf("AllTouch calls = %d, want 0", store.allTouched)
	}
}

func testStoragePath(field change.FieldID) change.Path {
	return change.Path{
		Nodes: [change.MaxPathDepth]change.PathNode{{Field: field}},
		Depth: 1,
	}
}
