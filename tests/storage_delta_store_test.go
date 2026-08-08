package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

type recordingDeltaStoreApplier struct {
	spec       meta.TypeSpec
	key        storage.RecordKey
	deltas     change.DeltaSet
	options    storage.ApplyOptions
	hasOptions bool
	err        error
}

func (a *recordingDeltaStoreApplier) ApplyDeltasWithOptions(_ context.Context, spec meta.TypeSpec, key storage.RecordKey, deltas change.DeltaSet, _ change.FieldNameResolver, _ change.DeltaValueResolver, options storage.ApplyOptions) error {
	a.spec = spec
	a.key = key
	a.deltas = deltas.Clone()
	a.options = options
	a.hasOptions = true
	return a.err
}

type testDeltaStore struct {
	spec       meta.TypeSpec
	key        storage.RecordKey
	deltas     change.DeltaSet
	defaults   storage.ApplyOptions
	view       bool
	allTouched int
	cleared    int
}

func (s *testDeltaStore) TypeSpec() meta.TypeSpec      { return s.spec }
func (s *testDeltaStore) RecordKey() storage.RecordKey { return s.key }
func (s *testDeltaStore) Deltas() change.DeltaSet      { return s.deltas }
func (s *testDeltaStore) FieldNameResolver() change.FieldNameResolver {
	return change.FieldNameResolverFunc(func(field change.FieldID, backend change.NameBackend) string { return "" })
}
func (s *testDeltaStore) DeltaValueResolver() change.DeltaValueResolver {
	return change.DeltaValueResolverFunc(func(change.DeltaPath) (any, bool) { return nil, false })
}
func (s *testDeltaStore) DefaultApplyOptions() storage.ApplyOptions { return s.defaults }
func (s *testDeltaStore) IsViewStore() bool                         { return s.view }
func (s *testDeltaStore) AllTouch() {
	s.allTouched++
	s.deltas = change.DeltaSet{{Path: testStorageDeltaPath(1), Kind: change.DeltaKindSet}}
}
func (s *testDeltaStore) ClearDeltas() {
	s.cleared++
	s.deltas = nil
}

func TestStorageCreateAppliesWholeStoreAndForcesUpsert(t *testing.T) {
	store := &testDeltaStore{
		spec:     meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{FullName: "GDRole"}}},
		key:      storage.RecordKey{ID: int64(1001)},
		defaults: storage.ApplyOptions{TTL: 600},
	}
	applier := &recordingDeltaStoreApplier{}

	if err := storage.Create(context.Background(), applier, store); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if store.allTouched != 1 {
		t.Fatalf("AllTouch calls = %d, want 1", store.allTouched)
	}
	if store.cleared != 1 {
		t.Fatalf("ClearDeltas calls = %d, want 1", store.cleared)
	}
	if !applier.hasOptions || !applier.options.Upsert || applier.options.TTL != 600 {
		t.Fatalf("options = %#v hasOptions=%v, want upsert with default TTL", applier.options, applier.hasOptions)
	}
	if len(applier.deltas) != 1 {
		t.Fatalf("deltas len = %d, want whole-store delta", len(applier.deltas))
	}
}

func TestStorageSaveAppliesDirtyDeltasAndForcesNonUpsert(t *testing.T) {
	delta := change.DeltaEntry{Path: testStorageDeltaPath(2), Kind: change.DeltaKindSet}
	store := &testDeltaStore{
		spec:     meta.TypeSpec{Type: meta.Type{Ref: meta.TypeRef{FullName: "GDRole"}}},
		key:      storage.RecordKey{ID: int64(1001)},
		deltas:   change.DeltaSet{delta},
		defaults: storage.ApplyOptions{TTL: 600},
	}
	applier := &recordingDeltaStoreApplier{}

	if err := storage.SaveWithOptions(context.Background(), applier, store, storage.ApplyOptions{Upsert: true, TTL: 120}); err != nil {
		t.Fatalf("SaveWithOptions returned error: %v", err)
	}
	if store.allTouched != 0 {
		t.Fatalf("AllTouch calls = %d, want 0", store.allTouched)
	}
	if store.cleared != 1 {
		t.Fatalf("ClearDeltas calls = %d, want 1", store.cleared)
	}
	if !applier.hasOptions || applier.options.Upsert || applier.options.TTL != 120 {
		t.Fatalf("options = %#v hasOptions=%v, want non-upsert with explicit TTL", applier.options, applier.hasOptions)
	}
	if len(applier.deltas) != 1 || applier.deltas[0].Path != delta.Path {
		t.Fatalf("deltas = %#v, want original dirty delta", applier.deltas)
	}
}

func TestStorageSaveFailureKeepsDeltas(t *testing.T) {
	saveErr := errors.New("boom")
	store := &testDeltaStore{
		key:    storage.RecordKey{ID: int64(1001)},
		deltas: change.DeltaSet{{Path: testStorageDeltaPath(2), Kind: change.DeltaKindSet}},
	}
	applier := &recordingDeltaStoreApplier{err: saveErr}

	if err := storage.Save(context.Background(), applier, store); !errors.Is(err, saveErr) {
		t.Fatalf("Save err = %v, want boom", err)
	}
	if store.cleared != 0 {
		t.Fatalf("ClearDeltas calls after failure = %d, want 0", store.cleared)
	}
}

func TestStorageSaveRejectsInvalidRecordKeyAndKeepsDeltas(t *testing.T) {
	store := &testDeltaStore{
		deltas: change.DeltaSet{{Path: testStorageDeltaPath(2), Kind: change.DeltaKindSet}},
	}
	applier := &recordingDeltaStoreApplier{}

	err := storage.Save(context.Background(), applier, store)
	if !errors.Is(err, storage.ErrInvalidRecordKey) {
		t.Fatalf("Save error = %v, want ErrInvalidRecordKey", err)
	}
	if applier.hasOptions || store.cleared != 0 {
		t.Fatalf("invalid key reached persistence path: applier=%#v store=%#v", applier, store)
	}
}

func TestStorageRejectsTypedNilDeltaApplier(t *testing.T) {
	var applier *recordingDeltaStoreApplier
	store := &testDeltaStore{deltas: change.DeltaSet{{Path: testStorageDeltaPath(2), Kind: change.DeltaKindSet}}}

	if err := storage.Save(context.Background(), applier, store); !errors.Is(err, storage.ErrNilDeltaApplier) {
		t.Fatalf("Save error = %v, want ErrNilDeltaApplier", err)
	}
	if store.cleared != 0 {
		t.Fatalf("ClearDeltas calls = %d, want 0", store.cleared)
	}
	if err := storage.Create(context.Background(), applier, store); !errors.Is(err, storage.ErrNilDeltaApplier) {
		t.Fatalf("Create error = %v, want ErrNilDeltaApplier", err)
	}
	if store.allTouched != 0 {
		t.Fatalf("AllTouch calls = %d, want 0", store.allTouched)
	}
}

func TestStorageSaveDeltasUsesProvidedProjectionAndSkipsEmptySet(t *testing.T) {
	delta := change.DeltaEntry{Path: testStorageDeltaPath(2), Kind: change.DeltaKindSet}
	store := &testDeltaStore{
		key:    storage.RecordKey{ID: int64(1001)},
		deltas: change.DeltaSet{{Path: testStorageDeltaPath(1), Kind: change.DeltaKindSet}},
	}
	applier := &recordingDeltaStoreApplier{}

	if err := storage.SaveDeltas(context.Background(), applier, store, change.DeltaSet{delta}); err != nil {
		t.Fatalf("SaveDeltas returned error: %v", err)
	}
	if len(applier.deltas) != 1 || !applier.deltas[0].Path.Equal(delta.Path) {
		t.Fatalf("applied deltas = %#v, want supplied projection", applier.deltas)
	}
	if store.cleared != 1 {
		t.Fatalf("ClearDeltas calls = %d, want 1", store.cleared)
	}

	store.cleared = 0
	applier.hasOptions = false
	if err := storage.SaveDeltas(context.Background(), applier, store, nil); err != nil {
		t.Fatalf("SaveDeltas empty set returned error: %v", err)
	}
	if applier.hasOptions || store.cleared != 0 {
		t.Fatalf("empty set reached persistence path: applier=%#v store=%#v", applier, store)
	}
}

func TestStorageRejectsViewStore(t *testing.T) {
	store := &testDeltaStore{view: true}
	applier := &recordingDeltaStoreApplier{}

	if err := storage.Save(context.Background(), applier, store); !errors.Is(err, storage.ErrReadOnlyStore) {
		t.Fatalf("Save err = %v, want ErrReadOnlyStore", err)
	}
	if err := storage.Create(context.Background(), applier, store); !errors.Is(err, storage.ErrReadOnlyStore) {
		t.Fatalf("Create err = %v, want ErrReadOnlyStore", err)
	}
	if applier.hasOptions || store.allTouched != 0 || store.cleared != 0 {
		t.Fatalf("view Store reached persistence path: applier=%#v store=%#v", applier, store)
	}
}

func testStorageDeltaPath(field change.FieldID) change.DeltaPath {
	return change.DeltaPath{
		Nodes: [change.MaxDeltaDepth]change.PathNode{{Field: field}},
		Depth: 1,
	}
}
