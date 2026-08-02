package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
)

type testFieldResolver map[change.FieldID]string

func (r testFieldResolver) FieldName(field change.FieldID) string {
	return r[field]
}

func TestFieldChangeFullPathUsesResolver(t *testing.T) {
	fieldChange := change.FieldChange{
		Field: change.FieldID(2),
		Kind:  change.KindSet,
		Value: "alice",
	}

	path := fieldChange.FullPath(testFieldResolver{2: "name"})
	if path.String() != "name" {
		t.Fatalf("path = %q, want name", path.String())
	}
}

type testFieldNameResolver map[change.NameBackend]map[change.FieldID]string

func (r testFieldNameResolver) FieldName(field change.FieldID, backend change.NameBackend) string {
	return r[backend][field]
}

type testDeltaValueResolver map[change.FieldID]any

func (r testDeltaValueResolver) DeltaValue(path change.DeltaPath) (any, bool) {
	if path.Depth == 0 {
		return nil, false
	}
	value, ok := r[path.Nodes[0].Field]
	return value, ok
}

func TestFieldNameResolverUsesBackend(t *testing.T) {
	resolver := testFieldNameResolver{
		change.NameBackendGo:      {1: "RoleId"},
		change.NameBackendJSON:    {1: "roleId"},
		change.NameBackendBSON:    {1: "_id"},
		change.NameBackendMsgpack: {1: "rid"},
	}

	tests := []struct {
		backend change.NameBackend
		want    string
	}{
		{change.NameBackendGo, "RoleId"},
		{change.NameBackendJSON, "roleId"},
		{change.NameBackendBSON, "_id"},
		{change.NameBackendMsgpack, "rid"},
	}
	for _, tt := range tests {
		if got := resolver.FieldName(1, tt.backend); got != tt.want {
			t.Fatalf("FieldName backend %q = %q, want %q", tt.backend, got, tt.want)
		}
	}
}

func TestDeltaPathFullPathUsesBackend(t *testing.T) {
	resolver := testFieldNameResolver{
		change.NameBackendGo:   {2: "Items", 3: "Quantity"},
		change.NameBackendBSON: {2: "items", 3: "q"},
	}
	path := deltaPath(intKeyNode(2, 1001), fieldNode(3))

	if got := path.FullPath(resolver, change.NameBackendGo).String(); got != "Items.1001.Quantity" {
		t.Fatalf("Go path = %q, want Items.1001.Quantity", got)
	}
	if got := path.FullPath(resolver, change.NameBackendBSON).String(); got != "items.1001.q" {
		t.Fatalf("BSON path = %q, want items.1001.q", got)
	}
}

func TestDeltaSetDocumentWithResolverUsesBackendNames(t *testing.T) {
	resolver := testFieldNameResolver{
		change.NameBackendGo:      {1: "Name", 2: "Items", 3: "Quantity"},
		change.NameBackendJSON:    {1: "name", 2: "items", 3: "quantity"},
		change.NameBackendMsgpack: {1: "n", 2: "it", 3: "q"},
	}
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(1)), Kind: change.DeltaKindSet},
		{Path: deltaPath(fieldNode(1)), Kind: change.DeltaKindSet},
		{
			Path: deltaPath(intKeyNode(2, 1001), fieldNode(3)),
			Kind: change.DeltaKindSet,
		},
		{Path: deltaPath(fieldNode(3)), Kind: change.DeltaDelete},
	}

	doc := deltas.DocumentWithResolver(resolver, change.NameBackendMsgpack, change.DeltaValueResolverFunc(func(path change.DeltaPath) (any, bool) {
		if path.Depth == 1 && path.Nodes[0].Field == 1 {
			return "bob", true
		}
		if path.Depth == 2 && path.Nodes[0].Field == 2 && path.Nodes[0].Key.Int == 1001 && path.Nodes[1].Field == 3 {
			return int64(7), true
		}
		return nil, false
	}))

	if got := doc.Set["n"]; got != "bob" {
		t.Fatalf("doc.Set[n] = %#v, want bob; doc=%#v", got, doc)
	}
	if got := doc.Set["it.1001.q"]; got != int64(7) {
		t.Fatalf("doc.Set[it.1001.q] = %#v, want 7; doc=%#v", got, doc)
	}
	if _, ok := doc.Set["name"]; ok {
		t.Fatalf("doc used json field name: %#v", doc)
	}
	if _, ok := doc.Clear["q"]; !ok {
		t.Fatalf("doc.Clear = %#v, want q", doc.Clear)
	}
}

func TestDeltaSetDocumentWithResolverDoesNotMutateSourceDeltas(t *testing.T) {
	resolver := testFieldNameResolver{
		change.NameBackendMsgpack: {1: "n"},
	}
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(1)), Kind: change.DeltaKindSet},
		{Path: deltaPath(fieldNode(1)), Kind: change.DeltaKindSet},
	}

	_ = deltas.DocumentWithResolver(resolver, change.NameBackendMsgpack, testDeltaValueResolver{1: "bob"})

	if len(deltas) != 2 {
		t.Fatalf("deltas len = %d, want 2", len(deltas))
	}
	for i, delta := range deltas {
		if delta.Path.Depth == 0 {
			t.Fatalf("deltas[%d] was compacted in place: %#v", i, deltas)
		}
	}
}

func TestDeltaSetDocumentWithResolverReadsCurrentValueFromResolver(t *testing.T) {
	resolver := testFieldNameResolver{
		change.NameBackendMsgpack: {1: "n"},
	}
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(1)), Kind: change.DeltaKindSet},
	}

	doc := deltas.DocumentWithResolver(resolver, change.NameBackendMsgpack, testDeltaValueResolver{1: "current"})

	if got := doc.Set["n"]; got != "current" {
		t.Fatalf("doc.Set[n] = %#v, want current; doc=%#v", got, doc)
	}
}

func TestFieldNameResolverFunc(t *testing.T) {
	resolver := change.FieldNameResolverFunc(func(field change.FieldID, backend change.NameBackend) string {
		if field == 1 && backend == change.NameBackendJSON {
			return "roleId"
		}
		return ""
	})

	if got := resolver.FieldName(1, change.NameBackendJSON); got != "roleId" {
		t.Fatalf("FieldNameResolverFunc returned %q, want roleId", got)
	}
}

func TestFieldChangeFullPathAppendsDynamicTail(t *testing.T) {
	fieldChange := change.FieldChange{
		Field: change.FieldID(3),
		Path:  change.Path{"1001", "quantity"},
		Kind:  change.KindSet,
		Value: int64(9),
	}

	path := fieldChange.FullPath(testFieldResolver{3: "items"})
	if path.String() != "items.1001.quantity" {
		t.Fatalf("path = %q, want items.1001.quantity", path.String())
	}
}

func TestTrackerRecordsFieldIDChangeWithoutPath(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.TouchField(change.FieldID(2), "alice"); err != nil {
		t.Fatalf("TouchField returned error: %v", err)
	}

	changes := tracker.Changes().Changes()
	if len(changes) != 1 {
		t.Fatalf("changes len = %d, want 1: %#v", len(changes), changes)
	}
	if changes[0].Field != change.FieldID(2) || len(changes[0].Path) != 0 {
		t.Fatalf("change target = %#v, want field-only target", changes[0])
	}
	if changes[0].FullPath(testFieldResolver{2: "name"}).String() != "name" {
		t.Fatalf("resolved path = %q, want name", changes[0].FullPath(testFieldResolver{2: "name"}).String())
	}
}

func TestTrackerUnTouchFieldRemovesFieldIDChange(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.TouchFieldPath(change.FieldID(3), change.Path{"1001"}, "sword"); err != nil {
		t.Fatalf("TouchFieldPath returned error: %v", err)
	}
	tracker.UnTouchFieldPath(change.FieldID(3), change.Path{"1001"})

	if !tracker.Changes().Empty() {
		t.Fatalf("changes = %#v, want empty", tracker.Changes().Changes())
	}
}

func TestTrackerParentFieldChangeCollapsesChildPathChanges(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.TouchFieldPath(change.FieldID(3), change.Path{"1001", "quantity"}, int64(3)); err != nil {
		t.Fatalf("TouchFieldPath returned error: %v", err)
	}
	if err := tracker.TouchFieldPath(change.FieldID(3), change.Path{"1002", "quantity"}, int64(4)); err != nil {
		t.Fatalf("TouchFieldPath returned error: %v", err)
	}
	if err := tracker.TouchField(change.FieldID(3), map[int64]int64{1001: 3, 1002: 4}); err != nil {
		t.Fatalf("TouchField returned error: %v", err)
	}

	changes := tracker.Changes().Changes()
	if len(changes) != 1 {
		t.Fatalf("changes len = %d, want parent field only: %#v", len(changes), changes)
	}
	if changes[0].Field != change.FieldID(3) || len(changes[0].Path) != 0 {
		t.Fatalf("change target = %#v, want parent field target", changes[0])
	}
	if got := changes[0].FullPath(testFieldResolver{3: "items"}).String(); got != "items" {
		t.Fatalf("resolved path = %q, want items", got)
	}
}

func TestTrackerChildPathChangeDoesNotExpandDirtyParentField(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.TouchField(change.FieldID(3), map[int64]int64{1001: 3}); err != nil {
		t.Fatalf("TouchField returned error: %v", err)
	}
	if err := tracker.TouchFieldPath(change.FieldID(3), change.Path{"1002", "quantity"}, int64(4)); err != nil {
		t.Fatalf("TouchFieldPath returned error: %v", err)
	}

	changes := tracker.Changes().Changes()
	if len(changes) != 1 {
		t.Fatalf("changes len = %d, want parent field only: %#v", len(changes), changes)
	}
	if changes[0].Field != change.FieldID(3) || len(changes[0].Path) != 0 {
		t.Fatalf("change target = %#v, want parent field target", changes[0])
	}
}

func TestTrackerRepeatedParentFieldChangeReplacesValue(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.TouchField(change.FieldID(3), map[int64]int64{1001: 3}); err != nil {
		t.Fatalf("TouchField returned error: %v", err)
	}
	if err := tracker.TouchField(change.FieldID(3), map[int64]int64{1002: 4}); err != nil {
		t.Fatalf("TouchField returned error: %v", err)
	}

	changes := tracker.Changes().Changes()
	if len(changes) != 1 {
		t.Fatalf("changes len = %d, want one parent field: %#v", len(changes), changes)
	}
	values, ok := changes[0].Value.(map[int64]int64)
	if !ok || values[1002] != 4 {
		t.Fatalf("change value = %#v, want replacement map", changes[0].Value)
	}
}
