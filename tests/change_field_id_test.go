package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
)

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
