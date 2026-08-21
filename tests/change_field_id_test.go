package tests

import (
	"strings"
	"testing"

	"github.com/ngnl47/tx/change"
)

type testFieldNameResolver map[change.FieldNameKind]map[change.FieldID]string

func (r testFieldNameResolver) FieldName(field change.FieldID, kind change.FieldNameKind) string {
	return r[kind][field]
}

type testDeltaValueResolver map[change.FieldID]any

func (r testDeltaValueResolver) DeltaValue(path change.DeltaPath) (any, bool) {
	if path.Depth == 0 {
		return nil, false
	}
	value, ok := r[path.Nodes[0].Field]
	return value, ok
}

type testNestedFieldNameResolver struct{}

func (testNestedFieldNameResolver) FieldName(field change.FieldID, kind change.FieldNameKind) string {
	if field == 2 {
		return "items"
	}
	return ""
}

func (testNestedFieldNameResolver) DeltaPathFieldName(path change.DeltaPath, index int, kind change.FieldNameKind) string {
	if index == 0 && path.Nodes[index].Field == 2 {
		return "items"
	}
	if index == 1 && path.Nodes[0].Field == 2 && path.Nodes[index].Field == 2 {
		return "quantity"
	}
	return ""
}

func TestFieldNameResolverUsesBackend(t *testing.T) {
	resolver := testFieldNameResolver{
		change.FieldNameGo:      {1: "RoleId"},
		change.FieldNameJSON:    {1: "roleId"},
		change.FieldNameBSON:    {1: "_id"},
		change.FieldNameMsgpack: {1: "rid"},
	}

	tests := []struct {
		kind change.FieldNameKind
		want string
	}{
		{change.FieldNameGo, "RoleId"},
		{change.FieldNameJSON, "roleId"},
		{change.FieldNameBSON, "_id"},
		{change.FieldNameMsgpack, "rid"},
	}
	for _, tt := range tests {
		if got := resolver.FieldName(1, tt.kind); got != tt.want {
			t.Fatalf("FieldName kind %q = %q, want %q", tt.kind, got, tt.want)
		}
	}
}

func TestDeltaPathFullPathUsesBackend(t *testing.T) {
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {2: "Items", 3: "Quantity"},
		change.FieldNameBSON: {2: "items", 3: "q"},
	}
	path := deltaPath(intKeyNode(2, 1001), fieldNode(3))

	if got := path.FullPath(resolver, change.FieldNameGo).String(); got != "Items.1001.Quantity" {
		t.Fatalf("Go path = %q, want Items.1001.Quantity", got)
	}
	if got := path.FullPath(resolver, change.FieldNameBSON).String(); got != "items.1001.q" {
		t.Fatalf("BSON path = %q, want items.1001.q", got)
	}
}

func TestDeltaPathFullPathCanResolveNestedTypeFields(t *testing.T) {
	path := deltaPath(intKeyNode(2, 1001), fieldNode(2))

	if got := path.FullPath(testNestedFieldNameResolver{}, change.FieldNameGo).String(); got != "items.1001.quantity" {
		t.Fatalf("nested path = %q, want items.1001.quantity", got)
	}
}

func TestDeltaPathFullPathRejectsUnknownKeyKind(t *testing.T) {
	path := change.DeltaPath{
		Nodes: [change.MaxDeltaDepth]change.PathNode{{
			Field: 1,
			Key:   change.PathKey{Kind: change.KeyKind(99)},
		}},
		Depth: 1,
	}
	resolver := testFieldNameResolver{change.FieldNameGo: {1: "Name"}}

	if got := path.FullPath(resolver, change.FieldNameGo); len(got) != 0 {
		t.Fatalf("FullPath = %q, want unresolved path", got)
	}
}

func TestDeltaSetDocumentWithResolverUsesBackendNames(t *testing.T) {
	resolver := testFieldNameResolver{
		change.FieldNameGo:      {1: "Name", 2: "Items", 3: "Quantity"},
		change.FieldNameJSON:    {1: "name", 2: "items", 3: "quantity"},
		change.FieldNameMsgpack: {1: "n", 2: "it", 3: "q"},
	}
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(1)), Op: change.OpSet},
		{Path: deltaPath(fieldNode(1)), Op: change.OpSet},
		{
			Path: deltaPath(intKeyNode(2, 1001), fieldNode(3)),
			Op:   change.OpSet,
		},
		{Path: deltaPath(fieldNode(3)), Op: change.OpDelete},
	}

	doc, err := deltas.DocumentWithResolver(resolver, change.FieldNameMsgpack, change.DeltaValueResolverFunc(func(path change.DeltaPath) (any, bool) {
		if path.Depth == 1 && path.Nodes[0].Field == 1 {
			return "bob", true
		}
		if path.Depth == 2 && path.Nodes[0].Field == 2 && path.Nodes[0].Key.Int == 1001 && path.Nodes[1].Field == 3 {
			return int64(7), true
		}
		return nil, false
	}))
	if err != nil {
		t.Fatalf("DocumentWithResolver returned error: %v", err)
	}

	if got := doc.Set["n"]; got != "bob" {
		t.Fatalf("doc.Set[n] = %#v, want bob; doc=%#v", got, doc)
	}
	if got := doc.Set["it.1001.q"]; got != int64(7) {
		t.Fatalf("doc.Set[it.1001.q] = %#v, want 7; doc=%#v", got, doc)
	}
	if _, ok := doc.Set["name"]; ok {
		t.Fatalf("doc used json field name: %#v", doc)
	}
	if _, ok := doc.Delete["q"]; !ok {
		t.Fatalf("doc.Delete = %#v, want q", doc.Delete)
	}
}

func TestDeltaSetDocumentWithResolverDoesNotMutateSourceDeltas(t *testing.T) {
	resolver := testFieldNameResolver{
		change.FieldNameMsgpack: {1: "n"},
	}
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(1)), Op: change.OpSet},
		{Path: deltaPath(fieldNode(1)), Op: change.OpSet},
	}

	if _, err := deltas.DocumentWithResolver(resolver, change.FieldNameMsgpack, testDeltaValueResolver{1: "bob"}); err != nil {
		t.Fatalf("DocumentWithResolver returned error: %v", err)
	}

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
		change.FieldNameMsgpack: {1: "n"},
	}
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(1)), Op: change.OpSet},
	}

	doc, err := deltas.DocumentWithResolver(resolver, change.FieldNameMsgpack, testDeltaValueResolver{1: "current"})
	if err != nil {
		t.Fatalf("DocumentWithResolver returned error: %v", err)
	}

	if got := doc.Set["n"]; got != "current" {
		t.Fatalf("doc.Set[n] = %#v, want current; doc=%#v", got, doc)
	}
}

func TestDeltaSetDocumentWithResolverRejectsMissingCurrentValue(t *testing.T) {
	resolver := testFieldNameResolver{
		change.FieldNameMsgpack: {1: "n"},
	}
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(1)), Op: change.OpSet},
	}

	_, err := deltas.DocumentWithResolver(resolver, change.FieldNameMsgpack, nil)
	if err == nil {
		t.Fatal("DocumentWithResolver returned nil error for an unresolved delta value")
	}
}

func TestDeltaSetDocumentWithResolverRejectsUnknownDeltaOp(t *testing.T) {
	resolver := testFieldNameResolver{
		change.FieldNameMsgpack: {1: "n"},
	}
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(1)), Op: change.DeltaOp(99)},
		{Path: deltaPath(fieldNode(1)), Op: change.OpSet},
	}

	_, err := deltas.DocumentWithResolver(resolver, change.FieldNameMsgpack, testDeltaValueResolver{1: "current"})
	if err == nil || !strings.Contains(err.Error(), "unknown delta operation") {
		t.Fatalf("DocumentWithResolver error = %v, want unknown delta operation error", err)
	}
}

func TestDeltaOpValid(t *testing.T) {
	for _, kind := range []change.DeltaOp{
		change.OpSet,
		change.OpReplace,
		change.OpDelete,
		change.OpClear,
	} {
		if !kind.Valid() {
			t.Fatalf("DeltaOp(%d).Valid() = false, want true", kind)
		}
	}
	if change.DeltaOp(99).Valid() {
		t.Fatal("DeltaOp(99).Valid() = true, want false")
	}
}

func TestDeltaSetValidateOpsSkipsCompactionTombstones(t *testing.T) {
	deltas := change.DeltaSet{
		{Op: change.DeltaOp(99)},
		{Path: deltaPath(fieldNode(1)), Op: change.OpSet},
	}
	if err := deltas.ValidateOps(); err != nil {
		t.Fatalf("ValidateOps returned error for empty path tombstone: %v", err)
	}
}

func TestFieldNameResolverFunc(t *testing.T) {
	resolver := change.FieldNameResolverFunc(func(field change.FieldID, kind change.FieldNameKind) string {
		if field == 1 && kind == change.FieldNameJSON {
			return "roleId"
		}
		return ""
	})

	if got := resolver.FieldName(1, change.FieldNameJSON); got != "roleId" {
		t.Fatalf("FieldNameResolverFunc returned %q, want roleId", got)
	}
}
