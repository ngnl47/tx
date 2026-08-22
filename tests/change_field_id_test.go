package tests

import (
	"errors"
	"strings"
	"testing"

	"github.com/ngnl47/tx/change"
)

type testFieldNameResolver map[change.FieldNameKind]map[change.FieldID]string

func (r testFieldNameResolver) FieldName(field change.FieldID, kind change.FieldNameKind) string {
	return r[kind][field]
}

type testValueResolver map[change.FieldID]any

func (r testValueResolver) ResolveValue(path change.Path) (any, bool) {
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

func (testNestedFieldNameResolver) PathFieldName(path change.Path, index int, kind change.FieldNameKind) string {
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

func TestPathFullPathUsesBackend(t *testing.T) {
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {2: "Items", 3: "Quantity"},
		change.FieldNameBSON: {2: "items", 3: "q"},
	}
	path := changePath(intKeyNode(2, 1001), fieldNode(3))

	if got := path.FullPath(resolver, change.FieldNameGo).String(); got != "Items.1001.Quantity" {
		t.Fatalf("Go path = %q, want Items.1001.Quantity", got)
	}
	if got := path.FullPath(resolver, change.FieldNameBSON).String(); got != "items.1001.q" {
		t.Fatalf("BSON path = %q, want items.1001.q", got)
	}
}

func TestPathFullPathCanResolveNestedTypeFields(t *testing.T) {
	path := changePath(intKeyNode(2, 1001), fieldNode(2))

	if got := path.FullPath(testNestedFieldNameResolver{}, change.FieldNameGo).String(); got != "items.1001.quantity" {
		t.Fatalf("nested path = %q, want items.1001.quantity", got)
	}
}

func TestBuildDocumentUsesRequestedFieldNameKind(t *testing.T) {
	path := changePath(fieldNode(1))
	source := recordedChanges{{Path: path, Op: change.OpSet}}
	resolver := testFieldNameResolver{
		change.FieldNameGo:      {1: "HeadImg"},
		change.FieldNameMsgpack: {1: "hi"},
	}
	values := testValueResolver{1: "avatar"}

	doc, err := change.BuildDocument(source, resolver, change.FieldNameMsgpack, values)
	if err != nil {
		t.Fatalf("BuildDocument returned error: %v", err)
	}
	if got := doc.Set["hi"]; got != "avatar" {
		t.Fatalf("msgpack field hi = %#v, want avatar; doc=%#v", got, doc)
	}
	if _, ok := doc.Set["HeadImg"]; ok {
		t.Fatalf("BuildDocument used Go field name: %#v", doc)
	}
}

func TestBuildDocumentRejectsNilSource(t *testing.T) {
	_, err := change.BuildDocument(nil, testFieldNameResolver{}, change.FieldNameGo, nil)
	if !errors.Is(err, change.ErrNilSource) {
		t.Fatalf("BuildDocument error = %v, want ErrNilSource", err)
	}
}

func TestBuildDocumentRejectsUnknownFieldNameKind(t *testing.T) {
	_, err := change.BuildDocument(recordedChanges{}, testFieldNameResolver{}, change.FieldNameKind("unknown"), nil)
	if err == nil || !strings.Contains(err.Error(), "unknown field name kind") {
		t.Fatalf("BuildDocument error = %v, want unknown field name kind", err)
	}
}

func TestPathFullPathRejectsUnknownKeyKind(t *testing.T) {
	path := change.Path{
		Nodes: [change.MaxPathDepth]change.PathNode{{
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

func TestBuildDocumentUsesBackendNames(t *testing.T) {
	resolver := testFieldNameResolver{
		change.FieldNameGo:      {1: "Name", 2: "Items", 3: "Quantity"},
		change.FieldNameJSON:    {1: "name", 2: "items", 3: "quantity"},
		change.FieldNameMsgpack: {1: "n", 2: "it", 3: "q"},
	}
	source := recordedChanges{
		{Path: changePath(fieldNode(1)), Op: change.OpSet},
		{Path: changePath(fieldNode(1)), Op: change.OpSet},
		{
			Path: changePath(intKeyNode(2, 1001), fieldNode(3)),
			Op:   change.OpSet,
		},
		{Path: changePath(fieldNode(3)), Op: change.OpDelete},
	}

	doc, err := change.BuildDocument(source, resolver, change.FieldNameMsgpack, change.ValueResolverFunc(func(path change.Path) (any, bool) {
		if path.Depth == 1 && path.Nodes[0].Field == 1 {
			return "bob", true
		}
		if path.Depth == 2 && path.Nodes[0].Field == 2 && path.Nodes[0].Key.Int == 1001 && path.Nodes[1].Field == 3 {
			return int64(7), true
		}
		return nil, false
	}))
	if err != nil {
		t.Fatalf("BuildDocument returned error: %v", err)
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

func TestBuildDocumentReadsCurrentValueFromResolver(t *testing.T) {
	resolver := testFieldNameResolver{
		change.FieldNameMsgpack: {1: "n"},
	}
	source := recordedChanges{
		{Path: changePath(fieldNode(1)), Op: change.OpSet},
	}

	doc, err := change.BuildDocument(source, resolver, change.FieldNameMsgpack, testValueResolver{1: "current"})
	if err != nil {
		t.Fatalf("BuildDocument returned error: %v", err)
	}

	if got := doc.Set["n"]; got != "current" {
		t.Fatalf("doc.Set[n] = %#v, want current; doc=%#v", got, doc)
	}
}

func TestBuildDocumentRejectsMissingCurrentValue(t *testing.T) {
	resolver := testFieldNameResolver{
		change.FieldNameMsgpack: {1: "n"},
	}
	source := recordedChanges{
		{Path: changePath(fieldNode(1)), Op: change.OpSet},
	}

	_, err := change.BuildDocument(source, resolver, change.FieldNameMsgpack, nil)
	if err == nil {
		t.Fatal("BuildDocument returned nil error for an unresolved change value")
	}
}

func TestBuildDocumentRejectsUnknownOp(t *testing.T) {
	resolver := testFieldNameResolver{
		change.FieldNameMsgpack: {1: "n"},
	}
	source := recordedChanges{
		{Path: changePath(fieldNode(1)), Op: change.Op(99)},
		{Path: changePath(fieldNode(1)), Op: change.OpSet},
	}

	_, err := change.BuildDocument(source, resolver, change.FieldNameMsgpack, testValueResolver{1: "current"})
	if err == nil || !strings.Contains(err.Error(), "unknown operation") {
		t.Fatalf("BuildDocument error = %v, want unknown operation error", err)
	}
}

func TestOpValid(t *testing.T) {
	for _, kind := range []change.Op{
		change.OpSet,
		change.OpReplace,
		change.OpDelete,
		change.OpClear,
	} {
		if !kind.Valid() {
			t.Fatalf("Op(%d).Valid() = false, want true", kind)
		}
	}
	if change.Op(99).Valid() {
		t.Fatal("Op(99).Valid() = true, want false")
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
