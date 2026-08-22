package tests

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	txmongo "github.com/ngnl47/tx/storage/mongo"
)

func TestMongoBuildUpdateDocumentUsesBSONNames(t *testing.T) {
	source := recordedChanges{
		{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet},
		{Path: changePath(fieldNode(changeFieldItems)), Op: change.OpClear},
	}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {changeFieldName: "Name", changeFieldItems: "Items"},
		change.FieldNameBSON: {changeFieldName: "rn", changeFieldItems: "items"},
	}
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{
				{Name: "Name", BSONName: "rn", Type: "string"},
				{Name: "Items", BSONName: "items", Type: "map[int64]any"},
			},
		},
	}

	update, err := txmongo.BuildUpdateDocument(spec, source, resolver, testValueResolver{changeFieldName: "alice"})
	if err != nil {
		t.Fatalf("BuildUpdateDocument returned error: %v", err)
	}
	setDoc := update["$set"].(map[string]any)
	if setDoc["rn"] != "alice" {
		t.Fatalf("update = %#v, want rn set to alice", update)
	}
	if _, ok := setDoc["items"].(map[string]any); !ok {
		t.Fatalf("items clear value = %#v, want empty document", setDoc["items"])
	}
}

func TestMongoBuildUpdateDocumentRejectsImmutableID(t *testing.T) {
	source := recordedChanges{
		{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet},
	}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {changeFieldName: "RoleId"},
		change.FieldNameBSON: {changeFieldName: "_id"},
	}

	_, err := txmongo.BuildUpdateDocument(meta.TypeSpec{}, source, resolver, testValueResolver{changeFieldName: int64(1001)})
	if err == nil {
		t.Fatal("BuildUpdateDocument returned nil error, want immutable _id error")
	}
}

func TestMongoBuildUpdateDocumentRejectsMissingCurrentValue(t *testing.T) {
	source := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {changeFieldName: "Name"},
		change.FieldNameBSON: {changeFieldName: "name"},
	}
	_, err := txmongo.BuildUpdateDocument(meta.TypeSpec{}, source, resolver, nil)
	if err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("BuildUpdateDocument error = %v, want unresolved name error", err)
	}
}

func TestMongoBuildUpdateDocumentRejectsUnresolvedFieldPath(t *testing.T) {
	const unknownField change.FieldID = 99
	source := recordedChanges{{Path: changePath(fieldNode(unknownField)), Op: change.OpSet}}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {changeFieldName: "Name"},
		change.FieldNameBSON: {changeFieldName: "name"},
	}

	_, err := txmongo.BuildUpdateDocument(meta.TypeSpec{}, source, resolver, testValueResolver{unknownField: "alice"})
	if err == nil || !strings.Contains(err.Error(), "field path") {
		t.Fatalf("BuildUpdateDocument error = %v, want unresolved field path error", err)
	}
}

func TestMongoBuildUpdateDocumentRejectsUnknownOp(t *testing.T) {
	source := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.Op(99)}}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {changeFieldName: "Name"},
		change.FieldNameBSON: {changeFieldName: "name"},
	}

	_, err := txmongo.BuildUpdateDocument(meta.TypeSpec{}, source, resolver, testValueResolver{changeFieldName: "alice"})
	if err == nil || !strings.Contains(err.Error(), "unknown operation") {
		t.Fatalf("BuildUpdateDocument error = %v, want unknown operation error", err)
	}
}

func TestMongoBuildUpdateDocumentAllowsResolvedNilValue(t *testing.T) {
	source := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {changeFieldName: "Name"},
		change.FieldNameBSON: {changeFieldName: "name"},
	}
	update, err := txmongo.BuildUpdateDocument(meta.TypeSpec{}, source, resolver, change.ValueResolverFunc(func(change.Path) (any, bool) {
		return nil, true
	}))
	if err != nil {
		t.Fatalf("BuildUpdateDocument returned error: %v", err)
	}
	if got := update["$set"].(map[string]any)["name"]; got != nil {
		t.Fatalf("update value = %#v, want nil", got)
	}
}

func TestMongoBuildUpdateDocumentDowngradesUnsafeStringMapKey(t *testing.T) {
	const fieldLabels change.FieldID = 9
	source := recordedChanges{
		{
			Path: changePath(change.PathNode{
				Field: fieldLabels,
				Key:   change.PathKey{Kind: change.KeyString, Str: "foo.bar"},
			}),
			Op: change.OpSet,
		},
	}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {fieldLabels: "Labels"},
		change.FieldNameBSON: {fieldLabels: "labels"},
	}
	fullLabels := map[string]int{"foo.bar": 7}
	values := change.ValueResolverFunc(func(path change.Path) (any, bool) {
		if path.Equal(changePath(fieldNode(fieldLabels))) {
			return fullLabels, true
		}
		return nil, false
	})

	update, err := txmongo.BuildUpdateDocument(meta.TypeSpec{}, source, resolver, values)
	if err != nil {
		t.Fatalf("BuildUpdateDocument returned error: %v", err)
	}
	setDoc := update["$set"].(map[string]any)
	if got := setDoc["labels"]; !reflect.DeepEqual(got, fullLabels) {
		t.Fatalf("labels fallback = %#v, want full map; update=%#v", got, update)
	}
	if _, exists := setDoc["labels.foo.bar"]; exists {
		t.Fatalf("unsafe key produced dotted Mongo path: %#v", update)
	}
}

func TestMongoBuildUpdateDocumentKeepsSafeAncestorForNestedUnsafeStringMapKey(t *testing.T) {
	const (
		fieldItems change.FieldID = iota + 20
		fieldLabels
		fieldValue
	)
	source := recordedChanges{
		{
			Path: changePath(
				change.PathNode{Field: fieldItems, Key: change.PathKey{Kind: change.KeyInt, Int: 1001}},
				change.PathNode{Field: fieldLabels, Key: change.PathKey{Kind: change.KeyString, Str: "safe"}},
				change.PathNode{Field: fieldValue},
			),
			Op: change.OpSet,
		},
		{
			Path: changePath(
				change.PathNode{Field: fieldItems, Key: change.PathKey{Kind: change.KeyInt, Int: 1001}},
				change.PathNode{Field: fieldLabels, Key: change.PathKey{Kind: change.KeyString, Str: "foo.bar"}},
				change.PathNode{Field: fieldValue},
			),
			Op: change.OpSet,
		},
	}
	resolver := testFieldNameResolver{
		change.FieldNameGo: {
			fieldItems: "Items", fieldLabels: "Labels", fieldValue: "Value",
		},
		change.FieldNameBSON: {
			fieldItems: "items", fieldLabels: "labels", fieldValue: "value",
		},
	}
	fullLabels := map[string]map[string]int{
		"foo.bar": {"value": 7},
		"safe":    {"value": 9},
	}
	values := change.ValueResolverFunc(func(path change.Path) (any, bool) {
		want := changePath(
			change.PathNode{Field: fieldItems, Key: change.PathKey{Kind: change.KeyInt, Int: 1001}},
			change.PathNode{Field: fieldLabels},
		)
		if path.Equal(want) {
			return fullLabels, true
		}
		safePath := changePath(
			change.PathNode{Field: fieldItems, Key: change.PathKey{Kind: change.KeyInt, Int: 1001}},
			change.PathNode{Field: fieldLabels, Key: change.PathKey{Kind: change.KeyString, Str: "safe"}},
			change.PathNode{Field: fieldValue},
		)
		if path.Equal(safePath) {
			return int64(9), true
		}
		return nil, false
	})

	update, err := txmongo.BuildUpdateDocument(meta.TypeSpec{}, source, resolver, values)
	if err != nil {
		t.Fatalf("BuildUpdateDocument returned error: %v", err)
	}
	setDoc := update["$set"].(map[string]any)
	if got := setDoc["items.1001.labels"]; !reflect.DeepEqual(got, fullLabels) {
		t.Fatalf("nested labels fallback = %#v, want full map; update=%#v", got, update)
	}
	if _, exists := setDoc["items"]; exists {
		t.Fatalf("nested unsafe key replaced root field: %#v", update)
	}
	if _, exists := setDoc["items.1001.labels.foo.bar.value"]; exists {
		t.Fatalf("unsafe key produced dotted Mongo path: %#v", update)
	}
	if _, exists := setDoc["items.1001.labels.safe.value"]; exists {
		t.Fatalf("whole map fallback must remove sibling child updates: %#v", update)
	}
}
