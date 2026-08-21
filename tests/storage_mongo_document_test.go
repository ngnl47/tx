package tests

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	txmongo "github.com/ngnl47/tx/storage/mongo"
)

func TestMongoBuildUpdateDocumentWithDeltasUsesBSONNames(t *testing.T) {
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet},
		{Path: deltaPath(fieldNode(deltaFieldItems)), Op: change.OpClear},
	}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {deltaFieldName: "Name", deltaFieldItems: "Items"},
		change.FieldNameBSON: {deltaFieldName: "rn", deltaFieldItems: "items"},
	}
	spec := meta.TypeSpec{
		Type: meta.Type{
			Fields: []meta.Field{
				{Name: "Name", BSONName: "rn", Type: "string"},
				{Name: "Items", BSONName: "items", Type: "map[int64]any"},
			},
		},
	}

	update, err := txmongo.BuildUpdateDocumentWithDeltas(spec, deltas, resolver, testDeltaValueResolver{deltaFieldName: "alice"})
	if err != nil {
		t.Fatalf("BuildUpdateDocumentWithDeltas returned error: %v", err)
	}
	setDoc := update["$set"].(map[string]any)
	if setDoc["rn"] != "alice" {
		t.Fatalf("update = %#v, want rn set to alice", update)
	}
	if _, ok := setDoc["items"].(map[string]any); !ok {
		t.Fatalf("items clear value = %#v, want empty document", setDoc["items"])
	}
}

func TestMongoBuildUpdateDocumentWithDeltasRejectsImmutableID(t *testing.T) {
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet},
	}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {deltaFieldName: "RoleId"},
		change.FieldNameBSON: {deltaFieldName: "_id"},
	}

	_, err := txmongo.BuildUpdateDocumentWithDeltas(meta.TypeSpec{}, deltas, resolver, testDeltaValueResolver{deltaFieldName: int64(1001)})
	if err == nil {
		t.Fatal("BuildUpdateDocumentWithDeltas returned nil error, want immutable _id error")
	}
}

func TestMongoBuildUpdateDocumentWithDeltasRejectsMissingCurrentValue(t *testing.T) {
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {deltaFieldName: "Name"},
		change.FieldNameBSON: {deltaFieldName: "name"},
	}
	_, err := txmongo.BuildUpdateDocumentWithDeltas(meta.TypeSpec{}, deltas, resolver, nil)
	if err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("BuildUpdateDocumentWithDeltas error = %v, want unresolved name error", err)
	}
}

func TestMongoBuildUpdateDocumentWithDeltasRejectsUnresolvedFieldPath(t *testing.T) {
	const unknownField change.FieldID = 99
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(unknownField)), Op: change.OpSet}}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {deltaFieldName: "Name"},
		change.FieldNameBSON: {deltaFieldName: "name"},
	}

	_, err := txmongo.BuildUpdateDocumentWithDeltas(meta.TypeSpec{}, deltas, resolver, testDeltaValueResolver{unknownField: "alice"})
	if err == nil || !strings.Contains(err.Error(), "field path") {
		t.Fatalf("BuildUpdateDocumentWithDeltas error = %v, want unresolved field path error", err)
	}
}

func TestMongoBuildUpdateDocumentWithDeltasRejectsUnknownDeltaOp(t *testing.T) {
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.DeltaOp(99)}}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {deltaFieldName: "Name"},
		change.FieldNameBSON: {deltaFieldName: "name"},
	}

	_, err := txmongo.BuildUpdateDocumentWithDeltas(meta.TypeSpec{}, deltas, resolver, testDeltaValueResolver{deltaFieldName: "alice"})
	if err == nil || !strings.Contains(err.Error(), "unknown delta operation") {
		t.Fatalf("BuildUpdateDocumentWithDeltas error = %v, want unknown delta operation error", err)
	}
}

func TestMongoBuildUpdateDocumentWithDeltasAllowsResolvedNilValue(t *testing.T) {
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet}}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {deltaFieldName: "Name"},
		change.FieldNameBSON: {deltaFieldName: "name"},
	}
	update, err := txmongo.BuildUpdateDocumentWithDeltas(meta.TypeSpec{}, deltas, resolver, change.DeltaValueResolverFunc(func(change.DeltaPath) (any, bool) {
		return nil, true
	}))
	if err != nil {
		t.Fatalf("BuildUpdateDocumentWithDeltas returned error: %v", err)
	}
	if got := update["$set"].(map[string]any)["name"]; got != nil {
		t.Fatalf("update value = %#v, want nil", got)
	}
}

func TestMongoBuildUpdateDocumentWithDeltasDoesNotMutateSource(t *testing.T) {
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet},
		{Path: deltaPath(fieldNode(deltaFieldName)), Op: change.OpSet},
	}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {deltaFieldName: "Name"},
		change.FieldNameBSON: {deltaFieldName: "name"},
	}
	if _, err := txmongo.BuildUpdateDocumentWithDeltas(meta.TypeSpec{}, deltas, resolver, testDeltaValueResolver{deltaFieldName: "alice"}); err != nil {
		t.Fatalf("BuildUpdateDocumentWithDeltas returned error: %v", err)
	}
	if len(deltas) != 2 || deltas[0].Path.Depth == 0 {
		t.Fatalf("source deltas mutated: %#v", deltas)
	}
}

func TestMongoBuildUpdateDocumentWithDeltasUsesAncestorReplacementForLaterChild(t *testing.T) {
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(deltaFieldItems)), Op: change.OpReplace},
		{Path: deltaPath(intKeyNode(deltaFieldItems, 1001), fieldNode(deltaFieldQty)), Op: change.OpDelete},
	}
	resolver := testFieldNameResolver{
		change.FieldNameGo: {
			deltaFieldItems: "Items",
			deltaFieldQty:   "Quantity",
		},
		change.FieldNameBSON: {
			deltaFieldItems: "items",
			deltaFieldQty:   "quantity",
		},
	}
	items := map[int64]map[string]int64{1002: {"quantity": 9}}
	values := change.DeltaValueResolverFunc(func(path change.DeltaPath) (any, bool) {
		if path.Equal(deltaPath(fieldNode(deltaFieldItems))) {
			return items, true
		}
		return nil, false
	})

	update, err := txmongo.BuildUpdateDocumentWithDeltas(meta.TypeSpec{}, deltas, resolver, values)
	if err != nil {
		t.Fatalf("BuildUpdateDocumentWithDeltas returned error: %v", err)
	}
	setDoc := update["$set"].(map[string]any)
	if got := setDoc["items"]; !reflect.DeepEqual(got, items) {
		t.Fatalf("items replacement = %#v, want %#v", got, items)
	}
	if _, ok := setDoc["items.1001.quantity"]; ok {
		t.Fatalf("update contains conflicting child set: %#v", update)
	}
	if unset, ok := update["$unset"].(map[string]any); ok {
		if _, exists := unset["items.1001.quantity"]; exists {
			t.Fatalf("update contains conflicting child unset: %#v", update)
		}
	}
}

func TestMongoBuildUpdateDocumentWithDeltasDowngradesUnsafeStringMapKey(t *testing.T) {
	const fieldLabels change.FieldID = 9
	deltas := change.DeltaSet{
		{
			Path: deltaPath(change.PathNode{
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
	values := change.DeltaValueResolverFunc(func(path change.DeltaPath) (any, bool) {
		if path.Equal(deltaPath(fieldNode(fieldLabels))) {
			return fullLabels, true
		}
		return nil, false
	})

	update, err := txmongo.BuildUpdateDocumentWithDeltas(meta.TypeSpec{}, deltas, resolver, values)
	if err != nil {
		t.Fatalf("BuildUpdateDocumentWithDeltas returned error: %v", err)
	}
	setDoc := update["$set"].(map[string]any)
	if got := setDoc["labels"]; !reflect.DeepEqual(got, fullLabels) {
		t.Fatalf("labels fallback = %#v, want full map; update=%#v", got, update)
	}
	if _, exists := setDoc["labels.foo.bar"]; exists {
		t.Fatalf("unsafe key produced dotted Mongo path: %#v", update)
	}
}

func TestMongoBuildUpdateDocumentWithDeltasKeepsSafeAncestorForNestedUnsafeStringMapKey(t *testing.T) {
	const (
		fieldItems change.FieldID = iota + 20
		fieldLabels
		fieldValue
	)
	deltas := change.DeltaSet{
		{
			Path: deltaPath(
				change.PathNode{Field: fieldItems, Key: change.PathKey{Kind: change.KeyInt, Int: 1001}},
				change.PathNode{Field: fieldLabels, Key: change.PathKey{Kind: change.KeyString, Str: "safe"}},
				change.PathNode{Field: fieldValue},
			),
			Op: change.OpSet,
		},
		{
			Path: deltaPath(
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
	values := change.DeltaValueResolverFunc(func(path change.DeltaPath) (any, bool) {
		want := deltaPath(
			change.PathNode{Field: fieldItems, Key: change.PathKey{Kind: change.KeyInt, Int: 1001}},
			change.PathNode{Field: fieldLabels},
		)
		if path.Equal(want) {
			return fullLabels, true
		}
		safePath := deltaPath(
			change.PathNode{Field: fieldItems, Key: change.PathKey{Kind: change.KeyInt, Int: 1001}},
			change.PathNode{Field: fieldLabels, Key: change.PathKey{Kind: change.KeyString, Str: "safe"}},
			change.PathNode{Field: fieldValue},
		)
		if path.Equal(safePath) {
			return int64(9), true
		}
		return nil, false
	})

	update, err := txmongo.BuildUpdateDocumentWithDeltas(meta.TypeSpec{}, deltas, resolver, values)
	if err != nil {
		t.Fatalf("BuildUpdateDocumentWithDeltas returned error: %v", err)
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
