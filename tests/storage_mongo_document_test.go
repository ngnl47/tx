package tests

import (
	"reflect"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	txmongo "github.com/ngnl47/tx/storage/mongo"
)

func TestMongoBuildUpdateDocumentWithDeltasUsesBSONNames(t *testing.T) {
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaKindSet},
		{Path: deltaPath(fieldNode(deltaFieldItems)), Kind: change.DeltaClear},
	}
	resolver := testFieldNameResolver{
		change.NameBackendGo:   {deltaFieldName: "Name", deltaFieldItems: "Items"},
		change.NameBackendBSON: {deltaFieldName: "rn", deltaFieldItems: "items"},
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
		{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaKindSet},
	}
	resolver := testFieldNameResolver{
		change.NameBackendGo:   {deltaFieldName: "RoleId"},
		change.NameBackendBSON: {deltaFieldName: "_id"},
	}

	_, err := txmongo.BuildUpdateDocumentWithDeltas(meta.TypeSpec{}, deltas, resolver, testDeltaValueResolver{deltaFieldName: int64(1001)})
	if err == nil {
		t.Fatal("BuildUpdateDocumentWithDeltas returned nil error, want immutable _id error")
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
			Kind: change.DeltaKindSet,
		},
	}
	resolver := testFieldNameResolver{
		change.NameBackendGo:   {fieldLabels: "Labels"},
		change.NameBackendBSON: {fieldLabels: "labels"},
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
