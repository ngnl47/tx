package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	txmongo "github.com/ngnl47/tx/storage/mongo"
)

func TestMongoBuildUpdateDocumentUsesSet(t *testing.T) {
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	update := txmongo.BuildUpdateDocument(set)
	setDoc := update["$set"].(map[string]any)
	if setDoc["name"] != "alice" {
		t.Fatalf("update = %#v", update)
	}
}

func TestMongoBuildUpdateDocumentUsesClear(t *testing.T) {
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"deletedAt"}, Kind: change.KindClear})

	update := txmongo.BuildUpdateDocument(set)
	clearDoc := update["$unset"].(map[string]any)
	if clearDoc["deletedAt"] != "" {
		t.Fatalf("update = %#v", update)
	}
}

func TestMongoBuildUpdateDocumentReturnsEmptyForNoChanges(t *testing.T) {
	update := txmongo.BuildUpdateDocument(change.ChangeSet{})
	if len(update) != 0 {
		t.Fatalf("update = %#v, want empty", update)
	}
}

func TestMongoBuildUpdateDocumentWithDeltasUsesBSONNames(t *testing.T) {
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaKindSet, Value: "alice"},
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

	update, err := txmongo.BuildUpdateDocumentWithDeltas(spec, deltas, resolver)
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
		{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaKindSet, Value: int64(1001)},
	}
	resolver := testFieldNameResolver{
		change.NameBackendGo:   {deltaFieldName: "RoleId"},
		change.NameBackendBSON: {deltaFieldName: "_id"},
	}

	_, err := txmongo.BuildUpdateDocumentWithDeltas(meta.TypeSpec{}, deltas, resolver)
	if err == nil {
		t.Fatal("BuildUpdateDocumentWithDeltas returned nil error, want immutable _id error")
	}
}
