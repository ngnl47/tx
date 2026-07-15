package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
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

func TestMongoBuildUpdateDocumentUsesUnset(t *testing.T) {
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"deletedAt"}, Kind: change.KindUnset})

	update := txmongo.BuildUpdateDocument(set)
	unsetDoc := update["$unset"].(map[string]any)
	if unsetDoc["deletedAt"] != "" {
		t.Fatalf("update = %#v", update)
	}
}

func TestMongoBuildUpdateDocumentReturnsEmptyForNoChanges(t *testing.T) {
	update := txmongo.BuildUpdateDocument(change.ChangeSet{})
	if len(update) != 0 {
		t.Fatalf("update = %#v, want empty", update)
	}
}
