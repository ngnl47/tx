package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/storage"
)

func TestChangeSetToDocumentUsesDotPathKeys(t *testing.T) {
	var set change.ChangeSet
	if err := set.Add(change.FieldChange{Path: change.Path{"items", "1", "quantity"}, Kind: change.KindSet, Value: int64(3)}); err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	doc := storage.ChangeSetToDocument(set)
	if doc["items.1.quantity"] != int64(3) {
		t.Fatalf("doc = %#v", doc)
	}
}
