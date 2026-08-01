package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
)

func TestChangeSetFilterKeepsMatchingChanges(t *testing.T) {
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})
	_ = set.Add(change.FieldChange{Path: change.Path{"secret"}, Kind: change.KindSet, Value: "hidden"})

	filtered := set.Filter(change.FilterFunc(func(fieldChange change.FieldChange) bool {
		return fieldChange.Path.String() != "secret"
	}))

	changes := filtered.Changes()
	if len(changes) != 1 || changes[0].Path.String() != "name" {
		t.Fatalf("filtered changes = %#v", changes)
	}
}

func TestChangeSetDocumentSplitsSetAndUnset(t *testing.T) {
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})
	_ = set.Add(change.FieldChange{Path: change.Path{"deletedAt"}, Kind: change.KindClear})

	doc := set.Document()
	if doc.Set["name"] != "alice" {
		t.Fatalf("set doc = %#v", doc.Set)
	}
	if _, ok := doc.Clear["deletedAt"]; !ok {
		t.Fatalf("clear doc = %#v", doc.Clear)
	}
}
