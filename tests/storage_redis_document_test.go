package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
	txredis "github.com/ngnl47/tx/storage/redis"
)

func TestRedisBuildHashChangesGroupsByTopLevelPath(t *testing.T) {
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"items", "1", "quantity"}, Kind: change.KindSet, Value: int64(5)})

	hashes := txredis.BuildHashChanges(set)
	if hashes["items"]["1.quantity"] != int64(5) {
		t.Fatalf("hashes = %#v", hashes)
	}
}

func TestRedisBuildHashChangesKeepsTopLevelScalar(t *testing.T) {
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	hashes := txredis.BuildHashChanges(set)
	if hashes["name"]["name"] != "alice" {
		t.Fatalf("hashes = %#v", hashes)
	}
}

func TestRedisBuildHashDeletesGroupsUnsetChanges(t *testing.T) {
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"items", "1", "quantity"}, Kind: change.KindUnset})

	deletes := txredis.BuildHashDeletes(set)
	if len(deletes["items"]) != 1 || deletes["items"][0] != "1.quantity" {
		t.Fatalf("deletes = %#v", deletes)
	}
}
