package tests

import (
	"strings"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	txredis "github.com/ngnl47/tx/storage/redis"
)

func TestRedisBuildDocumentPatchesGroupsSubKeyPath(t *testing.T) {
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"items", "1", "quantity"}, Kind: change.KindSet, Value: int64(5)})

	patches := txredis.BuildDocumentPatches(redisPatchSpec(), set)
	if patches["items"].Set[0].Path.String() != "items.1.quantity" || patches["items"].Set[0].Value != int64(5) {
		t.Fatalf("patches = %#v", patches)
	}
}

func TestRedisBuildDocumentPatchesKeepsTopLevelScalarInMainKey(t *testing.T) {
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"name"}, Kind: change.KindSet, Value: "alice"})

	patches := txredis.BuildDocumentPatches(redisPatchSpec(), set)
	if patches[""].Set[0].Path.String() != "name" || patches[""].Set[0].Value != "alice" {
		t.Fatalf("patches = %#v", patches)
	}
}

func TestRedisBuildDocumentPatchesGroupsUnsetChanges(t *testing.T) {
	var set change.ChangeSet
	_ = set.Add(change.FieldChange{Path: change.Path{"items", "1", "quantity"}, Kind: change.KindUnset})

	patches := txredis.BuildDocumentPatches(redisPatchSpec(), set)
	if len(patches["items"].Unset) != 1 || patches["items"].Unset[0].String() != "items.1.quantity" {
		t.Fatalf("patches = %#v", patches)
	}
}

func TestRedisEncodeDocumentPatchAllowsSafeIntegers(t *testing.T) {
	_, err := txredis.EncodeDocumentPatch(txredis.DocumentPatch{
		Set: []txredis.PathValue{{Path: change.Path{"roleId"}, Value: int64(1<<53 - 1)}},
	})
	if err != nil {
		t.Fatalf("EncodeDocumentPatch returned error: %v", err)
	}
}

func TestRedisEncodeDocumentPatchRejectsUnsafeIntegers(t *testing.T) {
	_, err := txredis.EncodeDocumentPatch(txredis.DocumentPatch{
		Set: []txredis.PathValue{{Path: change.Path{"roleId"}, Value: int64(1 << 53)}},
	})
	if err == nil {
		t.Fatal("EncodeDocumentPatch returned nil error, want unsafe integer error")
	}
	if !strings.Contains(err.Error(), "unsafe integer") {
		t.Fatalf("EncodeDocumentPatch error = %v, want unsafe integer", err)
	}
}

func redisPatchSpec() meta.TypeSpec {
	return meta.TypeSpec{
		Fields: map[string]meta.FieldSpec{
			"items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
}
