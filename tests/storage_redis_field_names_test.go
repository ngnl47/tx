package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage/redis"
)

func TestRedisPatchMapsGoFieldNameToRedisName(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{Fields: []meta.Field{{
			Name:      "Power",
			JSONName:  "power",
			RedisName: "p",
			Type:      "int64",
			Exported:  true,
		}}},
		Fields: map[string]meta.FieldSpec{"Power": {}},
	}
	var set change.ChangeSet
	if err := set.Add(change.FieldChange{
		Field: change.FieldID(1),
		Kind:  change.KindSet,
		Value: int64(100),
	}); err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	patches := redis.BuildDocumentPatchesWithResolver(spec, set, testFieldResolver{1: "Power"})
	patch := patches[""]
	if len(patch.Set) != 1 {
		t.Fatalf("patch.Set length = %d, want 1", len(patch.Set))
	}
	if got := patch.Set[0].Path.String(); got != "p" {
		t.Fatalf("Redis path = %q, want p", got)
	}
}

func TestRedisPatchUsesSubKeyAndRedisName(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{Fields: []meta.Field{{
			Name:      "Items",
			JSONName:  "items",
			RedisName: "it",
			Type:      "map[int32]int64",
			Exported:  true,
		}}},
		Fields: map[string]meta.FieldSpec{
			"Items": {Redis: &meta.RedisFieldSpec{SubKey: "bag"}},
		},
	}
	var set change.ChangeSet
	if err := set.Add(change.FieldChange{
		Field: change.FieldID(1),
		Path:  change.Path{"1001", "quantity"},
		Kind:  change.KindSet,
		Value: int64(9),
	}); err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	patches := redis.BuildDocumentPatchesWithResolver(spec, set, testFieldResolver{1: "Items"})
	patch := patches["bag"]
	if len(patch.Set) != 1 {
		t.Fatalf("patch.Set length = %d, want 1", len(patch.Set))
	}
	if got := patch.Set[0].Path.String(); got != "it.1001.quantity" {
		t.Fatalf("Redis path = %q, want it.1001.quantity", got)
	}
}
