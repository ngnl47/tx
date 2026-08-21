package tests

import (
	"strings"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage/redis"
)

func TestRedisPatchMapsGoFieldNameToJSONName(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{Fields: []meta.Field{{
			Name:     "Power",
			JSONName: "p",
			Type:     "int64",
			Exported: true,
		}}},
		Fields: map[string]meta.FieldSpec{"Power": {}},
	}
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(1)), Op: change.OpSet}}
	resolver := testNamedFieldResolver(map[change.FieldNameKind]map[change.FieldID]string{
		change.FieldNameGo:   {1: "Power"},
		change.FieldNameJSON: {1: "p"},
	})

	patches, err := redis.BuildDocumentPatchesWithDeltas(spec, deltas, resolver, testDeltaValueResolver{1: int64(100)})
	if err != nil {
		t.Fatalf("BuildDocumentPatchesWithDeltas returned error: %v", err)
	}
	patch := patches[""]
	if len(patch.Set) != 1 {
		t.Fatalf("patch.Set length = %d, want 1", len(patch.Set))
	}
	if got := patch.Set[0].Path.String(); got != "p" {
		t.Fatalf("Redis path = %q, want p", got)
	}
}

func TestRedisPatchUsesSubKeyAndJSONName(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{Fields: []meta.Field{{
			Name:     "Items",
			JSONName: "it",
			Type:     "map[int32]int64",
			Exported: true,
		}}},
		Fields: map[string]meta.FieldSpec{
			"Items": {Redis: &meta.RedisFieldSpec{SubKey: "bag"}},
		},
	}
	deltas := change.DeltaSet{{Path: deltaPath(intKeyNode(1, 1001), fieldNode(2)), Op: change.OpSet}}
	resolver := testNamedFieldResolver(map[change.FieldNameKind]map[change.FieldID]string{
		change.FieldNameGo:   {1: "Items", 2: "Quantity"},
		change.FieldNameJSON: {1: "it", 2: "quantity"},
	})

	patches, err := redis.BuildDocumentPatchesWithDeltas(spec, deltas, resolver, change.DeltaValueResolverFunc(func(path change.DeltaPath) (any, bool) {
		if path.Equal(deltaPath(intKeyNode(1, 1001), fieldNode(2))) {
			return int64(9), true
		}
		return nil, false
	}))
	if err != nil {
		t.Fatalf("BuildDocumentPatchesWithDeltas returned error: %v", err)
	}
	patch := patches["bag"]
	if len(patch.Set) != 1 {
		t.Fatalf("patch.Set length = %d, want 1", len(patch.Set))
	}
	if got := patch.Set[0].Path.String(); got != "it.1001.quantity" {
		t.Fatalf("Redis path = %q, want it.1001.quantity", got)
	}
}

func TestRedisPatchUsesOperationsForSliceElementField(t *testing.T) {
	spec := meta.TypeSpec{
		Type: meta.Type{Fields: []meta.Field{{
			Name:     "Queue",
			JSONName: "queue",
			Type:     "[]*Item",
			Exported: true,
		}}},
		Fields: map[string]meta.FieldSpec{
			"Queue": {Redis: &meta.RedisFieldSpec{SubKey: "queue"}},
		},
	}
	delta := deltaPath(intKeyNode(1, 0), fieldNode(2))
	resolver := testNamedFieldResolver(map[change.FieldNameKind]map[change.FieldID]string{
		change.FieldNameGo:   {1: "Queue", 2: "Count"},
		change.FieldNameJSON: {1: "queue", 2: "count"},
	})

	patches, err := redis.BuildDocumentPatchesWithDeltas(spec, change.DeltaSet{{Path: delta, Op: change.OpSet}}, resolver, change.DeltaValueResolverFunc(func(path change.DeltaPath) (any, bool) {
		if path.Equal(delta) {
			return int64(8), true
		}
		return nil, false
	}))
	if err != nil {
		t.Fatalf("BuildDocumentPatchesWithDeltas returned error: %v", err)
	}
	body, err := redis.EncodeDocumentPatch(patches["queue"])
	if err != nil {
		t.Fatalf("EncodeDocumentPatch returned error: %v", err)
	}
	for _, want := range []string{`"operations"`, `"path":["queue","0","count"]`, `"value":8`} {
		if !strings.Contains(body, want) {
			t.Fatalf("encoded slice patch = %s, want %s", body, want)
		}
	}
}
