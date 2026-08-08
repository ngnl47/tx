package tests

import (
	"strings"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	txredis "github.com/ngnl47/tx/storage/redis"
)

func TestRedisBuildDocumentPatchesWithDeltasUsesJSONNames(t *testing.T) {
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaKindSet},
		{Path: deltaPath(fieldNode(deltaFieldItems)), Kind: change.DeltaClear},
	}
	resolver := testFieldNameResolver{
		change.NameBackendGo:   {deltaFieldName: "Name", deltaFieldItems: "Items"},
		change.NameBackendJSON: {deltaFieldName: "roleName", deltaFieldItems: "items"},
	}

	patches, err := txredis.BuildDocumentPatchesWithDeltas(redisPatchSpec(), deltas, resolver, testDeltaValueResolver{deltaFieldName: "alice"})
	if err != nil {
		t.Fatalf("BuildDocumentPatchesWithDeltas returned error: %v", err)
	}

	if patches[""].Set[0].Path.String() != "roleName" || patches[""].Set[0].Value != "alice" {
		t.Fatalf("main patch = %#v", patches[""])
	}
	if patches["items"].Set[0].Path.String() != "items" {
		t.Fatalf("items patch path = %q, want items", patches["items"].Set[0].Path.String())
	}
	if _, ok := patches["items"].Set[0].Value.(map[string]any); !ok {
		t.Fatalf("items clear value = %#v, want empty document", patches["items"].Set[0].Value)
	}
}

func TestRedisBuildDocumentPatchesWithDeltasRejectsMissingCurrentValue(t *testing.T) {
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaKindSet}}
	resolver := testFieldNameResolver{
		change.NameBackendGo:   {deltaFieldName: "Name"},
		change.NameBackendJSON: {deltaFieldName: "name"},
	}
	_, err := txredis.BuildDocumentPatchesWithDeltas(redisPatchSpec(), deltas, resolver, nil)
	if err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("BuildDocumentPatchesWithDeltas error = %v, want unresolved name error", err)
	}
}

func TestRedisBuildDocumentPatchesWithDeltasRejectsUnresolvedFieldPath(t *testing.T) {
	const unknownField change.FieldID = 99
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(unknownField)), Kind: change.DeltaKindSet}}
	resolver := testFieldNameResolver{
		change.NameBackendGo:   {deltaFieldName: "Name"},
		change.NameBackendJSON: {deltaFieldName: "name"},
	}

	_, err := txredis.BuildDocumentPatchesWithDeltas(redisPatchSpec(), deltas, resolver, testDeltaValueResolver{unknownField: "alice"})
	if err == nil || !strings.Contains(err.Error(), "field path") {
		t.Fatalf("BuildDocumentPatchesWithDeltas error = %v, want unresolved field path error", err)
	}
}

func TestRedisBuildDocumentPatchesWithDeltasAllowsResolvedNilValue(t *testing.T) {
	deltas := change.DeltaSet{{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaKindSet}}
	resolver := testFieldNameResolver{
		change.NameBackendGo:   {deltaFieldName: "Name"},
		change.NameBackendJSON: {deltaFieldName: "name"},
	}
	patches, err := txredis.BuildDocumentPatchesWithDeltas(redisPatchSpec(), deltas, resolver, change.DeltaValueResolverFunc(func(change.DeltaPath) (any, bool) {
		return nil, true
	}))
	if err != nil {
		t.Fatalf("BuildDocumentPatchesWithDeltas returned error: %v", err)
	}
	if got := patches[""].Set[0].Value; got != nil {
		t.Fatalf("patch value = %#v, want nil", got)
	}
}

func TestRedisBuildDocumentPatchesWithDeltasDoesNotMutateSource(t *testing.T) {
	deltas := change.DeltaSet{
		{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaKindSet},
		{Path: deltaPath(fieldNode(deltaFieldName)), Kind: change.DeltaKindSet},
	}
	resolver := testFieldNameResolver{
		change.NameBackendGo:   {deltaFieldName: "Name"},
		change.NameBackendJSON: {deltaFieldName: "name"},
	}
	if _, err := txredis.BuildDocumentPatchesWithDeltas(redisPatchSpec(), deltas, resolver, testDeltaValueResolver{deltaFieldName: "alice"}); err != nil {
		t.Fatalf("BuildDocumentPatchesWithDeltas returned error: %v", err)
	}
	if len(deltas) != 2 || deltas[0].Path.Depth == 0 {
		t.Fatalf("source deltas mutated: %#v", deltas)
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
			"Items": {Redis: &meta.RedisFieldSpec{SubKey: "items"}},
		},
	}
}
