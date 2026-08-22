package tests

import (
	"strings"
	"testing"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	txredis "github.com/ngnl47/tx/storage/redis"
)

func TestRedisBuildDocumentPatchesUsesJSONNames(t *testing.T) {
	source := recordedChanges{
		{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet},
		{Path: changePath(fieldNode(changeFieldItems)), Op: change.OpClear},
	}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {changeFieldName: "Name", changeFieldItems: "Items"},
		change.FieldNameJSON: {changeFieldName: "roleName", changeFieldItems: "items"},
	}

	patches, err := txredis.BuildDocumentPatches(redisPatchSpec(), source, resolver, testValueResolver{changeFieldName: "alice"})
	if err != nil {
		t.Fatalf("BuildDocumentPatches returned error: %v", err)
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

func TestRedisBuildDocumentPatchesRejectsMissingCurrentValue(t *testing.T) {
	source := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {changeFieldName: "Name"},
		change.FieldNameJSON: {changeFieldName: "name"},
	}
	_, err := txredis.BuildDocumentPatches(redisPatchSpec(), source, resolver, nil)
	if err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("BuildDocumentPatches error = %v, want unresolved name error", err)
	}
}

func TestRedisBuildDocumentPatchesRejectsUnresolvedFieldPath(t *testing.T) {
	const unknownField change.FieldID = 99
	source := recordedChanges{{Path: changePath(fieldNode(unknownField)), Op: change.OpSet}}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {changeFieldName: "Name"},
		change.FieldNameJSON: {changeFieldName: "name"},
	}

	_, err := txredis.BuildDocumentPatches(redisPatchSpec(), source, resolver, testValueResolver{unknownField: "alice"})
	if err == nil || !strings.Contains(err.Error(), "field path") {
		t.Fatalf("BuildDocumentPatches error = %v, want unresolved field path error", err)
	}
}

func TestRedisBuildDocumentPatchesRejectsUnknownOp(t *testing.T) {
	source := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.Op(99)}}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {changeFieldName: "Name"},
		change.FieldNameJSON: {changeFieldName: "name"},
	}

	_, err := txredis.BuildDocumentPatches(redisPatchSpec(), source, resolver, testValueResolver{changeFieldName: "alice"})
	if err == nil || !strings.Contains(err.Error(), "unknown operation") {
		t.Fatalf("BuildDocumentPatches error = %v, want unknown operation error", err)
	}
}

func TestRedisBuildDocumentPatchesRejectsUnknownKindForIgnoredField(t *testing.T) {
	source := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.Op(99)}}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {changeFieldName: "Name"},
		change.FieldNameJSON: {changeFieldName: "name"},
	}
	spec := meta.TypeSpec{Fields: map[string]meta.FieldSpec{
		"Name": {Redis: &meta.RedisFieldSpec{Ignore: true}},
	}}

	_, err := txredis.BuildDocumentPatches(spec, source, resolver, testValueResolver{changeFieldName: "alice"})
	if err == nil || !strings.Contains(err.Error(), "unknown operation") {
		t.Fatalf("BuildDocumentPatches error = %v, want unknown operation error", err)
	}
}

func TestRedisBuildDocumentPatchesAllowsResolvedNilValue(t *testing.T) {
	source := recordedChanges{{Path: changePath(fieldNode(changeFieldName)), Op: change.OpSet}}
	resolver := testFieldNameResolver{
		change.FieldNameGo:   {changeFieldName: "Name"},
		change.FieldNameJSON: {changeFieldName: "name"},
	}
	patches, err := txredis.BuildDocumentPatches(redisPatchSpec(), source, resolver, change.ValueResolverFunc(func(change.Path) (any, bool) {
		return nil, true
	}))
	if err != nil {
		t.Fatalf("BuildDocumentPatches returned error: %v", err)
	}
	if got := patches[""].Set[0].Value; got != nil {
		t.Fatalf("patch value = %#v, want nil", got)
	}
}

func TestRedisEncodeDocumentPatchAllowsSafeIntegers(t *testing.T) {
	_, err := txredis.EncodeDocumentPatch(txredis.DocumentPatch{
		Set: []txredis.PathValue{{Path: change.DocumentPath{"roleId"}, Value: int64(1<<53 - 1)}},
	})
	if err != nil {
		t.Fatalf("EncodeDocumentPatch returned error: %v", err)
	}
}

func TestRedisEncodeDocumentPatchRejectsUnsafeIntegers(t *testing.T) {
	_, err := txredis.EncodeDocumentPatch(txredis.DocumentPatch{
		Set: []txredis.PathValue{{Path: change.DocumentPath{"roleId"}, Value: int64(1 << 53)}},
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
