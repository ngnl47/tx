package tests

import (
	"reflect"
	"testing"

	"github.com/ngnl47/tx/change"
)

func TestMergeDocumentsOverwritesRepeatedSet(t *testing.T) {
	merged, ok := change.MergeDocuments(
		change.Document{Set: map[string]any{"name": "old"}},
		change.Document{Set: map[string]any{"name": "new"}},
	)
	if !ok {
		t.Fatal("MergeDocuments returned ok=false")
	}
	if merged.Set["name"] != "new" {
		t.Fatalf("merged set = %#v", merged.Set)
	}
}

func TestMergeDocumentsMergesNestedMapValues(t *testing.T) {
	merged, ok := change.MergeDocuments(
		change.Document{Set: map[string]any{"battle": map[string]any{"hp": int64(10)}}},
		change.Document{Set: map[string]any{"battle": map[string]any{"mp": int64(5)}}},
	)
	if !ok {
		t.Fatal("MergeDocuments returned ok=false")
	}
	want := map[string]any{"hp": int64(10), "mp": int64(5)}
	if !reflect.DeepEqual(merged.Set["battle"], want) {
		t.Fatalf("merged battle = %#v", merged.Set["battle"])
	}
}

func TestMergeDocumentsRejectsChildPatchAfterScalarSet(t *testing.T) {
	_, ok := change.MergeDocuments(
		change.Document{Set: map[string]any{"battle": int64(1)}},
		change.Document{Set: map[string]any{"battle.hp": int64(10)}},
	)
	if ok {
		t.Fatal("MergeDocuments returned ok=true")
	}
}

func TestMergeDocumentsParentSetOverridesChildChanges(t *testing.T) {
	merged, ok := change.MergeDocuments(
		change.Document{Set: map[string]any{"items.1001.quantity": int64(3)}},
		change.Document{Set: map[string]any{"items": map[string]any{"1001": map[string]any{"quantity": int64(5)}}}},
	)
	if !ok {
		t.Fatal("MergeDocuments returned ok=false")
	}
	if _, ok := merged.Set["items.1001.quantity"]; ok {
		t.Fatalf("child set was not removed: %#v", merged.Set)
	}
	want := map[string]any{"1001": map[string]any{"quantity": int64(5)}}
	if !reflect.DeepEqual(merged.Set["items"], want) {
		t.Fatalf("merged items = %#v", merged.Set["items"])
	}
}

func TestMergeDocumentsClearRemovesCoveredSets(t *testing.T) {
	merged, ok := change.MergeDocuments(
		change.Document{Set: map[string]any{"items.1001.quantity": int64(3)}},
		change.Document{Clear: map[string]struct{}{"items": {}}},
	)
	if !ok {
		t.Fatal("MergeDocuments returned ok=false")
	}
	if len(merged.Set) != 0 {
		t.Fatalf("set doc = %#v", merged.Set)
	}
	if _, ok := merged.Clear["items"]; !ok {
		t.Fatalf("clear doc = %#v", merged.Clear)
	}
}

func TestMergeDocumentsRejectsSetBelowClearParent(t *testing.T) {
	_, ok := change.MergeDocuments(
		change.Document{Clear: map[string]struct{}{"items": {}}},
		change.Document{Set: map[string]any{"items.1001.quantity": int64(3)}},
	)
	if ok {
		t.Fatal("MergeDocuments returned ok=true")
	}
}

func TestCompactDocumentsMergesUntilFirstUnsafeBoundary(t *testing.T) {
	compacted := change.CompactDocuments([]change.Document{
		{Set: map[string]any{"name": "old"}},
		{Set: map[string]any{"name": "new"}},
		{Set: map[string]any{"battle": int64(1)}},
		{Set: map[string]any{"battle.hp": int64(10)}},
		{Set: map[string]any{"battle.mp": int64(5)}},
	})

	if len(compacted) != 2 {
		t.Fatalf("compacted len = %d, docs = %#v", len(compacted), compacted)
	}
	if compacted[0].Set["name"] != "new" || compacted[0].Set["battle"] != int64(1) {
		t.Fatalf("first compacted doc = %#v", compacted[0])
	}
	wantSecond := map[string]any{
		"battle.hp": int64(10),
		"battle.mp": int64(5),
	}
	if !reflect.DeepEqual(compacted[1].Set, wantSecond) {
		t.Fatalf("second compacted set = %#v", compacted[1].Set)
	}
}
