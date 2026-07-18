package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
)

type testFieldResolver map[change.FieldID]string

func (r testFieldResolver) FieldName(field change.FieldID) string {
	return r[field]
}

func TestFieldChangeFullPathUsesResolver(t *testing.T) {
	fieldChange := change.FieldChange{
		Field: change.FieldID(2),
		Kind:  change.KindSet,
		Value: "alice",
	}

	path := fieldChange.FullPath(testFieldResolver{2: "name"})
	if path.String() != "name" {
		t.Fatalf("path = %q, want name", path.String())
	}
}

func TestFieldChangeFullPathAppendsDynamicTail(t *testing.T) {
	fieldChange := change.FieldChange{
		Field: change.FieldID(3),
		Path:  change.Path{"1001", "quantity"},
		Kind:  change.KindSet,
		Value: int64(9),
	}

	path := fieldChange.FullPath(testFieldResolver{3: "items"})
	if path.String() != "items.1001.quantity" {
		t.Fatalf("path = %q, want items.1001.quantity", path.String())
	}
}

func TestTrackerRecordsFieldIDChangeWithoutPath(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.TouchField(change.FieldID(2), "alice"); err != nil {
		t.Fatalf("TouchField returned error: %v", err)
	}

	changes := tracker.Changes().Changes()
	if len(changes) != 1 {
		t.Fatalf("changes len = %d, want 1: %#v", len(changes), changes)
	}
	if changes[0].Field != change.FieldID(2) || len(changes[0].Path) != 0 {
		t.Fatalf("change target = %#v, want field-only target", changes[0])
	}
	if changes[0].FullPath(testFieldResolver{2: "name"}).String() != "name" {
		t.Fatalf("resolved path = %q, want name", changes[0].FullPath(testFieldResolver{2: "name"}).String())
	}
}

func TestTrackerUnTouchFieldRemovesFieldIDChange(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.TouchFieldPath(change.FieldID(3), change.Path{"1001"}, "sword"); err != nil {
		t.Fatalf("TouchFieldPath returned error: %v", err)
	}
	tracker.UnTouchFieldPath(change.FieldID(3), change.Path{"1001"})

	if !tracker.Changes().Empty() {
		t.Fatalf("changes = %#v, want empty", tracker.Changes().Changes())
	}
}

func TestTrackerParentFieldChangeCollapsesChildPathChanges(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.TouchFieldPath(change.FieldID(3), change.Path{"1001", "quantity"}, int64(3)); err != nil {
		t.Fatalf("TouchFieldPath returned error: %v", err)
	}
	if err := tracker.TouchFieldPath(change.FieldID(3), change.Path{"1002", "quantity"}, int64(4)); err != nil {
		t.Fatalf("TouchFieldPath returned error: %v", err)
	}
	if err := tracker.TouchField(change.FieldID(3), map[int64]int64{1001: 3, 1002: 4}); err != nil {
		t.Fatalf("TouchField returned error: %v", err)
	}

	changes := tracker.Changes().Changes()
	if len(changes) != 1 {
		t.Fatalf("changes len = %d, want parent field only: %#v", len(changes), changes)
	}
	if changes[0].Field != change.FieldID(3) || len(changes[0].Path) != 0 {
		t.Fatalf("change target = %#v, want parent field target", changes[0])
	}
	if got := changes[0].FullPath(testFieldResolver{3: "items"}).String(); got != "items" {
		t.Fatalf("resolved path = %q, want items", got)
	}
}

func TestTrackerChildPathChangeDoesNotExpandDirtyParentField(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.TouchField(change.FieldID(3), map[int64]int64{1001: 3}); err != nil {
		t.Fatalf("TouchField returned error: %v", err)
	}
	if err := tracker.TouchFieldPath(change.FieldID(3), change.Path{"1002", "quantity"}, int64(4)); err != nil {
		t.Fatalf("TouchFieldPath returned error: %v", err)
	}

	changes := tracker.Changes().Changes()
	if len(changes) != 1 {
		t.Fatalf("changes len = %d, want parent field only: %#v", len(changes), changes)
	}
	if changes[0].Field != change.FieldID(3) || len(changes[0].Path) != 0 {
		t.Fatalf("change target = %#v, want parent field target", changes[0])
	}
}
