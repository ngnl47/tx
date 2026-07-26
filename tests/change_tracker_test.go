package tests

import (
	"testing"

	"github.com/ngnl47/tx/change"
)

func TestTrackerRecordsFieldChanges(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.Touch(change.Path{"role", "name"}, "alice"); err != nil {
		t.Fatalf("Touch returned error: %v", err)
	}
	if err := tracker.Replace(change.Path{"role", "items"}, map[int32]int64{1: 2}); err != nil {
		t.Fatalf("Replace returned error: %v", err)
	}

	changes := tracker.Changes().Changes()
	if len(changes) != 2 {
		t.Fatalf("changes len = %d, want 2: %#v", len(changes), changes)
	}
	if changes[0].Kind != change.KindSet || changes[0].Path.String() != "role.name" || changes[0].Value != "alice" {
		t.Fatalf("first change = %#v", changes[0])
	}
	if changes[1].Kind != change.KindReplace || changes[1].Path.String() != "role.items" {
		t.Fatalf("second change = %#v", changes[1])
	}
}

func TestTrackerUnTouchRemovesFieldChange(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.Touch(change.Path{"role", "name"}, "alice"); err != nil {
		t.Fatalf("Touch returned error: %v", err)
	}
	tracker.UnTouch(change.Path{"role", "name"})

	if !tracker.Changes().Empty() {
		t.Fatalf("changes = %#v, want empty", tracker.Changes().Changes())
	}
}

func TestTrackerParentPathChangeCollapsesChildPathChanges(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.Touch(change.Path{"items", "1001", "quantity"}, int64(3)); err != nil {
		t.Fatalf("Touch returned error: %v", err)
	}
	if err := tracker.Touch(change.Path{"items", "1002", "quantity"}, int64(4)); err != nil {
		t.Fatalf("Touch returned error: %v", err)
	}
	if err := tracker.Touch(change.Path{"items"}, map[int64]int64{1001: 3, 1002: 4}); err != nil {
		t.Fatalf("Touch returned error: %v", err)
	}

	changes := tracker.Changes().Changes()
	if len(changes) != 1 {
		t.Fatalf("changes len = %d, want parent path only: %#v", len(changes), changes)
	}
	if got := changes[0].Path.String(); got != "items" {
		t.Fatalf("change path = %q, want items", got)
	}
}

func TestTrackerChildPathChangeDoesNotExpandDirtyParentPath(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.Touch(change.Path{"items"}, map[int64]int64{1001: 3}); err != nil {
		t.Fatalf("Touch returned error: %v", err)
	}
	if err := tracker.Touch(change.Path{"items", "1002", "quantity"}, int64(4)); err != nil {
		t.Fatalf("Touch returned error: %v", err)
	}

	changes := tracker.Changes().Changes()
	if len(changes) != 1 {
		t.Fatalf("changes len = %d, want parent path only: %#v", len(changes), changes)
	}
	if got := changes[0].Path.String(); got != "items" {
		t.Fatalf("change path = %q, want items", got)
	}
}

func TestTrackerRepeatedParentPathChangeReplacesValue(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.Touch(change.Path{"items"}, map[int64]int64{1001: 3}); err != nil {
		t.Fatalf("Touch returned error: %v", err)
	}
	if err := tracker.Touch(change.Path{"items"}, map[int64]int64{1002: 4}); err != nil {
		t.Fatalf("Touch returned error: %v", err)
	}

	changes := tracker.Changes().Changes()
	if len(changes) != 1 {
		t.Fatalf("changes len = %d, want one parent path: %#v", len(changes), changes)
	}
	values, ok := changes[0].Value.(map[int64]int64)
	if !ok || values[1002] != 4 {
		t.Fatalf("change value = %#v, want replacement map", changes[0].Value)
	}
}

func TestTrackerAllTouchAndAllUnTouch(t *testing.T) {
	var tracker change.Tracker

	tracker.AllTouch()
	if !tracker.AllDirty() {
		t.Fatalf("AllDirty = false, want true")
	}
	tracker.AllUnTouch()
	if tracker.AllDirty() || !tracker.Changes().Empty() {
		t.Fatalf("tracker was not cleared")
	}
}

func TestTrackerAllTouchClearsFieldChanges(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.Touch(change.Path{"items", "1001", "quantity"}, int64(3)); err != nil {
		t.Fatalf("Touch returned error: %v", err)
	}
	if err := tracker.TouchFieldPath(change.FieldID(3), change.Path{"1002", "quantity"}, int64(4)); err != nil {
		t.Fatalf("TouchFieldPath returned error: %v", err)
	}

	tracker.AllTouch()

	if !tracker.AllDirty() {
		t.Fatal("AllDirty = false, want true")
	}
	if !tracker.Changes().Empty() {
		t.Fatalf("changes = %#v, want empty after AllTouch", tracker.Changes().Changes())
	}
}

func TestTrackerRejectsEmptyPath(t *testing.T) {
	var tracker change.Tracker

	if err := tracker.Touch(nil, "alice"); err != change.ErrEmptyPath {
		t.Fatalf("err = %v, want ErrEmptyPath", err)
	}
}
