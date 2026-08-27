package server

import "testing"

func TestNewLogicalSlotBindsStableSlotToRuntimeInstance(t *testing.T) {
	slot, err := newLogicalSlot("game-1", "pod-uid-a")
	if err != nil {
		t.Fatalf("NewLogicalSlot returned error: %v", err)
	}
	if slot.ID != "game-1" || slot.RuntimeInstanceID != "pod-uid-a" {
		t.Fatalf("slot = %#v", slot)
	}
	if _, err := newLogicalSlot("game/1", "pod-uid-a"); err == nil {
		t.Fatal("NewLogicalSlot accepted invalid slot ID")
	}
	if _, err := newLogicalSlot("game-1", ""); err == nil {
		t.Fatal("NewLogicalSlot accepted empty runtime instance ID")
	}
}
