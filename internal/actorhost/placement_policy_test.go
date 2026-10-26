package actorhost

import (
	"testing"
)

func TestPlacementGroupsCrossTypesAndSeparatePools(t *testing.T) {
	factory := func(Context) (*Behavior, error) { return &Behavior{}, nil }
	for _, name := range []string{"role", "legion", "server"} {
		typ, err := NewType(name, factory, WithPlacement("server"))
		if err != nil || PlacementName(typ) != "server" {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := SlotFor(t.Context(), typ, Identity{Type: name, ID: "7"}); err == nil {
			t.Fatal("声明策略后不能回退默认分区")
		}
		if _, err := NewType(name, factory, WithPlacement("server"), WithSlotMapping(func(Identity) string { return "x" })); err == nil {
			t.Fatal("不能同时声明两种放置规则")
		}
	}
	a := PlacementSlot("game", "server", "1001")
	if a == PlacementSlot("fightroom", "server", "1001") || a == PlacementSlot("game", "room", "1001") || a == PlacementSlot("game", "server", "1002") {
		t.Fatal("放置域冲突")
	}
	if a == PartitionSlot(Identity{Type: "role", ID: "1001"}) {
		t.Fatal("与默认分区冲突")
	}
}
