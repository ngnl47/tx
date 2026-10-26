package actorhost

import (
	"errors"
	"testing"
	"time"
)

func TestTypeFactoryKeepsDeclarationLazyAndMappingStable(t *testing.T) {
	loaded := 0
	factory := func(Context) (*Behavior, error) { loaded++; return &Behavior{}, nil }
	role := MustType("role", factory)
	world := MustType("world", factory, WithSlotMapping(func(id Identity) string { return "world-" + id.ID }), WithIdlePassivation(time.Minute))
	create, err := TypeFactory([]Type{role, world}, map[string]time.Duration{"role": time.Second, "world": time.Second})
	if err != nil || loaded != 0 {
		t.Fatalf("声明提前加载：%d, %v", loaded, err)
	}
	for _, test := range []struct {
		k    Type
		slot string
		idle time.Duration
	}{
		{role, PartitionSlot(Identity{Type: "role", ID: "1"}), time.Second},
		{world, "world-1", time.Minute},
	} {
		id := Identity{Type: test.k.Name(), ID: "1"}
		if got, err := SlotFor(t.Context(), test.k, id); err != nil || got != test.slot {
			t.Fatalf("slot = %q", got)
		}
		b, err := create(Context{Identity: id})
		if err != nil || b.IdlePassivation != test.idle {
			t.Fatalf("策略 = %+v, %v", b, err)
		}
	}
	if _, err := create(Context{Identity: Identity{Type: "room", ID: "1"}}); !errors.Is(err, ErrActivationDenied) || loaded != 2 {
		t.Fatalf("未注册实体不应创建：%d, %v", loaded, err)
	}
}
