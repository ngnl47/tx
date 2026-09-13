package actorhost

import (
	"errors"
	"testing"
	"time"
)

func TestKindFactoryKeepsDeclarationLazyAndMappingStable(t *testing.T) {
	loaded := 0
	factory := func(Context) (*Behavior, error) { loaded++; return &Behavior{}, nil }
	role := MustKind("role", factory)
	world := MustKind("world", factory, WithSlotMapping(func(id Identity) string { return "world-" + id.Key }), WithIdlePassivation(time.Minute))
	create, err := KindFactory([]Kind{role, world}, map[string]time.Duration{"role": time.Second, "world": time.Second})
	if err != nil || loaded != 0 {
		t.Fatalf("声明提前加载：%d, %v", loaded, err)
	}
	for _, test := range []struct {
		k    Kind
		slot string
		idle time.Duration
	}{
		{role, PartitionSlot(Identity{Kind: "role", Key: "1"}), time.Second},
		{world, "world-1", time.Minute},
	} {
		id := Identity{Kind: test.k.Name(), Key: "1"}
		if got := SlotFor(test.k, id); got != test.slot {
			t.Fatalf("slot = %q", got)
		}
		b, err := create(Context{Identity: id})
		if err != nil || b.IdlePassivation != test.idle {
			t.Fatalf("策略 = %+v, %v", b, err)
		}
	}
	if _, err := create(Context{Identity: Identity{Kind: "room", Key: "1"}}); !errors.Is(err, ErrActivationDenied) || loaded != 2 {
		t.Fatalf("未注册实体不应创建：%d, %v", loaded, err)
	}
}
