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

func TestTypeFactoryIdlePassivationPrecedence(t *testing.T) {
	for _, test := range []struct {
		name    string
		options []TypeOption
		runtime time.Duration
		want    time.Duration
	}{
		{"未指定保留行为", nil, 0, time.Minute},
		{"未指定使用运行时", nil, time.Second, time.Second},
		{"类型正值优先", []TypeOption{WithIdlePassivation(2 * time.Second)}, time.Second, 2 * time.Second},
		{"显式禁用覆盖行为", []TypeOption{WithIdlePassivation(0)}, 0, 0},
		{"显式禁用覆盖运行时", []TypeOption{WithIdlePassivation(0)}, time.Second, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			kind, err := NewType("world", func(Context) (*Behavior, error) {
				return &Behavior{IdlePassivation: time.Minute}, nil
			}, test.options...)
			if err != nil {
				t.Fatal(err)
			}
			idle := map[string]time.Duration{}
			if test.runtime > 0 {
				idle[kind.Name()] = test.runtime
			}
			factory, err := TypeFactory([]Type{kind}, idle)
			if err != nil {
				t.Fatal(err)
			}
			behavior, err := factory(Context{Identity: Identity{Type: "world", ID: "1"}})
			if err != nil || behavior.IdlePassivation != test.want {
				t.Fatalf("空闲策略 = %+v, %v；期望 %s", behavior, err, test.want)
			}
		})
	}
}

func TestTypeIdlePassivationValidation(t *testing.T) {
	for _, timeout := range []time.Duration{-time.Second, time.Nanosecond, time.Millisecond - 1, 0, time.Millisecond} {
		t.Run(timeout.String(), func(t *testing.T) {
			_, err := NewType("world", func(Context) (*Behavior, error) { return &Behavior{}, nil }, WithIdlePassivation(timeout))
			valid := timeout == 0 || timeout == time.Millisecond
			if valid && err != nil || !valid && !errors.Is(err, ErrInvalidType) {
				t.Fatalf("空闲策略 %s 的校验错误：%v", timeout, err)
			}
		})
	}
}

func TestExplicitAssignmentTypeOption(t *testing.T) {
	factory := func(Context) (*Behavior, error) { return &Behavior{}, nil }
	ordinary, err := NewType("room", factory)
	if err != nil || ordinary.RequiresExplicitAssignment() {
		t.Fatal(err)
	}
	declared, err := NewType("world", factory, WithExplicitAssignment())
	if err != nil || !declared.RequiresExplicitAssignment() {
		t.Fatal(err)
	}
}
