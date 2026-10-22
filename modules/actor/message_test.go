package actor

import (
	"context"
	"errors"
	"testing"
)

func TestMessageTableSharesBindingsAndIsolatesActivationState(t *testing.T) {
	calls := 0
	table := NewMessageTable("world", []MessageBinding{{Name: "world.step", Local: true, Invoke: func(ctx MessageContext, payload []byte) (Result, error) {
		*ctx.State.(*int)++
		calls++
		return Result{}, nil
	}}})
	a, b := 0, 0
	ctx := Context{Context: context.Background(), Identity: Identity{Type: "world", ID: "i:1"}}
	_, matched, err := table.Dispatch(ctx, &a, Delivery{MessageType: "world.step"})
	if !matched || !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("remote local-only: %v %v", matched, err)
	}
	for _, state := range []*int{&a, &a, &b} {
		if _, _, err := table.Dispatch(ctx, state, NewLocalMessage("world.step", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if a != 2 || b != 1 || calls != 3 {
		t.Fatalf("state: %d %d calls %d", a, b, calls)
	}
}
