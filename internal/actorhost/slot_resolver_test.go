package actorhost

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/discovery"
)

func TestSlotResolverFailureStopsActivation(t *testing.T) {
	cause := errors.New("directory unavailable")
	loaded := false
	resolve := SlotResolver(func(ctx context.Context, id Identity) (string, error) { return "", cause })
	kind := MustType("role", func(Context) (*Behavior, error) { loaded = true; return &Behavior{}, nil })
	runtime := New(8, time.Second, kind.factory)
	runtime.BindSlots("host", initializationSlotReader(func(context.Context, string) (discovery.SlotAssignment, error) {
		t.Fatal("must not read ownership")
		return discovery.SlotAssignment{}, nil
	}), resolve)
	defer runtime.Drain(context.Background())
	future, err := runtime.Submit(Identity{Type: "role", ID: "1"}, "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := future.Await(context.Background()); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if loaded {
		t.Fatal("解析失败不能加载")
	}
}
