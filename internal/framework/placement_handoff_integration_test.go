package framework

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	txactor "github.com/ngnl47/tx/modules/actor"
)

// 多个类型共享分组槽位；实例交接只更新槽位归属，不重做业务分组查询。
func TestPlacementGroupHandoffKeepsIdentityCache(t *testing.T) {
	config, _ := ownershipTestConfig(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	var queries atomic.Int32
	start := func(label string) *Server {
		t.Helper()
		factory := func(txactor.Context) (*txactor.Behavior, error) {
			return &txactor.Behavior{Receive: func(txactor.Context, any) (any, error) {
				return txactor.Result{MessageType: "host", Payload: []byte(label)}, nil
			}}, nil
		}
		spec := Spec{Name: "grouped", Placements: []Placement{{Name: "group", Factory: func(Context) (txactor.GroupResolver, error) {
			return func(context.Context, txactor.Identity) (string, error) {
				queries.Add(1)
				return "shared", nil
			}, nil
		}}}}
		for _, name := range []string{"member", "team", "group"} {
			spec.ActorTypes = append(spec.ActorTypes, txactor.MustType(name, factory, txactor.WithPlacement("group")))
		}
		app, err := New(config, WithService(testService{spec: spec}))
		if err != nil {
			t.Fatal(err)
		}
		if err := app.Start(ctx, RunOptions{Services: []string{"grouped"}, RPCListen: "127.0.0.1:0"}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			stop, done := context.WithTimeout(context.Background(), 5*time.Second)
			defer done()
			if err := app.Stop(stop); err != nil {
				t.Error(err)
			}
		})
		return app
	}
	ids := []txactor.Identity{{Type: "member", ID: "1"}, {Type: "team", ID: "2"}, {Type: "group", ID: "3"}}
	call := func(app *Server, want string) {
		t.Helper()
		for _, id := range ids {
			result, err := app.Context().Actor().Call(ctx, id, txactor.Delivery{MessageType: "read"})
			if err != nil || string(result.Payload) != want {
				t.Fatalf("%+v: host=%s want=%s err=%v", id, result.Payload, want, err)
			}
		}
	}
	a := start("a")
	call(a, "a")
	slot := txactor.PlacementSlot("grouped", "group", "shared")
	current, err := a.discovery.AuthoritativeSlots().ResolveSlot(ctx, slot)
	if err != nil || current.RuntimeInstanceID != a.runtime.instanceSnapshot().ID {
		t.Fatal(fmt.Sprint(current), err)
	}
	b := start("b")
	waitOwnership(t, ctx, func() bool { return len(b.discovery.Directory().RunningInstances("grouped")) == 2 })
	call(b, "a") // 跨实例 Call 仍落到相同槽位的旧 owner，三个身份均已缓存。
	before := queries.Load()
	next, err := b.discovery.Ownership().Handoff(ctx, "grouped", current, b.runtime.instanceSnapshot().ID,
		func(context.Context, string) (bool, error) { return true, nil }, a.PrepareHandoff)
	if err != nil {
		t.Fatal(err)
	}
	waitOwnership(t, ctx, func() bool {
		got, err := b.discovery.Slots().ResolveSlot(ctx, slot)
		return err == nil && got == next
	})
	call(b, "b")
	if queries.Load() != before {
		t.Fatalf("交接不应使 Identity→SlotID 缓存失效: before=%d after=%d", before, queries.Load())
	}
}
