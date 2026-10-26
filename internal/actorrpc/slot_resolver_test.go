package actorrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/internal/actorhost"
)

// 归属数据失败必须发生在访问 etcd、容量探测与发消息之前。
func TestSlotResolverErrorsStopAllRouting(t *testing.T) {
	cause := errors.New("missing origin")
	router := &EntityRouter{Service: func(string) (string, error) { return "game", nil }, SlotID: func(ctx context.Context, _ actorhost.Identity) (string, error) {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", cause
	}}
	id := actorhost.Identity{Type: "role", ID: "1"}
	if _, err := router.Resolve(t.Context(), id); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if _, _, err := router.Ensure(t.Context(), id); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if _, err := router.Probe(t.Context(), id); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := router.Resolve(ctx, id); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	router.SlotID = func(context.Context, actorhost.Identity) (string, error) { return "", nil }
	if _, err := router.Resolve(t.Context(), id); err == nil {
		t.Fatal("空结果不能回退")
	}
}

func TestDifferentTypesShareSlotAndKeepMailboxes(t *testing.T) {
	mapping := actorhost.SlotMapping(func(id actorhost.Identity) string {
		if id.ID == "missing-zone" {
			return ""
		}
		return "game-1001"
	})
	testSharedPartition(t, mapping, []actorhost.Identity{{Type: "role", ID: "1"}, {Type: "legion", ID: "2"}, {Type: "server", ID: "1001"}}, "game-1001")
}
