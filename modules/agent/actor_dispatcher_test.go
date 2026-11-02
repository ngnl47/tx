package agent

import (
	"context"
	"testing"

	"github.com/ngnl47/tx/modules/actor"
)

type actorTellerFunc func(context.Context, actor.Identity, actor.Delivery) error

func (f actorTellerFunc) Tell(c context.Context, id actor.Identity, d actor.Delivery) error {
	return f(c, id, d)
}

func TestActorDispatcherBindsIdentityAndWritesStream(t *testing.T) {
	request := DispatchRequest{}
	request.Identity.RoleID = 42
	dispatch, err := NewActorDispatcher(ActorDispatcherOptions{
		Resolver: ActorDispatchResolverFunc(func(context.Context, DispatchRequest) (ActorDispatch, error) {
			return ActorDispatch{Target: actor.Identity{Type: "role", ID: "i:42"}, Delivery: actor.Delivery{}}, nil
		}),
		Teller: actorTellerFunc(func(c context.Context, id actor.Identity, d actor.Delivery) error {
			if _, ok := c.Deadline(); !ok || d.Identity.RoleID != 42 || d.Origin != actor.OriginClient || id.ID != "i:42" {
				t.Fatal("未绑定可信请求与期限")
			}
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = dispatch.Dispatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
}
