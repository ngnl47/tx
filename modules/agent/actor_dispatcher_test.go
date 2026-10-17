package agent

import (
	"context"
	"testing"

	"github.com/ngnl47/tx/modules/actor"
)

type actorCallerFunc func(context.Context, actor.Identity, actor.Delivery) (actor.Result, error)

func (f actorCallerFunc) Call(c context.Context, id actor.Identity, d actor.Delivery) (actor.Result, error) {
	return f(c, id, d)
}

func TestActorDispatcherBindsIdentityAndWritesStream(t *testing.T) {
	request := DispatchRequest{}
	request.Identity.RoleID = 42
	dispatch, err := NewActorDispatcher(ActorDispatcherOptions{
		Resolver: ActorDispatchResolverFunc(func(context.Context, DispatchRequest) (ActorDispatch, error) {
			return ActorDispatch{Target: actor.Identity{Kind: "role", Key: "i:42"}, Delivery: actor.Delivery{}}, nil
		}),
		Caller: actorCallerFunc(func(c context.Context, id actor.Identity, d actor.Delivery) (actor.Result, error) {
			if _, ok := c.Deadline(); !ok || d.Identity.RoleID != 42 || d.Origin != actor.OriginClient || id.Key != "i:42" {
				t.Fatal("未绑定可信请求与期限")
			}
			return actor.Result{}, nil
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

func (f actorCallerFunc) Enqueue(c context.Context, id actor.Identity, d actor.Delivery) error {
	_, err := f(c, id, d)
	return err
}
