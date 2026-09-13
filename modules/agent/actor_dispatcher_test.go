package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/ngnl47/tx/modules/actor"
)

type actorCallerFunc func(context.Context, actor.Identity, actor.Delivery) (actor.Result, error)

func (f actorCallerFunc) Call(c context.Context, id actor.Identity, d actor.Delivery) (actor.Result, error) {
	return f(c, id, d)
}

func TestActorDispatcherBindsSequenceAndRejectsMixedResult(t *testing.T) {
	request := DispatchRequest{Seq: 7}
	request.Identity.RoleID = 42
	for _, mixed := range []bool{false, true} {
		dispatch, err := NewActorDispatcher(ActorDispatcherOptions{
			Resolver: ActorDispatchResolverFunc(func(context.Context, DispatchRequest) (ActorDispatch, error) {
				return ActorDispatch{Target: actor.Identity{Kind: "role", Key: "i:42"}, Delivery: actor.Delivery{UpSeq: 999}}, nil
			}),
			Caller: actorCallerFunc(func(c context.Context, id actor.Identity, d actor.Delivery) (actor.Result, error) {
				if _, ok := c.Deadline(); !ok || d.UpSeq != 7 || d.Identity.RoleID != 42 || id.Key != "i:42" {
					t.Fatal("未绑定可信请求与期限")
				}
				result := actor.Result{ProcessedUpSeq: 7}
				if mixed {
					result.Payload = []byte{1}
				}
				return result, nil
			}),
		})
		if err != nil {
			t.Fatal(err)
		}
		result, err := dispatch.Dispatch(context.Background(), request)
		if mixed {
			if !errors.Is(err, ErrInvalidDispatchResult) {
				t.Fatal(err)
			}
		} else if err != nil || result.ProcessedUpSeq != 7 {
			t.Fatalf("%+v %v", result, err)
		}
	}
}
