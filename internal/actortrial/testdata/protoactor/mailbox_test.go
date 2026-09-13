package actor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	txlifecycle "github.com/ngnl47/tx/lifecycle"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

func TestDrainedMailboxRejectsRequestsBeforeExecution(t *testing.T) {
	for _, mode := range []actorpb.DeliveryMode{actorpb.DeliveryMode_DELIVERY_MODE_CALL, actorpb.DeliveryMode_DELIVERY_MODE_ACCEPTED} {
		t.Run(mode.String(), func(t *testing.T) {
			system := protoactor.NewActorSystem()
			defer system.Shutdown()
			lifecycle := &mailboxLifecycle{}
			lifecycle.bind(txlifecycle.NewWaitGroupContext(context.Background()))
			lifecycle.beginDrain()
			var executed atomic.Int32
			pid := system.Root.Spawn(protoactor.PropsFromFunc(func(ctx protoactor.Context) {
				if _, ok := ctx.Message().(*actorpb.Delivery); ok {
					executed.Add(1)
				}
			}, protoactor.WithMailbox(observedBoundedMailbox(Config{MailboxCapacity: 4}, "test", lifecycle))))
			calls := newPendingCalls(1)
			responder := system.Root.Spawn(protoactor.PropsFromProducer(func() protoactor.Actor { return responseActor{pending: calls} }))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			expected := pendingResult
			if mode == actorpb.DeliveryMode_DELIVERY_MODE_ACCEPTED {
				expected = pendingAccepted
			}
			id, future, err := calls.add(ctx, expected, 7)
			if err != nil {
				t.Fatal(err)
			}
			system.Root.RequestWithCustomSender(pid, &actorpb.Delivery{Mode: mode, RequestId: id, CommandId: 7}, responder)
			_, err = future.Await(ctx)
			var rejection *CallError
			if !errors.As(err, &rejection) {
				t.Fatalf("expected explicit rejection, got %v", err)
			}
			if rejection.Code != CallErrorCodeRejected {
				t.Fatalf("rejection code = %d", rejection.Code)
			}
			if executed.Load() != 0 || lifecycle.pending() != 0 {
				t.Fatal("rejected request entered business processing")
			}
		})
	}
}
