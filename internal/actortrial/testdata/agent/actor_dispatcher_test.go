package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	txactor "github.com/ngnl47/tx/internal/actortrial/testdata/protoactor"
	"github.com/ngnl47/tx/protocol"
)

func TestActorDispatcherWaitsForCompletionForEveryAgentMessage(t *testing.T) {
	pid := protoactor.NewPID("127.0.0.1:12000", "role-470001")
	messenger := &actorMessengerStub{result: txactor.Result{ProcessedUpSeq: 5}}
	dispatcher, err := NewActorDispatcher(ActorDispatcherOptions{
		Messenger: messenger,
		Resolver: ActorDispatchResolverFunc(func(context.Context, DispatchRequest) (ActorDispatch, error) {
			return ActorDispatch{Target: pid, Delivery: txactor.Delivery{MessageType: "agent.command"}}, nil
		}),
	})
	if err != nil {
		t.Fatalf("NewActorDispatcher returned error: %v", err)
	}

	result, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Seq: 5, CommandID: 100001})
	if err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if result.ProcessedUpSeq != 5 {
		t.Fatalf("processed seq = %d, want 5", result.ProcessedUpSeq)
	}
	if messenger.tellCalls != 0 || messenger.callCalls != 1 {
		t.Fatalf("tell/call calls = %d/%d, want 0/1", messenger.tellCalls, messenger.callCalls)
	}
}

func TestActorDispatcherReturnsProcessedConfirmationWithoutBusinessPayload(t *testing.T) {
	pid := protoactor.NewPID("127.0.0.1:12000", "role-470001")
	messenger := &actorMessengerStub{result: txactor.Result{ProcessedUpSeq: 7}}
	dispatcher, err := NewActorDispatcher(ActorDispatcherOptions{
		Messenger:   messenger,
		CallTimeout: time.Second,
		Resolver: ActorDispatchResolverFunc(func(context.Context, DispatchRequest) (ActorDispatch, error) {
			return ActorDispatch{
				Target:   pid,
				Delivery: txactor.Delivery{MessageType: "agent.command"},
			}, nil
		}),
	})
	if err != nil {
		t.Fatalf("NewActorDispatcher returned error: %v", err)
	}

	result, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Seq: 7, CommandID: 100001})
	if err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if result.ProcessedUpSeq != 7 {
		t.Fatalf("dispatch result = %#v, want processed seq 7", result)
	}
	if messenger.tellCalls != 0 || messenger.callCalls != 1 {
		t.Fatalf("tell/call calls = %d/%d, want 0/1", messenger.tellCalls, messenger.callCalls)
	}
	if !messenger.callHadDeadline {
		t.Fatal("Call context has no deadline")
	}
}

func TestActorDispatcherRejectsInvalidProcessedConfirmation(t *testing.T) {
	tests := []struct {
		name   string
		result txactor.Result
	}{
		{name: "mismatched sequence", result: txactor.Result{ProcessedUpSeq: 8}},
		{name: "business payload", result: txactor.Result{ProcessedUpSeq: 7, Payload: []byte("unexpected")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pid := protoactor.NewPID("127.0.0.1:12000", "role-470001")
			dispatcher, err := NewActorDispatcher(ActorDispatcherOptions{
				Messenger: &actorMessengerStub{result: test.result},
				Resolver: ActorDispatchResolverFunc(func(context.Context, DispatchRequest) (ActorDispatch, error) {
					return ActorDispatch{Target: pid, Delivery: txactor.Delivery{MessageType: "agent.command"}}, nil
				}),
			})
			if err != nil {
				t.Fatalf("NewActorDispatcher returned error: %v", err)
			}
			if _, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Seq: 7}); !errors.Is(err, ErrInvalidDispatchResult) {
				t.Fatalf("Dispatch error = %v, want ErrInvalidDispatchResult", err)
			}
		})
	}
}

func TestActorDispatcherOverridesResolverDeliveryIdentityAndUpSeq(t *testing.T) {
	pid := protoactor.NewPID("127.0.0.1:12000", "role-470001")
	forgedIdentity := protocol.SessionIdentity{
		SessionPrincipal: protocol.SessionPrincipal{
			AccountID: 99,
			ServerID:  9,
			RoleID:    990001,
			SessionID: 999,
		},
		ConnectionID: 9999,
	}
	wantIdentity := protocol.SessionIdentity{
		SessionPrincipal: protocol.SessionPrincipal{
			AccountID:     47,
			ServerID:      1,
			RoleID:        470001,
			SessionID:     99,
			CanCreateRole: true,
		},
		ConnectionID: 7,
	}
	messenger := &actorMessengerStub{result: txactor.Result{ProcessedUpSeq: 3}}
	dispatcher, err := NewActorDispatcher(ActorDispatcherOptions{
		Messenger: messenger,
		Resolver: ActorDispatchResolverFunc(func(context.Context, DispatchRequest) (ActorDispatch, error) {
			return ActorDispatch{
				Target: pid,
				Delivery: txactor.Delivery{
					MessageType: "agent.command",
					Identity:    &forgedIdentity,
					UpSeq:       999,
				},
			}, nil
		}),
	})
	if err != nil {
		t.Fatalf("NewActorDispatcher returned error: %v", err)
	}

	if _, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Identity: wantIdentity, Seq: 3}); err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if messenger.delivery.Identity == nil || *messenger.delivery.Identity != wantIdentity {
		t.Fatalf("messenger delivery identity = %#v, want %#v", messenger.delivery.Identity, wantIdentity)
	}
	if messenger.delivery.UpSeq != 3 {
		t.Fatalf("messenger delivery up seq = %d, want 3", messenger.delivery.UpSeq)
	}
}

func TestActorDispatcherRejectsMissingDependenciesAndTarget(t *testing.T) {
	resolver := ActorDispatchResolverFunc(func(context.Context, DispatchRequest) (ActorDispatch, error) {
		return ActorDispatch{}, nil
	})
	if _, err := NewActorDispatcher(ActorDispatcherOptions{Resolver: resolver}); !errors.Is(err, ErrActorMessengerNil) {
		t.Fatalf("missing messenger error = %v, want %v", err, ErrActorMessengerNil)
	}
	if _, err := NewActorDispatcher(ActorDispatcherOptions{Messenger: &actorMessengerStub{}}); !errors.Is(err, ErrActorDispatchResolverNil) {
		t.Fatalf("missing resolver error = %v, want %v", err, ErrActorDispatchResolverNil)
	}
	dispatcher, err := NewActorDispatcher(ActorDispatcherOptions{Messenger: &actorMessengerStub{}, Resolver: resolver})
	if err != nil {
		t.Fatalf("NewActorDispatcher returned error: %v", err)
	}
	if _, err := dispatcher.Dispatch(context.Background(), DispatchRequest{}); !errors.Is(err, ErrActorTargetNil) {
		t.Fatalf("nil target error = %v, want %v", err, ErrActorTargetNil)
	}
}

type actorMessengerStub struct {
	err             error
	onCall          func()
	result          txactor.Result
	delivery        txactor.Delivery
	tellCalls       int
	callCalls       int
	callHadDeadline bool
}

func (*actorMessengerStub) VirtualPID(string, string) (*protoactor.PID, error) { return nil, nil }

func (stub *actorMessengerStub) Tell(_ *protoactor.PID, delivery txactor.Delivery) error {
	stub.tellCalls++
	stub.delivery = delivery
	return nil
}

func (*actorMessengerStub) TellAccepted(context.Context, *protoactor.PID, txactor.Delivery) error {
	return nil
}

func (stub *actorMessengerStub) Call(ctx context.Context, _ *protoactor.PID, delivery txactor.Delivery) (txactor.Result, error) {
	stub.callCalls++
	if stub.onCall != nil {
		stub.onCall()
	}
	stub.delivery = delivery
	_, stub.callHadDeadline = ctx.Deadline()
	return stub.result, stub.err
}

func (*actorMessengerStub) CallFuture(context.Context, *protoactor.PID, txactor.Delivery) (*txactor.Future, error) {
	return nil, nil
}
