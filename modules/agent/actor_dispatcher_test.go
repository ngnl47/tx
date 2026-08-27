package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	txactor "github.com/ngnl47/tx/modules/actor"
)

func TestActorDispatcherUsesTellForOneWayMessage(t *testing.T) {
	pid := protoactor.NewPID("127.0.0.1:12000", "role-470001")
	messenger := &actorMessengerStub{}
	dispatcher, err := NewActorDispatcher(ActorDispatcherOptions{
		Messenger: messenger,
		Resolver: ActorDispatchResolverFunc(func(context.Context, DispatchRequest) (ActorDispatch, error) {
			return ActorDispatch{Target: pid, Delivery: txactor.Delivery{MessageType: "agent.command"}}, nil
		}),
	})
	if err != nil {
		t.Fatalf("NewActorDispatcher returned error: %v", err)
	}

	result, err := dispatcher.Dispatch(context.Background(), DispatchRequest{CommandID: 100001})
	if err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if result.Payload != nil {
		t.Fatalf("result payload = %v, want nil", result.Payload)
	}
	if messenger.tellCalls != 1 || messenger.callCalls != 0 {
		t.Fatalf("tell/call calls = %d/%d, want 1/0", messenger.tellCalls, messenger.callCalls)
	}
}

func TestActorDispatcherCallsForBusinessResult(t *testing.T) {
	pid := protoactor.NewPID("127.0.0.1:12000", "role-470001")
	messenger := &actorMessengerStub{result: txactor.Result{Payload: []byte("bon-response")}}
	dispatcher, err := NewActorDispatcher(ActorDispatcherOptions{
		Messenger:   messenger,
		CallTimeout: time.Second,
		Resolver: ActorDispatchResolverFunc(func(context.Context, DispatchRequest) (ActorDispatch, error) {
			return ActorDispatch{
				Target:      pid,
				Delivery:    txactor.Delivery{MessageType: "agent.command"},
				WantsResult: true,
			}, nil
		}),
	})
	if err != nil {
		t.Fatalf("NewActorDispatcher returned error: %v", err)
	}

	result, err := dispatcher.Dispatch(context.Background(), DispatchRequest{CommandID: 100001})
	if err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if string(result.Payload) != "bon-response" {
		t.Fatalf("result payload = %q", result.Payload)
	}
	if messenger.tellCalls != 0 || messenger.callCalls != 1 {
		t.Fatalf("tell/call calls = %d/%d, want 0/1", messenger.tellCalls, messenger.callCalls)
	}
	if !messenger.callHadDeadline {
		t.Fatal("Call context has no deadline")
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
	result          txactor.Result
	tellCalls       int
	callCalls       int
	callHadDeadline bool
}

func (*actorMessengerStub) VirtualPID(string, string) (*protoactor.PID, error) { return nil, nil }

func (stub *actorMessengerStub) Tell(*protoactor.PID, txactor.Delivery) error {
	stub.tellCalls++
	return nil
}

func (*actorMessengerStub) TellAccepted(context.Context, *protoactor.PID, txactor.Delivery) error {
	return nil
}

func (stub *actorMessengerStub) Call(ctx context.Context, _ *protoactor.PID, _ txactor.Delivery) (txactor.Result, error) {
	stub.callCalls++
	_, stub.callHadDeadline = ctx.Deadline()
	return stub.result, nil
}

func (*actorMessengerStub) CallFuture(context.Context, *protoactor.PID, txactor.Delivery) (*txactor.Future, error) {
	return nil, nil
}
