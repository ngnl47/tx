package agent

import (
	"context"
	"errors"
	"strconv"
	"testing"

	protoactor "github.com/asynkron/protoactor-go/actor"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
)

func TestActorDispatcherDoesNotCacheUncertainInstanceFailures(t *testing.T) {
	directory := discovery.NewServiceDirectory()
	instance := discovery.Instance{ID: "a", Services: []string{"game"}, Endpoint: "host:1", ActorEndpoint: "host:2", Status: discovery.StatusRunning}
	setInstance := func(id string) {
		t.Helper()
		instance.ID = id
		if err := directory.Replace([]discovery.Instance{instance}); err != nil {
			t.Fatal(err)
		}
	}
	setInstance("a")
	messenger := &actorMessengerStub{err: &txactor.CallError{Code: txactor.CallErrorCodeRejected, Message: "draining"}}
	dispatcher, err := NewActorDispatcher(ActorDispatcherOptions{
		Messenger: messenger, Directory: directory,
		Resolver: ActorDispatchResolverFunc(func(_ context.Context, req DispatchRequest) (ActorDispatch, error) {
			return ActorDispatch{Target: protoactor.NewPID("host:2", "role-"+strconv.FormatUint(uint64(req.CommandID), 10)), Delivery: txactor.Delivery{CommandID: uint64(req.CommandID)}}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	var commandID uint32
	call := func(wantCalls int) {
		t.Helper()
		commandID++
		_, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Seq: 1, CommandID: commandID})
		var failure *txactor.CallError
		if !errors.As(err, &failure) || failure.Code != txactor.CallErrorCodeRejected {
			t.Fatalf("rejection = %v", err)
		}
		if messenger.callCalls != wantCalls {
			t.Fatalf("remote calls = %d, want %d", messenger.callCalls, wantCalls)
		}
	}
	directory.Invalidate()
	call(1)
	call(2)
	setInstance("c")
	messenger.err = context.DeadlineExceeded
	for range 2 {
		_, _ = dispatcher.Dispatch(context.Background(), DispatchRequest{Seq: 1})
	}
	if messenger.callCalls != 4 {
		t.Fatal("timeout incorrectly cached as draining")
	}
	setInstance("d")
	messenger.err = &txactor.CallError{Code: txactor.CallErrorCodeRejected}
	messenger.onCall = func() { setInstance("e") }
	call(5)
	messenger.onCall = nil
	call(6)
}
