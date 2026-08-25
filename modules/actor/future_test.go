package actor

import (
	"context"
	"errors"
	"testing"
	"time"

	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

func TestPendingCallsEnforcesCapacityAndShutdownFailure(t *testing.T) {
	calls := newPendingCalls(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, future, err := calls.add(ctx, pendingResult, 1)
	if err != nil {
		t.Fatalf("add first pending call: %v", err)
	}
	if _, _, err := calls.add(ctx, pendingResult, 2); !errors.Is(err, ErrPendingCallsFull) {
		t.Fatalf("add second pending call error = %v, want ErrPendingCallsFull", err)
	}
	calls.failAll(ErrRuntimeStopped)
	if _, err := future.Await(ctx); !errors.Is(err, ErrRuntimeStopped) {
		t.Fatalf("Future.Await() error = %v, want ErrRuntimeStopped", err)
	}
}

func TestFutureAwaitCancellationDoesNotCancelRequest(t *testing.T) {
	calls := newPendingCalls(1)
	requestCtx, cancelRequest := context.WithTimeout(context.Background(), time.Second)
	defer cancelRequest()
	requestID, future, err := calls.add(requestCtx, pendingResult, 1)
	if err != nil {
		t.Fatalf("add pending call: %v", err)
	}
	waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelWait()
	if _, err := future.Await(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Await() error = %v, want context deadline", err)
	}
	calls.resolve(&actorpb.Result{RequestId: requestID, MessageType: "room.ready", Payload: []byte{7}})
	result, err := future.Await(requestCtx)
	if err != nil {
		t.Fatalf("second Await() error = %v", err)
	}
	if result.MessageType != "room.ready" || string(result.Payload) != string([]byte{7}) {
		t.Fatalf("second Await() result = %#v", result)
	}
}

func TestPendingCallsRejectsMismatchedAcceptance(t *testing.T) {
	calls := newPendingCalls(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	requestID, future, err := calls.add(ctx, pendingAccepted, 7)
	if err != nil {
		t.Fatalf("add pending acceptance: %v", err)
	}
	calls.accept(&actorpb.Acceptance{RequestId: requestID, CommandId: 8})
	if _, err := future.Await(ctx); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("Future.Await() error = %v, want ErrInvalidResponse", err)
	}
}

func TestPendingCallsRejectsMalformedResultError(t *testing.T) {
	calls := newPendingCalls(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	requestID, future, err := calls.add(ctx, pendingResult, 7)
	if err != nil {
		t.Fatalf("add pending result: %v", err)
	}
	calls.resolve(&actorpb.Result{RequestId: requestID, ErrorMessage: "missing error code"})
	if _, err := future.Await(ctx); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("Future.Await() error = %v, want ErrInvalidResponse", err)
	}
}

func TestPendingCallsRejectsEmptySuccessfulResult(t *testing.T) {
	calls := newPendingCalls(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	requestID, future, err := calls.add(ctx, pendingResult, 7)
	if err != nil {
		t.Fatalf("add pending result: %v", err)
	}
	calls.resolve(&actorpb.Result{RequestId: requestID})
	if _, err := future.Await(ctx); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("Future.Await() error = %v, want ErrInvalidResponse", err)
	}
}

func BenchmarkPendingCallsRoundTrip(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	calls := newPendingCalls(1)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		requestID, future, err := calls.add(ctx, pendingResult, 7)
		if err != nil {
			b.Fatal(err)
		}
		calls.resolve(&actorpb.Result{RequestId: requestID, MessageType: "role.pong"})
		if _, err := future.Await(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
