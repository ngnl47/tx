package control

import (
	"context"
	"errors"
	"testing"

	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"google.golang.org/grpc"
)

func TestHandoffClientRequiresMatchingConfirmation(t *testing.T) {
	for _, tc := range []struct {
		name, id, response string
		transportErr, want error
	}{
		{name: "confirmed", id: "startup-a", response: "startup-a"},
		{name: "wrong identity", id: "startup-a", response: "startup-b", want: ErrHandoffInstanceMismatch},
		{name: "missing confirmation", id: "startup-a", want: ErrHandoffInstanceMismatch},
		{name: "empty identity", want: ErrHandoffInstanceMismatch},
		{name: "unknown result", id: "startup-a", transportErr: context.DeadlineExceeded, want: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := handoffClientConn{invoke: func(ctx context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
				if method != controlpb.HandoffService_PrepareHandoff_FullMethodName || args.(*controlpb.PrepareHandoffRequest).GetRuntimeInstanceId() != tc.id {
					t.Fatal("请求未携带预期身份")
				}
				reply.(*controlpb.PrepareHandoffResponse).RuntimeInstanceId = tc.response
				return tc.transportErr
			}}
			err := NewHandoffClient(conn).PrepareHandoff(t.Context(), tc.id)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want=%v", err, tc.want)
			}
		})
	}
}

type handoffClientConn struct {
	grpc.ClientConnInterface
	invoke func(context.Context, string, any, any, ...grpc.CallOption) error
}

func (c handoffClientConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	return c.invoke(ctx, method, args, reply, opts...)
}
