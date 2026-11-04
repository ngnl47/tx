package agent

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/protocol"
)

// 真实 WebSocket 覆盖路由拒绝与永久失效；只关闭 socket 不能冒充应用结果。
func TestWebSocketAdmissionRejectionIsExplicit(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		kind FrameKind
		open bool
	}{
		{"route-unavailable", msgq.ErrRouteUnavailable, FrameHandshakeRejected, false},
		{"occupied", msgq.ErrSessionOccupied, FrameHandshakeRejected, true},
		{"session-ended", msgq.ErrSessionOffline, FrameSessionEnded, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, err := NewServer(ServerOptions{
				IdentityResolver: IdentityResolverFunc(func(context.Context, AuthRequest) (AuthResult, error) {
					return AuthResult{SessionPrincipal: protocol.SessionPrincipal{RoleID: 1, SessionID: 2}}, nil
				}),
				Dispatcher: DispatchFunc(func(context.Context, DispatchRequest) error { t.Error("拒绝接入不得分派业务"); return nil }),
				Sessions:   rejectionRouter{failure: test.err, open: test.open},
			})
			if err != nil {
				t.Fatal(err)
			}
			handler, err := NewWebSocketHandler(server, 256, WebSocketOptions{})
			if err != nil {
				t.Fatal(err)
			}
			httpServer := httptest.NewServer(handler)
			defer httpServer.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			conn, _, err := websocket.Dial(ctx, "ws"+httpServer.URL[len("http"):], nil)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseNow()
			payload, err := EncodeFrame(Frame{Kind: FrameHandshake, Restore: true, Payload: []byte("ticket")}, 256)
			if err != nil {
				t.Fatal(err)
			}
			if err = conn.Write(ctx, websocket.MessageBinary, payload); err != nil {
				t.Fatal(err)
			}
			_, payload, err = conn.Read(ctx)
			if err != nil {
				t.Fatalf("接入拒绝没有结果帧: %v", err)
			}
			frame, err := DecodeFrame(payload, 256)
			if err != nil || frame.Kind != test.kind {
				t.Fatalf("结果=%+v, error=%v", frame, err)
			}
		})
	}
}

type rejectionRouter struct {
	sessionControlRouter
	failure error
	open    bool
}

func (router rejectionRouter) Acquire(context.Context, msgq.AcquireSessionRequest) (msgq.SessionClient, error) {
	if router.open {
		return &admissionSessionClient{failure: router.failure}, nil
	}
	return nil, errors.Join(errors.New("接入失败"), router.failure)
}
