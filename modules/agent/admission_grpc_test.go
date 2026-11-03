package agent

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/msgq"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"github.com/ngnl47/tx/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestRemoteAdmissionRejectionReportsSessionEndedBeforeSocketClose(t *testing.T) {
	listener := bufconn.Listen(64 << 10)
	rpcServer := grpc.NewServer()
	msgqpb.RegisterMailboxServiceServer(rpcServer, rejectedAdmissionServer{})
	go func() { _ = rpcServer.Serve(listener) }()
	t.Cleanup(func() { rpcServer.Stop(); _ = listener.Close() })

	for _, restore := range []bool{false, true} {
		name := "fresh"
		if restore {
			name = "restore"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			dialer, err := msgq.NewGRPCDialer(insecure.NewCredentials(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return listener.DialContext(ctx)
			}))
			if err != nil {
				t.Fatal(err)
			}
			connection, err := dialer.DialSession(ctx, discovery.Instance{ID: "msgq", Endpoint: "memory"})
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			router, err := msgq.NewFixedSessionClientRouter(connection)
			if err != nil {
				t.Fatal(err)
			}
			server, err := NewServer(ServerOptions{
				IdentityResolver: IdentityResolverFunc(func(context.Context, AuthRequest) (AuthResult, error) {
					return AuthResult{SessionPrincipal: protocol.SessionPrincipal{RoleID: 1, SessionID: 2}}, nil
				}),
				Dispatcher: DispatchFunc(func(context.Context, DispatchRequest) error { t.Error("失效票据不得分派业务"); return nil }),
				Sessions:   router,
			})
			if err != nil {
				t.Fatal(err)
			}
			transport := &closedAdmissionTransport{newInitializingSessionTransport()}
			<-transport.reads
			transport.reads <- Frame{Kind: FrameHandshake, Payload: []byte("stale-ticket"), Restore: restore}
			if err := server.Serve(ctx, transport); !errors.Is(err, msgq.ErrSessionAdmission) {
				t.Fatalf("远端准入错误=%v", err)
			}
			select {
			case frame := <-transport.writes:
				if frame.Kind != FrameSessionEnded {
					t.Fatalf("永久拒绝返回帧=%+v", frame)
				}
			default:
				t.Fatal("远端永久拒绝先关闭 socket，客户端无法收到 SessionEnded")
			}
		})
	}
}

type rejectedAdmissionServer struct {
	msgqpb.UnimplementedMailboxServiceServer
}

func (rejectedAdmissionServer) MailboxStream(stream grpc.BidiStreamingServer[msgqpb.MailboxStreamRequest, msgqpb.MailboxStreamResponse]) error {
	for {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		response := &msgqpb.MailboxStreamResponse{Header: request.Header, Kind: msgqpb.MailboxStreamResponse_KIND_CLOSED}
		if request.Kind == msgqpb.MailboxStreamRequest_KIND_OPEN {
			response.Kind = msgqpb.MailboxStreamResponse_KIND_OPENED
			response.ErrorCode = int32(codes.PermissionDenied)
			response.ErrorMessage = msgq.ErrSessionAdmission.Error()
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

// 已关闭的真实 Socket 不能像带缓冲测试 channel 一样继续写入控制帧。
type closedAdmissionTransport struct{ *initializingSessionTransport }

func (transport *closedAdmissionTransport) Write(ctx context.Context, frame Frame) error {
	select {
	case <-transport.closed:
		return net.ErrClosed
	default:
		return transport.initializingSessionTransport.Write(ctx, frame)
	}
}
