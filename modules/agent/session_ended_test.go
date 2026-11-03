package agent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/protocol"
)

func TestSessionEndedFrameRoundTrip(t *testing.T) {
	encoded, err := EncodeFrame(Frame{Kind: FrameSessionEnded}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := DecodeFrame(encoded, 1024)
	if err != nil || frame.Kind != FrameSessionEnded {
		t.Fatalf("decode = %+v, %v", frame, err)
	}
	if _, err := EncodeFrame(Frame{Kind: FrameSessionEnded, Payload: []byte("invalid")}, 1024); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("payload = %v", err)
	}
}

func TestHandshakeReportsTerminalAdmissionSeparatelyFromTransientFailure(t *testing.T) {
	for _, failure := range []error{ErrSessionEnded, msgq.ErrSessionAdmission, msgq.ErrSessionOffline, errors.New("temporary connection failure")} {
		t.Run(failure.Error(), func(t *testing.T) {
			transport := newInitializingSessionTransport()
			client := &admissionSessionClient{failure: failure}
			server, err := NewServer(ServerOptions{
				IdentityResolver: IdentityResolverFunc(func(context.Context, AuthRequest) (AuthResult, error) {
					if errors.Is(failure, ErrSessionEnded) {
						return AuthResult{SessionPrincipal: protocol.SessionPrincipal{}}, failure
					}
					return AuthResult{SessionPrincipal: protocol.SessionPrincipal{RoleID: 1, SessionID: 2}, Admission: &msgq.SessionAdmission{}}, nil
				}),
				Dispatcher: DispatchFunc(func(context.Context, DispatchRequest) error { t.Error("rejected session dispatched"); return nil }),
				Sessions:   sessionControlRouter{client: client},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := server.Serve(context.Background(), transport); !errors.Is(err, failure) {
				t.Fatalf("serve = %v", err)
			}
			terminal := errors.Is(failure, ErrSessionEnded) || errors.Is(failure, msgq.ErrSessionAdmission) || errors.Is(failure, msgq.ErrSessionOffline)
			select {
			case frame := <-transport.writes:
				if !terminal || frame.Kind != FrameSessionEnded {
					t.Fatalf("unexpected frame %+v", frame)
				}
			default:
				if terminal {
					t.Fatal("missing SessionEnded")
				}
			}
			if !errors.Is(failure, ErrSessionEnded) && !client.exclusive {
				t.Fatal("exclusive admission option missing")
			}
		})
	}
}

type admissionSessionClient struct {
	failure   error
	exclusive bool
}

func TestTerminatedConnectionDoesNotAdmitAlreadyReadFrame(t *testing.T) {
	client := &streamOnlySessionClient{down: make(chan msgq.FetchResult), acks: make(chan int64, 1)}
	var dispatched atomic.Int64
	server, err := NewServer(ServerOptions{
		IdentityResolver: IdentityResolverFunc(func(context.Context, AuthRequest) (AuthResult, error) {
			return AuthResult{SessionPrincipal: protocol.SessionPrincipal{RoleID: 1, SessionID: 2}, Admission: &msgq.SessionAdmission{}}, nil
		}),
		Dispatcher: DispatchFunc(func(context.Context, DispatchRequest) error { dispatched.Add(1); return nil }),
		Sessions:   sessionControlRouter{client: client},
	})
	if err != nil {
		t.Fatal(err)
	}
	transport := &delayedReadTransport{initializingSessionTransport: newInitializingSessionTransport(), readStarted: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, transport) }()
	<-transport.writes
	<-transport.readStarted
	if _, err := server.TerminateSession(ctx, 2); err != nil {
		t.Fatal(err)
	}
	close(transport.release)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if dispatched.Load() != 0 {
		t.Fatal("terminated connection admitted buffered upstream")
	}
}

type delayedReadTransport struct {
	*initializingSessionTransport
	readStarted   chan struct{}
	release       chan struct{}
	readHandshake bool
}

func (transport *delayedReadTransport) Read(ctx context.Context) (Frame, error) {
	if !transport.readHandshake {
		transport.readHandshake = true
		return transport.initializingSessionTransport.Read(ctx)
	}
	close(transport.readStarted)
	// 模拟 Socket 解码已完成，但调用线程尚未进行逐帧准入。
	<-transport.release
	return Frame{Kind: FrameUpMsg, Seq: 1, CommandHash: 1}, nil
}

func (c *admissionSessionClient) OpenMailboxStream(_ context.Context, r msgq.InitRequest, _ func()) (msgq.MailboxStreamSession, error) {
	c.exclusive = r.Admission != nil
	return nil, c.failure
}
func (*admissionSessionClient) TerminateSession(context.Context, msgq.SessionHeader) error {
	return nil
}
