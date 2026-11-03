package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/protocol"
)

func TestReadyWaitsForMailboxAdmission(t *testing.T) {
	client := &pendingAdmissionClient{streamOnlySessionClient: streamOnlySessionClient{down: make(chan msgq.FetchResult)}, entered: make(chan struct{}), release: make(chan struct{})}
	server, err := NewServer(ServerOptions{
		IdentityResolver: IdentityResolverFunc(func(context.Context, AuthRequest) (AuthResult, error) {
			return AuthResult{SessionPrincipal: protocol.SessionPrincipal{RoleID: 1, SessionID: 2}}, nil
		}),
		Dispatcher: DispatchFunc(func(context.Context, DispatchRequest) error { return nil }),
		Sessions:   sessionControlRouter{client: client},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	transport := newInitializingSessionTransport()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, transport) }()
	<-client.entered
	select {
	case frame := <-transport.writes:
		t.Fatalf("远端接入尚未完成就返回 %+v", frame)
	default:
	}
	close(client.release)
	select {
	case frame := <-transport.writes:
		if frame.Kind != FrameReady {
			t.Fatalf("frame=%+v", frame)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
	<-done
}

type pendingAdmissionClient struct {
	streamOnlySessionClient
	entered chan struct{}
	release chan struct{}
}

func (client *pendingAdmissionClient) OpenMailboxStream(ctx context.Context, request msgq.InitRequest, revoke func()) (msgq.MailboxStreamSession, error) {
	close(client.entered)
	select {
	case <-client.release:
		return client.streamOnlySessionClient.OpenMailboxStream(ctx, request, revoke)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestOfflineLogoutDoesNotAcquireMailboxAndWaitsForConfirmation(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "confirmed", true: "failed"}[failed], func(t *testing.T) {
			router := &logoutRouter{entered: make(chan struct{}), release: make(chan struct{})}
			if failed {
				router.failure = errors.New("关闭未确认")
			}
			server, err := NewServer(ServerOptions{
				IdentityResolver: IdentityResolverFunc(func(_ context.Context, r AuthRequest) (AuthResult, error) {
					if !r.Logout || r.Restore || string(r.Token) != "signed-ticket" {
						t.Errorf("验票输入=%+v", r)
					}
					return AuthResult{SessionPrincipal: protocol.SessionPrincipal{RoleID: 1, SessionID: 2}}, nil
				}),
				Dispatcher: DispatchFunc(func(context.Context, DispatchRequest) error { t.Error("退出不得分派业务"); return nil }), Sessions: router,
			})
			if err != nil {
				t.Fatal(err)
			}
			transport := newInitializingSessionTransport()
			<-transport.reads
			transport.reads <- Frame{Kind: FrameLogout, Payload: []byte("signed-ticket")}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- server.Serve(ctx, transport) }()
			select {
			case <-router.entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case frame := <-transport.writes:
				t.Fatalf("提前返回退出确认=%+v", frame)
			default:
			}
			close(router.release)
			if got := <-done; !errors.Is(got, router.failure) {
				t.Fatalf("serve=%v", got)
			}
			select {
			case frame := <-transport.writes:
				if failed || frame.Kind != FrameLoggedOut {
					t.Fatalf("退出结果=%+v", frame)
				}
			default:
				if !failed {
					t.Fatal("缺少退出确认")
				}
			}
			if router.acquired {
				t.Fatal("离线退出不应创建邮箱归属")
			}
		})
	}
}

type logoutRouter struct {
	entered  chan struct{}
	release  chan struct{}
	failure  error
	acquired bool
}

func (router *logoutRouter) Acquire(context.Context, msgq.AcquireSessionRequest) (msgq.SessionClient, error) {
	router.acquired = true
	return nil, errors.New("不应创建邮箱")
}
func (router *logoutRouter) TerminateLogicalSession(ctx context.Context, roleID, sessionID int64) error {
	if roleID != 1 || sessionID != 2 {
		return errors.New("错误退出身份")
	}
	close(router.entered)
	select {
	case <-router.release:
		return router.failure
	case <-ctx.Done():
		return ctx.Err()
	}
}
