package msgq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMailboxStartupAdmission(t *testing.T) {
	var ready atomic.Bool
	store := &testSessionStore{}
	s, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: store, MaxBacklogMessages: 4,
		StartupAdmission: lifecycle.AdmissionFunc(func() (func(), error) {
			if !ready.Load() {
				return nil, lifecycle.ErrNotReady
			}
			return func() {}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	header := SessionHeader{RoleID: 1, SessionID: 2, ConnectionID: 3}
	local, err := NewLocalSessionClient(s)
	if err != nil {
		t.Fatal(err)
	}
	checks := map[string]func() error{
		"local init":     func() error { return local.Init(ctx, InitRequest{Header: header}) },
		"local stream":   func() error { _, err := local.OpenMailboxStream(ctx, header); return err },
		"publish":        func() error { _, err := s.PubSession(ctx, 2, []byte("down")); return err },
		"role broadcast": func() error { _, err := s.BroadcastRole(ctx, 1, []byte("down")); return err },
		"topic broadcast": func() error {
			_, err := s.BroadcastTopic(ctx, BroadcastTopicRequest{Topic: "all", Payload: []byte("down")})
			return err
		},
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			if err := check(); !errors.Is(err, lifecycle.ErrNotReady) {
				t.Errorf("启动前应拒绝请求: %v", err)
			}
		})
	}
	remote := startMailboxTestGRPC(t, s).client
	mailboxStream, err := remote.MailboxStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = mailboxStream.Recv(); status.Code(err) != codes.Unavailable {
		t.Errorf("启动前邮箱流 = %v", err)
	}
	publishStream, err := remote.PublishStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = publishStream.Recv(); status.Code(err) != codes.Unavailable {
		t.Errorf("启动前发布流 = %v", err)
	}
	if _, err := remote.Init(ctx, &msgqpb.InitRequest{Header: &msgqpb.SessionHeader{RoleId: 1, SessionId: 2, ConnectionId: 3}}); status.Code(err) != codes.Unavailable {
		t.Errorf("远端启动前应返回 Unavailable: %v", err)
	}
	if s.SessionCount() != 0 {
		t.Fatal("启动失败清理前已经创建会话")
	}
	if _, found, err := store.FindRole(ctx, 1); err != nil || found {
		t.Fatalf("启动前不应抢占角色归属: %v/%v", found, err)
	}
	ready.Store(true)
	if err := local.Init(ctx, InitRequest{Header: header}); err != nil {
		t.Fatal(err)
	}
	// 开放后保持现有邮箱收尾语义；启动检查不能在排水时切断已有下行。
	ready.Store(false)
	if _, err := remote.PubSession(ctx, &msgqpb.PubSessionRequest{SessionId: 2, Payload: []byte("down")}); err != nil {
		t.Fatal(err)
	}
	msg, err := s.Fetch(ctx, FetchRequest{Header: header})
	if err != nil || string(msg.Payload) != "down" {
		t.Fatalf("已开放邮箱的下行 = %v, %v", msg, err)
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := local.Init(ctx, InitRequest{Header: header}); !errors.Is(err, ErrSessionOffline) {
		t.Fatalf("停止后重建会话: %v", err)
	}
}

func TestMailboxFailedStartupCleanupNeverOpensAdmission(t *testing.T) {
	s, err := NewServer(ServerOptions{InstanceID: "msgq-1", Store: &testSessionStore{}, MaxBacklogMessages: 4,
		StartupAdmission: lifecycle.AdmissionFunc(func() (func(), error) { return nil, lifecycle.ErrNotReady }),
	})
	if err != nil {
		t.Fatal(err)
	}
	// 其他服务的 Start 失败，框架回滚已 Init 的 MsgQ，不能因清理而开放入口。
	if err := s.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	err = s.Init(t.Context(), InitRequest{Header: SessionHeader{RoleID: 1, SessionID: 2, ConnectionID: 3}})
	if !errors.Is(err, lifecycle.ErrNotReady) || s.SessionCount() != 0 {
		t.Fatalf("启动回滚后接受了会话: %v", err)
	}
}
