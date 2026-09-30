package msgq

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/ngnl47/tx/internal/instanceconn"
	"github.com/ngnl47/tx/modules/discovery"
)

var (
	ErrSessionClientDialerNil    = errors.New("msgq: session client dialer is nil")
	ErrSessionClientNil          = errors.New("msgq: session client is nil")
	ErrSessionClientRouterClosed = errors.New("msgq: session client router is closed")
)

// SessionClient 是 Agent 访问单个 msgq 实例所需的会话操作。
type SessionClient interface {
	Init(context.Context, InitRequest) (InitResult, error)
	OpenMailboxStream(context.Context, SessionHeader) (MailboxStreamSession, error)
	TerminateSession(context.Context, SessionHeader) error
	Close() error
}

// AcquireSessionRequest 描述 Agent 为逻辑会话选择 MsgQ owner 所需的可信身份。
type AcquireSessionRequest struct {
	RoleID    int64
	SessionID int64
	Restore   bool
}

// SessionClientDialer 建立到一个指定 msgq 实例的会话客户端。
type SessionClientDialer interface {
	DialSession(context.Context, discovery.Instance) (SessionClient, error)
}

// FixedSessionClientRouter 将同进程的共享 SessionClient 适配为 Agent 会话路由器。
// 它不拥有 client，因此 Close 不会关闭 msgq 服务本身。
type FixedSessionClientRouter struct {
	client SessionClient
}

// NewFixedSessionClientRouter 创建始终返回同一会话客户端的本地路由器。
func NewFixedSessionClientRouter(client SessionClient) (*FixedSessionClientRouter, error) {
	if client == nil {
		return nil, ErrSessionClientNil
	}
	return &FixedSessionClientRouter{client: client}, nil
}

// Acquire 返回共享会话客户端，并校验完整会话身份。
func (r *FixedSessionClientRouter) Acquire(_ context.Context, request AcquireSessionRequest) (SessionClient, error) {
	if r == nil || r.client == nil {
		return nil, ErrSessionClientNil
	}
	if request.RoleID <= 0 || request.SessionID <= 0 {
		return nil, ErrInvalidSession
	}
	return r.client, nil
}

// Close 是无操作：客户端由 msgq 模块在 Host 关闭时统一停止。
func (*FixedSessionClientRouter) Close() error { return nil }

// NewLocalSessionClient 将进程内邮箱适配为 SessionClient。
func NewLocalSessionClient(mailbox *Server) (SessionClient, error) {
	if mailbox == nil {
		return nil, ErrServerNil
	}
	return localSessionClient{mailbox: mailbox}, nil
}

type localSessionClient struct {
	mailbox *Server
}

func (c localSessionClient) Init(ctx context.Context, request InitRequest) (InitResult, error) {
	return c.mailbox.Init(ctx, request)
}

func (c localSessionClient) TerminateSession(ctx context.Context, header SessionHeader) error {
	return c.mailbox.TerminateSession(ctx, header)
}

func (c localSessionClient) OpenMailboxStream(ctx context.Context, header SessionHeader) (MailboxStreamSession, error) {
	if err := c.mailbox.Touch(ctx, TouchRequest{Header: header}); err != nil {
		return nil, err
	}
	return newLocalMailboxSession(ctx, c.mailbox, header), nil
}

func (localSessionClient) Close() error { return nil }

// SessionClientRouter 为认证后的 Agent 选择可用 msgq 实例。
type SessionClientRouter struct {
	serviceName string
	directory   discovery.Directory
	store       SessionStore
	dialer      SessionClientDialer

	connections *instanceconn.Cache[SessionClient]
}

// NewSessionClientRouter 创建基于既有 owner 和服务目录的会话客户端路由器。
func NewSessionClientRouter(serviceName string, directory discovery.Directory, store SessionStore, dialer SessionClientDialer) (*SessionClientRouter, error) {
	if serviceName == "" {
		return nil, ErrServiceNameRequired
	}
	if directory == nil {
		return nil, ErrDirectoryNil
	}
	if store == nil {
		return nil, ErrSessionStoreNil
	}
	if dialer == nil {
		return nil, ErrSessionClientDialerNil
	}
	return &SessionClientRouter{serviceName: serviceName, directory: directory, store: store, dialer: dialer, connections: instanceconn.New[SessionClient](directory)}, nil
}

// Acquire 为新会话复用角色归属；首次归属由运行中实例轮转提名并原子确认。恢复仍直达旧邮箱。
func (r *SessionClientRouter) Acquire(ctx context.Context, request AcquireSessionRequest) (SessionClient, error) {
	if r == nil || r.directory == nil {
		return nil, ErrDirectoryNil
	}
	if r.store == nil {
		return nil, ErrSessionStoreNil
	}
	if r.dialer == nil {
		return nil, ErrSessionClientDialerNil
	}
	if request.RoleID <= 0 || request.SessionID <= 0 {
		return nil, ErrInvalidSession
	}
	if !request.Restore {
		binding, found, err := r.store.FindRole(ctx, request.RoleID)
		if err != nil {
			return nil, err
		}
		if !found {
			candidate, available := r.directory.NextInstance(r.serviceName)
			if !available {
				return nil, ErrRouteUnavailable
			}
			binding, err = r.store.ClaimRole(ctx, RoleBinding{RoleID: request.RoleID, InstanceID: candidate.ID, Generation: uuid.NewString()})
			if err != nil {
				return nil, err
			}
		}
		if binding.Validate() != nil || binding.RoleID != request.RoleID {
			return nil, ErrSessionStoreResponse
		}
		instance, available := r.directory.LookupInstance(binding.InstanceID)
		if !available || instance.Status != discovery.StatusRunning || !providesService(instance, r.serviceName) {
			return nil, ErrRouteUnavailable
		}
		return r.connection(ctx, instance)
	}
	binding, found, err := r.store.Find(ctx, request.SessionID)
	if err != nil {
		return nil, err
	}
	if !found || binding.Validate() != nil || binding.SessionID != request.SessionID {
		return nil, ErrSessionOffline
	}
	if binding.RoleID != request.RoleID {
		return nil, ErrSessionChanged
	}
	instance, available := r.directory.LookupInstance(binding.InstanceID)
	if !available || instance.Status != discovery.StatusRunning || !providesService(instance, r.serviceName) {
		return nil, ErrRouteUnavailable
	}
	return r.connection(ctx, instance)
}

// Close 关闭按 endpoint 和实例身份缓存的远端会话客户端。
func (r *SessionClientRouter) Close() error {
	if r == nil {
		return nil
	}
	return r.connections.Close()
}

func (r *SessionClientRouter) connection(ctx context.Context, instance discovery.Instance) (SessionClient, error) {
	client, err := r.connections.Get(ctx, instance, r.dialer.DialSession)
	if errors.Is(err, instanceconn.ErrClosed) {
		return nil, ErrSessionClientRouterClosed
	}
	if errors.Is(err, instanceconn.ErrStaleInstance) {
		return nil, ErrRouteUnavailable
	}
	return client, err
}
