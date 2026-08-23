package msgq

import (
	"context"
	"errors"
	"sync"

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
	Rec(context.Context, RecRequest) (RecResult, error)
	FetchSome(context.Context, FetchSomeRequest) (FetchSomeResult, error)
	Confirm(context.Context, ConfirmRequest) error
	CloseSession(context.Context, SessionHeader) error
	Close() error
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

// Acquire 返回共享会话客户端。roleID 仍会校验，避免在接入层吞掉无效身份错误。
func (r *FixedSessionClientRouter) Acquire(_ context.Context, roleID int64) (SessionClient, error) {
	if r == nil || r.client == nil {
		return nil, ErrSessionClientNil
	}
	if roleID <= 0 {
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

func (c localSessionClient) Rec(ctx context.Context, request RecRequest) (RecResult, error) {
	return c.mailbox.Rec(ctx, request)
}

func (c localSessionClient) FetchSome(ctx context.Context, request FetchSomeRequest) (FetchSomeResult, error) {
	return c.mailbox.FetchSome(ctx, request)
}

func (c localSessionClient) Confirm(ctx context.Context, request ConfirmRequest) error {
	return c.mailbox.Confirm(ctx, request)
}

func (c localSessionClient) CloseSession(ctx context.Context, header SessionHeader) error {
	return c.mailbox.Close(ctx, header)
}

func (localSessionClient) Close() error { return nil }

// SessionClientRouter 为认证后的 Agent 选择可用 msgq 实例。
type SessionClientRouter struct {
	directory discovery.Directory
	store     SessionStore
	dialer    SessionClientDialer

	mu          sync.Mutex
	connections map[string]SessionClient
	closed      bool
}

// NewSessionClientRouter 创建基于既有 owner 和服务目录的会话客户端路由器。
func NewSessionClientRouter(directory discovery.Directory, store SessionStore, dialer SessionClientDialer) (*SessionClientRouter, error) {
	if directory == nil {
		return nil, ErrDirectoryNil
	}
	if store == nil {
		return nil, ErrSessionStoreNil
	}
	if dialer == nil {
		return nil, ErrSessionClientDialerNil
	}
	return &SessionClientRouter{directory: directory, store: store, dialer: dialer, connections: make(map[string]SessionClient)}, nil
}

// Acquire 优先连接角色已有的 running msgq owner；没有可用 owner 时轮转选择 running msgq。
func (r *SessionClientRouter) Acquire(ctx context.Context, roleID int64) (SessionClient, error) {
	if r == nil || r.directory == nil {
		return nil, ErrDirectoryNil
	}
	if r.store == nil {
		return nil, ErrSessionStoreNil
	}
	if r.dialer == nil {
		return nil, ErrSessionClientDialerNil
	}
	if roleID <= 0 {
		return nil, ErrInvalidSession
	}
	binding, found, err := r.store.Find(ctx, roleID)
	if err != nil {
		return nil, err
	}
	if found && binding.Validate() == nil && binding.RoleID == roleID {
		if instance, available := r.directory.LookupInstance(binding.InstanceID); available && instance.Status == discovery.StatusRunning && providesService(instance, "msgq") {
			return r.connection(ctx, instance)
		}
	}
	instance, available := r.directory.NextInstance("msgq")
	if !available {
		return nil, ErrRouteUnavailable
	}
	return r.connection(ctx, instance)
}

// Close 关闭按 endpoint 缓存的远端会话客户端。
func (r *SessionClientRouter) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	connections := r.connections
	r.connections = nil
	r.mu.Unlock()

	var errs []error
	for _, client := range connections {
		errs = append(errs, client.Close())
	}
	return errors.Join(errs...)
}

func (r *SessionClientRouter) connection(ctx context.Context, instance discovery.Instance) (SessionClient, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrSessionClientRouterClosed
	}
	if client := r.connections[instance.Endpoint]; client != nil {
		r.mu.Unlock()
		return client, nil
	}
	r.mu.Unlock()

	created, err := r.dialer.DialSession(ctx, instance)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		_ = created.Close()
		return nil, ErrSessionClientRouterClosed
	}
	if client := r.connections[instance.Endpoint]; client != nil {
		_ = created.Close()
		return client, nil
	}
	r.connections[instance.Endpoint] = created
	return created, nil
}
