// Package agent 提供玩家连接到逻辑会话的通用接入运行时。
package agent

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/protocol"
)

var (
	ErrServerNil              = errors.New("agent: server is nil")
	ErrTransportNil           = errors.New("agent: transport is nil")
	ErrIdentityResolverNil    = errors.New("agent: identity resolver is nil")
	ErrDispatcherNil          = errors.New("agent: dispatcher is nil")
	ErrSessionRouterNil       = errors.New("agent: session client router is nil")
	ErrInvalidServer          = errors.New("agent: invalid server options")
	ErrInvalidIdentity        = errors.New("agent: invalid identity")
	ErrHandshakeRequired      = errors.New("agent: handshake is required")
	ErrInvalidFrame           = errors.New("agent: invalid frame")
	ErrInvalidAcknowledgement = errors.New("agent: invalid DownMsg acknowledgement")
	ErrFrameTooLarge          = errors.New("agent: frame is too large")
)

// Identity 是 Agent 为当前连接构造的可信会话身份。
type Identity = protocol.SessionIdentity

// AuthRequest 是握手传入项目身份解析回调的输入。
type AuthRequest struct {
	Token   []byte
	Restore bool
	Ack     int64
}

// IdentityResolver 将项目不透明握手数据解析为已认证的会话主体。
// 它可以校验 token，也可以消费上游已认证的可信凭据。
type IdentityResolver interface {
	ResolveIdentity(context.Context, AuthRequest) (protocol.SessionPrincipal, error)
}

// IdentityResolverFunc 适合以内联函数实现 IdentityResolver。
type IdentityResolverFunc func(context.Context, AuthRequest) (protocol.SessionPrincipal, error)

func (f IdentityResolverFunc) ResolveIdentity(ctx context.Context, request AuthRequest) (protocol.SessionPrincipal, error) {
	return f(ctx, request)
}

// DispatchRequest 是已通过 Seq 校验的业务 BON 消息。
type DispatchRequest struct {
	Identity  Identity
	CommandID uint32
	RouteKey  RouteKey
	Payload   []byte
}

// Dispatcher 由项目实现业务命令路由；远端投递返回只代表本地写流完成。
type Dispatcher interface {
	Dispatch(context.Context, DispatchRequest) error
}

// DispatchFunc 适合以内联函数实现 Dispatcher。
type DispatchFunc func(context.Context, DispatchRequest) error

func (f DispatchFunc) Dispatch(ctx context.Context, request DispatchRequest) error {
	return f(ctx, request)
}

// Transport 是一个已完成帧读写的玩家连接。TCP 与 WebSocket 都适配为此接口。
type Transport interface {
	Read(context.Context) (Frame, error)
	Write(context.Context, Frame) error
	Close() error
}

// SessionClientRouter 为认证后的角色选择当前可用 msgq 会话客户端。
type SessionClientRouter interface {
	Acquire(context.Context, msgq.AcquireSessionRequest) (msgq.SessionClient, error)
}

// ConnectionIDGenerator 生成区分重连的正连接编号。
type ConnectionIDGenerator func() (int64, error)

// ServerOptions 配置传输无关的 Agent Session 运行时。
type ServerOptions struct {
	IdentityResolver IdentityResolver
	Dispatcher       Dispatcher
	Admission        lifecycle.Admission
	Sessions         SessionClientRouter
	ConnectionID     ConnectionIDGenerator
}
