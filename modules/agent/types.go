// Package agent 提供玩家连接到逻辑会话的通用接入运行时。
package agent

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/modules/msgq"
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
	ErrInvalidAcknowledgement = errors.New("agent: invalid outbound acknowledgement")
	ErrFrameTooLarge          = errors.New("agent: frame is too large")
)

// Identity 是项目解析后的逻辑玩家身份。
type Identity struct {
	RoleID    int64
	SessionID int64
}

func (i Identity) valid() bool {
	return i.RoleID > 0 && i.SessionID > 0
}

// AuthRequest 是握手传入项目身份解析回调的输入。
type AuthRequest struct {
	Token   []byte
	Restore bool
	Ack     int64
}

// IdentityResolver 将项目不透明握手数据解析为逻辑玩家身份。
// 它可以校验 token，也可以消费上游已认证的可信凭据。
type IdentityResolver interface {
	ResolveIdentity(context.Context, AuthRequest) (Identity, error)
}

// IdentityResolverFunc 适合以内联函数实现 IdentityResolver。
type IdentityResolverFunc func(context.Context, AuthRequest) (Identity, error)

func (f IdentityResolverFunc) ResolveIdentity(ctx context.Context, request AuthRequest) (Identity, error) {
	return f(ctx, request)
}

// DispatchRequest 是已通过 Seq 校验的业务 BON 消息。
type DispatchRequest struct {
	Identity Identity
	Seq      int64
	Payload  []byte
}

// Dispatcher 由项目实现业务命令解码与投递。
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
	Acquire(context.Context, int64) (msgq.SessionClient, error)
}

// ConnectionIDGenerator 生成区分重连的正连接编号。
type ConnectionIDGenerator func() (int64, error)

// ServerOptions 配置传输无关的 Agent Session 运行时。
type ServerOptions struct {
	IdentityResolver IdentityResolver
	Dispatcher       Dispatcher
	Sessions         SessionClientRouter
	ConnectionID     ConnectionIDGenerator
	FetchBatch       int
}
