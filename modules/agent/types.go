// Package agent 提供玩家连接到逻辑会话的通用接入运行时。
//
// 项目通过 IdentityResolver 验票、Dispatcher 投递业务消息，SessionClientRouter 定位 MsgQ。
// NewServer 装配这些能力，Server.Serve 接管单条 Transport 的收发和关闭。
// Agent 维护物理连接与上行游标；逻辑邮箱和未确认下行由 MsgQ 维护，登录授权由项目决定。
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
	// ErrSessionEnded 由身份解析器表示票据已永久失效，客户端应停止自动恢复。
	ErrSessionEnded = errors.New("agent: session has ended")
)

// Identity 是 Agent 为当前连接构造的可信会话身份。
type Identity = protocol.SessionIdentity

// AuthRequest 是握手传入项目身份解析回调的输入。
type AuthRequest struct {
	Token   []byte
	Restore bool
	Ack     int64
	// Logout 只验证并结束原会话，不建立新的邮箱或物理业务接入。
	Logout bool
}

// AuthResult 由项目验票返回可信身份和准入授权；Admission 不进入业务消息头。
type AuthResult struct {
	protocol.SessionPrincipal
	Admission *msgq.SessionAdmission
}

// IdentityResolver 将项目不透明握手数据解析为已认证的会话主体。
// 它可以校验 token，也可以消费上游已认证的可信凭据。
type IdentityResolver interface {
	// ResolveIdentity 返回可信身份；成功仅表示验票通过，不表示 MsgQ 已接纳。
	// 不同连接可并发调用；永久失效可返回 ErrSessionEnded，普通错误不解释为永久失效。
	ResolveIdentity(context.Context, AuthRequest) (AuthResult, error)
}

// IdentityResolverFunc 适合以内联函数实现 IdentityResolver。
type IdentityResolverFunc func(context.Context, AuthRequest) (AuthResult, error)

func (f IdentityResolverFunc) ResolveIdentity(ctx context.Context, request AuthRequest) (AuthResult, error) {
	return f(ctx, request)
}

// DispatchRequest 是已通过 Seq 校验的业务 BON 消息。
type DispatchRequest struct {
	Identity    Identity
	CommandHash uint32
	RouteKey    RouteKey
	Payload     []byte
}

// Dispatcher 由项目实现业务命令路由；远端投递返回只代表本地写流完成。
type Dispatcher interface {
	// Dispatch 在当前连接内顺序调用，不同连接可并发调用。
	// 成功不确认远端邮箱接纳或业务完成；错误会结束当前接入，不自动重投业务。
	Dispatch(context.Context, DispatchRequest) error
}

// DispatchFunc 适合以内联函数实现 Dispatcher。
type DispatchFunc func(context.Context, DispatchRequest) error

func (f DispatchFunc) Dispatch(ctx context.Context, request DispatchRequest) error {
	return f(ctx, request)
}

// Transport 是一个已完成帧读写的玩家连接。TCP 与 WebSocket 都适配为此接口。
type Transport interface {
	// Read 读取完整帧，须响应 ctx 取消或传输关闭；与 Write 可并行执行。
	Read(context.Context) (Frame, error)
	// Write 写入完整帧，须响应 ctx 取消或传输关闭；不确认客户端业务处理。
	Write(context.Context, Frame) error
	// Close 应可重复调用，并中断阻塞的读写；可能由连接撤销与收尾并发触发。
	Close() error
}

// SessionClientRouter 为认证后的角色选择当前可用 msgq 会话客户端。
type SessionClientRouter interface {
	// Acquire 借用目标 MsgQ 的会话操作入口，不创建邮箱，也不转移共享连接关闭权。
	Acquire(context.Context, msgq.AcquireSessionRequest) (msgq.SessionClient, error)
	// TerminateLogicalSession 只定位已有归属，不为退出创建新会话。
	TerminateLogicalSession(context.Context, int64, int64) error
}

// ConnectionIDGenerator 生成区分重连的正连接编号。
type ConnectionIDGenerator func() (int64, error)

// ServerOptions 配置传输无关的 Agent Session 运行时。
type ServerOptions struct {
	// IdentityResolver 由项目提供验票与身份解析，必填。
	IdentityResolver IdentityResolver
	// Dispatcher 由项目提供业务投递，必填；不随单条连接关闭。
	Dispatcher Dispatcher
	// Admission 控制逐条上行业务准入；nil 不附加该准入检查。
	Admission lifecycle.Admission
	// Connections 登记完整连接的实际执行期，独立于逐帧业务准入。
	// nil 不登记完整连接；由 TCP/WebSocket 等接入宿主使用。
	Connections lifecycle.Admission
	// Sessions 为必填共享路由能力，生命周期由装配方管理。
	Sessions SessionClientRouter
	// ConnectionID 每次 Serve 生成连接编号；nil 使用默认随机正编号生成器。
	ConnectionID ConnectionIDGenerator
}
