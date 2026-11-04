package actor

import (
	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/internal/actorresult"
)

// Result 是同实例与跨实例 Call/CallFuture 共用的业务结果。
// Completed 表示处理成功且无响应体，不是上行或邮箱接纳确认；
// Agent 上行不使用此结果作逐消息确认，客户端下行 Ack 用于裁剪 MsgQ 邮箱。
type Result = actorresult.Result

// Future 只提供结果等待，取消等待不撤销已接受工作。
// 创建时的 Context 约束调用等待生命周期；Await 的 Context 仅约束本次等待，
// 不能延长创建时的期限。取消或超时不能证明业务未执行，不应据此自动重投。
type Future = actorresult.Future

// CallError 保留远端显式错误码。
type CallError = actorresult.CallError

const (
	CallErrorCodeInternal = actorresult.CallErrorCodeInternal
	CallErrorCodeDispatch = actorresult.CallErrorCodeDispatch
	CallErrorCodeRejected = actorresult.CallErrorCodeRejected
)

var (
	ErrInvalidResponse = actorresult.ErrInvalidResponse
	ErrRuntimeDraining = actorhost.ErrDraining
)

// NewCallError 构造可跨实体边界传递的业务错误。
func NewCallError(code int32, cause error) *CallError { return actorresult.NewCallError(code, cause) }
