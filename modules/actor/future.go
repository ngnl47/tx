package actor

import (
	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/internal/actorresult"
)

// Result 是远程 Actor 调用的业务结果；客户端上行使用独立的邮箱接纳确认。
type Result = actorresult.Result

// Future 只提供结果等待，取消等待不撤销已接受工作。
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
