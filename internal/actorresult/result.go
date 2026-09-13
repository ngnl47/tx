package actorresult

import (
	"context"
	"errors"
	"strconv"

	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

const (
	// CallErrorCodeInternal 表示业务没有提供可公开错误码的内部失败。
	CallErrorCodeInternal int32 = -1
	// CallErrorCodeDispatch 表示 Actor 命令无法通过框架分派契约校验。
	CallErrorCodeDispatch int32 = -2
	// CallErrorCodeRejected 表示目标在入队前拒绝准入，业务尚未执行。
	CallErrorCodeRejected int32 = -3
)

var ErrInvalidResponse = errors.New("actor: invalid response")

// Result 是 Actor 完成业务处理后返回的不透明结果。普通调用携带消息元数据和
// Payload；Agent 上行处理完成确认只携带 ProcessedUpSeq。
type Result struct {
	MessageType    string
	CommandID      uint64
	Payload        []byte
	ProcessedUpSeq int64
}

// CallError 是远端业务返回的显式错误。
type CallError struct {
	Code    int32
	Message string
	cause   error
}

// Unwrap 保留远端拒绝类别，调用方仍可使用 errors.Is。
func (err *CallError) Unwrap() error { return err.cause }

func (err *CallError) Error() string {
	if err == nil {
		return ""
	}
	if err.Message == "" {
		return strconv.FormatInt(int64(err.Code), 10)
	}
	return strconv.FormatInt(int64(err.Code), 10) + ": " + err.Message
}

// ErrorCode 允许同一个错误在响应边界保留稳定错误码。
func (err *CallError) ErrorCode() int32 {
	if err == nil {
		return 0
	}
	return err.Code
}

// NewCallError 创建可跨 Actor 边界传递的业务错误。
func NewCallError(code int32, cause error) *CallError {
	if code == 0 {
		code = CallErrorCodeInternal
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	return &CallError{Code: code, Message: message}
}

// resultWaiter 避免公开结果契约反向依赖具体传输。
type resultWaiter interface {
	Await(context.Context) (*actorpb.Result, error)
}

// Future 只开放等待结果；完成权限保留在传输内部。
type Future struct{ pending resultWaiter }

func WrapFuture(f resultWaiter) *Future { return &Future{pending: f} }
func (f *Future) Await(ctx context.Context) (Result, error) {
	value, err := f.pending.Await(ctx)
	return FromProto(value, err)
}
func FromProto(value *actorpb.Result, err error) (Result, error) {
	if err != nil {
		var remote *actorhost.RemoteError
		if errors.As(err, &remote) {
			return Result{}, &CallError{Code: remote.Code, Message: remote.Message, cause: remote}
		}
		return Result{}, err
	}
	if value == nil {
		return Result{}, ErrInvalidResponse
	}
	return Result{MessageType: value.MessageType, CommandID: value.CommandId, Payload: value.Payload, ProcessedUpSeq: value.ProcessedUpSeq}, nil
}
func ToProto(result Result) *actorpb.Result {
	return &actorpb.Result{MessageType: result.MessageType, CommandId: result.CommandID, Payload: result.Payload, ProcessedUpSeq: result.ProcessedUpSeq}
}
