package rpc

import (
	"context"
	"errors"
	"strings"

	"github.com/ngnl47/tx/lifecycle"
	rpcpb "github.com/ngnl47/tx/modules/rpc/pb"
)

var (
	ErrCallReceiverNil = errors.New("rpc: call receiver is nil")
	ErrInvalidResult   = errors.New("rpc: invalid result")
)

// Result 表示一次同步 RPC 调用返回的不透明结果。
type Result struct {
	MessageType string
	CommandHash uint32
	Payload     []byte
}

// Validate 校验 Result 的传输契约。
func (r Result) Validate() error {
	if r.MessageType == "" || r.MessageType != strings.TrimSpace(r.MessageType) {
		return ErrInvalidResult
	}
	return nil
}

// CallReceiver 处理一条已经校验的同步内部消息，返回时提供业务结果。
// 不同请求可并发调用，Context 取消不强制终止业务；实现应遵循自身的执行与收尾边界。
type CallReceiver interface {
	Call(context.Context, Delivery) (Result, error)
}

// CallReceiverFunc 将函数适配为 CallReceiver。
type CallReceiverFunc func(context.Context, Delivery) (Result, error)

func (f CallReceiverFunc) Call(ctx context.Context, delivery Delivery) (Result, error) {
	return f(ctx, delivery)
}

type admissionCallReceiver struct {
	next      CallReceiver
	admission lifecycle.Admission
}

// NewAdmissionCallReceiver 为业务 CallReceiver 增加严格的服务入口控制。
// 排水开始后，新调用不再进入 next；登记从准入成功持续到 next.Call 返回。
// 如果 next 自行启动后台工作后返回，该工作不再受这层登记保护；
// 实现方须将其纳入接收队列或目标宿主的生命周期。
func NewAdmissionCallReceiver(next CallReceiver, admission lifecycle.Admission) (CallReceiver, error) {
	if next == nil {
		return nil, ErrCallReceiverNil
	}
	if admission == nil {
		return nil, ErrAdmissionNil
	}
	return &admissionCallReceiver{next: next, admission: admission}, nil
}

func (r *admissionCallReceiver) Call(ctx context.Context, delivery Delivery) (Result, error) {
	done, err := r.admission.Enter()
	if err != nil {
		return Result{}, err
	}
	defer done()
	return r.next.Call(ctx, delivery)
}

func resultToProto(result Result) (*rpcpb.Result, error) {
	if err := result.Validate(); err != nil {
		return nil, err
	}
	return &rpcpb.Result{
		MessageType: result.MessageType,
		CommandHash: result.CommandHash,
		Payload:     result.Payload,
	}, nil
}

func resultFromProto(result *rpcpb.Result) (Result, error) {
	if result == nil {
		return Result{}, ErrInvalidResult
	}
	converted := Result{
		MessageType: result.GetMessageType(),
		CommandHash: result.GetCommandHash(),
		Payload:     result.GetPayload(),
	}
	if err := converted.Validate(); err != nil {
		return Result{}, err
	}
	return converted, nil
}
