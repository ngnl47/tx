// Package control 提供进程优雅排水所需的最小运维 gRPC 接口，不感知应用服务或持久化实现。
package control

import (
	"context"
	"errors"

	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrControllerNil = errors.New("control: lifecycle controller is nil")

// State 描述进程的排水状态。
type State uint8

const (
	// StateRunning 表示进程正常接收外部工作。
	StateRunning State = iota + 1
	// StateDraining 表示入口已封闭，正在等待已接纳工作完成。
	StateDraining
	// StateDrained 表示已接受工作排空，不代表最终写回成功或可晋升槽位。
	StateDrained
	// StateFailed 表示排水过程失败，Error 保存原因。
	StateFailed
)

// Status 表示进程当前的运维状态。
type Status struct {
	// State 是当前生命周期状态。
	State State
	// Error 是 StateFailed 时的错误文本，其他状态保持为空。
	Error string
}

// LifecycleController 由应用宿主实现。BeginDrain 只负责封闭入口并启动排水，
// WaitDrained 负责等待排水完成。
type LifecycleController interface {
	BeginDrain(context.Context) error
	WaitDrained(context.Context) error
	Status() Status
}

// RegisterLifecycleService 将生命周期控制器注册到 gRPC 服务端。
func RegisterLifecycleService(registrar grpc.ServiceRegistrar, controller LifecycleController) error {
	if registrar == nil || controller == nil {
		return ErrControllerNil
	}
	controlpb.RegisterLifecycleServiceServer(registrar, &lifecycleService{controller: controller})
	return nil
}

type lifecycleService struct {
	controlpb.UnimplementedLifecycleServiceServer
	controller LifecycleController
}

func (s *lifecycleService) Status(context.Context, *controlpb.Empty) (*controlpb.LifecycleStatus, error) {
	return statusToProto(s.controller.Status()), nil
}

func (s *lifecycleService) Drain(ctx context.Context, _ *controlpb.Empty) (*controlpb.LifecycleStatus, error) {
	if err := s.controller.BeginDrain(ctx); err != nil {
		return nil, statusFromError(err)
	}
	return statusToProto(s.controller.Status()), nil
}

func (s *lifecycleService) WaitDrained(ctx context.Context, _ *controlpb.Empty) (*controlpb.LifecycleStatus, error) {
	if err := s.controller.WaitDrained(ctx); err != nil {
		return nil, statusFromError(err)
	}
	return statusToProto(s.controller.Status()), nil
}

func statusFromError(err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func statusToProto(value Status) *controlpb.LifecycleStatus {
	result := &controlpb.LifecycleStatus{Error: value.Error}
	switch value.State {
	case StateRunning:
		result.State = controlpb.LifecycleState_LIFECYCLE_STATE_RUNNING
	case StateDraining:
		result.State = controlpb.LifecycleState_LIFECYCLE_STATE_DRAINING
	case StateDrained:
		result.State = controlpb.LifecycleState_LIFECYCLE_STATE_DRAINED
	case StateFailed:
		result.State = controlpb.LifecycleState_LIFECYCLE_STATE_FAILED
	default:
		result.State = controlpb.LifecycleState_LIFECYCLE_STATE_UNSPECIFIED
	}
	return result
}
