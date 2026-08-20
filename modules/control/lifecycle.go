// Package control exposes a minimal operational gRPC surface for graceful
// process drain. It does not know application services or persistence.
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

// State describes one process drain state.
type State uint8

const (
	StateRunning State = iota + 1
	StateDraining
	StateDrained
	StateFailed
)

// Status is the current operational state of a process.
type Status struct {
	State State
	Error string
}

// LifecycleController is implemented by the application host. BeginDrain
// must only stop ingress and start drain work; WaitDrained waits for it.
type LifecycleController interface {
	BeginDrain(context.Context) error
	WaitDrained(context.Context) error
	Status() Status
}

// RegisterLifecycleService binds a lifecycle controller to a gRPC server.
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
