package control

import (
	"context"
	"errors"

	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrHandoffInstanceMismatch 表示请求不属于当前启动实例，不能执行交接准备。
	ErrHandoffInstanceMismatch = errors.New("control: handoff runtime instance mismatch")
	// ErrHandoffUnavailable 表示宿主尚未就绪、已经停止或身份失效。
	ErrHandoffUnavailable = errors.New("control: handoff unavailable")
)

// HandoffController 由宿主保证最终写回及资源保留，不执行槽位晋升。
type HandoffController interface {
	PrepareHandoff(context.Context, string) error
}

// RegisterHandoffService 注册可选的交接准备能力；不改变既有 LifecycleController 契约。
func RegisterHandoffService(registrar grpc.ServiceRegistrar, controller HandoffController) error {
	if registrar == nil || controller == nil {
		return ErrHandoffUnavailable
	}
	controlpb.RegisterHandoffServiceServer(registrar, &handoffService{controller: controller})
	return nil
}

type handoffService struct {
	controlpb.UnimplementedHandoffServiceServer
	controller HandoffController
}

func (s *handoffService) PrepareHandoff(ctx context.Context, request *controlpb.PrepareHandoffRequest) (*controlpb.PrepareHandoffResponse, error) {
	instanceID := request.GetRuntimeInstanceId()
	if instanceID == "" {
		return nil, status.Error(codes.InvalidArgument, "runtime instance ID is required")
	}
	if err := s.controller.PrepareHandoff(ctx, instanceID); err != nil {
		if errors.Is(err, ErrHandoffInstanceMismatch) || errors.Is(err, ErrHandoffUnavailable) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, statusFromError(err)
	}
	return &controlpb.PrepareHandoffResponse{RuntimeInstanceId: instanceID}, nil
}
