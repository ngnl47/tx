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
	// ErrHandoffInstanceMismatch 表示请求或确认响应的启动身份不匹配，不能继续交接。
	ErrHandoffInstanceMismatch = errors.New("control: handoff runtime instance mismatch")
	// ErrHandoffUnavailable 表示宿主尚未就绪、已经停止或身份失效。
	ErrHandoffUnavailable = errors.New("control: handoff unavailable")
)

// HandoffController 提供交接准备确认：宿主执行最终写回并保留资源，客户端校验远程确认；不执行槽位晋升。
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

// NewHandoffClient 复用调用方的连接，返回确认源实例收尾的窄能力；不接管连接或执行槽位晋升。
func NewHandoffClient(connection grpc.ClientConnInterface) HandoffController {
	return handoffClient{controlpb.NewHandoffServiceClient(connection)}
}

type handoffClient struct {
	client controlpb.HandoffServiceClient
}

func (c handoffClient) PrepareHandoff(ctx context.Context, instance string) error {
	response, err := c.client.PrepareHandoff(ctx, &controlpb.PrepareHandoffRequest{RuntimeInstanceId: instance})
	if err != nil {
		return err
	}
	if instance == "" || response.GetRuntimeInstanceId() != instance {
		return ErrHandoffInstanceMismatch
	}
	return nil
}
