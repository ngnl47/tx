package agent

import (
	"context"
	"errors"
	"fmt"

	txlog "github.com/ngnl47/tx/log"
	agentpb "github.com/ngnl47/tx/modules/agent/pb"
	"github.com/ngnl47/tx/modules/msgq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrControlRegistrarNil = errors.New("agent: control registrar is nil")

// RegisterControlService 将 Agent 本地 Session 控制能力注册到内部 gRPC Host。
func RegisterControlService(registrar grpc.ServiceRegistrar, server *Server) error {
	if registrar == nil {
		return ErrControlRegistrarNil
	}
	if server == nil {
		return ErrServerNil
	}
	agentpb.RegisterAgentControlServiceServer(registrar, &controlService{server: server})
	return nil
}

type controlService struct {
	agentpb.UnimplementedAgentControlServiceServer
	server *Server
}

func (service *controlService) DisconnectSession(ctx context.Context, request *agentpb.SessionControlRequest) (*agentpb.SessionControlResponse, error) {
	if request.GetSessionId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "agent: invalid session control request")
	}
	result := service.server.DisconnectSession(request.GetSessionId())
	logSessionControl(ctx, "disconnect", request.GetReason(), result, nil)
	return sessionControlResultToProto(result), nil
}

func (service *controlService) TerminateSession(ctx context.Context, request *agentpb.SessionControlRequest) (*agentpb.SessionControlResponse, error) {
	if request.GetSessionId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "agent: invalid session control request")
	}
	result, err := service.server.TerminateSession(ctx, request.GetSessionId())
	logSessionControl(ctx, "terminate", request.GetReason(), result, err)
	if err != nil {
		return nil, sessionControlStatusError(err)
	}
	return sessionControlResultToProto(result), nil
}

func sessionControlResultToProto(result SessionControlResult) *agentpb.SessionControlResponse {
	return &agentpb.SessionControlResponse{
		SessionId:    result.SessionID,
		ConnectionId: result.ConnectionID,
		Handled:      result.Handled,
	}
}

func sessionControlStatusError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, msgq.ErrSessionChanged):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, msgq.ErrSessionOffline), errors.Is(err, msgq.ErrRouteUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, fmt.Sprintf("agent session control failed: %v", err))
	}
}

func logSessionControl(ctx context.Context, action, reason string, result SessionControlResult, err error) {
	args := []any{
		"action", action,
		"reason", reason,
		"sessionID", result.SessionID,
		"connectionID", result.ConnectionID,
		"handled", result.Handled,
	}
	if err != nil {
		args = append(args, "err", err)
		txlog.ErrorContext(ctx, "agent session control failed", args...)
		return
	}
	txlog.InfoContext(ctx, "agent session controlled", args...)
}
