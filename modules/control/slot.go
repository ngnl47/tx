package control

import (
	"context"
	"errors"

	controlpb "github.com/ngnl47/tx/modules/control/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrSlotControllerNil = errors.New("control: slot controller is nil")

// SlotPromotion 描述一次受 generation 保护的逻辑槽位交接。Current 必须匹配持久化 assignment；
// 下一代 generation 由框架推导，不能由运维脚本独立提供。
type SlotPromotion struct {
	Current               discovery.SlotAssignment
	NextRuntimeInstanceID string
}

// NextAssignment 校验交接请求并返回确定的目标 assignment。
func (promotion SlotPromotion) NextAssignment() (discovery.SlotAssignment, error) {
	next := discovery.SlotAssignment{
		SlotID:            promotion.Current.SlotID,
		RuntimeInstanceID: promotion.NextRuntimeInstanceID,
		Generation:        promotion.Current.Generation + 1,
	}
	if err := discovery.ValidateSlotPromotion(promotion.Current, next); err != nil {
		return discovery.SlotAssignment{}, err
	}
	return next, nil
}

// SlotController 执行由存储保障的 CAS。实现必须在发布前确认目标运行实例已经就绪。
type SlotController interface {
	PromoteSlot(context.Context, SlotPromotion) error
}

// RegisterSlotService 将逻辑槽位操作注册到控制面 gRPC 服务端。
func RegisterSlotService(registrar grpc.ServiceRegistrar, controller SlotController) error {
	if registrar == nil || controller == nil {
		return ErrSlotControllerNil
	}
	controlpb.RegisterSlotServiceServer(registrar, &slotService{controller: controller})
	return nil
}

type slotService struct {
	controlpb.UnimplementedSlotServiceServer
	controller SlotController
}

func (service *slotService) PromoteSlot(ctx context.Context, request *controlpb.PromoteSlotRequest) (*controlpb.PromoteSlotResponse, error) {
	promotion, err := slotPromotionFromProto(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	assignment, err := promotion.NextAssignment()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := service.controller.PromoteSlot(ctx, promotion); err != nil {
		return nil, slotStatusFromError(err)
	}
	return &controlpb.PromoteSlotResponse{Assignment: slotAssignmentToProto(assignment)}, nil
}

func slotPromotionFromProto(request *controlpb.PromoteSlotRequest) (SlotPromotion, error) {
	if request == nil || request.GetCurrent() == nil {
		return SlotPromotion{}, discovery.ErrInvalidSlotPromotion
	}
	current := request.GetCurrent()
	promotion := SlotPromotion{
		Current: discovery.SlotAssignment{
			SlotID:            current.GetSlotId(),
			RuntimeInstanceID: current.GetRuntimeInstanceId(),
			Generation:        current.GetGeneration(),
		},
		NextRuntimeInstanceID: request.GetNextRuntimeInstanceId(),
	}
	if _, err := promotion.NextAssignment(); err != nil {
		return SlotPromotion{}, err
	}
	return promotion, nil
}

func slotAssignmentToProto(assignment discovery.SlotAssignment) *controlpb.SlotAssignment {
	return &controlpb.SlotAssignment{
		SlotId:            assignment.SlotID,
		RuntimeInstanceId: assignment.RuntimeInstanceID,
		Generation:        assignment.Generation,
	}
}

func slotStatusFromError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, discovery.ErrInvalidSlotAssignment), errors.Is(err, discovery.ErrInvalidSlotPromotion):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, discovery.ErrSlotNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, discovery.ErrSlotConflict):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, discovery.ErrSlotRuntimeUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
