package rpc

import (
	"context"
	"errors"
	"strings"

	"github.com/ngnl47/tx/lifecycle"
	rpcpb "github.com/ngnl47/tx/modules/rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrReceiverNil     = errors.New("rpc: receiver is nil")
	ErrAdmissionNil    = errors.New("rpc: admission is nil")
	ErrInvalidDelivery = errors.New("rpc: invalid delivery")
)

// Delivery 表示一条定向内部消息，Payload 使用调用方选择的编解码格式。
type Delivery struct {
	Route       Route
	MessageType string
	CommandID   uint64
	Payload     []byte
}

func (d Delivery) Validate() error {
	if err := d.Route.Validate(); err != nil {
		return err
	}
	if d.MessageType == "" || d.MessageType != strings.TrimSpace(d.MessageType) {
		return ErrInvalidDelivery
	}
	return nil
}

// Receiver 处理一条已经校验的内部消息。
type Receiver interface {
	Deliver(context.Context, Delivery) error
}

// ReceiverFunc 将函数适配为 Receiver。
type ReceiverFunc func(context.Context, Delivery) error

func (f ReceiverFunc) Deliver(ctx context.Context, delivery Delivery) error {
	return f(ctx, delivery)
}

type admissionReceiver struct {
	next      Receiver
	admission lifecycle.Admission
}

// NewAdmissionReceiver 为业务 Receiver 增加严格的服务入口控制。
// 排水开始后，新消息不再进入 next；已经接受的工作会持续登记到完成回调执行为止。
func NewAdmissionReceiver(next Receiver, admission lifecycle.Admission) (Receiver, error) {
	if next == nil {
		return nil, ErrReceiverNil
	}
	if admission == nil {
		return nil, ErrAdmissionNil
	}
	return &admissionReceiver{next: next, admission: admission}, nil
}

func (r *admissionReceiver) Deliver(ctx context.Context, delivery Delivery) error {
	done, err := r.admission.Enter()
	if err != nil {
		return err
	}
	defer done()
	return r.next.Deliver(ctx, delivery)
}

// RegisterDeliveryService 将生成的 gRPC 服务注册到 registrar。
func RegisterDeliveryService(registrar grpc.ServiceRegistrar, receiver Receiver, callReceiver CallReceiver) error {
	if registrar == nil || (receiver == nil && callReceiver == nil) {
		return ErrReceiverNil
	}
	rpcpb.RegisterDeliveryServiceServer(registrar, &deliveryService{
		receiver:     receiver,
		callReceiver: callReceiver,
	})
	return nil
}

type deliveryService struct {
	rpcpb.UnimplementedDeliveryServiceServer
	receiver     Receiver
	callReceiver CallReceiver
}

func (s *deliveryService) Deliver(ctx context.Context, request *rpcpb.Delivery) (*rpcpb.DeliveryAck, error) {
	if s.receiver == nil {
		return nil, status.Error(codes.Unimplemented, "method Deliver not implemented")
	}
	delivery, err := deliveryFromProto(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.receiver.Deliver(ctx, delivery); err != nil {
		if errors.Is(err, lifecycle.ErrDraining) || errors.Is(err, lifecycle.ErrStopping) {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &rpcpb.DeliveryAck{Accepted: true}, nil
}

func (s *deliveryService) Call(ctx context.Context, request *rpcpb.Delivery) (*rpcpb.Result, error) {
	if s.callReceiver == nil {
		return nil, status.Error(codes.Unimplemented, "method Call not implemented")
	}
	delivery, err := deliveryFromProto(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	result, err := s.callReceiver.Call(ctx, delivery)
	if err != nil {
		if errors.Is(err, lifecycle.ErrDraining) || errors.Is(err, lifecycle.ErrStopping) {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	response, err := resultToProto(result)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return response, nil
}

func deliveryFromProto(request *rpcpb.Delivery) (Delivery, error) {
	if request == nil || request.GetRoute() == nil {
		return Delivery{}, ErrInvalidDelivery
	}
	route, err := routeFromProto(request.GetRoute())
	if err != nil {
		return Delivery{}, err
	}
	delivery := Delivery{
		Route:       route,
		MessageType: request.GetMessageType(),
		CommandID:   request.GetCommandId(),
		Payload:     request.GetPayload(),
	}
	if err := delivery.Validate(); err != nil {
		return Delivery{}, err
	}
	return delivery, nil
}

func routeFromProto(route *rpcpb.Route) (Route, error) {
	if route == nil {
		return Route{}, ErrInvalidRoute
	}
	converted := Route{
		Service:    route.GetService(),
		InstanceID: route.GetInstanceId(),
	}
	switch route.GetKind() {
	case rpcpb.RouteKind_ROUTE_KIND_SERVICE:
		converted.Kind = RouteService
	case rpcpb.RouteKind_ROUTE_KIND_INSTANCE:
		converted.Kind = RouteInstance
	default:
		return Route{}, ErrInvalidRoute
	}
	if err := converted.Validate(); err != nil {
		return Route{}, err
	}
	return converted, nil
}
