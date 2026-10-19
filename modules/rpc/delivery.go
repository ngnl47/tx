package rpc

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	txmetrics "github.com/ngnl47/tx/metrics"
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
	CommandHash uint32
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

// StreamCallReceiver 处理长流上的独立调用；每项仍按自己的结果完成。
type StreamCallReceiver interface {
	CallStream(context.Context, Delivery) (Result, error)
}

// StreamSubmitReceiver 只在消息进入本地接收队列后返回，不等待业务执行。
// 服务端在读取循环中同步调用它，以保持同一条流的入队顺序。
type StreamSubmitReceiver interface {
	SubmitStream(context.Context, Delivery) error
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

func (s *deliveryService) CallStream(stream grpc.BidiStreamingServer[rpcpb.StreamCallRequest, rpcpb.StreamCallResponse]) error {
	receiver, ok := s.callReceiver.(StreamCallReceiver)
	if !ok {
		return status.Error(codes.Unimplemented, "method CallStream not implemented")
	}
	var workers sync.WaitGroup
	var sendMu sync.Mutex
	var sendErr error
	var callsMu sync.Mutex
	calls := make(map[uint64]context.CancelFunc)
	defer func() {
		callsMu.Lock()
		for _, cancel := range calls {
			cancel()
		}
		callsMu.Unlock()
	}()
	// 停止读取即可把 gRPC 流控传回发送方，避免无限创建等待 Actor 的 goroutine。
	slots := make(chan struct{}, 1024)
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			workers.Wait()
			return sendErr
		}
		if err != nil {
			return err
		}
		if request.GetId() == 0 {
			return status.Error(codes.InvalidArgument, "stream call id is zero")
		}
		if request.GetCancel() {
			callsMu.Lock()
			cancel := calls[request.GetId()]
			callsMu.Unlock()
			if cancel != nil {
				cancel()
			}
			continue
		}
		delivery, err := deliveryFromProto(request.GetDelivery())
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if request.GetOneWay() {
			submitter, ok := s.callReceiver.(StreamSubmitReceiver)
			if !ok {
				return status.Error(codes.Unimplemented, "method SubmitStream not implemented")
			}
			started := time.Now()
			err := submitter.SubmitStream(stream.Context(), delivery)
			txmetrics.RecordRPCSubmit(err, time.Since(started))
			if err != nil {
				txlog.Error("one-way stream delivery failed", "message_type", delivery.MessageType, "err", err)
			}
			continue
		}
		select {
		case slots <- struct{}{}:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
		itemCtx, cancel := context.WithCancel(stream.Context())
		callsMu.Lock()
		if calls[request.GetId()] != nil {
			callsMu.Unlock()
			cancel()
			<-slots
			return status.Error(codes.InvalidArgument, "duplicate stream call id")
		}
		calls[request.GetId()] = cancel
		callsMu.Unlock()
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-slots }()
			defer func() {
				callsMu.Lock()
				delete(calls, request.GetId())
				callsMu.Unlock()
				cancel()
			}()
			result, callErr := receiver.CallStream(itemCtx, delivery)
			response := &rpcpb.StreamCallResponse{Id: request.GetId()}
			if callErr != nil {
				setStreamCallError(response, callErr)
			} else {
				response.Result, callErr = resultToProto(result)
				if callErr != nil {
					response.ErrorCode = int32(codes.InvalidArgument)
					response.ErrorMessage = callErr.Error()
				}
			}
			sendMu.Lock()
			if sendErr == nil {
				sendErr = stream.Send(response)
			}
			sendMu.Unlock()
		}()
	}
}

func setStreamCallError(response *rpcpb.StreamCallResponse, err error) {
	response.ErrorCode = int32(codes.Internal)
	if errors.Is(err, lifecycle.ErrDraining) || errors.Is(err, lifecycle.ErrStopping) {
		response.ErrorCode = int32(codes.Unavailable)
	}
	response.ErrorMessage = err.Error()
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
		CommandHash: request.GetCommandHash(),
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
