package actor

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	protoactor "github.com/asynkron/protoactor-go/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	txprotocol "github.com/ngnl47/tx/protocol"
)

var (
	ErrInvalidDelivery = errors.New("actor: invalid delivery")
)

// Delivery 是投递给目标 Actor 的内部消息。目标位置由 PID 决定，Payload 保持调用方
// 编码后的 BON 字节，不在 Actor 运行时中反序列化为业务对象。
type Delivery struct {
	MessageType string
	CommandID   uint64
	RouteKey    txprotocol.RouteKey
	Payload     []byte

	mode      actorpb.DeliveryMode
	requestID uint64
}

// Validate 校验跨进程投递所需的最小元数据。
func (d Delivery) Validate() error {
	if d.MessageType == "" || d.MessageType != strings.TrimSpace(d.MessageType) {
		return ErrInvalidDelivery
	}
	return validateRouteKey(d.RouteKey)
}

// ToProto 将 Delivery 转换为 ProtoActor 可跨节点传输的 protobuf 信封。
func (d Delivery) ToProto() (*actorpb.Delivery, error) {
	return d.toProto(actorpb.DeliveryMode_DELIVERY_MODE_TELL, 0)
}

func (d Delivery) toProto(mode actorpb.DeliveryMode, requestID uint64) (*actorpb.Delivery, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	routeKeyKind, routeKeyNumeric, routeKeyString, err := routeKeyToProto(d.RouteKey)
	if err != nil {
		return nil, err
	}
	return &actorpb.Delivery{
		MessageType:     d.MessageType,
		CommandId:       d.CommandID,
		Payload:         append([]byte(nil), d.Payload...),
		Mode:            mode,
		RouteKeyKind:    routeKeyKind,
		RouteKeyNumeric: routeKeyNumeric,
		RouteKeyString:  routeKeyString,
		RequestId:       requestID,
	}, nil
}

// DeliveryFromProto 将收到的 protobuf 信封转换为业务使用的 Delivery。
func DeliveryFromProto(message *actorpb.Delivery) (Delivery, error) {
	if message == nil {
		return Delivery{}, ErrInvalidDelivery
	}
	routeKey, err := routeKeyFromProto(message.GetRouteKeyKind(), message.GetRouteKeyNumeric(), message.GetRouteKeyString())
	if err != nil {
		return Delivery{}, err
	}
	delivery := Delivery{
		MessageType: message.GetMessageType(),
		CommandID:   message.GetCommandId(),
		RouteKey:    routeKey,
		Payload:     append([]byte(nil), message.GetPayload()...),
		mode:        message.GetMode(),
		requestID:   message.GetRequestId(),
	}
	if err := delivery.Validate(); err != nil {
		return Delivery{}, err
	}
	switch delivery.mode {
	case actorpb.DeliveryMode_DELIVERY_MODE_TELL:
		if delivery.requestID != 0 {
			return Delivery{}, ErrInvalidDelivery
		}
	case actorpb.DeliveryMode_DELIVERY_MODE_ACCEPTED, actorpb.DeliveryMode_DELIVERY_MODE_CALL:
		if delivery.requestID == 0 {
			return Delivery{}, ErrInvalidDelivery
		}
	default:
		return Delivery{}, ErrInvalidDelivery
	}
	return delivery, nil
}

// WantsResult 判断当前消息是否由 Call 或 CallFuture 发起并要求业务结果。
func (d Delivery) WantsResult() bool {
	return d.mode == actorpb.DeliveryMode_DELIVERY_MODE_CALL && d.requestID != 0
}

func validateRouteKey(key txprotocol.RouteKey) error {
	switch key.Kind() {
	case txprotocol.RouteKeyNone:
		return nil
	case txprotocol.RouteKeyInt64:
		_, ok := key.Int64()
		if ok {
			return nil
		}
	case txprotocol.RouteKeyUint64:
		_, ok := key.Uint64()
		if ok {
			return nil
		}
	case txprotocol.RouteKeyString:
		value, ok := key.String()
		if ok && value != "" && utf8.ValidString(value) {
			return nil
		}
	}
	return ErrInvalidDelivery
}

func routeKeyToProto(key txprotocol.RouteKey) (actorpb.RouteKeyKind, uint64, string, error) {
	if err := validateRouteKey(key); err != nil {
		return actorpb.RouteKeyKind_ROUTE_KEY_NONE, 0, "", err
	}
	switch key.Kind() {
	case txprotocol.RouteKeyNone:
		return actorpb.RouteKeyKind_ROUTE_KEY_NONE, 0, "", nil
	case txprotocol.RouteKeyInt64:
		value, _ := key.Int64()
		return actorpb.RouteKeyKind_ROUTE_KEY_INT64, uint64(value), "", nil
	case txprotocol.RouteKeyUint64:
		value, _ := key.Uint64()
		return actorpb.RouteKeyKind_ROUTE_KEY_UINT64, value, "", nil
	case txprotocol.RouteKeyString:
		value, _ := key.String()
		return actorpb.RouteKeyKind_ROUTE_KEY_STRING, 0, value, nil
	default:
		return actorpb.RouteKeyKind_ROUTE_KEY_NONE, 0, "", ErrInvalidDelivery
	}
}

func routeKeyFromProto(kind actorpb.RouteKeyKind, numeric uint64, text string) (txprotocol.RouteKey, error) {
	switch kind {
	case actorpb.RouteKeyKind_ROUTE_KEY_NONE:
		if numeric != 0 || text != "" {
			return txprotocol.RouteKey{}, ErrInvalidDelivery
		}
		return txprotocol.RouteKey{}, nil
	case actorpb.RouteKeyKind_ROUTE_KEY_INT64:
		if text != "" {
			return txprotocol.RouteKey{}, ErrInvalidDelivery
		}
		return txprotocol.Int64RouteKey(int64(numeric)), nil
	case actorpb.RouteKeyKind_ROUTE_KEY_UINT64:
		if text != "" {
			return txprotocol.RouteKey{}, ErrInvalidDelivery
		}
		return txprotocol.Uint64RouteKey(numeric), nil
	case actorpb.RouteKeyKind_ROUTE_KEY_STRING:
		if numeric != 0 || text == "" || !utf8.ValidString(text) {
			return txprotocol.RouteKey{}, ErrInvalidDelivery
		}
		return txprotocol.StringRouteKey(text), nil
	default:
		return txprotocol.RouteKey{}, ErrInvalidDelivery
	}
}

// TellAccepted 投递消息并等待目标 Actor 从邮箱取出。它不等待业务处理完成。
func (r *Runtime) TellAccepted(ctx context.Context, pid *protoactor.PID, delivery Delivery) error {
	future, err := r.sendRequest(ctx, pid, delivery, actorpb.DeliveryMode_DELIVERY_MODE_ACCEPTED, pendingAccepted)
	if err != nil {
		return err
	}
	_, err = future.Await(ctx)
	return err
}

// CallFuture 投递业务请求并立即返回 Future。
func (r *Runtime) CallFuture(ctx context.Context, pid *protoactor.PID, delivery Delivery) (*Future, error) {
	return r.sendRequest(ctx, pid, delivery, actorpb.DeliveryMode_DELIVERY_MODE_CALL, pendingResult)
}

// Call 投递业务请求并等待结果。它与 CallFuture 共用同一套 pending 和响应协议。
func (r *Runtime) Call(ctx context.Context, pid *protoactor.PID, delivery Delivery) (Result, error) {
	future, err := r.CallFuture(ctx, pid, delivery)
	if err != nil {
		return Result{}, err
	}
	return future.Await(ctx)
}

func (r *Runtime) sendRequest(ctx context.Context, pid *protoactor.PID, delivery Delivery, mode actorpb.DeliveryMode, expected pendingKind) (*Future, error) {
	if err := requireDeadline(ctx); err != nil {
		return nil, err
	}
	if pid == nil {
		return nil, ErrInvalidPID
	}
	if err := delivery.Validate(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrRuntimeStopped
	}
	if r.draining {
		return nil, ErrRuntimeDraining
	}
	if !r.started || r.cluster == nil || r.responder == nil {
		return nil, ErrRuntimeNotStarted
	}
	requestID, future, err := r.pending.add(ctx, expected, delivery.CommandID)
	if err != nil {
		return nil, err
	}
	message, err := delivery.toProto(mode, requestID)
	if err != nil {
		r.pending.fail(requestID, err)
		return nil, err
	}
	r.cluster.ActorSystem.Root.RequestWithCustomSender(pid, message, r.responder)
	return future, nil
}
