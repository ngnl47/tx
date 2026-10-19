package actor

import (
	"errors"
	"strings"
	"unicode/utf8"

	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	txprotocol "github.com/ngnl47/tx/protocol"
)

var (
	ErrInvalidDelivery = errors.New("actor: invalid delivery")
)

// DeliveryOrigin 表示投递入口语义，与业务节点类型、传输协议及实现语言无关。
// 它不由 MessageType 推断，也不代替身份认证。
type DeliveryOrigin uint8

const (
	// OriginInternal 表示普通内部调用，也是零值。
	OriginInternal DeliveryOrigin = iota
	// OriginClient 表示客户端命令，由 Agent 设置，业务响应通过下行链路发送。
	OriginClient
	// OriginTask 由可靠任务消费者设置，消费确认等待业务完成。
	OriginTask
)

// Delivery 是投递给目标 Actor 的内部消息。目标位置由实体身份解析，Payload 保持调用方
// 编码后的 BON 字节，不在 Actor 运行时中反序列化为业务对象。
type Delivery struct {
	// Origin 由 Agent 和可靠任务消费入口覆盖；普通内部调用默认为 OriginInternal。
	Origin      DeliveryOrigin
	MessageType string
	CommandHash uint32
	RouteKey    txprotocol.RouteKey
	Payload     []byte
	Identity    *txprotocol.SessionIdentity

	mode      actorpb.DeliveryMode
	requestID uint64
}

// Validate 校验跨进程投递所需的最小元数据。
func (d Delivery) Validate() error {
	if d.Origin > OriginTask {
		return ErrInvalidDelivery
	}
	if d.MessageType == "" || d.MessageType != strings.TrimSpace(d.MessageType) {
		return ErrInvalidDelivery
	}
	if d.Identity != nil && d.Identity.Validate() != nil {
		return ErrInvalidDelivery
	}
	return validateRouteKey(d.RouteKey)
}

// ToProto 将 Delivery 转换为 跨节点传输的 protobuf 信封。
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
	identity, err := sessionIdentityToProto(d.Identity)
	if err != nil {
		return nil, err
	}
	return &actorpb.Delivery{
		MessageType:     d.MessageType,
		Origin:          actorpb.DeliveryOrigin(d.Origin),
		CommandHash:     d.CommandHash,
		Payload:         append([]byte(nil), d.Payload...),
		Mode:            mode,
		RouteKeyKind:    routeKeyKind,
		RouteKeyNumeric: routeKeyNumeric,
		RouteKeyString:  routeKeyString,
		RequestId:       requestID,
		Identity:        identity,
	}, nil
}

// DeliveryFromProto 将收到的 protobuf 信封转换为业务使用的 Delivery。
func DeliveryFromProto(message *actorpb.Delivery) (Delivery, error) {
	if message == nil {
		return Delivery{}, ErrInvalidDelivery
	}
	if message.GetOrigin() < actorpb.DeliveryOrigin_DELIVERY_ORIGIN_INTERNAL || message.GetOrigin() > actorpb.DeliveryOrigin_DELIVERY_ORIGIN_TASK {
		return Delivery{}, ErrInvalidDelivery
	}
	routeKey, err := routeKeyFromProto(message.GetRouteKeyKind(), message.GetRouteKeyNumeric(), message.GetRouteKeyString())
	if err != nil {
		return Delivery{}, err
	}
	identity, err := sessionIdentityFromProto(message.GetIdentity())
	if err != nil {
		return Delivery{}, err
	}
	delivery := Delivery{
		MessageType: message.GetMessageType(),
		Origin:      DeliveryOrigin(message.GetOrigin()),
		CommandHash: message.GetCommandHash(),
		RouteKey:    routeKey,
		Payload:     append([]byte(nil), message.GetPayload()...),
		Identity:    identity,
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

func sessionIdentityToProto(identity *txprotocol.SessionIdentity) (*actorpb.SessionIdentity, error) {
	if identity == nil {
		return nil, nil
	}
	accessMode, err := accessModeToProto(identity.AccessMode)
	if err != nil {
		return nil, err
	}
	return &actorpb.SessionIdentity{
		AccountId:       identity.AccountID,
		BorrowAccountId: identity.BorrowAccountID,
		ServerId:        identity.ServerID,
		RoleId:          identity.RoleID,
		SessionId:       identity.SessionID,
		ConnectionId:    identity.ConnectionID,
		AccessMode:      accessMode,
		CanCreateRole:   identity.CanCreateRole,
	}, nil
}

func sessionIdentityFromProto(message *actorpb.SessionIdentity) (*txprotocol.SessionIdentity, error) {
	if message == nil {
		return nil, nil
	}
	accessMode, err := accessModeFromProto(message.GetAccessMode())
	if err != nil {
		return nil, err
	}
	return &txprotocol.SessionIdentity{
		SessionPrincipal: txprotocol.SessionPrincipal{
			AccountID:       message.GetAccountId(),
			BorrowAccountID: message.GetBorrowAccountId(),
			ServerID:        message.GetServerId(),
			RoleID:          message.GetRoleId(),
			SessionID:       message.GetSessionId(),
			AccessMode:      accessMode,
			CanCreateRole:   message.GetCanCreateRole(),
		},
		ConnectionID: message.GetConnectionId(),
	}, nil
}

func accessModeToProto(mode txprotocol.AccessMode) (actorpb.AccessMode, error) {
	switch mode {
	case txprotocol.AccessModeNormal:
		return actorpb.AccessMode_ACCESS_MODE_NORMAL, nil
	case txprotocol.AccessModeBorrow:
		return actorpb.AccessMode_ACCESS_MODE_BORROW, nil
	default:
		return actorpb.AccessMode_ACCESS_MODE_NORMAL, ErrInvalidDelivery
	}
}

func accessModeFromProto(mode actorpb.AccessMode) (txprotocol.AccessMode, error) {
	switch mode {
	case actorpb.AccessMode_ACCESS_MODE_NORMAL:
		return txprotocol.AccessModeNormal, nil
	case actorpb.AccessMode_ACCESS_MODE_BORROW:
		return txprotocol.AccessModeBorrow, nil
	default:
		return txprotocol.AccessModeNormal, ErrInvalidDelivery
	}
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
