package actor

import (
	"errors"
	"strings"

	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

var ErrInvalidDelivery = errors.New("actor: invalid delivery")

// Delivery 是投递给目标 Actor 的内部消息。目标位置由 PID 决定，Payload 保持调用方
// 编码后的 BON 字节，不在 Actor 运行时中反序列化为业务对象。
type Delivery struct {
	MessageType string
	CommandID   uint64
	Payload     []byte
}

// Validate 校验跨进程投递所需的最小元数据。
func (d Delivery) Validate() error {
	if d.MessageType == "" || d.MessageType != strings.TrimSpace(d.MessageType) {
		return ErrInvalidDelivery
	}
	return nil
}

// ToProto 将 Delivery 转换为 ProtoActor 可跨节点传输的 protobuf 信封。
func (d Delivery) ToProto() (*actorpb.Delivery, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return &actorpb.Delivery{
		MessageType: d.MessageType,
		CommandId:   d.CommandID,
		Payload:     append([]byte(nil), d.Payload...),
	}, nil
}

// DeliveryFromProto 将收到的 protobuf 信封转换为业务使用的 Delivery。
func DeliveryFromProto(message *actorpb.Delivery) (Delivery, error) {
	if message == nil {
		return Delivery{}, ErrInvalidDelivery
	}
	delivery := Delivery{
		MessageType: message.GetMessageType(),
		CommandID:   message.GetCommandId(),
		Payload:     append([]byte(nil), message.GetPayload()...),
	}
	if err := delivery.Validate(); err != nil {
		return Delivery{}, err
	}
	return delivery, nil
}
