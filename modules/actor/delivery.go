package actor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	protoactor "github.com/asynkron/protoactor-go/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

var (
	ErrInvalidDelivery         = errors.New("actor: invalid delivery")
	ErrConfirmDeadlineRequired = errors.New("actor: confirmation requires a context deadline")
	ErrConfirmationRejected    = errors.New("actor: invalid confirmation response")
)

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
	return d.toProto(false)
}

func (d Delivery) toProto(requireAcceptance bool) (*actorpb.Delivery, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return &actorpb.Delivery{
		MessageType:       d.MessageType,
		CommandId:         d.CommandID,
		Payload:           append([]byte(nil), d.Payload...),
		RequireAcceptance: requireAcceptance,
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

// Confirm 投递一条要求确认的 Delivery，并等待目标 Actor 从邮箱取出该消息。
// 它不确认后续业务处理是否成功，且必须传入带 deadline 的 context。
func (r *Runtime) Confirm(ctx context.Context, pid *protoactor.PID, delivery Delivery) error {
	if pid == nil {
		return ErrInvalidPID
	}
	if ctx == nil {
		return ErrConfirmDeadlineRequired
	}
	system, err := r.ActorSystem()
	if err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return ErrConfirmDeadlineRequired
	}
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return context.DeadlineExceeded
	}
	message, err := delivery.toProto(true)
	if err != nil {
		return err
	}
	response, err := system.Root.RequestFuture(pid, message, timeout).Result()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("actor: confirm delivery: %w", err)
	}
	acceptance, ok := response.(*actorpb.Acceptance)
	if !ok || acceptance.GetCommandId() != delivery.CommandID {
		return ErrConfirmationRejected
	}
	return nil
}
