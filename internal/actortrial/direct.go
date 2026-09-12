package actortrial

import (
	"context"
	"encoding/json"
	"errors"

	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/rpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// DirectTransport 复用共享 RPC Sender 的连接与凭据；关闭由持有 Sender 的组合根负责。
// 仅提供完成结果 Call，不回退到 NATS，也不自动重投失败业务。
type DirectTransport struct{ Sender *rpc.Sender }

func (d *DirectTransport) Call(ctx context.Context, target Target, delivery *actorpb.Delivery) (*actorpb.Result, error) {
	if err := validateRequest(ctx, target, true); err != nil {
		return nil, err
	}
	if delivery == nil {
		return nil, errors.New("actortrial: nil delivery")
	}
	delivery = proto.Clone(delivery).(*actorpb.Delivery)
	delivery.Mode = actorpb.DeliveryMode_DELIVERY_MODE_CALL
	data, err := proto.Marshal(delivery)
	if err != nil {
		return nil, err
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	if md == nil {
		md = metadata.MD{}
	}
	// 二进制 metadata 支持任意合法 UTF-8 路由键，不占用业务 RequestId 或 RouteKey。
	md.Set("actor-kind-bin", target.Identity.Kind)
	md.Set("actor-key-bin", target.Identity.Key)
	response, err := d.Sender.Call(metadata.NewOutgoingContext(ctx, md), rpc.Delivery{
		Route: rpc.InstanceRoute(target.Instance), MessageType: "actor.call", Payload: data,
	})
	if err != nil {
		return nil, err
	}
	if response.MessageType == "actor.error" {
		var failure RemoteError
		if err := json.Unmarshal(response.Payload, &failure); err != nil {
			return nil, err
		}
		return nil, &failure
	}
	if response.MessageType != "actor.result" {
		return nil, errors.New("actortrial: invalid direct response")
	}
	var result actorpb.Result
	if err := proto.Unmarshal(response.Payload, &result); err != nil {
		return nil, err
	}
	return &result, resultError(&result)
}

// NewDirectReceiver 注册到现有 DeliveryService，复用 SlotHost 所绑定的同一个 Runtime。
// 实例身份在入队前验证，防止旧连接地址被新进程复用；归属与容量仍在实体加载入口验证。
func NewDirectReceiver(instance string, runtime *Runtime) rpc.CallReceiver {
	return rpc.CallReceiverFunc(func(ctx context.Context, request rpc.Delivery) (rpc.Result, error) {
		var result *actorpb.Result
		var err error
		kinds := metadata.ValueFromIncomingContext(ctx, "actor-kind-bin")
		keys := metadata.ValueFromIncomingContext(ctx, "actor-key-bin")
		switch {
		case request.Route.Kind != rpc.RouteInstance || request.Route.InstanceID != instance:
			err = ErrNotOwner
		case request.MessageType != "actor.call" || len(kinds) != 1 || len(keys) != 1:
			err = errors.New("actortrial: invalid direct request")
		default:
			target := Target{instance, Identity{kinds[0], keys[0]}}
			err = validateRequest(ctx, target, true)
			var delivery actorpb.Delivery
			if err == nil {
				err = proto.Unmarshal(request.Payload, &delivery)
			}
			if err == nil {
				delivery.Mode = actorpb.DeliveryMode_DELIVERY_MODE_CALL
				var value any
				value, err = runtime.Call(ctx, target.Identity, &delivery)
				result, _ = value.(*actorpb.Result)
				if err == nil {
					err = resultError(result)
				}
			}
		}
		// 业务和框架错误独立编码，避免普通 RPC 的 gRPC 状态包装丢失分类；正常载荷仍为 protobuf。
		if err != nil {
			data, encodeErr := json.Marshal(remoteError(err))
			return rpc.Result{MessageType: "actor.error", Payload: data}, encodeErr
		}
		data, err := proto.Marshal(result)
		return rpc.Result{MessageType: "actor.result", Payload: data}, err
	})
}
