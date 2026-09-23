// Package actorrpc 提供私有实体传输适配，复用现有 RPC 连接和本地宿主。
package actorrpc

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/rpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// DirectTransport 复用共享 RPC Sender 的连接与凭据；关闭由持有 Sender 的组合根负责。
// 仅提供完成结果 Call，不回退到 NATS，也不自动重投失败业务。
type DirectTransport struct{ Sender *rpc.Sender }

func (d *DirectTransport) Call(ctx context.Context, target actorhost.Target, delivery *actorpb.Delivery) (*actorpb.Result, error) {
	if err := ValidateRequest(ctx, target, true); err != nil {
		return nil, err
	}
	if delivery == nil {
		return nil, errors.New("actortrial: nil delivery")
	}
	delivery = proto.Clone(delivery).(*actorpb.Delivery)
	delivery.Mode = actorpb.DeliveryMode_DELIVERY_MODE_CALL
	// 复用现有业务信封的非零 Call 标记；响应关联由 Future/inbox 或 gRPC 请求承担。
	delivery.RequestId = 1
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
	return decodeDirectResult(response)
}

func decodeDirectResult(response rpc.Result) (*actorpb.Result, error) {
	if response.MessageType == "actor.error" {
		var failure actorhost.RemoteError
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
	return &result, ResultError(&result)
}

// StreamingDirectTransport 以目标实例为单位复用长流，逐项保留 Actor 的完成结果。
type StreamingDirectTransport struct{ Sender *rpc.Sender }

func (d *StreamingDirectTransport) Call(ctx context.Context, target actorhost.Target, delivery *actorpb.Delivery) (*actorpb.Result, error) {
	if err := ValidateRequest(ctx, target, true); err != nil {
		return nil, err
	}
	if delivery == nil {
		return nil, errors.New("actortrial: nil delivery")
	}
	deadline, _ := ctx.Deadline()
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return nil, context.DeadlineExceeded
	}
	copy := proto.Clone(delivery).(*actorpb.Delivery)
	copy.Mode = actorpb.DeliveryMode_DELIVERY_MODE_CALL
	copy.RequestId = 1
	data, err := proto.Marshal(&actorpb.DirectStreamCall{Kind: target.Identity.Kind, Key: target.Identity.Key, Delivery: copy, TimeoutNano: int64(timeout)})
	if err != nil {
		return nil, err
	}
	response, err := d.Sender.CallStream(ctx, rpc.Delivery{Route: rpc.InstanceRoute(target.Instance), MessageType: "actor.streamcall", Payload: data})
	if err != nil {
		return nil, err
	}
	return decodeDirectResult(response)
}

// NewDirectReceiver 注册到现有 DeliveryService，复用组合根已绑定归属的同一个 actorhost.Runtime。
// 实例身份在入队前验证，防止旧连接地址被新进程复用；归属与容量仍在实体加载入口验证。
func NewDirectReceiver(instance string, runtime *actorhost.Runtime) rpc.CallReceiver {
	return &directReceiver{instance: instance, runtime: runtime}
}

type directReceiver struct {
	instance string
	runtime  *actorhost.Runtime
}

func (r *directReceiver) Call(ctx context.Context, request rpc.Delivery) (rpc.Result, error) {
	var result *actorpb.Result
	var err error
	kinds := metadata.ValueFromIncomingContext(ctx, "actor-kind-bin")
	keys := metadata.ValueFromIncomingContext(ctx, "actor-key-bin")
	switch {
	case request.Route.Kind != rpc.RouteInstance || request.Route.InstanceID != r.instance:
		err = actorhost.ErrNotOwner
	case request.MessageType != "actor.call" || len(kinds) != 1 || len(keys) != 1:
		err = errors.New("actortrial: invalid direct request")
	default:
		target := actorhost.Target{r.instance, actorhost.Identity{kinds[0], keys[0]}}
		var delivery actorpb.Delivery
		err = proto.Unmarshal(request.Payload, &delivery)
		if err == nil {
			result, err = executeDirectCall(ctx, r.runtime, target, &delivery)
		}
	}
	return encodeDirectResult(result, err)
}

func (r *directReceiver) CallStream(ctx context.Context, request rpc.Delivery) (rpc.Result, error) {
	var result *actorpb.Result
	var err error
	switch {
	case request.Route.Kind != rpc.RouteInstance || request.Route.InstanceID != r.instance:
		err = actorhost.ErrNotOwner
	case request.MessageType != "actor.streamcall":
		err = errors.New("actortrial: invalid direct stream request")
	default:
		var item actorpb.DirectStreamCall
		err = proto.Unmarshal(request.Payload, &item)
		if err == nil {
			if item.TimeoutNano <= 0 {
				err = context.DeadlineExceeded
			} else {
				itemCtx, cancel := context.WithTimeout(ctx, time.Duration(item.TimeoutNano))
				result, err = executeDirectCall(itemCtx, r.runtime, actorhost.Target{Instance: r.instance, Identity: actorhost.Identity{Kind: item.Kind, Key: item.Key}}, item.Delivery)
				cancel()
			}
		}
	}
	return encodeDirectResult(result, err)
}

func encodeDirectResult(result *actorpb.Result, err error) (rpc.Result, error) {
	// 业务和框架错误独立编码，避免普通 RPC 的 gRPC 状态包装丢失分类；正常载荷仍为 protobuf。
	if err != nil {
		data, encodeErr := json.Marshal(actorhost.RemoteErrorFrom(err))
		return rpc.Result{MessageType: "actor.error", Payload: data}, encodeErr
	}
	data, err := proto.Marshal(result)
	return rpc.Result{MessageType: "actor.result", Payload: data}, err
}

func executeDirectCall(ctx context.Context, runtime *actorhost.Runtime, target actorhost.Target, delivery *actorpb.Delivery) (*actorpb.Result, error) {
	if err := ValidateRequest(ctx, target, true); err != nil {
		return nil, err
	}
	if delivery == nil {
		return nil, errors.New("actortrial: nil delivery")
	}
	delivery.Mode = actorpb.DeliveryMode_DELIVERY_MODE_CALL
	value, err := runtime.Call(ctx, target.Identity, delivery)
	result, _ := value.(*actorpb.Result)
	if err == nil {
		err = ResultError(result)
	}
	return result, err
}
