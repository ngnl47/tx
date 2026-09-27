// Package actorrpc 提供私有实体传输适配，复用现有 RPC 连接和本地宿主。
package actorrpc

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ngnl47/tx/internal/actorhost"
	txactor "github.com/ngnl47/tx/modules/actor"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/rpc"
	"google.golang.org/protobuf/proto"
)

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

// DirectTransport 以目标实例为单位复用长流；Tell 单向写入，Call 保留逐项结果。
type DirectTransport struct{ Sender *rpc.Sender }

func (d *DirectTransport) Enqueue(ctx context.Context, target actorhost.Target, delivery *actorpb.Delivery) error {
	if err := ValidateRequest(ctx, target, false); err != nil {
		return err
	}
	data, err := directSubmitPayload(target, delivery)
	if err != nil {
		return err
	}
	return d.Sender.SendStream(ctx, rpc.Delivery{Route: rpc.InstanceRoute(target.Instance), MessageType: "actor.streamsubmit", Payload: data})
}

func directSubmitPayload(target actorhost.Target, delivery *actorpb.Delivery) ([]byte, error) {
	if delivery == nil {
		return nil, errors.New("actortrial: nil delivery")
	}
	copy := proto.Clone(delivery).(*actorpb.Delivery)
	copy.Mode = actorpb.DeliveryMode_DELIVERY_MODE_TELL
	copy.RequestId = 0
	return proto.Marshal(&actorpb.DirectStreamCall{Kind: target.Identity.Kind, Key: target.Identity.Key, Delivery: copy})
}

func (d *DirectTransport) Call(ctx context.Context, target actorhost.Target, delivery *actorpb.Delivery) (*actorpb.Result, error) {
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
func NewDirectReceiver(instance string, runtime *actorhost.Runtime) *directReceiver {
	return &directReceiver{instance: instance, runtime: runtime}
}

type directReceiver struct {
	instance string
	runtime  *actorhost.Runtime
}

func (*directReceiver) Deliver(context.Context, rpc.Delivery) error { return actorhost.ErrNotOwner }

func (r *directReceiver) SubmitStream(ctx context.Context, request rpc.Delivery) error {
	if request.Route.Kind != rpc.RouteInstance || request.Route.InstanceID != r.instance || request.MessageType != "actor.streamsubmit" {
		return actorhost.ErrNotOwner
	}
	return r.enqueue(ctx, request.Payload)
}

func (r *directReceiver) enqueue(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var item actorpb.DirectStreamCall
	if err := proto.Unmarshal(payload, &item); err != nil {
		return err
	}
	target := actorhost.Target{Instance: r.instance, Identity: actorhost.Identity{Kind: item.Kind, Key: item.Key}}
	if err := ValidateRequest(ctx, target, false); err != nil {
		return err
	}
	delivery, err := txactor.DeliveryFromProto(item.Delivery)
	if err != nil {
		return err
	}
	_, err = r.runtime.Submit(target.Identity, delivery)
	return err
}

func (*directReceiver) Call(context.Context, rpc.Delivery) (rpc.Result, error) {
	return rpc.Result{}, actorhost.ErrNotOwner
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
