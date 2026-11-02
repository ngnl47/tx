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
	"github.com/ngnl47/tx/modules/discovery"
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
type DirectTransport struct {
	Sender          *rpc.Sender
	SourceInstance  string
	InvalidateRoute func(discovery.SlotAssignment)
}

func (d *DirectTransport) invalidate(target actorhost.Target) {
	if d.InvalidateRoute != nil && target.Generation != 0 {
		d.InvalidateRoute(target.Assignment())
	}
}

func (d *DirectTransport) Tell(ctx context.Context, target actorhost.Target, delivery *actorpb.Delivery) error {
	if err := ValidateRequest(ctx, target, false); err != nil {
		return err
	}
	data, err := directSubmitPayload(target, delivery, d.SourceInstance)
	if err != nil {
		return err
	}
	err = d.Sender.TellStream(ctx, rpc.Delivery{Route: rpc.InstanceRoute(target.Instance), MessageType: "actor.streamsubmit", Payload: data})
	if err != nil && ctx.Err() == nil {
		d.invalidate(target)
	}
	return err
}

func directSubmitPayload(target actorhost.Target, delivery *actorpb.Delivery, source string) ([]byte, error) {
	if delivery == nil {
		return nil, errors.New("actortrial: nil delivery")
	}
	copy := proto.Clone(delivery).(*actorpb.Delivery)
	copy.Mode = actorpb.DeliveryMode_DELIVERY_MODE_TELL
	copy.RequestId = 0
	return proto.Marshal(&actorpb.DirectStreamCall{Type: target.Identity.Type, Id: target.Identity.ID, Delivery: copy, SlotId: target.SlotID, Generation: target.Generation, SourceInstance: source})
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
	data, err := proto.Marshal(&actorpb.DirectStreamCall{Type: target.Identity.Type, Id: target.Identity.ID, Delivery: copy, TimeoutNano: int64(timeout), SlotId: target.SlotID, Generation: target.Generation, SourceInstance: d.SourceInstance})
	if err != nil {
		return nil, err
	}
	response, err := d.Sender.CallStream(ctx, rpc.Delivery{Route: rpc.InstanceRoute(target.Instance), MessageType: "actor.streamcall", Payload: data})
	if err != nil {
		if ctx.Err() == nil {
			d.invalidate(target)
		}
		return nil, err
	}
	result, err := decodeDirectResult(response)
	if actorhost.IsRouteRejection(err) {
		d.invalidate(target)
	}
	return result, err
}

// NewDirectReceiver 注册到现有 DeliveryService，复用组合根已绑定归属的同一个 actorhost.Runtime。
// 实例身份在入队前验证，防止旧连接地址被新进程复用；归属与容量仍在实体加载入口验证。
func NewDirectReceiver(instance string, runtime *actorhost.Runtime) *directReceiver {
	return &directReceiver{instance: instance, runtime: runtime}
}

type directReceiver struct {
	instance    string
	runtime     *actorhost.Runtime
	RejectRoute func(string, discovery.SlotAssignment, error)
}

func (*directReceiver) Deliver(context.Context, rpc.Delivery) error { return actorhost.ErrNotOwner }

func (r *directReceiver) SubmitStream(ctx context.Context, request rpc.Delivery) error {
	var item actorpb.DirectStreamCall
	if err := proto.Unmarshal(request.Payload, &item); err != nil {
		return err
	}
	target := actorhost.Target{Instance: request.Route.InstanceID, Identity: actorhost.Identity{Type: item.Type, ID: item.Id}, SlotID: item.SlotId, Generation: item.Generation}
	reject := func(err error) {
		if r.RejectRoute != nil {
			r.RejectRoute(item.SourceInstance, target.Assignment(), err)
		}
	}
	if request.Route.Kind != rpc.RouteInstance || request.Route.InstanceID != r.instance || request.MessageType != "actor.streamsubmit" {
		err := &actorhost.RouteRejection{Err: actorhost.ErrNotOwner}
		reject(err)
		return err
	}
	if err := ValidateRequest(ctx, target, false); err != nil {
		return err
	}
	delivery, err := txactor.DeliveryFromProto(item.Delivery)
	if err != nil {
		return err
	}
	_, err = r.runtime.SubmitWithReply(target.Identity, delivery, func(_ any, err error) { reject(err) })
	if err != nil {
		reject(err)
	}
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
		err = &actorhost.RouteRejection{Err: actorhost.ErrNotOwner}
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
				result, err = executeDirectCall(itemCtx, r.runtime, actorhost.Target{Instance: r.instance, Identity: actorhost.Identity{Type: item.Type, ID: item.Id}}, item.Delivery)
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
