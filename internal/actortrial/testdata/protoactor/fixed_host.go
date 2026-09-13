package actor

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	protoactor "github.com/asynkron/protoactor-go/actor"
	txlog "github.com/ngnl47/tx/log"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	txprotocol "github.com/ngnl47/tx/protocol"
)

var errFixedRouteKeyRequired = errors.New("actor: fixed Actor requires a route key")

type fixedKind struct {
	name  string
	props *protoactor.Props
}

type fixedHost struct {
	kind     string
	props    *protoactor.Props
	entities map[txprotocol.RouteKey]*protoactor.PID
	keys     map[string]txprotocol.RouteKey
}

func newFixedHostProps(kind string, childProps *protoactor.Props) (*protoactor.Props, error) {
	kind = strings.TrimSpace(kind)
	if kind == "" || childProps == nil {
		return nil, ErrInvalidConfig
	}
	return protoactor.PropsFromProducer(func() protoactor.Actor {
		return &fixedHost{
			kind:     kind,
			props:    childProps,
			entities: make(map[txprotocol.RouteKey]*protoactor.PID),
			keys:     make(map[string]txprotocol.RouteKey),
		}
	}), nil
}

func (host *fixedHost) Receive(ctx protoactor.Context) {
	switch message := ctx.Message().(type) {
	case *actorpb.Delivery:
		host.deliver(ctx, message)
	case *protoactor.Terminated:
		host.remove(message.Who)
	}
}

func (host *fixedHost) deliver(ctx protoactor.Context, message *actorpb.Delivery) {
	key, err := routeKeyFromProto(message.GetRouteKeyKind(), message.GetRouteKeyNumeric(), message.GetRouteKeyString())
	if err != nil {
		host.logRejected(message, err)
		return
	}
	if key.Kind() == txprotocol.RouteKeyNone {
		host.logRejected(message, errFixedRouteKeyRequired)
		return
	}
	entity := host.entities[key]
	if entity == nil {
		entity = ctx.Spawn(host.props)
		ctx.Watch(entity)
		host.entities[key] = entity
		host.keys[entity.Id] = key
	}
	ctx.Forward(entity)
}

func (host *fixedHost) logRejected(message *actorpb.Delivery, err error) {
	txlog.Error("fixed actor delivery rejected",
		"kind", host.kind,
		"messageType", message.GetMessageType(),
		"commandID", message.GetCommandId(),
		"err", err,
	)
}

func (host *fixedHost) remove(entity *protoactor.PID) {
	if entity == nil {
		return
	}
	key, exists := host.keys[entity.Id]
	if !exists {
		return
	}
	delete(host.keys, entity.Id)
	if current := host.entities[key]; current != nil && current.Id == entity.Id {
		delete(host.entities, key)
	}
}

func fixedKindHostID(kind string) (string, error) {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return "", ErrInvalidPID
	}
	return "tx-fixed-" + base64.RawURLEncoding.EncodeToString([]byte(kind)), nil
}

// FixedKindPID 返回目标实例中固定 Kind 宿主的直连 PID。实体身份由 Delivery.RouteKey
// 携带，调用方不需要也不应自行拼接实体 Actor ID。
func FixedKindPID(endpoint, kind string) (*protoactor.PID, error) {
	hostID, err := fixedKindHostID(kind)
	if err != nil {
		return nil, err
	}
	pid, err := FixedPID(endpoint, hostID)
	if err != nil {
		return nil, fmt.Errorf("actor: build fixed kind PID %q: %w", kind, err)
	}
	return pid, nil
}
