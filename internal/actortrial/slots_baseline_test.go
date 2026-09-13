package actortrial

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/ngnl47/tx/internal/actorhost"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"github.com/ngnl47/tx/modules/discovery"
)

// SlotHost 为独立实例控制订阅和业务运行时持有同一连接。
type SlotHost struct {
	*Transport
	flush    func(context.Context) error
	mu       sync.Mutex
	quiesced bool
}

func ConnectSlotHost(url, namespace, instance string, runtime *Runtime, maxPending int, flush func(context.Context) error) (*SlotHost, error) {
	p, err := Connect(url, namespace, instance, runtime, maxPending)
	if err != nil {
		return nil, err
	}
	h := &SlotHost{Transport: p, flush: flush}
	if _, err = p.conn.Subscribe(p.slotSubject(instance), h.receiveControl); err == nil {
		err = p.conn.FlushTimeout(runtime.Timeout())
	}
	if err != nil {
		p.conn.Close()
		return nil, err
	}
	return h, nil
}

func (h *SlotHost) Drain(ctx context.Context) error {
	if err := h.quiesce(ctx); err != nil {
		return err
	}
	h.conn.Close()
	return nil
}

// quiesce 保留控制连接，成功回执后控制方才可晋升槽位；失败不会重新开放准入。
func (h *SlotHost) quiesce(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.quiesced {
		return nil
	}
	if err := h.runtime.Drain(ctx); err != nil {
		return err
	}
	if h.flush != nil {
		if err := h.flush(ctx); err != nil {
			return err
		}
	}
	if err := h.conn.FlushWithContext(ctx); err != nil {
		return err
	}
	h.quiesced = true
	return nil
}

type slotControl struct {
	Action   string
	Identity Identity
}

func (p *Transport) slotSubject(instance string) string {
	return p.namespace + ".slot-control." + base64.RawURLEncoding.EncodeToString([]byte(instance))
}

func (p *Transport) slotCommand(ctx context.Context, instance, action string, id Identity) error {
	if err := validateRequest(ctx, Target{instance, id}, true); err != nil {
		return err
	}
	data, _ := json.Marshal(slotControl{action, id})
	f, err := p.requestFuture(ctx, &nats.Msg{Subject: p.slotSubject(instance), Data: data})
	if err != nil {
		return err
	}
	result, err := f.Await(ctx)
	if err == nil && result.MessageType != "actor.slot" {
		return errors.New("actortrial: invalid slot control response")
	}
	return err
}

func (h *SlotHost) receiveControl(msg *nats.Msg) {
	var command slotControl
	err := json.Unmarshal(msg.Data, &command)
	if err == nil && command.Identity.Kind != "" && command.Identity.Key != "" {
		switch command.Action {
		case "capacity":
			err = h.runtime.CheckActivationCapacity(command.Identity)
		case "quiesce":
			ctx, cancel := context.WithTimeout(context.Background(), h.runtime.Timeout())
			defer cancel()
			err = h.quiesce(ctx)
		default:
			err = errors.New("actortrial: invalid slot control")
		}
	} else {
		err = errors.New("actortrial: invalid slot control")
	}
	h.reply(msg, &actorpb.Result{MessageType: "actor.slot"}, err)
}

// SlotAllocator 绑定 Actor 身份和执行入口；归属规则统一由 Discovery 的 L2 组件维护。
type SlotAllocator struct {
	ownership *discovery.SlotOwnership
	transport *Transport
	service   string
	slotID    actorhost.SlotMapping
}

// NewSlotAllocator 的空 slotID 使用普通实体固定分区；显式映射用于世界等独立槽位。
func NewSlotAllocator(ownership *discovery.SlotOwnership, transport *Transport, service string, slotID actorhost.SlotMapping) *SlotAllocator {
	if ownership == nil || transport == nil || service == "" {
		panic("actortrial: invalid slot allocator configuration")
	}
	return &SlotAllocator{ownership, transport, service, slotID}
}

func (a *SlotAllocator) Ensure(ctx context.Context, id Identity) (discovery.SlotAssignment, ActivationStatus, error) {
	current, created, err := a.ownership.Ensure(ctx, a.service, a.slotID.Resolve(id), func(ctx context.Context, instance string) (bool, error) {
		err := a.transport.slotCommand(ctx, instance, "capacity", id)
		if errors.Is(err, ErrCapacity) || errors.Is(err, ErrDraining) {
			return false, nil
		}
		return err == nil, err
	})
	if errors.Is(err, discovery.ErrSlotCapacityUnavailable) {
		err = ErrCapacity
	}
	if err != nil {
		return current, ActivationStatus{}, err
	}
	target := Target{current.RuntimeInstanceID, id}
	var status ActivationStatus
	if created {
		status, err = a.transport.RetryActivation(ctx, target)
	} else {
		status, err = a.transport.EnsureActivation(ctx, target)
	}
	return current, status, err
}

func (a *SlotAllocator) Handoff(ctx context.Context, id Identity, current discovery.SlotAssignment, nextInstance string) (discovery.SlotAssignment, ActivationStatus, error) {
	if current.SlotID != a.slotID.Resolve(id) {
		return current, ActivationStatus{}, discovery.ErrInvalidSlotPromotion
	}
	next, err := a.ownership.Handoff(ctx, a.service, current, nextInstance, func(ctx context.Context, instance string) (bool, error) {
		err := a.transport.slotCommand(ctx, instance, "capacity", id)
		return err == nil, err
	}, func(ctx context.Context, instance string) error {
		return a.transport.slotCommand(ctx, instance, "quiesce", id)
	})
	if err != nil {
		return next, ActivationStatus{}, err
	}
	status, err := a.transport.RetryActivation(ctx, Target{nextInstance, id})
	return next, status, err
}
