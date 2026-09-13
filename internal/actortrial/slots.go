package actortrial

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/nats-io/nats.go"
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
		err = p.conn.FlushTimeout(runtime.timeout)
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
			ctx, cancel := context.WithTimeout(context.Background(), h.runtime.timeout)
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

// SlotAllocator 只负责低频首次选址和滚动交接。业务消息继续使用缓存路由及 Transport。
type SlotAllocator struct {
	store     discovery.SlotStore
	directory discovery.Directory
	router    *discovery.SlotRouter
	transport *Transport
	service   string
	slotID    func(Identity) string
}

func NewSlotAllocator(store discovery.SlotStore, directory discovery.Directory, transport *Transport, service string, slotID func(Identity) string) *SlotAllocator {
	if transport == nil || service == "" || slotID == nil {
		panic("actortrial: invalid slot allocator configuration")
	}
	router, err := discovery.NewSlotRouter(store, directory)
	if err != nil {
		panic(err)
	}
	return &SlotAllocator{store, directory, router, transport, service, slotID}
}

// Ensure 保持已有 assignment。仅无归属时选址；容量快照变化导致的最终拒绝不会触发改派。
func (a *SlotAllocator) Ensure(ctx context.Context, id Identity) (discovery.SlotAssignment, ActivationStatus, error) {
	slotID := a.slotID(id)
	current, err := a.store.ResolveSlot(ctx, slotID)
	created := false
	if errors.Is(err, discovery.ErrSlotNotFound) {
		candidates := a.directory.RunningInstances(a.service)
		if len(candidates) == 0 {
			return current, ActivationStatus{}, discovery.ErrSlotRuntimeUnavailable
		}
		// 哈希只决定首次选址的起点；成员增减不重算已存在的归属。
		start := int(xxhash.Sum64String(slotID) % uint64(len(candidates)))
		err = ErrCapacity
		for i := range candidates {
			candidate := candidates[(start+i)%len(candidates)].ID
			check := a.transport.slotCommand(ctx, candidate, "capacity", id)
			if errors.Is(check, ErrCapacity) || errors.Is(check, ErrDraining) {
				continue
			}
			if check != nil {
				return current, ActivationStatus{}, check
			}
			current = discovery.SlotAssignment{SlotID: slotID, RuntimeInstanceID: candidate, Generation: 1}
			err = a.router.CreateSlot(ctx, current)
			if errors.Is(err, discovery.ErrSlotConflict) {
				current, err = a.store.ResolveSlot(ctx, slotID)
			} else if err == nil {
				created = true
			}
			break // CAS 或响应结果未知时不能尝试另一个候选。
		}
	}
	if err != nil {
		return current, ActivationStatus{}, err
	}
	instance, ok := a.directory.LookupInstance(current.RuntimeInstanceID)
	if !ok || instance.Status != discovery.StatusRunning || !slices.Contains(instance.Services, a.service) {
		return current, ActivationStatus{}, discovery.ErrSlotRuntimeUnavailable
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

// Handoff 是实例滚动交接：第一次调用排空旧实例的所有实体，之后逐槽 CAS。
// 源响应或 CAS 结果未知时返回错误，由下一次显式操作重新读取归属，不重投业务请求。
func (a *SlotAllocator) Handoff(ctx context.Context, id Identity, current discovery.SlotAssignment, nextInstance string) (discovery.SlotAssignment, ActivationStatus, error) {
	next := discovery.SlotAssignment{SlotID: current.SlotID, RuntimeInstanceID: nextInstance, Generation: current.Generation + 1}
	if current.SlotID != a.slotID(id) || discovery.ValidateSlotPromotion(current, next) != nil {
		return current, ActivationStatus{}, discovery.ErrInvalidSlotPromotion
	}
	stored, err := a.store.ResolveSlot(ctx, current.SlotID)
	if err != nil {
		return current, ActivationStatus{}, err
	}
	if stored != current {
		return stored, ActivationStatus{}, discovery.ErrSlotConflict
	}
	destination, ok := a.directory.LookupInstance(nextInstance)
	if !ok || destination.Status != discovery.StatusRunning || !slices.Contains(destination.Services, a.service) {
		return current, ActivationStatus{}, discovery.ErrSlotRuntimeUnavailable
	}
	if err = a.transport.slotCommand(ctx, nextInstance, "capacity", id); err != nil {
		return current, ActivationStatus{}, err
	}
	if err = a.transport.slotCommand(ctx, current.RuntimeInstanceID, "quiesce", id); err != nil {
		return current, ActivationStatus{}, err
	}
	if err = a.router.PromoteSlot(ctx, current, next); err != nil {
		return discovery.SlotAssignment{}, ActivationStatus{}, err
	}
	status, err := a.transport.RetryActivation(ctx, Target{nextInstance, id})
	return next, status, err
}
