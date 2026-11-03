package discovery

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ngnl47/tx/lifecycle"
)

// 每槽共享一个限时查询/等待，不为每条消息创建轮询定时器。
const slotRefreshTimeout = 3 * time.Second
const slotRefreshInterval = 100 * time.Millisecond

type slotRefreshKey struct{ service, slot string }
type slotRefreshCall struct {
	done   chan struct{}
	query  context.Context
	target SlotTarget
	err    error
}
type slotRefresh struct {
	tasks    lifecycle.Tasks
	resolver *SlotResolver

	mu      sync.Mutex
	pending map[slotRefreshKey]*slotRefreshCall
}

// BindRefresh 在开放调用前绑定进程任务视图；查询取消和等待所有权仍属于宿主。
func (r *SlotResolver) BindRefresh(tasks lifecycle.Tasks) {
	r.refresh = &slotRefresh{pending: make(map[slotRefreshKey]*slotRefreshCall), tasks: tasks, resolver: r}
}

// Invalidate 只标记观察到的旧归属，下一次寻址再回源；不发送业务消息。
func (r *SlotResolver) Invalidate(observed SlotAssignment) {
	if store, ok := r.store.(*CachedSlotStore); ok {
		store.directory.invalidateAssignment(observed)
	}
}

func (f *slotRefresh) resolve(ctx context.Context, service, slot string) (SlotTarget, error) {
	if err := ctx.Err(); err != nil {
		return SlotTarget{}, err
	}
	key := slotRefreshKey{service, slot}
	f.mu.Lock()
	call := f.pending[key]
	if call == nil {
		query, cancel := context.WithTimeout(f.tasks, slotRefreshTimeout)
		call = &slotRefreshCall{done: make(chan struct{}), query: query}
		f.pending[key] = call
		if err := f.tasks.Go(func(context.Context) {
			defer cancel()
			target, err := f.load(query, service, slot)
			if query.Err() != nil {
				err = query.Err()
			}
			f.mu.Lock()
			call.target, call.err = target, err
			delete(f.pending, key)
			close(call.done)
			f.mu.Unlock()
		}); err != nil {
			cancel()
			delete(f.pending, key)
			f.mu.Unlock()
			return SlotTarget{}, err
		}
	}
	f.mu.Unlock()
	select {
	case <-ctx.Done():
		return SlotTarget{}, ctx.Err()
	case <-call.query.Done():
		select {
		case <-call.done:
		default:
			return SlotTarget{}, call.query.Err()
		}
	case <-call.done:
	}
	if err := ctx.Err(); err != nil {
		return SlotTarget{}, err
	}
	return call.target, call.err
}

func (f *slotRefresh) load(ctx context.Context, service, slot string) (SlotTarget, error) {
	for {
		var assignment SlotAssignment
		var err error
		var changed <-chan struct{}
		cached, ok := f.resolver.store.(*CachedSlotStore)
		if ok {
			assignment, err = cached.refreshSlot(ctx, slot)
			changed = cached.directory.changes()
		} else {
			assignment, err = f.resolver.store.ResolveSlot(ctx, slot)
		}
		if err != nil && !errors.Is(err, ErrSlotRouteStale) {
			return SlotTarget{}, err
		}
		if err == nil && assignment.Released() {
			return SlotTarget{Assignment: assignment}, ErrSlotReleased
		}
		if err == nil {
			instance, ok := f.resolver.lookupRouteInstance(service, assignment.RuntimeInstanceID)
			if ok && instance.Status == StatusRunning {
				return SlotTarget{Assignment: assignment, Instance: instance}, nil
			}
		}
		timer := time.NewTimer(slotRefreshInterval)
	wait:
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				return SlotTarget{}, ctx.Err()
			case <-changed:
				changed = cached.directory.changes()
				// 目录通知只重查本地缓存，不缩短权威查询间隔，避免不同槽位相互唤醒。
				target, err := f.resolver.resolveSlot(ctx, service, slot)
				if err == nil {
					timer.Stop()
					return target, nil
				}
				if errors.Is(err, ErrSlotDirectoryUnavailable) {
					timer.Stop()
					return SlotTarget{}, err
				}
			case <-timer.C:
				break wait
			}
		}
	}
}
