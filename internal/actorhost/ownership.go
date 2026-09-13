package actorhost

import (
	"context"
	"errors"

	"github.com/ngnl47/tx/modules/discovery"
)

// BindSlots 在开放任何入口前绑定权威加载校验，两种传输共用同一实体运行时。
// slotID 为空时使用普通实体的固定分区；世界可提供独立槽位映射。
// Store 不得是 Watch 缓存；只在首次加载时读取，不为每条业务消息访问 etcd。
func (r *Runtime) BindSlots(instance string, store discovery.SlotReader, slotID SlotMapping) {
	if instance == "" || store == nil {
		panic("actortrial: invalid slot ownership configuration")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) != 0 || r.draining {
		panic("actortrial: bind slot ownership before use")
	}
	factory := r.factory
	r.factory = func(c Context) (*Behavior, error) {
		// 权威查询单独限时，不能把其 deadline 带入后续业务初始化。
		query, cancel := context.WithTimeout(c.Context, r.timeout)
		err := discovery.CheckSlotOwner(query, store, slotID.Resolve(c.Identity), instance)
		cancel()
		if errors.Is(err, discovery.ErrSlotNotOwner) {
			return nil, ErrNotOwner
		}
		if err != nil {
			return nil, err
		}
		return factory(c)
	}
}
