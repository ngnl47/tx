package actortrial

import "github.com/ngnl47/tx/modules/discovery"

// BindSlots 在开放任何入口前绑定权威加载校验，两种传输共用同一实体运行时。
// Store 不得是 Watch 缓存；只在首次加载时读取，不为每条业务消息访问 etcd。
func (r *Runtime) BindSlots(instance string, store discovery.SlotReader, slotID func(Identity) string) {
	if instance == "" || store == nil || slotID == nil {
		panic("actortrial: invalid slot ownership configuration")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) != 0 || r.draining {
		panic("actortrial: bind slot ownership before use")
	}
	factory := r.factory
	r.factory = func(c Context) (*Behavior, error) {
		assignment, err := store.ResolveSlot(c, slotID(c.Identity))
		if err != nil {
			return nil, err
		}
		if assignment.RuntimeInstanceID != instance {
			return nil, ErrNotOwner
		}
		return factory(c)
	}
}
