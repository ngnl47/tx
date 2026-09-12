package actortrial

import (
	"errors"
	"slices"

	"github.com/cespare/xxhash/v2"
	rendezvous "github.com/dgryski/go-rendezvous"
)

// ErrNotOwner 是可重新解析路由的拒绝，不是业务永久加载拒绝。
var ErrNotOwner = errors.New("actortrial: not current owner")

// Placement 是虚拟 Kind 的不可变成员快照；固定槽位不使用本类型。
// 相同快照给出相同目标，不承诺异步 Watch 期间跨进程唯一激活。
type Placement struct {
	local string
	kinds map[string]*rendezvous.Rendezvous
}

func NewPlacement(local string, members map[string][]string) *Placement {
	if local == "" {
		panic("actortrial: empty local instance")
	}
	p := &Placement{local: local, kinds: make(map[string]*rendezvous.Rendezvous, len(members))}
	for kind, instances := range members {
		ordered := slices.Clone(instances)
		slices.Sort(ordered)
		p.kinds[kind] = rendezvous.New(ordered, xxhash.Sum64String)
	}
	return p
}
func (p *Placement) Target(id Identity) (Target, error) {
	if owners := p.kinds[id.Kind]; owners != nil {
		if instance := owners.Lookup(id.Key); instance != "" {
			return Target{Instance: instance, Identity: id}, nil
		}
	}
	return Target{}, ErrNotOwner
}

// SetPlacement 与准入共用锁：新快照生效时，失去归属的旧激活同时关闸。
// 排队工作仍执行，清理完成前保留占位；重新获得归属也不能提前重建。
// 调用方按 Watch 顺序发布完整快照；本方法不协调另一进程何时启用新 owner。
func (r *Runtime) SetPlacement(p *Placement) {
	if p == nil {
		panic("actortrial: nil placement")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.placement = p
	for id, e := range r.entries {
		target, err := p.Target(id)
		if err != nil || target.Instance != p.local {
			r.closeLocked(e)
		}
	}
}
