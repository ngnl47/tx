package msgq

import (
	"context"
	"strconv"
	"sync"

	"github.com/maypok86/otter/v2"
	"github.com/ngnl47/tx/modules/discovery"
	"golang.org/x/sync/singleflight"
)

// RoleBinding 标识角色唯一的 MsgQ 归属；重新申请必须使用新的 Generation。
type RoleBinding struct {
	RoleID     int64  `json:"roleId"`
	InstanceID string `json:"instanceId"`
	Generation string `json:"generation"`
}

func (b RoleBinding) Validate() error {
	if b.RoleID <= 0 || !validValue(b.InstanceID) || b.Generation == "" {
		return ErrInvalidSession
	}
	return nil
}

// RoleStore 管理短期角色归属，不保存邮箱内容或 Session 集合。
// ReleaseRole 只能由已排除并发初始化、且确认没有剩余邮箱的归属管理者调用。
type RoleStore interface {
	ClaimRole(context.Context, RoleBinding) (RoleBinding, error)
	FindRole(context.Context, int64) (RoleBinding, bool, error)
	RefreshRole(context.Context, RoleBinding) (bool, error)
	ReleaseRole(context.Context, RoleBinding) (bool, error)
}

// RoleLoader 读取权威角色归属，不得在查询时分配新归属。
type RoleLoader func(context.Context, int64) (RoleBinding, bool, error)

// RoleTarget 是一次定向角色投递的归属及当前服务发现快照。
type RoleTarget struct {
	Binding  RoleBinding
	Instance discovery.Instance
}

// RoleRouter 缓存角色归属并合并冷查询；不发送消息，不自动重试业务投递。
type RoleRouter struct {
	directory discovery.Directory
	cache     *otter.Cache[int64, RoleBinding]
	loader    RoleLoader
	loads     singleflight.Group
	// 冷加载与失效串行交接，防止已失效的在途读取重新填入缓存。
	loading sync.RWMutex
}

// NewRoleRouter 创建容量有界的角色路由缓存。实例状态始终从本地服务发现读取。
func NewRoleRouter(directory discovery.Directory, load RoleLoader, maximumSize int) (*RoleRouter, error) {
	if directory == nil {
		return nil, ErrDirectoryNil
	}
	if load == nil {
		return nil, ErrSessionStoreNil
	}
	if maximumSize <= 0 {
		return nil, ErrInvalidSessionOptions
	}
	cache, err := otter.New(&otter.Options[int64, RoleBinding]{MaximumSize: maximumSize, InitialCapacity: min(maximumSize, 16)})
	if err != nil {
		return nil, err
	}
	return &RoleRouter{directory: directory, cache: cache, loader: load}, nil
}

// Resolve 命中缓存时不访问绑定存储；归属不存在时返回 ErrSessionOffline。
func (r *RoleRouter) Resolve(ctx context.Context, roleID int64) (RoleTarget, error) {
	if roleID <= 0 {
		return RoleTarget{}, ErrInvalidSession
	}
	if err := ctx.Err(); err != nil {
		return RoleTarget{}, err
	}
	binding, found := r.cache.GetIfPresent(roleID)
	if !found {
		loaded := r.loads.DoChan(strconv.FormatInt(roleID, 10), func() (any, error) {
			r.loading.RLock()
			defer r.loading.RUnlock()
			if current, found := r.cache.GetIfPresent(roleID); found {
				return current, nil
			}
			current, found, err := r.loader(ctx, roleID)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, ErrSessionOffline
			}
			if current.Validate() != nil || current.RoleID != roleID {
				return nil, ErrSessionStoreResponse
			}
			r.cache.Set(roleID, current)
			return current, nil
		})
		select {
		case <-ctx.Done():
			return RoleTarget{}, ctx.Err()
		case result := <-loaded:
			if result.Err != nil {
				return RoleTarget{}, result.Err
			}
			binding = result.Val.(RoleBinding)
		}
	}
	instance, found := r.directory.LookupInstance(binding.InstanceID)
	if !found || instance.Status != discovery.StatusRunning || !providesService(instance, "msgq") {
		r.Invalidate(binding)
		return RoleTarget{}, ErrRouteUnavailable
	}
	return RoleTarget{Binding: binding, Instance: instance}, nil
}

// Invalidate 只删除完整匹配的旧归属。仅用于明确的归属拒绝或服务发现失效，不能由投递超时推导。
func (r *RoleRouter) Invalidate(expected RoleBinding) {
	r.loading.Lock()
	defer r.loading.Unlock()
	r.cache.Compute(expected.RoleID, func(current RoleBinding, found bool) (RoleBinding, otter.ComputeOp) {
		if found && current == expected {
			return current, otter.InvalidateOp
		}
		return current, otter.CancelOp
	})
}
