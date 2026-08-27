package framework

import (
	"context"
	"strings"

	"github.com/ngnl47/tx/modules/discovery"
)

type discoveryRuntime = discovery.Runtime

// LogicalSlot 是部署为有状态服务显式声明的稳定业务槽位。它与每次启动都会变化的
// RuntimeInstanceID 分离且不会参与实例 ID 拼接；业务使用它决定 FixedSlot 路由，
// Runtime.Slots 保存实际 generation 归属。
type LogicalSlot struct {
	// ID 是部署控制面分配的稳定逻辑槽位，不随进程重启变化。
	ID string
	// RuntimeInstanceID 是本次进程实例身份，用于校验槽位当前归属；它不包含逻辑槽位 ID。
	RuntimeInstanceID string
}

// newLogicalSlot 校验当前运行实例对稳定逻辑槽位的部署声明。它不创建 SlotStore
// assignment，也不接管 generation；这些动作必须由发布控制面在优雅交接后执行。
func newLogicalSlot(id, runtimeInstanceID string) (*LogicalSlot, error) {
	assignment := discovery.SlotAssignment{
		SlotID:            strings.TrimSpace(id),
		RuntimeInstanceID: strings.TrimSpace(runtimeInstanceID),
		Generation:        1,
	}
	if err := assignment.Validate(); err != nil {
		return nil, err
	}
	return &LogicalSlot{ID: assignment.SlotID, RuntimeInstanceID: assignment.RuntimeInstanceID}, nil
}

func newDiscovery(ctx context.Context, config discovery.EtcdConfig, instance discovery.Instance) (*discoveryRuntime, error) {
	return discovery.NewRuntime(ctx, config, instance)
}
