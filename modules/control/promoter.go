package control

import (
	"context"

	"github.com/ngnl47/tx/modules/discovery"
)

// SlotPromoter 将显式槽位写入适配为控制面协议，不负责选址、排水或修改服务发现租约。
// 常规交接由 discovery.SlotOwnership 编排；直接使用本接口时，调用方必须先完成源实例收尾。
type SlotPromoter struct {
	router *discovery.SlotRouter
}

// NewSlotPromoter 创建受 generation 保护的槽位交接器。
func NewSlotPromoter(store discovery.SlotStore, directory discovery.Directory) (*SlotPromoter, error) {
	router, err := discovery.NewSlotRouter(store, directory)
	if err != nil {
		return nil, err
	}
	return &SlotPromoter{router: router}, nil
}

// CreateSlot 确认初始目标实例可用后，发布 generation 1 的槽位归属。
func (promoter *SlotPromoter) CreateSlot(ctx context.Context, creation SlotCreation) error {
	assignment, err := creation.InitialAssignment()
	if err != nil {
		return err
	}
	return promoter.router.CreateSlot(ctx, assignment)
}

// PromoteSlot 确认目标实例可用后，以 CAS 发布下一代槽位归属。
func (promoter *SlotPromoter) PromoteSlot(ctx context.Context, promotion SlotPromotion) error {
	next, err := promotion.NextAssignment()
	if err != nil {
		return err
	}
	return promoter.router.PromoteSlot(ctx, promotion.Current, next)
}
