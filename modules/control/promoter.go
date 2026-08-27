package control

import (
	"context"

	"github.com/ngnl47/tx/modules/discovery"
)

// SlotPromoter 将槽位存储和实时服务目录适配为控制面的交接契约。
// 它不创建初始 assignment，也不修改服务发现 lease。
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

// PromoteSlot 确认目标实例可用后，以 CAS 发布下一代槽位归属。
func (promoter *SlotPromoter) PromoteSlot(ctx context.Context, promotion SlotPromotion) error {
	next, err := promotion.NextAssignment()
	if err != nil {
		return err
	}
	return promoter.router.PromoteSlot(ctx, promotion.Current, next)
}
