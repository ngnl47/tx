package server

import (
	"context"

	"github.com/ngnl47/tx/modules/control"
	"github.com/ngnl47/tx/modules/discovery"
)

// slotPromoter 将项目 discovery store 和实时目录适配到运维控制契约。
// 它不会创建 assignment 或修改实例 lease，只执行受 generation 保护的交接。
type slotPromoter struct {
	router *discovery.SlotRouter
}

func newSlotPromoter(store discovery.SlotStore, directory discovery.Directory) (*slotPromoter, error) {
	router, err := discovery.NewSlotRouter(store, directory)
	if err != nil {
		return nil, err
	}
	return &slotPromoter{router: router}, nil
}

func (promoter *slotPromoter) PromoteSlot(ctx context.Context, promotion control.SlotPromotion) error {
	next, err := promotion.NextAssignment()
	if err != nil {
		return err
	}
	return promoter.router.PromoteSlot(ctx, promotion.Current, next)
}
