package server

import (
	"context"

	"github.com/ngnl47/tx/modules/control"
	"github.com/ngnl47/tx/modules/discovery"
)

// slotPromoter adapts the project discovery store and live directory to the
// operational control contract. It never creates assignments or changes an
// instance lease; it only performs a generation-fenced handoff.
type slotPromoter struct {
	router *discovery.SlotRouter
}

func newSlotPromoter(store discovery.SlotStore, directory *discovery.ServiceDirectory) (*slotPromoter, error) {
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
