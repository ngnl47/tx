package framework

import (
	"github.com/ngnl47/tx/modules/control"
	"github.com/ngnl47/tx/modules/discovery"
)

type slotPromoter = control.SlotPromoter

func newSlotPromoter(store discovery.SlotStore, directory discovery.Directory) (*slotPromoter, error) {
	return control.NewSlotPromoter(store, directory)
}
