package ant

import (
	"context"
	"testing"

	framework "github.com/ngnl47/tx/internal/framework"
	"github.com/ngnl47/tx/modules/discovery"
)

func TestContextSlotResolverUsesDeclaredReadCapabilities(t *testing.T) {
	assignment := discovery.SlotAssignment{SlotID: "world-1", RuntimeInstanceID: "world-a", Generation: 2}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{ID: "world-a", Endpoint: "127.0.0.1:9000", Services: []string{"world"}, Status: discovery.StatusRunning}}); err != nil {
		t.Fatal(err)
	}
	kernel := &slotContextKernel{directory: directory, slots: slotContextStore{assignment: assignment}}
	view := newContextView(kernel, "service world", []CModID{CModDiscovery})
	target, err := view.CModSlotResolver().ResolveSlot(context.Background(), assignment.SlotID)
	if err != nil || target.Assignment != assignment || target.Instance.ID != assignment.RuntimeInstanceID {
		t.Fatalf("解析结果 = %+v, %v", target, err)
	}
	view.validating = true
	err = func() (err error) {
		defer framework.RecoverCapability(&err)
		view.CModSlotResolver()
		return nil
	}()
	if err == nil {
		t.Fatal("Validate 阶段取得了运行时解析器")
	}
}

type slotContextKernel struct {
	frameworkContext
	directory discovery.Directory
	slots     discovery.SlotStore
}

func (k *slotContextKernel) Directory() discovery.Directory { return k.directory }
func (k *slotContextKernel) Advanced() framework.Advanced   { return slotContextAdvanced{slots: k.slots} }

type slotContextAdvanced struct {
	framework.Advanced
	slots discovery.SlotStore
}

func (a slotContextAdvanced) Slots() discovery.SlotStore { return a.slots }

type slotContextStore struct {
	discovery.SlotStore
	assignment discovery.SlotAssignment
}

func (s slotContextStore) ResolveSlot(context.Context, string) (discovery.SlotAssignment, error) {
	return s.assignment, nil
}
