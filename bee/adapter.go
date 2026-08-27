package bee

import (
	"context"

	framework "github.com/ngnl47/tx/internal/framework"
)

type cmodAdapter struct {
	mod CMod
}

func (adapter *cmodAdapter) Spec() framework.CModSpec {
	spec := adapter.mod.Spec()
	return framework.CModSpec{
		ID:         toFrameworkCModID(spec.ID),
		Requires:   toFrameworkCModIDs(spec.Requires),
		After:      toFrameworkCModIDs(spec.After),
		Components: toFrameworkComponentSpecs(spec.Components),
	}
}

func (adapter *cmodAdapter) Validate(runtime framework.CModContext) error {
	validator, ok := adapter.mod.(CModValidator)
	if !ok || isNil(validator) {
		return nil
	}
	spec := adapter.mod.Spec()
	return validator.Validate(newContextView(runtime, "CMod \""+string(spec.ID)+"\"", spec.Requires))
}

func (adapter *cmodAdapter) Start(runtime framework.CModContext) error {
	spec := adapter.mod.Spec()
	return adapter.mod.Start(newContextView(runtime, "CMod \""+string(spec.ID)+"\"", spec.Requires))
}

func (adapter *cmodAdapter) Drain(ctx context.Context) error {
	drainer, ok := adapter.mod.(CModDrainer)
	if !ok || isNil(drainer) {
		return nil
	}
	return drainer.Drain(ctx)
}

func (adapter *cmodAdapter) Stop(ctx context.Context) error {
	stopper, ok := adapter.mod.(CModStopper)
	if !ok || isNil(stopper) {
		return nil
	}
	return stopper.Stop(ctx)
}
