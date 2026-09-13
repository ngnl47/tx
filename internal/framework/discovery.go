package framework

import (
	"context"

	"github.com/ngnl47/tx/modules/discovery"
)

type discoveryRuntime = discovery.Runtime

func newDiscovery(ctx context.Context, config discovery.EtcdConfig, appID string, descriptor discovery.Instance) (*discoveryRuntime, error) {
	return discovery.NewRuntime(ctx, config, appID, descriptor)
}
