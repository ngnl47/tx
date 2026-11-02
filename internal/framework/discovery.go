package framework

import (
	"context"
	"errors"
	"strings"
	"sync"

	txetcd "github.com/ngnl47/tx/etcd"
	"github.com/ngnl47/tx/modules/discovery"
)

// discoveryRuntime 拥有进程共享连接，Discovery 与发号器仅借用。
type discoveryRuntime struct {
	*discovery.Runtime
	client       *txetcd.Client
	sessionIDKey string
	closeOnce    sync.Once
	closeErr     error
}

func newDiscovery(ctx context.Context, config discovery.EtcdConfig, appID string, descriptor discovery.Instance) (*discoveryRuntime, error) {
	config, err := config.Normalize()
	if err != nil {
		return nil, err
	}
	client, err := txetcd.New(ctx, txetcd.Config{Endpoints: config.Endpoints, DialTimeout: config.DialTimeout})
	if err != nil {
		return nil, err
	}
	runtime, err := discovery.NewRuntimeWithClient(ctx, client, config, appID, descriptor)
	if err != nil {
		return nil, errors.Join(err, client.Close())
	}
	return &discoveryRuntime{
		Runtime: runtime,
		client:  client,
		// 发号写入不得影响 Discovery 根前缀的槽位容量 CAS。
		sessionIDKey: strings.TrimSuffix(config.Prefix, "/instances") + "-session-ids/issuer",
	}, nil
}

func (runtime *discoveryRuntime) Close(ctx context.Context) error {
	runtime.closeOnce.Do(func() {
		runtime.closeErr = errors.Join(runtime.Runtime.Close(ctx), runtime.client.Close())
	})
	return runtime.closeErr
}
