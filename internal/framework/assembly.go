package framework

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/txn"
	txstorage "github.com/ngnl47/tx/storage"
)

func (server *Server) startControl(endpoint string) error {
	if strings.TrimSpace(endpoint) == "" {
		return nil
	}
	var slots *slotPromoter
	if server.runtime.slots != nil && server.runtime.directory != nil {
		promoter, err := newSlotPromoter(server.runtime.slots, server.runtime.directory)
		if err != nil {
			return fmt.Errorf("framework: initialize slot control: %w", err)
		}
		slots = promoter
	}
	controlServer, err := startControlServer(server.runtime.lifecycle, endpoint, controlServerOptions{
		Lifecycle: newDrainCoordinator(server, server.config.Runtime.Lifecycle.DrainTimeout),
		Handoff:   server,
		Slots:     slots,
		StorageIO: server,
	})
	if err != nil {
		return fmt.Errorf("framework: start control endpoint: %w", err)
	}
	server.control = controlServer
	return nil
}

func lifecycleRoot(ctx context.Context) *lifecycle.WaitGroupContext {
	if ctx == nil {
		ctx = context.Background()
	}
	return lifecycle.NewWaitGroupContext(context.WithoutCancel(ctx))
}

func selectedServiceNames(services []Service) []string {
	names := make([]string, 0, len(services))
	for _, service := range services {
		names = append(names, service.Spec().Name)
	}
	return names
}

func (server *Server) startDiscovery() error {
	instance := server.runtime.instanceSnapshot()
	discoveryRuntime, err := newDiscovery(server.runtime.lifecycle, server.config.Discovery.Etcd, server.config.AppID, instance)
	if err != nil {
		return fmt.Errorf("framework: initialize discovery: %w", err)
	}
	server.discovery = discoveryRuntime
	server.runtime.setInstance(discoveryRuntime.Instance())
	server.runtime.directory = discoveryRuntime.Directory()
	server.runtime.slots = discoveryRuntime.Slots()
	if err := server.runtime.lifecycle.Go(func(ctx context.Context) {
		select {
		case <-ctx.Done():
			return
		case <-discoveryRuntime.Done():
		}
		err := discoveryRuntime.Err()
		if errors.Is(err, discovery.ErrRuntimeClosed) {
			return
		}
		if errors.Is(err, discovery.ErrRuntimeIdentityLost) {
			txlog.ErrorContext(ctx, "service discovery runtime identity lost", "err", err)
			server.reportFatal(err)
		}
	}); err != nil {
		return fmt.Errorf("framework: observe discovery runtime: %w", err)
	}
	if err := server.runtime.lifecycle.Go(func(ctx context.Context) {
		if err := discoveryRuntime.Watch(ctx); err != nil && !errors.Is(err, context.Canceled) {
			txlog.ErrorContext(ctx, "service discovery watch stopped", "err", err)
		}
	}); err != nil {
		return fmt.Errorf("framework: start discovery watch: %w", err)
	}
	readyTimeout := server.config.Discovery.Etcd.DialTimeout
	if readyTimeout <= 0 {
		readyTimeout = 5 * time.Second
	}
	readyCtx, cancelReady := context.WithTimeout(context.Background(), readyTimeout)
	defer cancelReady()
	if err := discoveryRuntime.WaitReady(readyCtx); err != nil {
		return fmt.Errorf("framework: wait discovery snapshot: %w", err)
	}
	return nil
}

func (server *Server) startActor() error {
	runtime, err := txactor.New(server.config.Actor.Runtime, server.runtime.actorKinds...)
	if err != nil {
		return fmt.Errorf("framework: initialize actor runtime: %w", err)
	}
	if err := runtime.Start(server.runtime.lifecycle); err != nil {
		return fmt.Errorf("framework: start actor runtime: %w", err)
	}
	endpoint, err := runtime.Endpoint()
	if err != nil {
		_ = runtime.Shutdown()
		return fmt.Errorf("framework: resolve actor endpoint: %w", err)
	}
	server.runtime.actor = runtime
	server.runtime.setActorEndpoint(endpoint)
	return nil
}

func (server *Server) startTransaction() error {
	config := server.config.Runtime.Transaction
	txn.Configure(config.Module)
	server.runtime.transaction = txn.NewRuntime()
	return nil
}

func startStorageHealth(root *lifecycle.WaitGroupContext, store *storageRuntime, config HealthConfig, backend txstorage.Backend) error {
	if root == nil || store == nil {
		return errors.New("framework: storage health dependencies are not configured")
	}
	return root.Go(func(ctx context.Context) {
		ticker := time.NewTicker(config.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				probeCtx, cancel := context.WithTimeout(ctx, config.Timeout)
				_ = store.ProbeBackend(probeCtx, backend)
				cancel()
			}
		}
	})
}
