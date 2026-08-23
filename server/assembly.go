package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/txn"
)

func (server *Server) startControl(endpoint string) error {
	if strings.TrimSpace(endpoint) == "" {
		return nil
	}
	var slots *slotPromoter
	if server.runtime.slots != nil && server.runtime.directory != nil {
		promoter, err := newSlotPromoter(server.runtime.slots, server.runtime.directory)
		if err != nil {
			return fmt.Errorf("server: initialize slot control: %w", err)
		}
		slots = promoter
	}
	controlServer, err := startControlServer(server.runtime.lifecycle, endpoint, controlServerOptions{
		Lifecycle: newDrainCoordinator(server),
		Slots:     slots,
	})
	if err != nil {
		return fmt.Errorf("server: start control endpoint: %w", err)
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

func requiredCapabilities(services []Service) Capabilities {
	var capabilities Capabilities
	for _, service := range services {
		capabilities |= service.Spec().Requires
	}
	return capabilities
}

func selectedServiceNames(services []Service) []string {
	names := make([]string, 0, len(services))
	for _, service := range services {
		names = append(names, service.Spec().Name)
	}
	return names
}

func (server *Server) startDiscovery(options RunOptions) error {
	if !server.config.Discovery.Enabled {
		if strings.TrimSpace(options.LogicalSlot) != "" {
			return errors.New("server: logical slot requires discovery")
		}
		return nil
	}
	instance := server.runtime.instance
	if err := instance.Validate(); err != nil {
		return fmt.Errorf("server: discovery instance: %w", err)
	}
	discoveryRuntime, err := newDiscovery(server.runtime.lifecycle, server.config.Discovery.Etcd, instance)
	if err != nil {
		return fmt.Errorf("server: initialize discovery: %w", err)
	}
	server.discovery = discoveryRuntime
	server.runtime.directory = discoveryRuntime.directorySnapshot()
	server.runtime.slots = discoveryRuntime.slotStore()
	server.runtime.instance = instance
	if strings.TrimSpace(options.LogicalSlot) != "" {
		logicalSlot, err := newLogicalSlot(options.LogicalSlot, instance.ID)
		if err != nil {
			return fmt.Errorf("server: logical slot: %w", err)
		}
		server.runtime.logicalSlot = logicalSlot
	}
	if err := server.runtime.lifecycle.Go(func(ctx context.Context) {
		if err := discoveryRuntime.watch(ctx); err != nil && !errors.Is(err, context.Canceled) {
			txlog.ErrorContext(ctx, "service discovery watch stopped", "err", err)
		}
	}); err != nil {
		return fmt.Errorf("server: start discovery watch: %w", err)
	}
	server.runtime.capabilities |= CapabilityDiscovery
	return nil
}

func (server *Server) startActor() error {
	if !server.config.Actor.Enabled {
		return nil
	}
	runtime, err := txactor.New(server.config.Actor.Runtime)
	if err != nil {
		return fmt.Errorf("server: initialize actor runtime: %w", err)
	}
	if err := runtime.Start(server.runtime.lifecycle); err != nil {
		return fmt.Errorf("server: start actor runtime: %w", err)
	}
	server.runtime.actor = runtime
	server.runtime.capabilities |= CapabilityActor
	return nil
}

func (server *Server) startTransaction() error {
	config := server.config.Runtime.Transaction
	config.Runtime.WriteAdmission = server.runtime.storage
	txn.Configure(config.Module)
	runtime := txn.NewRuntime(server.runtime.storage.ChangeWriter(), config.Runtime)
	if err := runtime.Start(server.runtime.lifecycle); err != nil {
		return fmt.Errorf("server: start transaction runtime: %w", err)
	}
	server.runtime.transaction = runtime
	server.runtime.capabilities |= CapabilityTransaction
	return nil
}

func startStorageHealth(root *lifecycle.WaitGroupContext, store *storageRuntime, config HealthConfig) error {
	if root == nil || store == nil {
		return errors.New("server: storage health dependencies are not configured")
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
				_ = store.probe(probeCtx)
				cancel()
			}
		}
	})
}
