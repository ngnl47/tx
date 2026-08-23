package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	txlog "github.com/ngnl47/tx/log"
	"github.com/ngnl47/tx/modules/discovery"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	"github.com/ngnl47/tx/storage/fusion"
)

type Option func(*Server) error

// WithService registers a process-selectable service in declaration order.
func WithService(services ...Service) Option {
	return func(server *Server) error {
		server.services = append(server.services, services...)
		return nil
	}
}

// WithGeneratedData connects the generated type registry and default readers to
// the framework. bind is normally data.RegisterStorageRuntime.
func WithGeneratedData(types fusion.TypeRegistry, bind func(StorageReaders)) Option {
	return func(server *Server) error {
		if types == nil {
			return errors.New("server: generated type registry is nil")
		}
		if bind == nil {
			return errors.New("server: generated storage binding is nil")
		}
		server.types = types
		server.bindStorage = bind
		return nil
	}
}

// WithSetup registers project glue that runs after framework capabilities are
// ready and before services start. The callback receives the same ownership-safe
// Context contract as services; cleanup runs in reverse order.
func WithSetup(setup func(Context) (cleanup func(), err error)) Option {
	return func(server *Server) error {
		if setup == nil {
			return errors.New("server: setup is nil")
		}
		server.setups = append(server.setups, setup)
		return nil
	}
}

// Server owns the common process runtime and selected service lifecycle.
type Server struct {
	config Config

	services    []Service
	types       fusion.TypeRegistry
	bindStorage func(StorageReaders)
	setups      []func(Context) (func(), error)

	mu        sync.Mutex
	started   bool
	stopping  bool
	runtime   *runtime
	host      *serviceHost
	discovery *discoveryRuntime
	control   *controlServer
	cleanups  []func()
	stopDone  chan struct{}
	stopErr   error
}

func New(config Config, options ...Option) (*Server, error) {
	config = config.clone()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	server := &Server{config: config, stopDone: make(chan struct{})}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("server: option is nil")
		}
		if err := option(server); err != nil {
			return nil, err
		}
	}
	if server.types == nil || server.bindStorage == nil {
		return nil, errors.New("server: generated data binding is required")
	}
	if _, err := newRegistry(server.services...); err != nil {
		return nil, err
	}
	return server, nil
}

// Run starts selected services, waits for ctx cancellation, and performs the
// complete drain and shutdown sequence.
func (server *Server) Run(ctx context.Context, options RunOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := server.Start(context.WithoutCancel(ctx), options); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
	case <-server.runtime.lifecycle.Done():
	}
	return server.Stop(context.Background())
}

// Start assembles framework resources and starts the selected services. The
// supplied context contributes values but is intentionally detached from its
// cancellation signal; Run controls shutdown order so accepted work drains
// before the global WaitGroupContext is canceled.
func (server *Server) Start(ctx context.Context, options RunOptions) (err error) {
	if server == nil {
		return errors.New("server: server is nil")
	}
	server.mu.Lock()
	if server.started || server.stopping {
		server.mu.Unlock()
		return errors.New("server: server is already started or stopping")
	}
	server.started = true
	server.mu.Unlock()
	defer func() {
		if err != nil {
			err = errors.Join(err, server.stop(context.Background(), false))
		}
	}()
	if err := configureLogger(server.config.Log); err != nil {
		return fmt.Errorf("server: configure logger: %w", err)
	}

	registry, err := newRegistry(server.services...)
	if err != nil {
		return err
	}
	selected, err := registry.selectServices(options.Services)
	if err != nil {
		return err
	}
	requires := requiredCapabilities(selected)

	root := lifecycleRoot(ctx)
	store, err := newStorage(root, server.config.Storage, server.config.Runtime.storageOptions(), server.types)
	if err != nil {
		return fmt.Errorf("server: initialize storage: %w", err)
	}
	rpcListen := strings.TrimSpace(options.RPCListen)
	if rpcListen == "" {
		rpcListen = strings.TrimSpace(options.InstanceEndpoint)
	}
	if rpcListen == "" {
		rpcListen = "127.0.0.1:0"
	}
	rpcHost, err := txrpc.NewHost(rpcListen)
	if err != nil {
		_ = store.close(context.Background())
		return fmt.Errorf("server: initialize RPC host: %w", err)
	}

	instanceEndpoint := strings.TrimSpace(options.InstanceEndpoint)
	if instanceEndpoint == "" {
		instanceEndpoint = rpcHost.Endpoint()
	}
	runtime := &runtime{
		config:       server.config,
		lifecycle:    root,
		storage:      store,
		rpc:          rpcHost,
		capabilities: CapabilityStorage | CapabilityRPC,
		instance: discovery.Instance{
			ID:       strings.TrimSpace(options.InstanceID),
			Endpoint: instanceEndpoint,
			Version:  strings.TrimSpace(options.InstanceVersion),
			Services: selectedServiceNames(selected),
			Status:   discovery.StatusStarting,
		},
	}
	server.mu.Lock()
	server.runtime = runtime
	server.mu.Unlock()

	if err := server.startDiscovery(options); err != nil {
		return err
	}
	if err := server.startActor(); err != nil {
		return err
	}
	if requires&CapabilityTransaction != 0 {
		if err := server.startTransaction(); err != nil {
			return err
		}
	}
	if missing := requires &^ runtime.capabilities; missing != 0 {
		return fmt.Errorf("server: selected services require unavailable capabilities %s", missing)
	}

	server.bindStorage(store)
	setupContext := newServiceContext(runtime, nil)
	for _, setup := range server.setups {
		cleanup, setupErr := setup(setupContext)
		if setupErr != nil {
			return fmt.Errorf("server: setup: %w", setupErr)
		}
		if cleanup != nil {
			server.cleanups = append(server.cleanups, cleanup)
		}
	}
	if server.config.Runtime.BackgroundWorkers {
		if err := store.startBackgroundWorkers(root); err != nil {
			return fmt.Errorf("server: start storage workers: %w", err)
		}
	}
	if err := startStorageHealth(root, store, server.config.Runtime.Health); err != nil {
		return err
	}

	host := newServiceHost(registry, runtime)
	server.host = host
	if err := host.startServices(selected); err != nil {
		return err
	}
	if err := rpcHost.Start(root); err != nil {
		return fmt.Errorf("server: start RPC host: %w", err)
	}
	if server.discovery != nil {
		if err := server.discovery.markRunning(context.Background()); err != nil {
			return fmt.Errorf("server: mark discovery running: %w", err)
		}
	}
	if err := server.startControl(options.ControlListen); err != nil {
		return err
	}
	txlog.Info("server initialized", "app", server.config.AppID, "services", options.Services, "endpoint", rpcHost.Endpoint())
	return nil
}

// Context returns the assembled ownership-safe capability view after Start succeeds.
func (server *Server) Context() Context {
	if server == nil {
		return nil
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.runtime == nil {
		return nil
	}
	return newServiceContext(server.runtime, nil)
}
