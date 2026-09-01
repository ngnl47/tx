package framework

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"

	txlog "github.com/ngnl47/tx/log"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/storage/fusion"
)

type Option func(*Server) error

// WithService 按声明顺序注册一个可由进程选择的服务。
func WithService(services ...Service) Option {
	return func(server *Server) error {
		server.services = append(server.services, services...)
		return nil
	}
}

// WithCMod 注册项目能力模组。项目服务应通过构造函数接收其强类型实例，
// 不应在运行期通过 CModID 做动态类型断言。
func WithCMod(mods ...CMod) Option {
	return func(server *Server) error {
		server.cmods = append(server.cmods, mods...)
		return nil
	}
}

// WithCModConfigProvider 注册惰性配置提供者。
// 普通业务代码不需要使用它；项目组合根可借此只转换本次启动闭包需要的配置。
func WithCModConfigProvider(provider CModConfigProvider) Option {
	return func(server *Server) error {
		if provider == nil {
			return errors.New("framework: CMod config provider is nil")
		}
		if server.configProvider != nil {
			return errors.New("framework: CMod config provider is already registered")
		}
		server.configProvider = provider
		return nil
	}
}

// WithGeneratedData 将生成的类型注册表和默认 reader 接入框架。
// bind 通常使用 data.RegisterStorageRuntime。
func WithGeneratedData(types fusion.TypeRegistry, bind func(StorageReaders)) Option {
	return func(server *Server) error {
		if types == nil {
			return errors.New("framework: generated type registry is nil")
		}
		if bind == nil {
			return errors.New("framework: generated storage binding is nil")
		}
		server.types = types
		server.bindStorage = bind
		return nil
	}
}

// WithSetup 注册项目胶水逻辑，它会在框架能力就绪后、服务启动前运行。
// 回调接收与服务相同且隔离所有权的 Context 契约，cleanup 按相反顺序执行。
func WithSetup(setup func(Context) (cleanup func(), err error)) Option {
	return func(server *Server) error {
		if setup == nil {
			return errors.New("framework: setup is nil")
		}
		server.setups = append(server.setups, setup)
		return nil
	}
}

// Server 持有进程公共 runtime 和选中服务的生命周期。
type Server struct {
	config Config

	services       []Service
	cmods          []CMod
	configProvider CModConfigProvider
	types          fusion.TypeRegistry
	bindStorage    func(StorageReaders)
	setups         []func(Context) (func(), error)

	mu        sync.Mutex
	starting  bool
	started   bool
	stopping  bool
	startDone chan struct{}
	runtime   *runtime
	host      *serviceHost
	cmodHost  *cmodHost
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
			return nil, errors.New("framework: option is nil")
		}
		if err := option(server); err != nil {
			return nil, err
		}
	}
	if _, err := newRegistry(server.services...); err != nil {
		return nil, err
	}
	return server, nil
}

// Run 启动选中的服务、等待 ctx 取消，并执行完整的排水和关闭流程。
func (server *Server) Run(ctx context.Context, options RunOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := server.Start(context.WithoutCancel(ctx), options); err != nil {
		return err
	}
	server.mu.Lock()
	runtime := server.runtime
	server.mu.Unlock()
	if runtime == nil || runtime.lifecycle == nil {
		return server.Stop(context.Background())
	}
	select {
	case <-ctx.Done():
	case <-runtime.lifecycle.Done():
	}
	return server.Stop(context.Background())
}

// Start 装配框架资源并启动选中的服务。传入 context 的值会被保留，但取消信号会被刻意分离；
// Run 负责控制关闭顺序，确保已接受工作排空后再取消全局 WaitGroupContext。
func (server *Server) Start(ctx context.Context, options RunOptions) (err error) {
	if server == nil {
		return errors.New("framework: server is nil")
	}
	server.mu.Lock()
	if server.starting || server.started || server.stopping {
		server.mu.Unlock()
		return errors.New("framework: server is already started or stopping")
	}
	server.starting = true
	server.startDone = make(chan struct{})
	server.mu.Unlock()
	defer func() {
		var cleanupErr error
		if err != nil {
			cleanupErr = server.shutdown(context.Background(), false)
			err = errors.Join(err, cleanupErr)
		}
		server.mu.Lock()
		server.starting = false
		if err == nil {
			server.started = true
		} else {
			server.stopping = true
			server.stopErr = cleanupErr
			server.clearRuntimeLocked()
			close(server.stopDone)
		}
		close(server.startDone)
		server.mu.Unlock()
	}()
	registry, err := newRegistry(server.services...)
	if err != nil {
		return err
	}
	selected, err := registry.selectServices(options.Services)
	if err != nil {
		return err
	}
	actorKinds, err := collectActorKinds(selected)
	if err != nil {
		return err
	}
	allCMods := append(server.builtInCMods(options), server.cmods...)
	graph, err := resolveCModGraph(allCMods, selected)
	if err != nil {
		return err
	}
	if server.configProvider != nil {
		resolved, resolveErr := server.configProvider(newAssemblySelection(
			newCModSelection(graph.IDs()...),
			newServiceSelection(selected),
		))
		if resolveErr != nil {
			return fmt.Errorf("framework: resolve CMod config: %w", resolveErr)
		}
		resolved = resolved.clone()
		if resolveErr := resolved.Validate(); resolveErr != nil {
			return resolveErr
		}
		if resolved.AppID != server.config.AppID {
			return fmt.Errorf("framework: CMod config provider changed app ID from %q to %q", server.config.AppID, resolved.AppID)
		}
		server.config = resolved
	}
	if err := configureLogger(server.config.Log); err != nil {
		return fmt.Errorf("framework: configure logger: %w", err)
	}

	root := lifecycleRoot(ctx)
	runtime := &runtime{
		config:        server.config,
		lifecycle:     root,
		selectedCMods: newCModSelection(graph.IDs()...).ids,
		actorKinds:    append([]txactor.Kind(nil), actorKinds...),
		instance: discovery.Instance{
			ID:        strings.TrimSpace(options.InstanceID),
			Endpoint:  strings.TrimSpace(options.InstanceEndpoint),
			Endpoints: maps.Clone(options.InstanceEndpoints),
			Version:   strings.TrimSpace(options.InstanceVersion),
			Services:  selectedServiceNames(selected),
			Status:    discovery.StatusStarting,
		},
	}
	server.mu.Lock()
	server.runtime = runtime
	server.mu.Unlock()

	cmodHost := newCModHost(newCModContext(server), graph)
	server.cmodHost = cmodHost
	if err := cmodHost.start(); err != nil {
		return err
	}

	if server.bindStorage != nil {
		server.bindStorage(runtime.storage)
	}
	setupContext := newServiceContext(runtime, nil)
	for _, setup := range server.setups {
		cleanup, setupErr := setup(setupContext)
		if setupErr != nil {
			return fmt.Errorf("framework: setup: %w", setupErr)
		}
		if cleanup != nil {
			server.cleanups = append(server.cleanups, cleanup)
		}
	}
	host := newServiceHost(registry, runtime)
	server.host = host
	if err := host.startServices(selected); err != nil {
		return err
	}
	if runtime.rpc != nil {
		if err := runtime.rpc.Start(root); err != nil {
			return fmt.Errorf("framework: start RPC host: %w", err)
		}
	}
	if err := server.startControl(options.ControlListen); err != nil {
		return err
	}
	if server.discovery != nil {
		if err := server.discovery.MarkRunning(context.Background()); err != nil {
			return fmt.Errorf("framework: mark discovery running: %w", err)
		}
	}
	runtime.setInstanceStatus(discovery.StatusRunning)
	txlog.Info(
		"server started successfully",
		"app", server.config.AppID,
		"services", strings.Join(selectedServiceNames(selected), ","),
		"cmods", strings.Join(cmodIDStrings(graph.IDs()), ","),
		"components", strings.Join(cmodTopology(graph.ordered, newServiceSelection(selected)), ";"),
		"endpoint", runtime.instanceSnapshot().Endpoint,
	)
	return nil
}

// Context 在 Start 成功后返回已经装配且隔离所有权的能力视图。
func (server *Server) Context() Context {
	if server == nil {
		return nil
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if !server.started || server.stopping || server.runtime == nil {
		return nil
	}
	return newServiceContext(server.runtime, nil)
}

func (server *Server) clearRuntimeLocked() {
	server.started = false
	server.runtime = nil
	server.host = nil
	server.cmodHost = nil
	server.discovery = nil
	server.control = nil
	server.cleanups = nil
}
