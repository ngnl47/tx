package framework

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

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

	servicePhases  map[string]*servicePhase
	services       []Service
	cmods          []CMod
	configProvider CModConfigProvider
	types          fusion.TypeRegistry
	bindStorage    func(StorageReaders)
	setups         []func(Context) (func(), error)

	mu             sync.Mutex
	starting       bool
	started        bool
	stopping       bool
	startDone      chan struct{}
	startCancel    context.CancelCauseFunc
	runtime        *runtime
	host           *serviceHost
	cmodHost       *cmodHost
	discovery      *discoveryRuntime
	control        *controlServer
	cleanups       []func()
	stopDone       chan struct{}
	stopErr        error
	stopAttempt    *drainAttempt
	prepareAttempt *drainAttempt
	prepared       bool
	finalizing     bool
	releaseDone    bool
	finalizeErr    error
	fatalOnce      sync.Once
	fatalErr       error
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

// Run 启动服务并等待退出信号。关闭失败后间隔一秒重试三次，仍失败则返回错误，
// 由进程入口记录并非零退出；不会把未完成的持久化视为成功。
func (server *Server) Run(ctx context.Context, options RunOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	startErr := server.Start(ctx, options)
	if startErr != nil && !errors.Is(startErr, errStopDeferred) {
		return startErr
	}
	server.mu.Lock()
	runtime := server.runtime
	server.mu.Unlock()
	if startErr == nil && runtime != nil && runtime.lifecycle != nil {
		select {
		case <-ctx.Done():
		case <-runtime.lifecycle.Done():
		}
	}
	for attempt := 0; ; attempt++ {
		err := server.Stop(context.Background())
		if !errors.Is(err, errStopDeferred) {
			return errors.Join(startErr, err)
		}
		txlog.Error("server shutdown failed", "app", server.config.AppID, "attempt", attempt+1, "error", err)
		if attempt == 3 {
			txlog.Error("server shutdown retries exhausted; process must exit with failure; unflushed data may be lost", "app", server.config.AppID, "error", err)
			return errors.Join(startErr, fmt.Errorf("framework: shutdown failed after three retries: %w", err))
		}
		time.Sleep(time.Second)
	}
}

// Start 装配框架资源并启动选中的服务；启动钩子遵守调用方的取消信号。
// 托管后台任务另用脱离取消的生命周期，关闭时先排空已接受工作再取消。
func (server *Server) Start(ctx context.Context, options RunOptions) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if server == nil {
		return errors.New("framework: server is nil")
	}
	server.mu.Lock()
	if server.starting || server.started || server.stopping || server.runtime != nil {
		server.mu.Unlock()
		return errors.New("framework: server is already started or stopping")
	}
	ctx, cancelStart := context.WithCancelCause(ctx)
	defer cancelStart(nil)
	server.startCancel = cancelStart
	server.starting = true
	server.startDone = make(chan struct{})
	server.mu.Unlock()
	var selected []Service
	var graph cmodGraph
	defer func() {
		// 启动成功与致命错误登记共用同一把锁；已知失败不能提交为 Running。
		server.mu.Lock()
		err = errors.Join(err, context.Cause(ctx), server.fatalErr)
		if err == nil {
			server.started = true
			txlog.Info("server started successfully",
				"app", server.config.AppID,
				"services", strings.Join(selectedServiceNames(selected), ","),
				"cmods", strings.Join(cmodIDStrings(graph.IDs()), ","),
				"components", strings.Join(cmodTopology(graph.ordered, newServiceSelection(selected)), ";"),
				"endpoint", server.runtime.instanceSnapshot().Endpoint,
			)
			server.starting = false
			server.startCancel = nil
			close(server.startDone)
			server.mu.Unlock()
			return
		}
		server.mu.Unlock()
		cleanupErr := server.shutdown(context.Background(), false)
		err = errors.Join(err, cleanupErr)
		server.mu.Lock()
		server.starting = false
		server.startCancel = nil
		if !errors.Is(cleanupErr, errStopDeferred) {
			server.stopping = true
			server.stopErr = cleanupErr
			server.clearRuntimeLocked()
			close(server.stopDone)
		}
		close(server.startDone)
		server.mu.Unlock()
	}()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	registry, err := newRegistry(server.services...)
	if err != nil {
		return err
	}
	selected, err = registry.selectServices(options.Services)
	if err != nil {
		return err
	}
	var actorTypes []txactor.Type
	for _, service := range selected {
		actorTypes = append(actorTypes, service.Spec().ActorTypes...)
	}
	allCMods := append(server.builtInCMods(options), server.cmods...)
	graph, err = resolveCModGraph(allCMods, selected)
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
		actorTypes:    actorTypes,
		actorCatalog:  registry.actorCatalog,
		instance: discovery.Instance{
			Endpoint:  strings.TrimSpace(options.InstanceEndpoint),
			Endpoints: maps.Clone(options.InstanceEndpoints),
			Version:   strings.TrimSpace(options.InstanceVersion),
			Services:  selectedServiceNames(selected),
			Status:    discovery.StatusStarting,
		},
	}
	server.mu.Lock()
	runtime.serviceAccess = make(map[string]*runtimeAccess, len(selected))
	runtime.servicePhases = make(map[string]*servicePhase, len(selected))
	server.servicePhases = runtime.servicePhases
	for _, service := range selected {
		spec := service.Spec()
		runtime.serviceAccess[spec.Name] = newRuntimeAccess(runtime, spec)
		runtime.servicePhases[spec.Name] = &servicePhase{}
	}
	server.runtime = runtime
	server.mu.Unlock()

	cmodHost := newCModHost(ctx, newCModContext(server), graph)
	server.cmodHost = cmodHost
	if err := cmodHost.start(); err != nil {
		return err
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}

	if server.bindStorage != nil {
		server.bindStorage(runtime.storage)
	}
	setupContext := newServiceContext(runtime, nil)
	for _, setup := range server.setups {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		cleanup, setupErr := setup(setupContext)
		if setupErr != nil {
			return fmt.Errorf("framework: setup: %w", setupErr)
		}
		if cleanup != nil {
			server.cleanups = append(server.cleanups, cleanup)
		}
	}
	if runtime.placement != nil {
		if err := runtime.placement.initialize(runtime, selected); err != nil {
			return err
		}
	}
	host := newServiceHost(registry, runtime)
	server.host = host
	if err := host.initServices(ctx, selected); err != nil {
		return err
	}
	if runtime.rpc != nil {
		if err := runtime.rpc.Start(root, server.reportFatal); err != nil {
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
	if err := host.startServices(ctx); err != nil {
		return err
	}
	server.mu.Lock()
	startupErr := errors.Join(context.Cause(ctx), server.fatalErr)
	if startupErr == nil {
		runtime.enableSessionIDs()
		host.openServices()
	}
	server.mu.Unlock()
	if startupErr != nil {
		return startupErr
	}
	// 任务只能在服务开放后领取，避免启动窗口消耗有限投递次数。
	if err := server.startTaskConsumer(ctx); err != nil {
		return err
	}

	return nil
}

func (server *Server) reportFatal(err error) {
	if server == nil || err == nil {
		return
	}
	server.fatalOnce.Do(func() {
		server.mu.Lock()
		server.fatalErr = err
		if server.starting {
			cancel := server.startCancel
			server.mu.Unlock()
			cancel(err)
			return
		}
		server.mu.Unlock()
		go func() {
			// 若加入的是租约失效前已经开始的普通关闭，须再执行异常终止，
			// 不能让该轮保留资源的结果把身份已失效的进程留在运行中。
			if err := server.Stop(context.Background()); errors.Is(err, errStopDeferred) {
				_ = server.Stop(context.Background())
			}
		}()
	})
}

// Context 在 Start 成功后返回已经装配且隔离所有权的能力视图。
func (server *Server) Context() Context {
	if server == nil {
		return nil
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if !server.started || server.stopping || server.finalizing || server.runtime == nil {
		return nil
	}
	return newServiceContext(server.runtime, nil)
}

func (server *Server) clearRuntimeLocked() {
	server.started = false
	if server.runtime != nil {
		server.runtime.accessClosed.Store(true)
	}
	server.runtime = nil
	server.host = nil
	server.cmodHost = nil
	server.discovery = nil
	server.control = nil
	server.cleanups = nil
}
