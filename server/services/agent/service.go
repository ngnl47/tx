// Package agent 提供标准的 TCP 和 WebSocket 玩家入口服务。
package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"

	txlog "github.com/ngnl47/tx/log"
	txagent "github.com/ngnl47/tx/modules/agent"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
	txserver "github.com/ngnl47/tx/server"
	"github.com/ngnl47/tx/server/adapters/msgqredis"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// ServiceName 是 Agent 在启动参数和服务发现中的稳定名称。
	ServiceName     = "agent"
	msgqServiceName = "msgq"
)

// LocalMsgQ 提供同进程 msgq 的会话客户端，避免绕行服务发现和网络 RPC。
type LocalMsgQ interface {
	SessionClient() (txmsgq.SessionClient, error)
}

// HandlerProvider 在 Agent 启动时根据当前服务能力惰性创建身份解析器和业务分发器。
type HandlerProvider func(txserver.Context) (txagent.IdentityResolver, txagent.Dispatcher, error)

// OptionsProvider 在 agent 被选中时转换项目配置。
type OptionsProvider func() (Options, error)

// Options 描述 Agent 的监听地址、帧限制和项目扩展点。
type Options struct {
	// TCPListen 是原生 TCP 监听地址；空字符串表示不启动 TCP 入口。
	TCPListen string
	// WebSocketListen 是 WebSocket 监听地址；空字符串表示不启动 WebSocket 入口。
	WebSocketListen string
	// MaxFrameBytes 限制单个协议帧的最大字节数。
	MaxFrameBytes int
	// FetchBatch 控制每轮从会话邮箱拉取的最大下行消息数。
	FetchBatch int
	// Identity 与 Dispatcher 同时设置时优先作为静态业务处理器。
	Identity txagent.IdentityResolver
	// Dispatcher 与 Identity 同时设置时优先作为静态业务处理器。
	Dispatcher txagent.Dispatcher
	// Handlers 在未同时设置 Identity 和 Dispatcher 时惰性提供业务处理器。
	Handlers HandlerProvider
	// Sessions 显式覆盖会话邮箱路由；设置后不再自动选择本地或远端 msgq。
	Sessions txagent.SessionClientRouter
	// LocalMsgQ 在当前进程同时选择 msgq 时提供零网络跳转的会话客户端。
	LocalMsgQ LocalMsgQ
	// SessionStore 配置通过 Redis 和服务发现访问远端 msgq 的会话绑定。
	SessionStore msgqredis.Options
}

type Service struct {
	options       Options
	provider      OptionsProvider
	configureOnce sync.Once
	configureErr  error

	mu                sync.RWMutex
	tcp               *txagent.TCPServer
	websocket         *http.Server
	websocketListener net.Listener
	websocketAddr     string
	sessionCloser     interface{ Close() error }
	draining          bool
}

// New 使用已经转换完成的强类型配置创建 Agent 服务。
func New(options Options) (*Service, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	return &Service{options: options}, nil
}

// NewConfigured 将项目配置转换推迟到当前进程选中该服务之后。
func NewConfigured(provider OptionsProvider) (*Service, error) {
	if provider == nil {
		return nil, fmt.Errorf("agent service: options provider is nil")
	}
	return &Service{provider: provider}, nil
}

// Spec 声明 Agent 的基础能力依赖。
func (service *Service) Spec() txserver.Spec {
	return txserver.Spec{
		Name: ServiceName,
		Requires: []txserver.CModID{
			txserver.CModRPC,
			txserver.CModRedisStorage,
			txserver.CModDiscovery,
			txserver.CModActor,
		},
	}
}

// RequiredCMods 根据本轮是否同时启动 msgq，校验远端会话路由配置。
// Agent 的 Actor 与服务发现依赖由 Spec 固定声明。
func (service *Service) RequiredCMods(selection txserver.ServiceSelection) ([]txserver.CModID, error) {
	if err := service.configure(); err != nil {
		return nil, err
	}
	usesLocal := service.options.LocalMsgQ != nil && selection.Has(msgqServiceName)
	usesRemote := service.options.Sessions == nil && !usesLocal
	if !usesRemote {
		return nil, nil
	}
	if strings.TrimSpace(service.options.SessionStore.RouteType) == "" || strings.TrimSpace(service.options.SessionStore.Prefix) == "" || service.options.SessionStore.TTL <= 0 {
		return nil, fmt.Errorf("agent service: remote session routing is not configured")
	}
	return nil, nil
}

// Start 解析会话路由并启动选中的 TCP、WebSocket 入口。
func (service *Service) Start(runtime txserver.Context) error {
	if service == nil || runtime == nil || runtime.Tasks() == nil || runtime.Admission() == nil {
		return fmt.Errorf("agent service: lifecycle and ingress are required")
	}
	if err := service.configure(); err != nil {
		return err
	}
	identity, dispatcher, err := service.resolveHandlers(runtime)
	if err != nil {
		return err
	}
	sessions, closer, err := service.resolveSessions(runtime)
	if err != nil {
		return err
	}
	agentRuntime, err := txagent.NewServer(txagent.ServerOptions{
		IdentityResolver: identity,
		Dispatcher:       dispatcher,
		Admission:        runtime.Admission(),
		Sessions:         sessions,
		FetchBatch:       service.options.FetchBatch,
	})
	if err != nil {
		_ = closer.Close()
		return fmt.Errorf("agent service: create session runtime: %w", err)
	}
	var tcp *txagent.TCPServer
	if service.options.TCPListen != "" {
		tcp, err = txagent.ListenTCP(service.options.TCPListen, agentRuntime, service.options.MaxFrameBytes)
		if err != nil {
			_ = closer.Close()
			return fmt.Errorf("agent service: listen TCP %s: %w", service.options.TCPListen, err)
		}
	}
	websocket, listener, err := service.startWebSocket(runtime, agentRuntime, tcp)
	if err != nil {
		_ = closer.Close()
		return err
	}
	if tcp != nil {
		if err := runtime.Tasks().Go(func(ctx context.Context) {
			if serveErr := tcp.Serve(ctx); serveErr != nil && ctx.Err() == nil {
				txlog.ErrorContext(ctx, "agent TCP service stopped unexpectedly", "err", serveErr)
			}
		}); err != nil {
			if websocket != nil {
				_ = websocket.Close()
			}
			_ = tcp.Close()
			_ = closer.Close()
			return err
		}
	}
	service.mu.Lock()
	service.tcp = tcp
	service.websocket = websocket
	service.websocketListener = listener
	if websocket != nil {
		service.websocketAddr = websocket.Addr
	}
	service.sessionCloser = closer
	service.draining = false
	service.mu.Unlock()
	txlog.Info("agent service initialized", "tcp", service.TCPAddress(), "webSocket", service.WebSocketAddress(), "fetchBatch", service.options.FetchBatch)
	return nil
}

func (service *Service) configure() error {
	if service == nil {
		return fmt.Errorf("agent service: service is nil")
	}
	service.configureOnce.Do(func() {
		if service.provider == nil {
			service.configureErr = validateOptions(service.options)
			return
		}
		options, err := service.provider()
		if err != nil {
			service.configureErr = fmt.Errorf("agent service: configure: %w", err)
			return
		}
		if err := validateOptions(options); err != nil {
			service.configureErr = err
			return
		}
		service.options = options
	})
	return service.configureErr
}

func (service *Service) startWebSocket(runtime txserver.Context, agentRuntime *txagent.Server, tcp *txagent.TCPServer) (*http.Server, net.Listener, error) {
	if service.options.WebSocketListen == "" {
		return nil, nil, nil
	}
	handler, err := txagent.NewWebSocketHandler(agentRuntime, service.options.MaxFrameBytes)
	if err != nil {
		if tcp != nil {
			_ = tcp.Close()
		}
		return nil, nil, fmt.Errorf("agent service: create WebSocket handler: %w", err)
	}
	listener, err := net.Listen("tcp", service.options.WebSocketListen)
	if err != nil {
		if tcp != nil {
			_ = tcp.Close()
		}
		return nil, nil, fmt.Errorf("agent service: listen WebSocket %s: %w", service.options.WebSocketListen, err)
	}
	httpServer := &http.Server{
		Addr:    listener.Addr().String(),
		Handler: handler,
		BaseContext: func(net.Listener) context.Context {
			return runtime.Tasks()
		},
	}
	if err := runtime.Tasks().Go(func(ctx context.Context) {
		if serveErr := httpServer.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) && ctx.Err() == nil && !service.isDraining() {
			txlog.ErrorContext(ctx, "agent WebSocket service stopped unexpectedly", "err", serveErr)
		}
	}); err != nil {
		_ = httpServer.Close()
		if tcp != nil {
			_ = tcp.Close()
		}
		return nil, nil, err
	}
	return httpServer, listener, nil
}

func (service *Service) Drain(context.Context) error {
	if service == nil {
		return nil
	}
	service.mu.Lock()
	service.draining = true
	tcp, listener := service.tcp, service.websocketListener
	service.mu.Unlock()
	var errs []error
	if tcp != nil {
		errs = append(errs, tcp.Close())
	}
	if listener != nil {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (service *Service) Stop(context.Context) error {
	if service == nil {
		return nil
	}
	service.mu.Lock()
	tcp, websocket, listener, closer := service.tcp, service.websocket, service.websocketListener, service.sessionCloser
	service.tcp = nil
	service.websocket = nil
	service.websocketListener = nil
	service.websocketAddr = ""
	service.sessionCloser = nil
	service.mu.Unlock()
	var errs []error
	if tcp != nil {
		errs = append(errs, tcp.Close())
	}
	if websocket != nil {
		errs = append(errs, websocket.Close())
	}
	if listener != nil {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	if closer != nil {
		errs = append(errs, closer.Close())
	}
	return errors.Join(errs...)
}

func (service *Service) TCPAddress() string {
	if service == nil {
		return ""
	}
	service.mu.RLock()
	defer service.mu.RUnlock()
	if service.tcp == nil || service.tcp.Addr() == nil {
		return ""
	}
	return service.tcp.Addr().String()
}

func (service *Service) WebSocketAddress() string {
	if service == nil {
		return ""
	}
	service.mu.RLock()
	defer service.mu.RUnlock()
	return service.websocketAddr
}

func (service *Service) resolveSessions(runtime txserver.Context) (txagent.SessionClientRouter, interface{ Close() error }, error) {
	if service.options.Sessions != nil {
		if closer, ok := service.options.Sessions.(interface{ Close() error }); ok {
			return service.options.Sessions, closer, nil
		}
		return service.options.Sessions, noOpCloser{}, nil
	}
	selectedServices := runtime.Instance().Services
	usesLocal := service.options.LocalMsgQ != nil && (len(selectedServices) == 0 || slices.Contains(selectedServices, msgqServiceName))
	if usesLocal {
		client, err := service.options.LocalMsgQ.SessionClient()
		if err != nil {
			return nil, nil, fmt.Errorf("agent service: resolve local msgq: %w", err)
		}
		router, err := txmsgq.NewFixedSessionClientRouter(client)
		if err != nil {
			return nil, nil, err
		}
		return router, router, nil
	}
	if runtime.Directory() == nil {
		return nil, nil, fmt.Errorf("agent service: remote msgq requires service discovery")
	}
	advanced := runtime.Advanced()
	if advanced == nil || advanced.Redis() == nil {
		return nil, nil, fmt.Errorf("agent service: Redis routes are unavailable")
	}
	store, err := msgqredis.New(advanced.Redis(), service.options.SessionStore)
	if err != nil {
		return nil, nil, err
	}
	dialer, err := txmsgq.NewGRPCDialer(insecure.NewCredentials())
	if err != nil {
		return nil, nil, err
	}
	router, err := txmsgq.NewSessionClientRouter(runtime.Directory(), store, dialer)
	if err != nil {
		return nil, nil, err
	}
	return router, router, nil
}

func (service *Service) resolveHandlers(runtime txserver.Context) (txagent.IdentityResolver, txagent.Dispatcher, error) {
	if service.options.Identity != nil && service.options.Dispatcher != nil {
		return service.options.Identity, service.options.Dispatcher, nil
	}
	if service.options.Handlers == nil {
		return nil, nil, fmt.Errorf("agent service: business handlers are not configured")
	}
	identity, dispatcher, err := service.options.Handlers(runtime)
	if err != nil {
		return nil, nil, err
	}
	if identity == nil || dispatcher == nil {
		return nil, nil, fmt.Errorf("agent service: handler provider returned nil handlers")
	}
	return identity, dispatcher, nil
}

func (service *Service) isDraining() bool {
	if service == nil {
		return true
	}
	service.mu.RLock()
	defer service.mu.RUnlock()
	return service.draining
}

func validateOptions(options Options) error {
	if strings.TrimSpace(options.TCPListen) == "" && strings.TrimSpace(options.WebSocketListen) == "" {
		return fmt.Errorf("agent service: at least one TCP or WebSocket listen address is required")
	}
	if options.MaxFrameBytes <= 0 || options.FetchBatch <= 0 {
		return fmt.Errorf("agent service: frame limits and business handlers are required")
	}
	if (options.Identity == nil || options.Dispatcher == nil) && options.Handlers == nil {
		return fmt.Errorf("agent service: business handlers are required")
	}
	if options.Sessions == nil && options.LocalMsgQ == nil && (strings.TrimSpace(options.SessionStore.RouteType) == "" || strings.TrimSpace(options.SessionStore.Prefix) == "" || options.SessionStore.TTL <= 0) {
		return fmt.Errorf("agent service: session routing is not configured")
	}
	return nil
}

type noOpCloser struct{}

func (noOpCloser) Close() error { return nil }
