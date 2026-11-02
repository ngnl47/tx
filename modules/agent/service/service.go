// Package agentservice 提供不依赖框架门面的标准 TCP 和 WebSocket 玩家入口。
package agentservice

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	txagent "github.com/ngnl47/tx/modules/agent"
	"github.com/ngnl47/tx/modules/discovery"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/modules/msgq/msgqredis"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	txgoredis "github.com/ngnl47/tx/storage/redis/goredis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// LocalMsgQ 提供同进程 msgq 的会话客户端，避免绕行服务发现和网络 RPC。
type LocalMsgQ interface {
	SessionClient() (txmsgq.SessionClient, error)
}

// Dependencies 是 Agent 入口启动所需的最小基础能力集合。
type Dependencies struct {
	// Tasks 托管监听器及完整连接的实际执行期。
	Tasks lifecycle.Tasks
	// ReportFatal 将入口非预期退出交给宿主统一处理；独立使用时可留空。
	ReportFatal func(error)
	// Admission 在排水开始后拒绝新的外部业务帧与项目普通 HTTP 请求。
	Admission lifecycle.Admission
	// RPC 将 Agent 控制服务注册到进程共享的内部 gRPC Host。
	RPC txrpc.Registrar
	// Directory 为远端 msgq 会话路由提供本地服务发现快照。
	Directory discovery.Directory
	// Redis 为远端 msgq 会话 owner 绑定提供脚本路由。
	Redis txgoredis.ClientProvider
}

// Options 描述 Agent 监听地址、帧限制和业务处理器。
type Options struct {
	TCPListen       string
	WebSocketListen string
	// WebSocketOrigins 是允许跨域连接的完整浏览器 Origin 白名单。
	WebSocketOrigins []string
	// HTTPHandler 承载项目普通 HTTP 请求，根路径仍保留给 WebSocket。
	HTTPHandler   http.Handler
	MaxFrameBytes int
	Identity      txagent.IdentityResolver
	Dispatcher    txagent.Dispatcher
	Sessions      txagent.SessionClientRouter
	LocalMsgQ     LocalMsgQ
	// MsgQService 是项目声明的远端邮箱服务名；使用自定义或本地 Sessions 时不参与查找。
	MsgQService  string
	SessionStore msgqredis.Options
}

// Service 承载玩家连接入口和会话邮箱客户端。
type Service struct {
	options      Options
	dependencies Dependencies

	mu                sync.RWMutex
	tasks             *lifecycle.WaitGroupContext
	runtime           *txagent.Server
	tcp               *txagent.TCPServer
	websocket         *http.Server
	websocketListener net.Listener
	websocketAddr     string
	sessionCloser     interface{ Close() error }
	draining          bool
}

// New 校验配置和显式依赖，构造时不启动监听器。
func New(options Options, dependencies Dependencies) (*Service, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	if dependencies.Tasks == nil || dependencies.Admission == nil || dependencies.RPC == nil {
		return nil, fmt.Errorf("agent service: lifecycle, ingress, and RPC are required")
	}
	return &Service{options: options, dependencies: dependencies}, nil
}

// Init 解析会话路由并启动 TCP、WebSocket 入口。
func (service *Service) Init(ctx context.Context) (initErr error) {
	if service == nil {
		return fmt.Errorf("agent service: service is nil")
	}
	sessions, closer, err := service.resolveSessions()
	if err != nil {
		return err
	}
	work := lifecycle.NewWaitGroupContext(service.dependencies.Tasks)
	service.mu.Lock()
	service.tasks = work
	service.sessionCloser = closer
	service.mu.Unlock()
	defer func() {
		if initErr != nil {
			initErr = errors.Join(initErr, service.Stop(ctx))
		}
	}()
	if err := service.dependencies.Tasks.Go(func(context.Context) {
		work.WaitDone()
		// 启动失败可能先取消任务再调用 Stop；关闭监听以唤醒 Serve。
		_ = service.Drain(context.Background())
		// 父取消也要先关闭登记边界，再等待实际退出，避免新连接与 Wait 交错。
		_ = work.CancelAndWait(context.Background())
	}); err != nil {
		return err
	}
	agentRuntime, err := txagent.NewServer(txagent.ServerOptions{
		IdentityResolver: service.options.Identity,
		Dispatcher:       service.options.Dispatcher,
		Admission:        service.dependencies.Admission,
		Connections:      lifecycle.AdmissionFunc(work.Enter),
		Sessions:         sessions,
	})
	if err != nil {
		return fmt.Errorf("agent service: create session runtime: %w", err)
	}
	if err := service.dependencies.RPC.Register(func(registrar grpc.ServiceRegistrar) error {
		return txagent.RegisterControlService(registrar, agentRuntime)
	}); err != nil {
		return fmt.Errorf("agent service: register control RPC: %w", err)
	}
	var tcp *txagent.TCPServer
	if service.options.TCPListen != "" {
		tcp, err = txagent.ListenTCP(service.options.TCPListen, agentRuntime, service.options.MaxFrameBytes)
		if err != nil {
			return fmt.Errorf("agent service: listen TCP %s: %w", service.options.TCPListen, err)
		}
	}
	service.mu.Lock()
	service.runtime, service.tcp = agentRuntime, tcp
	service.mu.Unlock()
	websocket, listener, err := service.startWebSocket(agentRuntime)
	if err != nil {
		return err
	}
	service.mu.Lock()
	service.websocket = websocket
	service.websocketListener = listener
	if websocket != nil {
		service.websocketAddr = websocket.Addr
	}
	service.draining = false
	service.mu.Unlock()
	if websocket != nil {
		if err := lifecycle.TaskView(work).Go(func(ctx context.Context) {
			service.reportServeExit(ctx, "WebSocket", websocket.Serve(listener))
		}); err != nil {
			return err
		}
	}
	if tcp != nil {
		if err := lifecycle.TaskView(work).Go(func(ctx context.Context) {
			service.reportServeExit(ctx, "TCP", tcp.Serve(ctx))
		}); err != nil {
			return err
		}
	}
	txlog.Info("agent service initialized", "tcp", service.TCPAddress(), "webSocket", service.WebSocketAddress())
	return nil
}

func (service *Service) startWebSocket(agentRuntime *txagent.Server) (*http.Server, net.Listener, error) {
	if service.options.WebSocketListen == "" {
		return nil, nil, nil
	}
	websocketHandler, err := txagent.NewWebSocketHandler(agentRuntime, service.options.MaxFrameBytes, txagent.WebSocketOptions{
		AllowedOrigins: service.options.WebSocketOrigins,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("agent service: create WebSocket handler: %w", err)
	}
	listener, err := net.Listen("tcp", service.options.WebSocketListen)
	if err != nil {
		return nil, nil, fmt.Errorf("agent service: listen WebSocket %s: %w", service.options.WebSocketListen, err)
	}
	httpServer := &http.Server{
		Addr:    listener.Addr().String(),
		Handler: service.composeHTTPHandler(websocketHandler),
		BaseContext: func(net.Listener) context.Context {
			return lifecycle.TaskView(service.tasks)
		},
	}
	return httpServer, listener, nil
}

func (service *Service) reportServeExit(ctx context.Context, transport string, err error) {
	if ctx.Err() != nil || service.isDraining() {
		return
	}
	if err == nil {
		err = errors.New("listener exited")
	}
	err = fmt.Errorf("agent %s service stopped unexpectedly: %w", transport, err)
	if service.dependencies.ReportFatal != nil {
		service.dependencies.ReportFatal(err)
	} else {
		txlog.ErrorContext(ctx, "agent service stopped unexpectedly", "err", err)
	}
}

// composeHTTPHandler 复用同一个 HTTP Server 承载根路径 WebSocket 和项目普通 HTTP Handler。
func (service *Service) composeHTTPHandler(websocketHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			websocketHandler.ServeHTTP(w, r)
			return
		}
		if service.options.HTTPHandler == nil {
			http.NotFound(w, r)
			return
		}
		done, err := service.dependencies.Admission.Enter()
		if err != nil {
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		defer done()
		service.options.HTTPHandler.ServeHTTP(w, r)
	})
}

// Drain 关闭入口监听器，不中断已经接纳的业务工作。
func (service *Service) Drain(ctx context.Context) error {
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

// Stop 释放监听器与会话路由资源。该方法可以重复调用。
func (service *Service) Stop(ctx context.Context) error {
	if service == nil {
		return nil
	}
	service.mu.Lock()
	service.draining = true
	tasks, tcp, websocket, listener := service.tasks, service.tcp, service.websocket, service.websocketListener
	service.mu.Unlock()
	if tasks != nil {
		tasks.Cancel()
	}
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
	if tasks != nil {
		if err := tasks.CancelAndWait(ctx); err != nil {
			return errors.Join(append(errs, err)...)
		}
	}
	service.mu.Lock()
	closer := service.sessionCloser
	service.tcp = nil
	service.runtime = nil
	service.websocket = nil
	service.websocketListener = nil
	service.websocketAddr = ""
	service.sessionCloser = nil
	service.mu.Unlock()
	if closer != nil {
		errs = append(errs, closer.Close())
	}
	return errors.Join(errs...)
}

// TCPAddress 返回实际 TCP 监听地址。
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

// WebSocketAddress 返回实际 WebSocket 监听地址。
func (service *Service) WebSocketAddress() string {
	if service == nil {
		return ""
	}
	service.mu.RLock()
	defer service.mu.RUnlock()
	return service.websocketAddr
}

func (service *Service) resolveSessions() (txagent.SessionClientRouter, interface{ Close() error }, error) {
	if service.options.Sessions != nil {
		if closer, ok := service.options.Sessions.(interface{ Close() error }); ok {
			return service.options.Sessions, closer, nil
		}
		return service.options.Sessions, noOpCloser{}, nil
	}
	usesLocal := service.options.LocalMsgQ != nil
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
	if service.dependencies.Directory == nil {
		return nil, nil, fmt.Errorf("agent service: remote msgq requires service discovery")
	}
	if service.dependencies.Redis == nil {
		return nil, nil, fmt.Errorf("agent service: Redis clients are unavailable")
	}
	store, err := msgqredis.New(service.dependencies.Redis, service.options.SessionStore)
	if err != nil {
		return nil, nil, err
	}
	dialer, err := txmsgq.NewGRPCDialer(insecure.NewCredentials())
	if err != nil {
		return nil, nil, err
	}
	router, err := txmsgq.NewSessionClientRouter(service.options.MsgQService, service.dependencies.Directory, store, dialer)
	if err != nil {
		return nil, nil, err
	}
	return router, router, nil
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
	if options.MaxFrameBytes <= 0 || options.Identity == nil || options.Dispatcher == nil {
		return fmt.Errorf("agent service: frame limits and command handlers are required")
	}
	if options.Sessions == nil && options.LocalMsgQ == nil && (strings.TrimSpace(options.SessionStore.StoreName) == "" || strings.TrimSpace(options.SessionStore.Prefix) == "" || options.SessionStore.TTL <= 0) {
		return fmt.Errorf("agent service: session routing is not configured")
	}
	return nil
}

type noOpCloser struct{}

func (noOpCloser) Close() error { return nil }
