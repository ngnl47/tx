// Package agent provides the standard TCP and WebSocket player ingress service.
package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

	txlog "github.com/ngnl47/tx/log"
	txagent "github.com/ngnl47/tx/modules/agent"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
	txserver "github.com/ngnl47/tx/server"
	msgqservice "github.com/ngnl47/tx/server/services/msgq"
	"google.golang.org/grpc/credentials/insecure"
)

const ServiceName = "agent"

type LocalMsgQ interface {
	SessionClient() (txmsgq.SessionClient, error)
}

type HandlerProvider func() (txagent.IdentityResolver, txagent.Dispatcher, error)

// OptionsProvider converts project configuration when agent is selected.
type OptionsProvider func() (Options, error)

type Options struct {
	TCPListen       string
	WebSocketListen string
	MaxFrameBytes   int
	FetchBatch      int
	Identity        txagent.IdentityResolver
	Dispatcher      txagent.Dispatcher
	Handlers        HandlerProvider
	Sessions        txagent.SessionClientRouter
	LocalMsgQ       LocalMsgQ
	SessionStore    msgqservice.SessionStoreOptions
}

type Service struct {
	options  Options
	provider OptionsProvider

	mu                sync.RWMutex
	tcp               *txagent.TCPServer
	websocket         *http.Server
	websocketListener net.Listener
	websocketAddr     string
	sessionCloser     interface{ Close() error }
	draining          bool
}

func New(options Options) (*Service, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	return &Service{options: options}, nil
}

// NewConfigured defers project configuration conversion until the service is
// selected for this process.
func NewConfigured(provider OptionsProvider) (*Service, error) {
	if provider == nil {
		return nil, fmt.Errorf("agent service: options provider is nil")
	}
	return &Service{provider: provider}, nil
}

func (service *Service) Spec() txserver.Spec {
	requires := txserver.CapabilityStorage
	if service != nil && service.provider == nil && service.options.Sessions == nil && service.options.LocalMsgQ == nil {
		requires |= txserver.CapabilityDiscovery
	}
	return txserver.Spec{Name: ServiceName, Requires: requires}
}

func (service *Service) Start(runtime *txserver.Runtime) error {
	if service == nil || runtime == nil || runtime.Lifecycle == nil || runtime.Ingress == nil {
		return fmt.Errorf("agent service: lifecycle and ingress are required")
	}
	if err := service.configure(); err != nil {
		return err
	}
	identity, dispatcher, err := service.resolveHandlers()
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
		Admission:        runtime.Ingress,
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
		if err := runtime.Lifecycle.Go(func(ctx context.Context) {
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
	if service.provider == nil {
		return validateOptions(service.options)
	}
	options, err := service.provider()
	if err != nil {
		return fmt.Errorf("agent service: configure: %w", err)
	}
	if err := validateOptions(options); err != nil {
		return err
	}
	service.options = options
	return nil
}

func (service *Service) startWebSocket(runtime *txserver.Runtime, agentRuntime *txagent.Server, tcp *txagent.TCPServer) (*http.Server, net.Listener, error) {
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
			return runtime.Lifecycle
		},
	}
	if err := runtime.Lifecycle.Go(func(ctx context.Context) {
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

func (service *Service) resolveSessions(runtime *txserver.Runtime) (txagent.SessionClientRouter, interface{ Close() error }, error) {
	if service.options.Sessions != nil {
		if closer, ok := service.options.Sessions.(interface{ Close() error }); ok {
			return service.options.Sessions, closer, nil
		}
		return service.options.Sessions, noOpCloser{}, nil
	}
	if service.options.LocalMsgQ != nil {
		if client, err := service.options.LocalMsgQ.SessionClient(); err == nil {
			router, err := txmsgq.NewFixedSessionClientRouter(client)
			if err != nil {
				return nil, nil, err
			}
			return router, router, nil
		}
	}
	if runtime.Directory == nil {
		return nil, nil, fmt.Errorf("agent service: remote msgq requires service discovery")
	}
	store, err := msgqservice.NewSessionStore(runtime.Storage, service.options.SessionStore)
	if err != nil {
		return nil, nil, err
	}
	dialer, err := txmsgq.NewGRPCDialer(insecure.NewCredentials())
	if err != nil {
		return nil, nil, err
	}
	router, err := txmsgq.NewSessionClientRouter(runtime.Directory, store, dialer)
	if err != nil {
		return nil, nil, err
	}
	return router, router, nil
}

func (service *Service) resolveHandlers() (txagent.IdentityResolver, txagent.Dispatcher, error) {
	if service.options.Identity != nil && service.options.Dispatcher != nil {
		return service.options.Identity, service.options.Dispatcher, nil
	}
	if service.options.Handlers == nil {
		return nil, nil, fmt.Errorf("agent service: business handlers are not configured")
	}
	identity, dispatcher, err := service.options.Handlers()
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
