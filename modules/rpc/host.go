package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	"google.golang.org/grpc"
)

var (
	errHostRegistrationNil = errors.New("rpc: host registration is nil")
	errHostStarted         = errors.New("rpc: host is already started")
	errHostStopped         = errors.New("rpc: host is stopped")
)

// Host owns the ordinary gRPC listener shared by services in one process.
// Services register protocols before Start; Host owns serving and shutdown.
type Host struct {
	registrationMu sync.Mutex
	mu             sync.Mutex
	listener       net.Listener
	server         *grpc.Server

	started   bool
	stopped   bool
	serveDone chan struct{}
	stopDone  chan struct{}
	serveErr  error
	stopErr   error
}

// NewHost binds the process endpoint before service registration so callers
// can publish the actual address selected for a port-zero listener.
func NewHost(address string, options ...grpc.ServerOption) (*Host, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, fmt.Errorf("rpc: host listen address is empty")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("rpc: host listen %s: %w", address, err)
	}
	return &Host{
		listener: listener,
		server:   grpc.NewServer(options...),
		stopDone: make(chan struct{}),
	}, nil
}

// Register adds one generated gRPC service before Host starts serving.
func (host *Host) Register(register func(grpc.ServiceRegistrar) error) error {
	if host == nil || register == nil {
		return errHostRegistrationNil
	}
	host.registrationMu.Lock()
	defer host.registrationMu.Unlock()

	host.mu.Lock()
	if host.stopped {
		host.mu.Unlock()
		return errHostStopped
	}
	if host.started {
		host.mu.Unlock()
		return errHostStarted
	}
	server := host.server
	host.mu.Unlock()
	return register(server)
}

// Start serves all registered protocols as work owned by root.
func (host *Host) Start(root *lifecycle.WaitGroupContext) error {
	if host == nil {
		return fmt.Errorf("rpc: host is nil")
	}
	if root == nil {
		return fmt.Errorf("rpc: host lifecycle is not configured")
	}
	host.registrationMu.Lock()
	defer host.registrationMu.Unlock()

	host.mu.Lock()
	if host.stopped {
		host.mu.Unlock()
		return errHostStopped
	}
	if host.started {
		host.mu.Unlock()
		return errHostStarted
	}
	host.started = true
	host.serveDone = make(chan struct{})
	server, listener, done := host.server, host.listener, host.serveDone
	host.mu.Unlock()

	if err := root.Go(func(ctx context.Context) {
		err := server.Serve(listener)
		host.mu.Lock()
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) && ctx.Err() == nil {
			host.serveErr = fmt.Errorf("rpc: host serve: %w", err)
			txlog.Error("shared gRPC host stopped unexpectedly", "err", err)
		}
		close(done)
		host.mu.Unlock()
	}); err != nil {
		host.mu.Lock()
		host.started = false
		host.serveDone = nil
		host.mu.Unlock()
		return err
	}
	return nil
}

// Stop closes the shared listener once. Protocol-specific state remains owned
// by the service that registered it.
func (host *Host) Stop(ctx context.Context) error {
	if host == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	host.registrationMu.Lock()
	host.mu.Lock()
	if host.stopped {
		done := host.stopDone
		host.mu.Unlock()
		host.registrationMu.Unlock()
		select {
		case <-done:
			host.mu.Lock()
			err := host.stopErr
			host.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	host.stopped = true
	started, server, listener, serveDone := host.started, host.server, host.listener, host.serveDone
	host.mu.Unlock()
	host.registrationMu.Unlock()

	if started {
		server.Stop()
	}
	var closeErr error
	if listener != nil {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErr = err
		}
	}
	if serveDone != nil {
		select {
		case <-serveDone:
		case <-ctx.Done():
			closeErr = errors.Join(closeErr, ctx.Err())
		}
	}
	host.mu.Lock()
	host.stopErr = errors.Join(closeErr, host.serveErr)
	close(host.stopDone)
	err := host.stopErr
	host.mu.Unlock()
	return err
}

// Endpoint returns the bound address, including the selected port when NewHost
// received a port-zero address.
func (host *Host) Endpoint() string {
	if host == nil {
		return ""
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if host.listener == nil {
		return ""
	}
	return host.listener.Addr().String()
}
