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

// Registrar 是共享 gRPC Host 面向服务的一侧，进程启动和停止仍由 server 装配层私有管理。
type Registrar interface {
	Register(func(grpc.ServiceRegistrar) error) error
	Endpoint() string
}

var _ Registrar = (*Host)(nil)

// Host 持有同一进程内各服务共享的普通 gRPC listener。
// 各服务在 Start 前注册协议，Host 负责对外服务和关闭。
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

// NewHost 在服务注册前绑定进程 endpoint，使调用方能够发布端口为零时实际选择的地址。
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

// Register 在 Host 开始服务前添加一个生成的 gRPC 服务。
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

// Start 启动全部已注册协议，并将服务任务登记到 root。
// 非预期退出同步通知 onFailure；回调只报告错误，不应在回调内等待本 Host 关闭。
func (host *Host) Start(root *lifecycle.WaitGroupContext, onFailure func(error)) error {
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
		// 最终取消必须唤醒 Serve，宿主才能等完根任务后执行组件 Stop。
		stop := context.AfterFunc(ctx, server.Stop)
		defer stop()
		err := server.Serve(listener)
		host.mu.Lock()
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) && ctx.Err() == nil && !host.stopped {
			host.serveErr = fmt.Errorf("rpc: host serve: %w", err)
			txlog.Error("shared gRPC host stopped unexpectedly", "err", err)
		}
		failure := host.serveErr
		close(done)
		host.mu.Unlock()
		if failure != nil && onFailure != nil {
			onFailure(failure)
		}
	}); err != nil {
		host.mu.Lock()
		host.started = false
		host.serveDone = nil
		host.mu.Unlock()
		return err
	}
	return nil
}

// Stop 关闭一次共享 listener。协议专属状态仍由注册该协议的服务负责。
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

// Endpoint 返回实际绑定地址；NewHost 使用零端口时，返回值包含系统选择的端口。
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
