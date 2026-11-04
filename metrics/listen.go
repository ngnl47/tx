package metrics

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"time"
)

// Listener 承载显式配置的指标 HTTP 端口。
type Listener struct {
	server *http.Server
	addr   string
	stop   context.CancelFunc
}

// Listen 在指定地址启动独立采集入口；ctx 取消会关闭监听，调用方也可显式调用 Close。
func Listen(ctx context.Context, address string) (*Listener, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	serveCtx, stop := context.WithCancel(ctx)
	result := &Listener{server: server, addr: listener.Addr().String(), stop: stop}
	go func() {
		<-serveCtx.Done()
		_ = server.Close()
	}()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics listener stopped: %v", err)
		}
	}()
	return result, nil
}

// Close 关闭 HTTP 服务及连接，不等待已开始的采集回调退出。
func (listener *Listener) Close() error {
	listener.stop()
	return listener.server.Close()
}
