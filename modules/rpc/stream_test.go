package rpc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type cancelStreamReceiver struct {
	cancelled chan struct{}
}

func (r *cancelStreamReceiver) Call(context.Context, Delivery) (Result, error) {
	return Result{}, errors.New("不应调用 Unary")
}

func (r *cancelStreamReceiver) CallStream(ctx context.Context, delivery Delivery) (Result, error) {
	if delivery.MessageType == "slow" {
		<-ctx.Done()
		close(r.cancelled)
		return Result{}, ctx.Err()
	}
	return Result{MessageType: "fast"}, nil
}

// 取消单项调用须传到服务端，但不能关闭其他项共用的流。
func TestCallStreamCancelsOnlyExpiredItem(t *testing.T) {
	receiver := &cancelStreamReceiver{cancelled: make(chan struct{})}
	host, err := NewHost("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	root := lifecycle.NewWaitGroupContext(context.Background())
	defer func() { _ = host.Stop(context.Background()); root.Cancel(); root.Wait() }()
	if err := host.Register(func(registrar grpc.ServiceRegistrar) error {
		return RegisterDeliveryService(registrar, nil, receiver)
	}); err != nil {
		t.Fatal(err)
	}
	if err := host.Start(root); err != nil {
		t.Fatal(err)
	}
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{ID: "game-a", Services: []string{"game"}, Endpoint: host.Endpoint(), Status: discovery.StatusRunning}}); err != nil {
		t.Fatal(err)
	}
	dialer, err := NewGRPCDialer(insecure.NewCredentials())
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewSender(directory, dialer)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err = sender.CallStream(ctx, Delivery{Route: InstanceRoute("game-a"), MessageType: "slow"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("超时调用返回 %v", err)
	}
	select {
	case <-receiver.cancelled:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("调用方已取消，但服务端仍在执行")
	}
	result, err := sender.CallStream(context.Background(), Delivery{Route: InstanceRoute("game-a"), MessageType: "fast"})
	if err != nil || result.MessageType != "fast" {
		t.Fatalf("后续调用受影响: result=%v err=%v", result, err)
	}
}

type interruptedStreamReceiver struct {
	entered chan struct{}
	calls   atomic.Int32
}

func (r *interruptedStreamReceiver) Call(context.Context, Delivery) (Result, error) {
	return Result{}, errors.New("不应调用 Unary")
}

func (r *interruptedStreamReceiver) CallStream(ctx context.Context, delivery Delivery) (Result, error) {
	r.calls.Add(1)
	if delivery.MessageType == "block" {
		r.entered <- struct{}{}
		<-ctx.Done()
		return Result{}, ctx.Err()
	}
	return Result{MessageType: "recovered"}, nil
}

// 服务端中途断流必须让在途项失败；新的调用由调用方发起，不自动重投旧项。
func TestCallStreamHostFailureAndReconnect(t *testing.T) {
	root := lifecycle.NewWaitGroupContext(context.Background())
	defer func() { root.Cancel(); root.Wait() }()
	receiver := &interruptedStreamReceiver{entered: make(chan struct{}, 2)}
	host, err := NewHost("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Register(func(registrar grpc.ServiceRegistrar) error {
		return RegisterDeliveryService(registrar, nil, receiver)
	}); err != nil {
		t.Fatal(err)
	}
	if err := host.Start(root); err != nil {
		t.Fatal(err)
	}
	endpoint := host.Endpoint()
	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{ID: "game-a", Services: []string{"game"}, Endpoint: endpoint, Status: discovery.StatusRunning}}); err != nil {
		t.Fatal(err)
	}
	dialer, err := NewGRPCDialer(insecure.NewCredentials())
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewSender(directory, dialer)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	callCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	finished := make(chan error, 2)
	for range 2 {
		go func() {
			_, callErr := sender.CallStream(callCtx, Delivery{Route: InstanceRoute("game-a"), MessageType: "block"})
			finished <- callErr
		}()
	}
	for range 2 {
		select {
		case <-receiver.entered:
		case <-callCtx.Done():
			t.Fatal("在途请求未抵达服务端")
		}
	}
	if err := host.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case err := <-finished:
			if err == nil || callCtx.Err() != nil {
				t.Fatalf("断流未及时返回明确失败: %v", err)
			}
		case <-callCtx.Done():
			t.Fatal("断流后的调用仍在等待")
		}
	}
	if got := receiver.calls.Load(); got != 2 {
		t.Fatalf("断流后自动重投了旧请求: calls=%d", got)
	}
	sender.mu.Lock()
	connection := sender.connections[endpoint].(*grpcConnection)
	sender.mu.Unlock()
	connection.streamMu.Lock()
	broken := connection.stream
	connection.streamMu.Unlock()
	if broken != nil {
		t.Fatal("调用方收到断流错误后，连接仍保留已关闭的流")
	}

	restarted, err := NewHost(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Stop(context.Background())
	if err := restarted.Register(func(registrar grpc.ServiceRegistrar) error {
		return RegisterDeliveryService(registrar, nil, &interruptedStreamReceiver{})
	}); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Start(root); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		result, callErr := sender.CallStream(ctx, Delivery{Route: InstanceRoute("game-a"), MessageType: "recover"})
		cancel()
		if callErr == nil {
			if result.MessageType != "recovered" {
				t.Fatalf("重连结果错误: %+v", result)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("新请求未在重连后恢复: %v", callErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
