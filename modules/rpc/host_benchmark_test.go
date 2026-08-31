package rpc

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// BenchmarkDeliveryTransport 对比相同 Delivery 的进程内接收与真实 loopback gRPC 路径。
func BenchmarkDeliveryTransport(b *testing.B) {
	delivery := benchmarkDelivery()
	receiver := benchmarkDeliveryReceiver{expected: delivery}

	b.Run("DirectReceiver", func(b *testing.B) {
		b.Run("Serial", func(b *testing.B) {
			benchmarkDirectDeliverySerial(b, receiver, delivery)
		})
		b.Run("Parallel", func(b *testing.B) {
			benchmarkDirectDeliveryParallel(b, receiver, delivery)
		})
	})

	b.Run("LoopbackGRPC", func(b *testing.B) {
		sender := newLoopbackBenchmarkSender(b, receiver)
		benchmarkContext, cancel := context.WithTimeout(b.Context(), 30*time.Second)
		b.Cleanup(cancel)
		if err := sender.Send(benchmarkContext, delivery); err != nil {
			b.Fatalf("预热 loopback gRPC 发送: %v", err)
		}

		b.Run("Serial", func(b *testing.B) {
			benchmarkSenderDeliverySerial(b, sender, benchmarkContext, delivery)
		})
		b.Run("Parallel", func(b *testing.B) {
			benchmarkSenderDeliveryParallel(b, sender, benchmarkContext, delivery)
		})
	})
}

func benchmarkDirectDeliverySerial(b *testing.B, receiver Receiver, delivery Delivery) {
	b.SetBytes(int64(len(delivery.Payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := receiver.Deliver(context.Background(), delivery); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkDirectDeliveryParallel(b *testing.B, receiver Receiver, delivery Delivery) {
	b.SetBytes(int64(len(delivery.Payload)))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			if err := receiver.Deliver(context.Background(), delivery); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func benchmarkSenderDeliverySerial(b *testing.B, sender *Sender, ctx context.Context, delivery Delivery) {
	b.SetBytes(int64(len(delivery.Payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := sender.Send(ctx, delivery); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSenderDeliveryParallel(b *testing.B, sender *Sender, ctx context.Context, delivery Delivery) {
	b.SetBytes(int64(len(delivery.Payload)))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			if err := sender.Send(ctx, delivery); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func newLoopbackBenchmarkSender(b *testing.B, receiver Receiver) *Sender {
	b.Helper()
	root := lifecycle.NewWaitGroupContext(context.Background())
	host, err := NewHost("127.0.0.1:0")
	if err != nil {
		b.Fatalf("创建 loopback gRPC Host: %v", err)
	}
	if err := host.Register(func(registrar grpc.ServiceRegistrar) error {
		return RegisterDeliveryService(registrar, receiver, nil)
	}); err != nil {
		b.Fatalf("注册 DeliveryService: %v", err)
	}
	if err := host.Start(root); err != nil {
		b.Fatalf("启动 loopback gRPC Host: %v", err)
	}

	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       "benchmark-rpc",
		Services: []string{"benchmark-rpc"},
		Endpoint: host.Endpoint(),
		Status:   discovery.StatusRunning,
	}}); err != nil {
		b.Fatalf("写入 loopback 服务目录: %v", err)
	}
	dialer, err := NewGRPCDialer(insecure.NewCredentials())
	if err != nil {
		b.Fatalf("创建 loopback gRPC Dialer: %v", err)
	}
	sender, err := NewSender(directory, dialer)
	if err != nil {
		b.Fatalf("创建 loopback Sender: %v", err)
	}
	b.Cleanup(func() {
		if err := sender.Close(); err != nil {
			b.Errorf("关闭 loopback Sender: %v", err)
		}
		if err := host.Stop(context.Background()); err != nil {
			b.Errorf("停止 loopback gRPC Host: %v", err)
		}
		root.Cancel()
		root.Wait()
	})
	return sender
}

func benchmarkDelivery() Delivery {
	payload := make([]byte, 256)
	for index := range payload {
		payload[index] = byte(index)
	}
	return Delivery{
		Route:       ServiceRoute("benchmark-rpc"),
		MessageType: "benchmark.delivery",
		CommandID:   470001,
		Payload:     payload,
	}
}

type benchmarkDeliveryReceiver struct {
	expected Delivery
}

func (receiver benchmarkDeliveryReceiver) Deliver(_ context.Context, delivery Delivery) error {
	if delivery.Route != receiver.expected.Route ||
		delivery.MessageType != receiver.expected.MessageType ||
		delivery.CommandID != receiver.expected.CommandID ||
		!bytes.Equal(delivery.Payload, receiver.expected.Payload) {
		return fmt.Errorf("收到的 benchmark Delivery 与预期不一致")
	}
	return nil
}
