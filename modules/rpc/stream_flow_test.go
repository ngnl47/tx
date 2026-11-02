package rpc

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	rpcpb "github.com/ngnl47/tx/modules/rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// 服务端不读流，使用真实 gRPC 流控复现发送阻塞，不依赖网络或远端逐项响应。
type unreadStreamServer struct {
	rpcpb.UnimplementedDeliveryServiceServer
}

func (*unreadStreamServer) CallStream(stream rpcpb.DeliveryService_CallStreamServer) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

type discardStreamServer struct {
	rpcpb.UnimplementedDeliveryServiceServer
}

func (*discardStreamServer) CallStream(stream rpcpb.DeliveryService_CallStreamServer) error {
	for {
		if _, err := stream.Recv(); err != nil {
			return err
		}
	}
}

func newBufferedStreamConnection(t testing.TB, server rpcpb.DeliveryServiceServer, options ...grpc.ServerOption) *grpcConnection {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	host := grpc.NewServer(options...)
	rpcpb.RegisterDeliveryServiceServer(host, server)
	done := make(chan struct{})
	go func() { defer close(done); _ = host.Serve(listener) }()
	connection, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	client := &grpcConnection{connection: connection, client: rpcpb.NewDeliveryServiceClient(connection)}
	t.Cleanup(func() { _ = client.Close(); host.Stop(); _ = listener.Close(); <-done })
	return client
}

func TestTellStreamDeadlineInterruptsGRPCFlowControl(t *testing.T) {
	connection := newBufferedStreamConnection(t, &unreadStreamServer{}, grpc.StaticStreamWindowSize(65535), grpc.StaticConnWindowSize(65535))
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		for {
			err := connection.TellStream(ctx, Delivery{Route: InstanceRoute("game"), MessageType: "large", Payload: make([]byte, 1024*1024)})
			if err != nil {
				done <- err
				return
			}
		}
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("流控发送未按操作期限结束：%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("真实 gRPC 流控 Send 未随期限退出")
	}
	ctx2, cancel2 := context.WithTimeout(t.Context(), time.Second)
	defer cancel2()
	if err := connection.TellStream(ctx2, Delivery{Route: InstanceRoute("game"), MessageType: "next"}); err != nil {
		t.Fatalf("后续新请求无法重新开流：%v", err)
	}
}

func BenchmarkTellStreamGRPC(b *testing.B) {
	connection := newBufferedStreamConnection(b, &discardStreamServer{})
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()
	delivery := Delivery{Route: InstanceRoute("game"), MessageType: "bench", Payload: make([]byte, 128)}
	if err := connection.TellStream(ctx, delivery); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := connection.TellStream(ctx, delivery); err != nil {
			b.Fatal(err)
		}
	}
}
