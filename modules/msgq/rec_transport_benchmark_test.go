package msgq

import (
	"context"
	"net"
	"testing"

	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// BenchmarkRecTransport 对比同一邮箱的进程内调用与复用连接的 gRPC 调用。
func BenchmarkRecTransport(b *testing.B) {
	for _, remote := range []bool{false, true} {
		name := "local"
		if remote {
			name = "grpc-loopback"
		}
		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			mailbox, err := NewServer(ServerOptions{InstanceID: "msgq-bench", Store: &testSessionStore{}, MaxBacklogMessages: 4})
			if err != nil {
				b.Fatal(err)
			}
			var client SessionClient
			if remote {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					b.Fatal(err)
				}
				server := grpc.NewServer()
				if err := RegisterMailboxService(server, mailbox); err != nil {
					b.Fatal(err)
				}
				go func() { _ = server.Serve(listener) }()
				b.Cleanup(func() { server.Stop(); _ = listener.Close() })
				transport, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = transport.Close() })
				client = &grpcConnection{client: msgqpb.NewMailboxServiceClient(transport)}
			} else {
				client, err = NewLocalSessionClient(mailbox)
				if err != nil {
					b.Fatal(err)
				}
			}
			header := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}
			if _, err := client.Init(ctx, InitRequest{Header: header}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				seq := int64(index + 1)
				result, err := client.Rec(ctx, RecRequest{Header: header, Seq: seq})
				if err != nil || result.UpMsgSeq != seq {
					b.Fatalf("Rec(%d) = %+v, %v", seq, result, err)
				}
			}
		})
	}
}
