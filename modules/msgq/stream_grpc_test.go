package msgq

import (
	"context"
	"net"
	"testing"
	"time"

	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestStreamFetchRestoresUnconfirmedDownlink(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mailbox, err := NewServer(ServerOptions{InstanceID: "msgq-a", Store: &testSessionStore{}, MaxBacklogMessages: 4})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	if err := RegisterMailboxService(server, mailbox); err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	transport, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Close() })
	client := &grpcConnection{client: msgqpb.NewMailboxServiceClient(transport)}
	header := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 1}
	if _, err := client.Init(ctx, InitRequest{Header: header}); err != nil {
		t.Fatal(err)
	}
	streamCtx, stopStream := context.WithCancel(ctx)
	stream, err := client.StreamFetch(streamCtx, FetchSomeRequest{Header: header, Max: 1})
	if err != nil {
		t.Fatal(err)
	}
	for seq := int64(1); seq <= 2; seq++ {
		if _, err := mailbox.PubSession(ctx, header.SessionID, []byte{byte(seq)}); err != nil {
			t.Fatal(err)
		}
		batch, err := stream.Recv()
		if err != nil || len(batch.Messages) != 1 || batch.Messages[0].Seq != seq || len(batch.Messages[0].Payload) != 1 || batch.Messages[0].Payload[0] != byte(seq) {
			t.Fatalf("stream batch %d = %+v, %v", seq, batch, err)
		}
	}
	stopStream()
	header.ConnectionID = 2
	if _, err := client.Init(ctx, InitRequest{Header: header, Restore: true, Ack: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := mailbox.PubSession(ctx, header.SessionID, []byte{3}); err != nil {
		t.Fatal(err)
	}
	replayed, err := client.StreamFetch(ctx, FetchSomeRequest{Header: header, Max: 2})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := replayed.Recv()
	if err != nil || len(batch.Messages) != 2 || batch.Messages[0].Seq != 2 || batch.Messages[1].Seq != 3 {
		t.Fatalf("restored batch = %+v, %v", batch, err)
	}
}
