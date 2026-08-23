package msgq

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	"github.com/ngnl47/tx/modules/discovery"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	txserver "github.com/ngnl47/tx/server"
	"github.com/ngnl47/tx/storage/redis/goredis"
	redis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServiceStartsMsgQGRPCServiceWithRedisSessionStore(t *testing.T) {
	endpoint := os.Getenv("TX_REDIS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set TX_REDIS_ENDPOINT to run msgq service integration test")
	}
	ctx := context.Background()
	rawRedis := redis.NewClient(&redis.Options{Addr: endpoint})
	defer rawRedis.Close()
	if err := rawRedis.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis %q: %v", endpoint, err)
	}
	prefix := fmt.Sprintf("txgame:test:msgq:%d", time.Now().UnixNano())
	defer rawRedis.Del(ctx, prefix+":session:{470001}")
	store, err := txmsgq.NewRedisSessionStore(goredis.NewClient(rawRedis), txmsgq.SessionRedisOptions{
		Prefix: prefix,
		TTL:    time.Minute,
	})
	if err != nil {
		t.Fatalf("new redis session store: %v", err)
	}
	service, err := New(Options{
		InstanceID:         "msgq-test-1",
		Store:              store,
		MaxBacklogMessages: 4,
	})
	if err != nil {
		t.Fatalf("new msgq service: %v", err)
	}
	root := lifecycle.NewWaitGroupContext(ctx)
	rpcHost, err := txrpc.NewHost("127.0.0.1:0")
	if err != nil {
		t.Fatalf("new gRPC host: %v", err)
	}
	if err := service.Start(&txserver.Runtime{Lifecycle: root, RPC: rpcHost}); err != nil {
		t.Fatalf("start msgq service: %v", err)
	}
	if err := rpcHost.Start(root); err != nil {
		t.Fatalf("start RPC host: %v", err)
	}
	t.Cleanup(func() {
		if err := service.Stop(context.Background()); err != nil {
			t.Errorf("stop msgq service: %v", err)
		}
		if err := rpcHost.Stop(context.Background()); err != nil {
			t.Errorf("stop RPC host: %v", err)
		}
		root.Cancel()
		root.WaitDoneAndWaitGroupWait()
	})

	conn, err := grpc.NewClient(rpcHost.Endpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial msgq service: %v", err)
	}
	defer conn.Close()
	client := msgqpb.NewMailboxServiceClient(conn)
	header := &msgqpb.SessionHeader{RoleId: 470001, SessionId: 99, ConnectionId: 7}
	if _, err := client.Init(ctx, &msgqpb.InitRequest{Header: header}); err != nil {
		t.Fatalf("init through msgq service: %v", err)
	}
	if _, err := client.Pub(ctx, &msgqpb.PubRequest{Header: &msgqpb.SessionHeader{RoleId: 470001}, Payload: []byte("service-bon")}); err != nil {
		t.Fatalf("publish through msgq service: %v", err)
	}
	message, err := client.Fetch(ctx, &msgqpb.FetchRequest{Header: header})
	if err != nil {
		t.Fatalf("fetch through msgq service: %v", err)
	}
	if message.GetSeq() != 1 || string(message.GetPayload()) != "service-bon" {
		t.Fatalf("message = %#v", message)
	}

	directory := discovery.NewServiceDirectory()
	if err := directory.Replace([]discovery.Instance{{
		ID:       "msgq-test-1",
		Services: []string{"msgq"},
		Endpoint: rpcHost.Endpoint(),
		Status:   discovery.StatusRunning,
	}}); err != nil {
		t.Fatalf("replace msgq directory: %v", err)
	}
	router, err := txmsgq.NewSessionRouter(directory, store)
	if err != nil {
		t.Fatalf("new msgq session router: %v", err)
	}
	dialer, err := txmsgq.NewGRPCDialer(insecure.NewCredentials())
	if err != nil {
		t.Fatalf("new msgq sender dialer: %v", err)
	}
	sender, err := txmsgq.NewSender(router, dialer)
	if err != nil {
		t.Fatalf("new msgq sender: %v", err)
	}
	defer sender.Close()
	if _, err := sender.Pub(ctx, 470001, []byte("routed-bon")); err != nil {
		t.Fatalf("publish through routed msgq sender: %v", err)
	}
	message, err = client.Fetch(ctx, &msgqpb.FetchRequest{Header: header})
	if err != nil {
		t.Fatalf("fetch routed msgq message: %v", err)
	}
	if message.GetSeq() != 2 || string(message.GetPayload()) != "routed-bon" {
		t.Fatalf("routed message = %#v", message)
	}
}
