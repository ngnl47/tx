package agent

import (
	"context"
	"net"
	"testing"

	agentpb "github.com/ngnl47/tx/modules/agent/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestAgentControlServiceReturnsNotHandledForUnknownLocalSession(t *testing.T) {
	service := &controlService{server: &Server{activeSessions: newSessionRegistry()}}
	response, err := service.DisconnectSession(context.Background(), &agentpb.SessionControlRequest{SessionId: 99, Reason: "duplicate_login"})
	if err != nil {
		t.Fatalf("disconnect session: %v", err)
	}
	if response.GetHandled() || response.GetSessionId() != 99 || response.GetConnectionId() != 0 {
		t.Fatalf("response = %#v", response)
	}
}

func TestAgentControlGRPCReturnsNotHandledForUnknownLocalSession(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	if err := RegisterControlService(grpcServer, &Server{activeSessions: newSessionRegistry()}); err != nil {
		t.Fatalf("RegisterControlService returned error: %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		<-serveDone
	})

	connection, err := grpc.NewClient("passthrough:///agent-control", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}))
	if err != nil {
		t.Fatalf("grpc.NewClient returned error: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	response, err := agentpb.NewAgentControlServiceClient(connection).TerminateSession(context.Background(), &agentpb.SessionControlRequest{SessionId: 99, Reason: "account_banned"})
	if err != nil {
		t.Fatalf("TerminateSession returned error: %v", err)
	}
	if response.GetHandled() || response.GetSessionId() != 99 || response.GetConnectionId() != 0 {
		t.Fatalf("response = %#v", response)
	}
}
