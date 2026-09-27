package msgq

import (
	"context"
	"errors"
	"testing"

	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
)

type nilResponseMailboxClient struct {
	msgqpb.MailboxServiceClient
}

func (*nilResponseMailboxClient) Init(context.Context, *msgqpb.InitRequest, ...grpc.CallOption) (*msgqpb.InitResponse, error) {
	return nil, nil
}

func (*nilResponseMailboxClient) PubSession(context.Context, *msgqpb.PubSessionRequest, ...grpc.CallOption) (*msgqpb.PubSessionResponse, error) {
	return nil, nil
}

func (*nilResponseMailboxClient) PubRole(context.Context, *msgqpb.PubRoleRequest, ...grpc.CallOption) (*msgqpb.BroadcastResponse, error) {
	return nil, nil
}

func (*nilResponseMailboxClient) JoinTopics(context.Context, *msgqpb.TopicRequest, ...grpc.CallOption) (*msgqpb.Empty, error) {
	return nil, nil
}

func (*nilResponseMailboxClient) BroadcastTopic(context.Context, *msgqpb.BroadcastTopicRequest, ...grpc.CallOption) (*msgqpb.BroadcastResponse, error) {
	return nil, nil
}

func (*nilResponseMailboxClient) TerminateSession(context.Context, *msgqpb.TerminateSessionRequest, ...grpc.CallOption) (*msgqpb.Empty, error) {
	return nil, nil
}

func TestGRPCConnectionRejectsNilSuccessResponses(t *testing.T) {
	connection := &grpcConnection{client: &nilResponseMailboxClient{}}
	header := SessionHeader{RoleID: 47, SessionID: 1, ConnectionID: 2}
	tests := map[string]func() error{
		"init": func() error {
			_, err := connection.Init(context.Background(), InitRequest{Header: header})
			return err
		},
		"pub session": func() error {
			_, err := connection.PubSession(context.Background(), header.SessionID, []byte("body"))
			return err
		},
		"pub role": func() error {
			_, err := connection.PubRole(context.Background(), RoleBinding{RoleID: 47, InstanceID: "msgq-a", Generation: "current"}, []byte("body"))
			return err
		},
		"join topics": func() error {
			return connection.JoinTopics(context.Background(), TopicRequest{Header: header, Topics: []string{"guild:1"}})
		},
		"broadcast topic": func() error {
			_, err := connection.BroadcastTopic(context.Background(), BroadcastTopicRequest{Topic: "guild:1", Payload: []byte("body")})
			return err
		},
		"close": func() error {
			return connection.TerminateSession(context.Background(), header)
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			if err := run(); !errors.Is(err, ErrInvalidMailboxResponse) {
				t.Fatalf("error = %v, want %v", err, ErrInvalidMailboxResponse)
			}
		})
	}
}

type recordingTopicMailboxClient struct {
	msgqpb.MailboxServiceClient
	joinRequest      *msgqpb.TopicRequest
	broadcastRequest *msgqpb.BroadcastTopicRequest
}

type recordingSessionMailboxClient struct {
	msgqpb.MailboxServiceClient
	initRequest *msgqpb.InitRequest
	pubRequest  *msgqpb.PubSessionRequest
}

func (client *recordingSessionMailboxClient) Init(_ context.Context, request *msgqpb.InitRequest, _ ...grpc.CallOption) (*msgqpb.InitResponse, error) {
	client.initRequest = request
	return &msgqpb.InitResponse{}, nil
}

func (client *recordingSessionMailboxClient) PubSession(_ context.Context, request *msgqpb.PubSessionRequest, _ ...grpc.CallOption) (*msgqpb.PubSessionResponse, error) {
	client.pubRequest = request
	return &msgqpb.PubSessionResponse{Seq: 3}, nil
}

func TestGRPCConnectionConvertsInitAndExactPublish(t *testing.T) {
	client := &recordingSessionMailboxClient{}
	connection := &grpcConnection{client: client}
	header := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 13}
	if _, err := connection.Init(context.Background(), InitRequest{Header: header}); err != nil {
		t.Fatalf("Init returned error: %v", err)
	}
	if request := client.initRequest; request.GetHeader().GetSessionId() != 11 {
		t.Fatalf("init request = %#v", request)
	}
	result, err := connection.PubSession(context.Background(), 11, []byte("encoded-bon"))
	if err != nil {
		t.Fatalf("PubSession returned error: %v", err)
	}
	if result.Seq != 3 || client.pubRequest.GetSessionId() != 11 || string(client.pubRequest.GetPayload()) != "encoded-bon" {
		t.Fatalf("publish result = %#v, request = %#v", result, client.pubRequest)
	}
}

func (client *recordingTopicMailboxClient) JoinTopics(_ context.Context, request *msgqpb.TopicRequest, _ ...grpc.CallOption) (*msgqpb.Empty, error) {
	client.joinRequest = request
	return &msgqpb.Empty{}, nil
}

func (client *recordingTopicMailboxClient) BroadcastTopic(_ context.Context, request *msgqpb.BroadcastTopicRequest, _ ...grpc.CallOption) (*msgqpb.BroadcastResponse, error) {
	client.broadcastRequest = request
	return &msgqpb.BroadcastResponse{Attempted: 3, Delivered: 2, Skipped: 1, Overflowed: 0}, nil
}

func TestGRPCConnectionConvertsTopicOperations(t *testing.T) {
	client := &recordingTopicMailboxClient{}
	connection := &grpcConnection{client: client}
	header := SessionHeader{RoleID: 47, SessionID: 11, ConnectionID: 13}

	if err := connection.JoinTopics(context.Background(), TopicRequest{Header: header, Topics: []string{"guild:1", "region:2"}}); err != nil {
		t.Fatalf("JoinTopics returned error: %v", err)
	}
	if request := client.joinRequest; request.GetHeader().GetRoleId() != 47 || request.GetHeader().GetSessionId() != 11 || request.GetHeader().GetConnectionId() != 13 || len(request.GetTopics()) != 2 {
		t.Fatalf("join request = %#v", request)
	}

	result, err := connection.BroadcastTopic(context.Background(), BroadcastTopicRequest{Topic: "guild:1", Payload: []byte("encoded-bon")})
	if err != nil {
		t.Fatalf("BroadcastTopic returned error: %v", err)
	}
	if result != (BroadcastResult{Attempted: 3, Delivered: 2, Skipped: 1}) {
		t.Fatalf("broadcast result = %#v", result)
	}
	if request := client.broadcastRequest; request.GetTopic() != "guild:1" || string(request.GetPayload()) != "encoded-bon" {
		t.Fatalf("broadcast request = %#v", request)
	}
}

type invalidSequenceMailboxClient struct {
	msgqpb.MailboxServiceClient
}

func (*invalidSequenceMailboxClient) Init(context.Context, *msgqpb.InitRequest, ...grpc.CallOption) (*msgqpb.InitResponse, error) {
	return &msgqpb.InitResponse{ReceivedSeq: -1}, nil
}

func (*invalidSequenceMailboxClient) PubSession(context.Context, *msgqpb.PubSessionRequest, ...grpc.CallOption) (*msgqpb.PubSessionResponse, error) {
	return &msgqpb.PubSessionResponse{}, nil
}

func TestGRPCConnectionRejectsInvalidResponseSequences(t *testing.T) {
	connection := &grpcConnection{client: &invalidSequenceMailboxClient{}}
	header := SessionHeader{RoleID: 47, SessionID: 1, ConnectionID: 2}
	tests := map[string]func() error{
		"init": func() error {
			_, err := connection.Init(context.Background(), InitRequest{Header: header})
			return err
		},
		"pub session": func() error {
			_, err := connection.PubSession(context.Background(), header.SessionID, []byte("body"))
			return err
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			if err := run(); !errors.Is(err, ErrInvalidMailboxResponse) {
				t.Fatalf("error = %v, want %v", err, ErrInvalidMailboxResponse)
			}
		})
	}
}
