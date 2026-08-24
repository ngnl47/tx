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

func (*nilResponseMailboxClient) Rec(context.Context, *msgqpb.RecRequest, ...grpc.CallOption) (*msgqpb.RecResponse, error) {
	return nil, nil
}

func (*nilResponseMailboxClient) Pub(context.Context, *msgqpb.PubRequest, ...grpc.CallOption) (*msgqpb.PubResponse, error) {
	return nil, nil
}

func (*nilResponseMailboxClient) FetchSome(context.Context, *msgqpb.FetchSomeRequest, ...grpc.CallOption) (*msgqpb.FetchSomeResponse, error) {
	return nil, nil
}

func (*nilResponseMailboxClient) Confirm(context.Context, *msgqpb.ConfirmRequest, ...grpc.CallOption) (*msgqpb.Empty, error) {
	return nil, nil
}

func (*nilResponseMailboxClient) Close(context.Context, *msgqpb.CloseRequest, ...grpc.CallOption) (*msgqpb.Empty, error) {
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
		"rec": func() error {
			_, err := connection.Rec(context.Background(), RecRequest{Header: header, Seq: 1})
			return err
		},
		"pub": func() error {
			_, err := connection.Pub(context.Background(), PubRequest{Header: header, Payload: []byte("body")})
			return err
		},
		"fetch some": func() error {
			_, err := connection.FetchSome(context.Background(), FetchSomeRequest{Header: header, Max: 1})
			return err
		},
		"confirm": func() error {
			return connection.Confirm(context.Background(), ConfirmRequest{Header: header, Ack: 1})
		},
		"close": func() error {
			return connection.CloseSession(context.Background(), header)
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

type invalidSequenceMailboxClient struct {
	msgqpb.MailboxServiceClient
}

func (*invalidSequenceMailboxClient) Init(context.Context, *msgqpb.InitRequest, ...grpc.CallOption) (*msgqpb.InitResponse, error) {
	return &msgqpb.InitResponse{ReceivedSeq: -1}, nil
}

func (*invalidSequenceMailboxClient) Rec(context.Context, *msgqpb.RecRequest, ...grpc.CallOption) (*msgqpb.RecResponse, error) {
	return &msgqpb.RecResponse{ReceivedSeq: -1}, nil
}

func (*invalidSequenceMailboxClient) Pub(context.Context, *msgqpb.PubRequest, ...grpc.CallOption) (*msgqpb.PubResponse, error) {
	return &msgqpb.PubResponse{}, nil
}

func (*invalidSequenceMailboxClient) FetchSome(context.Context, *msgqpb.FetchSomeRequest, ...grpc.CallOption) (*msgqpb.FetchSomeResponse, error) {
	return &msgqpb.FetchSomeResponse{Messages: []*msgqpb.FetchResponse{{Seq: 2}, {Seq: 1}}}, nil
}

func TestGRPCConnectionRejectsInvalidResponseSequences(t *testing.T) {
	connection := &grpcConnection{client: &invalidSequenceMailboxClient{}}
	header := SessionHeader{RoleID: 47, SessionID: 1, ConnectionID: 2}
	tests := map[string]func() error{
		"init": func() error {
			_, err := connection.Init(context.Background(), InitRequest{Header: header})
			return err
		},
		"rec": func() error {
			_, err := connection.Rec(context.Background(), RecRequest{Header: header, Seq: 1})
			return err
		},
		"pub": func() error {
			_, err := connection.Pub(context.Background(), PubRequest{Header: header, Payload: []byte("body")})
			return err
		},
		"fetch some": func() error {
			_, err := connection.FetchSome(context.Background(), FetchSomeRequest{Header: header, Max: 2})
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
