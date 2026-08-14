package msgq

import (
	"context"
	"errors"
	"sync"

	"github.com/ngnl47/tx/modules/discovery"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	ErrDialerNil               = errors.New("msgq: dialer is nil")
	ErrSessionRouterNil        = errors.New("msgq: session router is nil")
	ErrTransportCredentialsNil = errors.New("msgq: transport credentials are nil")
	ErrSenderClosed            = errors.New("msgq: sender is closed")
)

// Connection 向一个已解析的 msgq 实例投递下行消息。
type Connection interface {
	Pub(context.Context, PubRequest) (PubResult, error)
	Close() error
}

// Dialer 建立到一个 msgq 实例的连接。
type Dialer interface {
	Dial(context.Context, discovery.Instance) (Connection, error)
}

// GRPCDialer 通过显式配置的凭证建立 msgq gRPC 连接。
type GRPCDialer struct {
	credentials credentials.TransportCredentials
	options     []grpc.DialOption
}

// NewGRPCDialer 创建 msgq gRPC dialer。调用方必须显式选择 TLS 或 insecure 凭证。
func NewGRPCDialer(transportCredentials credentials.TransportCredentials, options ...grpc.DialOption) (*GRPCDialer, error) {
	if transportCredentials == nil {
		return nil, ErrTransportCredentialsNil
	}
	return &GRPCDialer{
		credentials: transportCredentials,
		options:     append([]grpc.DialOption(nil), options...),
	}, nil
}

// Dial 打开到实例 endpoint 的 gRPC 连接。
func (d *GRPCDialer) Dial(_ context.Context, instance discovery.Instance) (Connection, error) {
	return d.dial(instance)
}

// DialSession 打开到实例 endpoint 的会话 gRPC 客户端。
func (d *GRPCDialer) DialSession(_ context.Context, instance discovery.Instance) (SessionClient, error) {
	return d.dial(instance)
}

func (d *GRPCDialer) dial(instance discovery.Instance) (*grpcConnection, error) {
	if d == nil || d.credentials == nil {
		return nil, ErrTransportCredentialsNil
	}
	options := make([]grpc.DialOption, 0, len(d.options)+1)
	options = append(options, grpc.WithTransportCredentials(d.credentials))
	options = append(options, d.options...)
	connection, err := grpc.NewClient("passthrough:///"+instance.Endpoint, options...)
	if err != nil {
		return nil, err
	}
	return &grpcConnection{
		connection: connection,
		client:     msgqpb.NewMailboxServiceClient(connection),
	}, nil
}

type grpcConnection struct {
	connection *grpc.ClientConn
	client     msgqpb.MailboxServiceClient
}

func (c *grpcConnection) Pub(ctx context.Context, request PubRequest) (PubResult, error) {
	response, err := c.client.Pub(ctx, &msgqpb.PubRequest{
		Header: &msgqpb.SessionHeader{
			RoleId:       request.Header.RoleID,
			SessionId:    request.Header.SessionID,
			ConnectionId: request.Header.ConnectionID,
		},
		Message: request.Message,
	})
	if err != nil {
		return PubResult{}, mailboxClientError(err)
	}
	return PubResult{Seq: response.GetSeq()}, nil
}

func (c *grpcConnection) Init(ctx context.Context, request InitRequest) (InitResult, error) {
	response, err := c.client.Init(ctx, &msgqpb.InitRequest{Header: headerToProto(request.Header), Restore: request.Restore, Ack: request.Ack})
	if err != nil {
		return InitResult{}, mailboxClientError(err)
	}
	return InitResult{ReceivedSeq: response.GetReceivedSeq()}, nil
}

func (c *grpcConnection) Rec(ctx context.Context, request RecRequest) (RecResult, error) {
	response, err := c.client.Rec(ctx, &msgqpb.RecRequest{Header: headerToProto(request.Header), Seq: request.Seq, Ack: request.Ack})
	if err != nil {
		return RecResult{}, mailboxClientError(err)
	}
	return RecResult{ReceivedSeq: response.GetReceivedSeq(), Ack: response.GetAck()}, nil
}

func (c *grpcConnection) FetchSome(ctx context.Context, request FetchSomeRequest) (FetchSomeResult, error) {
	response, err := c.client.FetchSome(ctx, &msgqpb.FetchSomeRequest{Header: headerToProto(request.Header), Max: int32(request.Max)})
	if err != nil {
		return FetchSomeResult{}, mailboxClientError(err)
	}
	result := FetchSomeResult{Messages: make([]FetchResult, len(response.GetMessages()))}
	for index, message := range response.GetMessages() {
		result.Messages[index] = FetchResult{Seq: message.GetSeq(), Message: message.GetMessage()}
	}
	return result, nil
}

func (c *grpcConnection) Confirm(ctx context.Context, request ConfirmRequest) error {
	_, err := c.client.Confirm(ctx, &msgqpb.ConfirmRequest{Header: headerToProto(request.Header), Ack: request.Ack})
	return mailboxClientError(err)
}

func (c *grpcConnection) CloseSession(ctx context.Context, header SessionHeader) error {
	_, err := c.client.Close(ctx, &msgqpb.CloseRequest{Header: headerToProto(header)})
	return mailboxClientError(err)
}

func (c *grpcConnection) Close() error {
	return c.connection.Close()
}

func headerToProto(header SessionHeader) *msgqpb.SessionHeader {
	return &msgqpb.SessionHeader{
		RoleId:       header.RoleID,
		SessionId:    header.SessionID,
		ConnectionId: header.ConnectionID,
	}
}

// Sender 按当前会话 owner 路由下行消息，并复用每个 endpoint 的连接直到 Close。
type Sender struct {
	router *SessionRouter
	dialer Dialer

	mu          sync.Mutex
	connections map[string]Connection
	closed      bool
}

// NewSender 创建按 roleID 路由的 msgq 发送器。
func NewSender(router *SessionRouter, dialer Dialer) (*Sender, error) {
	if router == nil {
		return nil, ErrSessionRouterNil
	}
	if dialer == nil {
		return nil, ErrDialerNil
	}
	return &Sender{
		router:      router,
		dialer:      dialer,
		connections: make(map[string]Connection),
	}, nil
}

// Pub 向 roleID 当前会话投递已编码的 BON payload。
func (s *Sender) Pub(ctx context.Context, roleID int64, message []byte) (PubResult, error) {
	if s == nil {
		return PubResult{}, ErrSenderClosed
	}
	target, err := s.router.Resolve(ctx, roleID, 0)
	if err != nil {
		return PubResult{}, err
	}
	connection, err := s.connection(ctx, target.Instance)
	if err != nil {
		return PubResult{}, err
	}
	return connection.Pub(ctx, PubRequest{
		Header: SessionHeader{
			RoleID:    target.Binding.RoleID,
			SessionID: target.Binding.SessionID,
		},
		Message: message,
	})
}

// Close 关闭所有缓存 endpoint 的连接。
func (s *Sender) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	connections := s.connections
	s.connections = nil
	s.mu.Unlock()

	var errs []error
	for _, connection := range connections {
		errs = append(errs, connection.Close())
	}
	return errors.Join(errs...)
}

func (s *Sender) connection(ctx context.Context, instance discovery.Instance) (Connection, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrSenderClosed
	}
	if connection := s.connections[instance.Endpoint]; connection != nil {
		s.mu.Unlock()
		return connection, nil
	}
	s.mu.Unlock()

	created, err := s.dialer.Dial(ctx, instance)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = created.Close()
		return nil, ErrSenderClosed
	}
	if connection := s.connections[instance.Endpoint]; connection != nil {
		_ = created.Close()
		return connection, nil
	}
	s.connections[instance.Endpoint] = created
	return created, nil
}
