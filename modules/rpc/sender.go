package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ngnl47/tx/modules/discovery"
	rpcpb "github.com/ngnl47/tx/modules/rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	ErrDialerNil               = errors.New("rpc: dialer is nil")
	ErrTransportCredentialsNil = errors.New("rpc: transport credentials are nil")
	ErrSenderClosed            = errors.New("rpc: sender is closed")
	ErrDeliveryRejected        = errors.New("rpc: delivery rejected")
)

// Connection 通过已经解析的 endpoint 发送消息。
type Connection interface {
	Deliver(context.Context, Delivery) error
	Close() error
}

// Dialer 建立到指定服务实例的连接。
type Dialer interface {
	Dial(context.Context, discovery.Instance) (Connection, error)
}

// GRPCDialer 使用显式配置的凭据建立 gRPC 连接。
type GRPCDialer struct {
	credentials credentials.TransportCredentials
	options     []grpc.DialOption
}

// NewGRPCDialer 创建 gRPC dialer。调用方必须显式选择 TLS 或不安全凭据。
func NewGRPCDialer(transportCredentials credentials.TransportCredentials, options ...grpc.DialOption) (*GRPCDialer, error) {
	if transportCredentials == nil {
		return nil, ErrTransportCredentialsNil
	}
	return &GRPCDialer{
		credentials: transportCredentials,
		options:     append([]grpc.DialOption(nil), options...),
	}, nil
}

func (d *GRPCDialer) Dial(_ context.Context, instance discovery.Instance) (Connection, error) {
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
		client:     rpcpb.NewDeliveryServiceClient(connection),
	}, nil
}

type grpcConnection struct {
	connection *grpc.ClientConn
	client     rpcpb.DeliveryServiceClient
}

func (c *grpcConnection) Deliver(ctx context.Context, delivery Delivery) error {
	request, err := deliveryToProto(delivery)
	if err != nil {
		return err
	}
	ack, err := c.client.Deliver(ctx, request)
	if err != nil {
		return err
	}
	if ack == nil {
		return ErrDeliveryRejected
	}
	if ack.GetErrorCode() != "" {
		return fmt.Errorf("%w: %s", ErrDeliveryRejected, ack.GetErrorCode())
	}
	if !ack.GetAccepted() {
		return ErrDeliveryRejected
	}
	return nil
}

func (c *grpcConnection) Close() error {
	return c.connection.Close()
}

// Sender 解析路由，并在 Close 前为每个 endpoint 复用同一连接。
type Sender struct {
	directory discovery.Directory
	dialer    Dialer

	mu          sync.Mutex
	connections map[string]Connection
	closed      bool
}

// NewSender 创建支持路由的消息发送器。
func NewSender(directory discovery.Directory, dialer Dialer) (*Sender, error) {
	if directory == nil {
		return nil, ErrDirectoryNil
	}
	if dialer == nil {
		return nil, ErrDialerNil
	}
	return &Sender{
		directory:   directory,
		dialer:      dialer,
		connections: make(map[string]Connection),
	}, nil
}

// Send 解析消息路由并将消息发送到选中的 endpoint。
func (s *Sender) Send(ctx context.Context, delivery Delivery) error {
	if s == nil {
		return ErrSenderClosed
	}
	if err := delivery.Validate(); err != nil {
		return err
	}
	instance, err := Resolve(s.directory, delivery.Route)
	if err != nil {
		return err
	}
	connection, err := s.connection(ctx, instance)
	if err != nil {
		return err
	}
	return connection.Deliver(ctx, delivery)
}

// Close 关闭全部缓存的 endpoint 连接。
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

func deliveryToProto(delivery Delivery) (*rpcpb.Delivery, error) {
	if err := delivery.Validate(); err != nil {
		return nil, err
	}
	route, err := routeToProto(delivery.Route)
	if err != nil {
		return nil, err
	}
	return &rpcpb.Delivery{
		Route:       route,
		MessageType: delivery.MessageType,
		CommandId:   delivery.CommandID,
		Payload:     delivery.Payload,
	}, nil
}

func routeToProto(route Route) (*rpcpb.Route, error) {
	if err := route.Validate(); err != nil {
		return nil, err
	}
	converted := &rpcpb.Route{
		Service:    route.Service,
		InstanceId: route.InstanceID,
	}
	switch route.Kind {
	case RouteService:
		converted.Kind = rpcpb.RouteKind_ROUTE_KIND_SERVICE
	case RouteInstance:
		converted.Kind = rpcpb.RouteKind_ROUTE_KIND_INSTANCE
	default:
		return nil, ErrInvalidRoute
	}
	return converted, nil
}
