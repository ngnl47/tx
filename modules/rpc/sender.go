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

// Connection sends deliveries through one resolved endpoint.
type Connection interface {
	Deliver(context.Context, Delivery) error
	Close() error
}

// Dialer opens a connection to one discovered instance.
type Dialer interface {
	Dial(context.Context, discovery.Instance) (Connection, error)
}

// GRPCDialer opens gRPC connections with explicitly configured credentials.
type GRPCDialer struct {
	credentials credentials.TransportCredentials
	options     []grpc.DialOption
}

// NewGRPCDialer creates a gRPC dialer. Callers must explicitly select TLS or insecure credentials.
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
	if ack == nil || !ack.GetAccepted() {
		if ack == nil || ack.GetErrorCode() == "" {
			return ErrDeliveryRejected
		}
		return fmt.Errorf("%w: %s", ErrDeliveryRejected, ack.GetErrorCode())
	}
	return nil
}

func (c *grpcConnection) Close() error {
	return c.connection.Close()
}

// Sender resolves routes and reuses one connection for each endpoint until Close.
type Sender struct {
	directory *discovery.ServiceDirectory
	dialer    Dialer

	mu          sync.Mutex
	connections map[string]Connection
	closed      bool
}

// NewSender creates a route-aware delivery sender.
func NewSender(directory *discovery.ServiceDirectory, dialer Dialer) (*Sender, error) {
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

// Send resolves a delivery route and sends it to the selected endpoint.
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

// Close closes every cached endpoint connection.
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
		ActorKind:  route.ActorKind,
		Identity:   route.Identity,
		RoleId:     route.RoleID,
		SessionId:  route.SessionID,
	}
	switch route.Kind {
	case RouteService:
		converted.Kind = rpcpb.RouteKind_ROUTE_KIND_SERVICE
	case RouteInstance:
		converted.Kind = rpcpb.RouteKind_ROUTE_KIND_INSTANCE
	case RouteSession:
		converted.Kind = rpcpb.RouteKind_ROUTE_KIND_SESSION
	case RouteActor:
		converted.Kind = rpcpb.RouteKind_ROUTE_KIND_ACTOR
	default:
		return nil, ErrInvalidRoute
	}
	return converted, nil
}
