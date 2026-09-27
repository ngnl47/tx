package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ngnl47/tx/internal/instanceconn"
	"github.com/ngnl47/tx/modules/discovery"
	rpcpb "github.com/ngnl47/tx/modules/rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var (
	ErrDialerNil               = errors.New("rpc: dialer is nil")
	ErrTransportCredentialsNil = errors.New("rpc: transport credentials are nil")
	ErrSenderClosed            = errors.New("rpc: sender is closed")
	ErrDeliveryRejected        = errors.New("rpc: delivery rejected")
	ErrStreamCallUnsupported   = errors.New("rpc: stream call is unsupported by connection")
	ErrStreamSendUnsupported   = errors.New("rpc: stream send is unsupported by connection")
)

// Connection 通过已经解析的 endpoint 发送消息。
type Connection interface {
	Deliver(context.Context, Delivery) error
	Call(context.Context, Delivery) (Result, error)
	Close() error
}

// StreamCallConnection 在目标连接上复用一条双向流完成独立调用。
type StreamCallConnection interface {
	CallStream(context.Context, Delivery) (Result, error)
}

// StreamSendConnection 将消息写入长流，不等待远端逐项响应。
type StreamSendConnection interface {
	SendStream(context.Context, Delivery) error
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
	streamMu   sync.Mutex
	stream     *callStream
	closed     bool
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

func (c *grpcConnection) Call(ctx context.Context, delivery Delivery) (Result, error) {
	request, err := deliveryToProto(delivery)
	if err != nil {
		return Result{}, err
	}
	response, err := c.client.Call(ctx, request)
	if err != nil {
		return Result{}, err
	}
	return resultFromProto(response)
}

func (c *grpcConnection) Close() error {
	c.streamMu.Lock()
	c.closed = true
	stream := c.stream
	c.stream = nil
	c.streamMu.Unlock()
	if stream != nil {
		stream.fail(ErrSenderClosed)
	}
	return c.connection.Close()
}

type streamCallReply struct {
	result Result
	err    error
}

type callStream struct {
	client      rpcpb.DeliveryService_CallStreamClient
	ctx         context.Context
	cancel      context.CancelFunc
	cancelQueue chan uint64
	sendMu      sync.Mutex
	mu          sync.Mutex
	pending     map[uint64]chan streamCallReply
	nextID      uint64
	closed      bool
	onClose     func(*callStream)
}

func (c *grpcConnection) CallStream(ctx context.Context, delivery Delivery) (Result, error) {
	return c.streamRequest(ctx, delivery)
}

func (c *grpcConnection) SendStream(ctx context.Context, delivery Delivery) error {
	request, err := deliveryToProto(delivery)
	if err != nil {
		return err
	}
	stream, err := c.getCallStream()
	if err != nil {
		return err
	}
	stream.sendMu.Lock()
	defer stream.sendMu.Unlock()
	stream.mu.Lock()
	if stream.closed {
		stream.mu.Unlock()
		return status.Error(codes.Unavailable, "rpc stream closed")
	}
	stream.nextID++
	id := stream.nextID
	stream.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := stream.client.Send(&rpcpb.StreamCallRequest{Id: id, Delivery: request, OneWay: true}); err != nil {
		stream.fail(err)
		return err
	}
	return nil
}

func (c *grpcConnection) streamRequest(ctx context.Context, delivery Delivery) (Result, error) {
	request, err := deliveryToProto(delivery)
	if err != nil {
		return Result{}, err
	}
	stream, err := c.getCallStream()
	if err != nil {
		return Result{}, err
	}
	reply := make(chan streamCallReply, 1)
	stream.mu.Lock()
	if stream.closed {
		stream.mu.Unlock()
		return Result{}, status.Error(codes.Unavailable, "rpc stream closed")
	}
	stream.nextID++
	id := stream.nextID
	stream.pending[id] = reply
	stream.mu.Unlock()
	defer func() {
		stream.mu.Lock()
		delete(stream.pending, id)
		stream.mu.Unlock()
	}()
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	default:
	}
	stream.sendMu.Lock()
	if err = ctx.Err(); err == nil {
		err = stream.client.Send(&rpcpb.StreamCallRequest{Id: id, Delivery: request})
	}
	stream.sendMu.Unlock()
	if err != nil {
		if ctx.Err() == nil {
			stream.fail(err)
		}
		return Result{}, err
	}
	select {
	case outcome := <-reply:
		return outcome.result, outcome.err
	case <-ctx.Done():
		select {
		case stream.cancelQueue <- id:
		default:
			stream.fail(status.Error(codes.ResourceExhausted, "stream cancellation queue full"))
		}
		return Result{}, ctx.Err()
	}
}

func (c *grpcConnection) getCallStream() (*callStream, error) {
	c.streamMu.Lock()
	if c.closed {
		c.streamMu.Unlock()
		return nil, ErrSenderClosed
	}
	stream := c.stream
	if stream == nil {
		streamCtx, cancel := context.WithCancel(context.Background())
		client, openErr := c.client.CallStream(streamCtx)
		if openErr != nil {
			cancel()
			c.streamMu.Unlock()
			return nil, openErr
		}
		stream = &callStream{client: client, ctx: streamCtx, cancel: cancel, cancelQueue: make(chan uint64, 1024), pending: make(map[uint64]chan streamCallReply)}
		stream.onClose = func(current *callStream) {
			c.streamMu.Lock()
			if c.stream == current {
				c.stream = nil
			}
			c.streamMu.Unlock()
		}
		c.stream = stream
		go stream.receive()
		go stream.sendCancels()
	}
	c.streamMu.Unlock()
	return stream, nil
}

func (stream *callStream) sendCancels() {
	for {
		select {
		case id := <-stream.cancelQueue:
			stream.sendMu.Lock()
			err := stream.client.Send(&rpcpb.StreamCallRequest{Id: id, Cancel: true})
			stream.sendMu.Unlock()
			if err != nil {
				stream.fail(err)
				return
			}
		case <-stream.ctx.Done():
			return
		}
	}
}

func (stream *callStream) receive() {
	for {
		response, err := stream.client.Recv()
		if err != nil {
			stream.fail(err)
			return
		}
		stream.mu.Lock()
		reply := stream.pending[response.GetId()]
		delete(stream.pending, response.GetId())
		stream.mu.Unlock()
		if reply == nil {
			continue // 调用方已超时；远端是否执行过保持未知。
		}
		if response.GetErrorCode() != 0 {
			reply <- streamCallReply{err: status.Error(codes.Code(response.GetErrorCode()), response.GetErrorMessage())}
			continue
		}
		result, err := resultFromProto(response.GetResult())
		reply <- streamCallReply{result: result, err: err}
	}
}

func (stream *callStream) fail(err error) {
	stream.mu.Lock()
	if stream.closed {
		stream.mu.Unlock()
		return
	}
	stream.closed = true
	pending := stream.pending
	stream.pending = nil
	stream.mu.Unlock()
	stream.cancel()
	stream.onClose(stream)
	for _, reply := range pending {
		reply <- streamCallReply{err: err}
	}
}

// Sender 解析路由，并为每个实例身份复用连接。
type Sender struct {
	directory   discovery.Directory
	dialer      Dialer
	connections *instanceconn.Cache[Connection]
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
		connections: instanceconn.New[Connection](directory),
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

// Call 解析消息路由并将消息发送到选中的 endpoint，等待远端返回同步结果。
func (s *Sender) Call(ctx context.Context, delivery Delivery) (Result, error) {
	if s == nil {
		return Result{}, ErrSenderClosed
	}
	if err := delivery.Validate(); err != nil {
		return Result{}, err
	}
	instance, err := Resolve(s.directory, delivery.Route)
	if err != nil {
		return Result{}, err
	}
	connection, err := s.connection(ctx, instance)
	if err != nil {
		return Result{}, err
	}
	return connection.Call(ctx, delivery)
}

// CallStream 在目标实例的长流上发送单项调用，不在断流后重投结果未知的请求。
func (s *Sender) CallStream(ctx context.Context, delivery Delivery) (Result, error) {
	if s == nil {
		return Result{}, ErrSenderClosed
	}
	if err := delivery.Validate(); err != nil {
		return Result{}, err
	}
	instance, err := Resolve(s.directory, delivery.Route)
	if err != nil {
		return Result{}, err
	}
	connection, err := s.connection(ctx, instance)
	if err != nil {
		return Result{}, err
	}
	stream, ok := connection.(StreamCallConnection)
	if !ok {
		return Result{}, ErrStreamCallUnsupported
	}
	return stream.CallStream(ctx, delivery)
}

// SendStream 将消息写入目标实例的 CallStream，不等待远端逐项响应。
func (s *Sender) SendStream(ctx context.Context, delivery Delivery) error {
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
	stream, ok := connection.(StreamSendConnection)
	if !ok {
		return ErrStreamSendUnsupported
	}
	return stream.SendStream(ctx, delivery)
}

// Close 关闭全部缓存的实例连接。
func (s *Sender) Close() error {
	if s == nil {
		return nil
	}
	return s.connections.Close()
}

func (s *Sender) connection(ctx context.Context, instance discovery.Instance) (Connection, error) {
	connection, err := s.connections.Get(ctx, instance, s.dialer.Dial)
	if errors.Is(err, instanceconn.ErrClosed) {
		return nil, ErrSenderClosed
	}
	if errors.Is(err, instanceconn.ErrStaleInstance) {
		return nil, ErrRouteUnavailable
	}
	return connection, err
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
