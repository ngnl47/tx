package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

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
	ErrBatchCallUnsupported    = errors.New("rpc: batch call is unsupported by connection")
	ErrStreamCallUnsupported   = errors.New("rpc: stream call is unsupported by connection")
)

// Connection 通过已经解析的 endpoint 发送消息。
type Connection interface {
	Deliver(context.Context, Delivery) error
	Call(context.Context, Delivery) (Result, error)
	Close() error
}

// BatchCallConnection 是支持逐项流式结果的连接能力。
type BatchCallConnection interface {
	CallBatch(context.Context, Delivery, func(Result) error) error
}

// StreamCallConnection 在目标连接上复用一条双向流完成独立调用。
type StreamCallConnection interface {
	CallStream(context.Context, Delivery) (Result, error)
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

func (c *grpcConnection) CallBatch(ctx context.Context, delivery Delivery, accept func(Result) error) error {
	request, err := deliveryToProto(delivery)
	if err != nil {
		return err
	}
	stream, err := c.client.CallBatch(ctx, request)
	if err != nil {
		return err
	}
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		result, err := resultFromProto(response)
		if err != nil {
			return err
		}
		if err := accept(result); err != nil {
			return err
		}
	}
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
	request, err := deliveryToProto(delivery)
	if err != nil {
		return Result{}, err
	}
	c.streamMu.Lock()
	if c.closed {
		c.streamMu.Unlock()
		return Result{}, ErrSenderClosed
	}
	stream := c.stream
	if stream == nil {
		streamCtx, cancel := context.WithCancel(context.Background())
		client, openErr := c.client.CallStream(streamCtx)
		if openErr != nil {
			cancel()
			c.streamMu.Unlock()
			return Result{}, openErr
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

// CallBatch 复用目标实例连接发送一个批次，并逐项交付已经完成的结果。
func (s *Sender) CallBatch(ctx context.Context, delivery Delivery, accept func(Result) error) error {
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
	batch, ok := connection.(BatchCallConnection)
	if !ok {
		return ErrBatchCallUnsupported
	}
	return batch.CallBatch(ctx, delivery, accept)
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
