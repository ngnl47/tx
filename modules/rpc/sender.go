package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngnl47/tx/internal/instanceconn"
	txmetrics "github.com/ngnl47/tx/metrics"
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
	ErrStreamTellUnsupported   = errors.New("rpc: stream tell is unsupported by connection")
)

// Connection 通过已经解析的 endpoint 发送消息；TellAccepted 等待接收端回执，Call 等待结果。
type Connection interface {
	TellAccepted(context.Context, Delivery) error
	Call(context.Context, Delivery) (Result, error)
	Close() error
}

// StreamCallConnection 在目标连接上复用一条双向流完成独立调用。
type StreamCallConnection interface {
	CallStream(context.Context, Delivery) (Result, error)
}

// StreamTellConnection 将消息写入长流，不等待远端接纳回执或业务结果。
type StreamTellConnection interface {
	TellStream(context.Context, Delivery) error
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
	opening    *streamOpening
	closed     bool
}

// 开流在连接锁外进行；并发调用者只等待同一次开流，各自可以取消等待。
type streamOpening struct {
	done   chan struct{}
	cancel context.CancelFunc
	stream *callStream
	err    error
}

func (c *grpcConnection) TellAccepted(ctx context.Context, delivery Delivery) error {
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
	if c.opening != nil {
		c.opening.cancel()
	}
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
	sendGate    chan struct{}
	mu          sync.Mutex
	sending     context.Context
	sendVersion uint64
	sendChanged chan struct{}
	pending     map[uint64]chan streamCallReply
	nextID      uint64
	sendSamples atomic.Uint64
	closed      bool
	onClose     func(*callStream)
}

func (c *grpcConnection) CallStream(ctx context.Context, delivery Delivery) (Result, error) {
	return c.streamRequest(ctx, delivery)
}

func (c *grpcConnection) TellStream(ctx context.Context, delivery Delivery) error {
	request, err := deliveryToProto(delivery)
	if err != nil {
		return err
	}
	stream, err := c.getCallStream(ctx)
	if err != nil {
		return err
	}
	sample := stream.sendSamples.Add(1)&63 == 0
	var lockStarted time.Time
	if sample {
		lockStarted = time.Now()
	}
	if err := stream.lockSend(ctx); err != nil {
		return err
	}
	var lockWait time.Duration
	var sendDuration time.Duration
	var recorded bool
	defer func() {
		<-stream.sendGate
		if recorded {
			txmetrics.RecordRPCStreamSend(lockWait, sendDuration)
		}
	}()
	if sample {
		lockWait = time.Since(lockStarted)
	}
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
	var sendStarted time.Time
	if sample {
		sendStarted = time.Now()
	}
	err = stream.send(ctx, &rpcpb.StreamCallRequest{Id: id, Delivery: request, OneWay: true})
	if sample {
		sendDuration = time.Since(sendStarted)
		recorded = true
	}
	return err
}

func (c *grpcConnection) streamRequest(ctx context.Context, delivery Delivery) (Result, error) {
	request, err := deliveryToProto(delivery)
	if err != nil {
		return Result{}, err
	}
	stream, err := c.getCallStream(ctx)
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
	if err := stream.lockSend(ctx); err != nil {
		return Result{}, err
	}
	err = stream.send(ctx, &rpcpb.StreamCallRequest{Id: id, Delivery: request})
	<-stream.sendGate
	if err != nil {
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

func (c *grpcConnection) getCallStream(ctx context.Context) (*callStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.streamMu.Lock()
	if c.closed {
		c.streamMu.Unlock()
		return nil, ErrSenderClosed
	}
	if stream := c.stream; stream != nil {
		stream.mu.Lock()
		closed := stream.closed || stream.ctx.Err() != nil
		stream.mu.Unlock()
		if !closed {
			c.streamMu.Unlock()
			return stream, nil
		}
	}
	// 取消已生效时不等待接收循环回调摘除；旧 onClose 按指针比较，不影响新流。
	c.stream = nil
	if opening := c.opening; opening != nil {
		c.streamMu.Unlock()
		select {
		case <-opening.done:
			return opening.stream, opening.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	opening := &streamOpening{done: make(chan struct{}), cancel: cancel}
	c.opening = opening
	c.streamMu.Unlock()

	stop := context.AfterFunc(ctx, cancel)
	client, err := c.client.CallStream(streamCtx)
	if !stop() {
		err = ctx.Err()
	}
	c.streamMu.Lock()
	c.opening = nil
	if c.closed {
		err = ErrSenderClosed
	}
	if err != nil {
		cancel()
		opening.err = err
	} else {
		stream := &callStream{
			client:      client,
			ctx:         streamCtx,
			cancel:      cancel,
			cancelQueue: make(chan uint64, 1024),
			sendGate:    make(chan struct{}, 1),
			sendChanged: make(chan struct{}, 1),
			pending:     make(map[uint64]chan streamCallReply),
		}
		stream.onClose = func(current *callStream) {
			c.streamMu.Lock()
			if c.stream == current {
				c.stream = nil
			}
			c.streamMu.Unlock()
		}
		c.stream = stream
		opening.stream = stream
		go stream.receive()
		go stream.sendCancels()
		go stream.watchSend()
	}
	close(opening.done)
	c.streamMu.Unlock()
	return opening.stream, opening.err
}

// 等待发送权的取消只退出当前等待，不中断正在发送的其他请求。
func (stream *callStream) lockSend(ctx context.Context) error {
	select {
	case stream.sendGate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-stream.sendGate
			return err
		}
		if stream.ctx.Err() == nil {
			return nil
		}
		<-stream.sendGate
	case <-ctx.Done():
		return ctx.Err()
	case <-stream.ctx.Done():
	}
	return status.Error(codes.Unavailable, "rpc stream closed")
}

// 调用者持有发送权。gRPC 的 Send 没有逐项 Context；阻塞时只能取消整条流，
// 并等待 Send 实际返回。取消与完成交错时先使旧流失效，不让取消误伤后续新请求。
func (stream *callStream) send(ctx context.Context, request *rpcpb.StreamCallRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stream.mu.Lock()
	stream.sending = ctx
	stream.sendVersion++
	stream.mu.Unlock()
	select {
	case stream.sendChanged <- struct{}{}:
	default:
	}
	err := stream.client.Send(request)
	stream.mu.Lock()
	stream.sending = nil
	canceled := ctx.Err() != nil && stream.ctx.Err() != nil
	stream.mu.Unlock()
	if canceled {
		stream.fail(ctx.Err())
		return ctx.Err()
	}
	if err != nil {
		stream.fail(err)
	}
	return err
}

// 每条共享流复用一个取消监视，避免在高频发送中逐项分配 AfterFunc 或 goroutine。
// 版本和当前发送者在同一锁内复核，已结束发送的迟到取消不能关闭后续请求的流。
func (stream *callStream) watchSend() {
	for {
		stream.mu.Lock()
		ctx, version := stream.sending, stream.sendVersion
		stream.mu.Unlock()
		var done <-chan struct{}
		if ctx != nil {
			done = ctx.Done()
		}
		select {
		case <-stream.ctx.Done():
			return
		case <-stream.sendChanged:
		case <-done:
			stream.mu.Lock()
			current := stream.sending != nil && stream.sendVersion == version
			if current {
				stream.cancel()
			}
			stream.mu.Unlock()
			if current {
				return
			}
		}
	}
}

func (stream *callStream) sendCancels() {
	for {
		select {
		case id := <-stream.cancelQueue:
			if err := stream.lockSend(stream.ctx); err != nil {
				return
			}
			err := stream.client.Send(&rpcpb.StreamCallRequest{Id: id, Cancel: true})
			<-stream.sendGate
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

// TellAccepted 等待目标实例 Receiver 返回接收回执，不承诺持久化或后续业务完成。
func (s *Sender) TellAccepted(ctx context.Context, delivery Delivery) error {
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
	return connection.TellAccepted(ctx, delivery)
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

// TellStream 将消息写入目标实例的 CallStream，不等待远端接纳回执或业务结果。
func (s *Sender) TellStream(ctx context.Context, delivery Delivery) error {
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
	stream, ok := connection.(StreamTellConnection)
	if !ok {
		return ErrStreamTellUnsupported
	}
	return stream.TellStream(ctx, delivery)
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
		CommandHash: delivery.CommandHash,
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
