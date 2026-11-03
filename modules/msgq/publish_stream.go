package msgq

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	txlog "github.com/ngnl47/tx/log"
	txmetrics "github.com/ngnl47/tx/metrics"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
)

type grpcPublishItem struct {
	request  *msgqpb.PublishRequest
	close    bool
	done     chan error
	queuedAt time.Time
}

type grpcPublishStream struct {
	ctx             context.Context
	cancel          context.CancelFunc
	client          grpc.BidiStreamingClient[msgqpb.PublishRequest, msgqpb.PublishRoleRejection]
	reject          func(context.Context, RoleBinding, []byte)
	items           chan grpcPublishItem
	done            chan struct{}
	closeMu         sync.Mutex
	closed          bool
	historicalClose bool
	once            sync.Once
	closeErr        error
	samples         atomic.Uint64
}

type publishStreamOpening struct {
	done   chan struct{}
	cancel context.CancelFunc
	stream *grpcPublishStream
	err    error
}

func newGRPCPublishStream(ctx context.Context, cancel context.CancelFunc, client msgqpb.MailboxServiceClient, reject func(context.Context, RoleBinding, []byte)) (*grpcPublishStream, error) {
	stream, err := client.PublishStream(ctx)
	if err != nil {
		cancel()
		return nil, mailboxClientError(err)
	}
	publisher := &grpcPublishStream{
		ctx: ctx, cancel: cancel, client: stream, reject: reject,
		items: make(chan grpcPublishItem, 256), done: make(chan struct{}),
	}
	go publisher.writeLoop()
	go publisher.readLoop()
	return publisher, nil
}

func (publisher *grpcPublishStream) send(ctx context.Context, request *msgqpb.PublishRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// 本进程构造的发布消息只复制协议字段，不复制 protobuf 的内部状态。
	queued := &msgqpb.PublishRequest{
		Kind:       request.Kind,
		SessionId:  request.SessionId,
		RoleId:     request.RoleId,
		InstanceId: request.InstanceId,
		Generation: request.Generation,
		Payload:    bytes.Clone(request.Payload),
	}
	item := grpcPublishItem{request: queued}
	if publisher.samples.Add(1)&63 == 0 {
		item.queuedAt = time.Now()
	}
	publisher.closeMu.Lock()
	if publisher.closed {
		publisher.closeMu.Unlock()
		return ErrSenderClosed
	}
	if err := ctx.Err(); err != nil {
		publisher.closeMu.Unlock()
		return err
	}
	select {
	case <-publisher.done:
		publisher.closeMu.Unlock()
		return ErrRouteUnavailable
	default:
	}
	select {
	case <-ctx.Done():
		publisher.closeMu.Unlock()
		return ctx.Err()
	case <-publisher.done:
		publisher.closeMu.Unlock()
		return ErrRouteUnavailable
	case publisher.items <- item:
		publisher.closeMu.Unlock()
		return nil
	}
}

func (publisher *grpcPublishStream) writeLoop() {
	for {
		select {
		case <-publisher.ctx.Done():
			publisher.finish(ErrSenderClosed)
			return
		case item := <-publisher.items:
			if item.close {
				err := publisher.client.CloseSend()
				if err != nil {
					err = mailboxClientError(err)
					publisher.finish(err)
					item.done <- err
					return
				}
				<-publisher.done
				item.done <- publisher.closeErr
				return
			}
			var started time.Time
			if !item.queuedAt.IsZero() {
				started = time.Now()
			}
			err := publisher.client.Send(item.request)
			if !started.IsZero() {
				txmetrics.RecordPublishStream(started.Sub(item.queuedAt), time.Since(started))
			}
			if err != nil {
				err = mailboxClientError(err)
				txmetrics.RecordPublishStreamError()
				txlog.Error("async downlink publish stream send failed", "err", err, "queued", len(publisher.items))
				publisher.finish(err)
				return
			}
		}
	}
}

func (publisher *grpcPublishStream) readLoop() {
	for {
		rejection, err := publisher.client.Recv()
		if errors.Is(err, io.EOF) {
			publisher.finish(nil)
			return
		}
		if err != nil {
			publisher.finish(mailboxClientError(err))
			return
		}
		binding := RoleBinding{
			RoleID: rejection.GetRoleId(), InstanceID: rejection.GetInstanceId(), Generation: rejection.GetGeneration(),
		}
		if binding.Validate() != nil {
			publisher.finish(ErrInvalidMailboxResponse)
			return
		}
		if publisher.reject != nil {
			publisher.reject(publisher.ctx, binding, rejection.GetPayload())
		}
	}
}

func (publisher *grpcPublishStream) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return publisher.closeContext(ctx)
}

func (publisher *grpcPublishStream) closeContext(ctx context.Context) error {
	// 只把进入关闭前已经结束的流归为历史故障，不能吞掉本次排水期间的错误。
	historical := false
	select {
	case <-publisher.done:
		historical = true
	default:
	}
	// 截止时取消底层流，也唤醒持有关闭锁、等待队列空间的发送者。
	stop := context.AfterFunc(ctx, func() { publisher.finish(ctx.Err()) })
	defer stop()

	publisher.closeMu.Lock()
	if publisher.closed {
		historical = publisher.historicalClose
		publisher.closeMu.Unlock()
		<-publisher.done
		if historical {
			return nil
		}
		return publisher.closeErr
	}
	publisher.closed = true
	if historical {
		publisher.historicalClose = true
		publisher.closeMu.Unlock()
		if publisher.closeErr != nil {
			txlog.Error("closing previously failed downlink publish stream", "err", publisher.closeErr)
		}
		return nil
	}
	item := grpcPublishItem{close: true, done: make(chan error, 1)}
	select {
	case publisher.items <- item:
		publisher.closeMu.Unlock()
	case <-publisher.done:
		publisher.closeMu.Unlock()
		return publisher.closeErr
	}
	select {
	case err := <-item.done:
		return err
	case <-publisher.done:
		return publisher.closeErr
	}
}

func (publisher *grpcPublishStream) finish(err error) {
	publisher.once.Do(func() {
		publisher.closeErr = err
		publisher.cancel()
		close(publisher.done)
	})
}

func (c *grpcConnection) publish(ctx context.Context) (*grpcPublishStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.streamMu.Lock()
	if c.closed {
		c.streamMu.Unlock()
		return nil, ErrSenderClosed
	}
	if c.publisher != nil {
		select {
		case <-c.publisher.done:
			c.publisher = nil
		default:
			stream := c.publisher
			c.streamMu.Unlock()
			return stream, nil
		}
	}
	if c.closedCh == nil {
		c.closedCh = make(chan struct{})
	}
	closed := c.closedCh
	opening := c.publisherOpening
	if opening == nil {
		streamCtx, cancel := context.WithCancel(context.Background())
		opening = &publishStreamOpening{done: make(chan struct{}), cancel: cancel}
		c.publisherOpening = opening
		go c.openPublisher(streamCtx, opening)
	}
	c.streamMu.Unlock()
	select {
	case <-opening.done:
		return opening.stream, opening.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-closed:
		return nil, ErrSenderClosed
	}
}

func (c *grpcConnection) openPublisher(ctx context.Context, opening *publishStreamOpening) {
	stream, err := newGRPCPublishStream(ctx, opening.cancel, c.client, c.rejectRole)
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	c.publisherOpening = nil
	if c.closed {
		err = ErrSenderClosed
		if stream != nil {
			stream.finish(err)
			stream = nil
		}
	}
	if err == nil {
		c.publisher = stream
	}
	opening.stream, opening.err = stream, err
	close(opening.done)
}

func (c *grpcConnection) SendSession(ctx context.Context, sessionID int64, payload []byte) error {
	stream, err := c.publish(ctx)
	if err != nil {
		return err
	}
	return stream.send(ctx, &msgqpb.PublishRequest{Kind: msgqpb.PublishRequest_KIND_SESSION, SessionId: sessionID, Payload: payload})
}

func (c *grpcConnection) SendRole(ctx context.Context, binding RoleBinding, payload []byte) error {
	stream, err := c.publish(ctx)
	if err != nil {
		return err
	}
	return stream.send(ctx, &msgqpb.PublishRequest{
		Kind: msgqpb.PublishRequest_KIND_ROLE, RoleId: binding.RoleID,
		InstanceId: binding.InstanceID, Generation: binding.Generation, Payload: payload,
	})
}

func (c *grpcConnection) closePublisher() error {
	c.streamMu.Lock()
	publisher := c.publisher
	c.publisher = nil
	c.streamMu.Unlock()
	if publisher == nil {
		return nil
	}
	return publisher.Close()
}
