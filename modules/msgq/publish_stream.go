package msgq

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
)

type grpcPublishItem struct {
	request *msgqpb.PublishRequest
	close   bool
	done    chan error
}

type grpcPublishStream struct {
	ctx      context.Context
	cancel   context.CancelFunc
	client   grpc.BidiStreamingClient[msgqpb.PublishRequest, msgqpb.PublishRoleRejection]
	reject   func(RoleBinding)
	items    chan grpcPublishItem
	done     chan struct{}
	closeMu  sync.Mutex
	closed   bool
	once     sync.Once
	closeErr error
}

func newGRPCPublishStream(client msgqpb.MailboxServiceClient, reject func(RoleBinding)) (*grpcPublishStream, error) {
	ctx, cancel := context.WithCancel(context.Background())
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
	item := grpcPublishItem{request: request, done: make(chan error, 1)}
	publisher.closeMu.Lock()
	if publisher.closed {
		publisher.closeMu.Unlock()
		return ErrSenderClosed
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
	}
	select {
	case err := <-item.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-publisher.done:
		select {
		case err := <-item.done:
			return err
		default:
			return ErrRouteUnavailable
		}
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
			err := publisher.client.Send(item.request)
			if err != nil {
				err = mailboxClientError(err)
			}
			item.done <- err
			if err != nil {
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
			publisher.reject(binding)
		}
	}
}

func (publisher *grpcPublishStream) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return publisher.closeContext(ctx)
}

func (publisher *grpcPublishStream) closeContext(ctx context.Context) error {
	// 截止时取消底层流，也唤醒持有关闭锁、等待队列空间的发送者。
	stop := context.AfterFunc(ctx, func() { publisher.finish(ctx.Err()) })
	defer stop()

	publisher.closeMu.Lock()
	if publisher.closed {
		publisher.closeMu.Unlock()
		<-publisher.done
		return publisher.closeErr
	}
	publisher.closed = true
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

func (c *grpcConnection) publish() (*grpcPublishStream, error) {
	c.publisherMu.Lock()
	defer c.publisherMu.Unlock()
	if c.publisher != nil {
		select {
		case <-c.publisher.done:
			c.publisher = nil
		default:
			return c.publisher, nil
		}
	}
	stream, err := newGRPCPublishStream(c.client, c.rejectRole)
	if err != nil {
		return nil, err
	}
	c.publisher = stream
	return stream, nil
}

func (c *grpcConnection) SendSession(ctx context.Context, sessionID int64, payload []byte) error {
	stream, err := c.publish()
	if err != nil {
		return err
	}
	return stream.send(ctx, &msgqpb.PublishRequest{Kind: msgqpb.PublishRequest_KIND_SESSION, SessionId: sessionID, Payload: payload})
}

func (c *grpcConnection) SendRole(ctx context.Context, binding RoleBinding, payload []byte) error {
	stream, err := c.publish()
	if err != nil {
		return err
	}
	return stream.send(ctx, &msgqpb.PublishRequest{
		Kind: msgqpb.PublishRequest_KIND_ROLE, RoleId: binding.RoleID,
		InstanceId: binding.InstanceID, Generation: binding.Generation, Payload: payload,
	})
}

func (c *grpcConnection) closePublisher() error {
	c.publisherMu.Lock()
	publisher := c.publisher
	c.publisher = nil
	c.publisherMu.Unlock()
	if publisher == nil {
		return nil
	}
	return publisher.Close()
}
