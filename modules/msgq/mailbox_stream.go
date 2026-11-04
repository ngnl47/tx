package msgq

import (
	"context"
	"errors"
	"sync"
	"time"

	txmetrics "github.com/ngnl47/tx/metrics"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MailboxStreamSession 是共享 MailboxStream 上的一条已确认接入。
// 普通下行与 Ack/Renew 保持单向传输，接入、解除接入和逻辑退出单独确认。
type MailboxStreamSession interface {
	// Context 表示本次接入的使用期，Cause 保留取消原因；普通关闭、撤销、断流或
	// 下行消费失败均可取消，不表示逻辑邮箱已销毁。共享流失败会影响其全部接入。
	Context() context.Context
	// Recv 等待一条下行或接入结束；由单一消费方按顺序读取，不代表客户端已收到。
	Recv() (FetchResult, error)
	// Ack 上报客户端累计确认的下行序号。远端成功只表示提交本地流队列，
	// 不等待 MsgQ 裁剪回执；同进程实现直接执行裁剪。
	Ack(context.Context, int64) error
	// Renew 续期。远端成功不等待处理回执，同进程实现直接执行。
	Renew(context.Context) error
	// Close 解除本次连接绑定并保留逻辑邮箱；调用前须停止本地上行。
	// 使用独立有限期限等待解除接入确认，失败可保留句柄重试；不关闭共享连接。
	Close() error
	// Terminate 等待当前逻辑会话退出，成功后不再用于重连；调用前须停止本地上行。
	// ctx 取消或超时不证明远端未执行，不得作为退出成功。
	Terminate(context.Context) error
}

type grpcMailboxStream struct {
	ctx      context.Context
	cancel   context.CancelFunc
	client   grpc.BidiStreamingClient[msgqpb.MailboxStreamRequest, msgqpb.MailboxStreamResponse]
	requests chan *msgqpb.MailboxStreamRequest
	reopen   func(context.Context) (*grpcMailboxStream, error)

	// 按完整接入身份分派；同一 Session 重连时，旧连接仍可能等待关闭回执。
	mu       sync.Mutex
	sessions map[SessionHeader]*grpcMailboxSession

	once sync.Once
	done chan struct{}

	// 仅由 readLoop 更新，不与会话目录共用锁。
	receiveSamples uint64
}

type mailboxStreamOpening struct {
	done   chan struct{}
	cancel context.CancelFunc
	stream *grpcMailboxStream
	err    error
}

type mailboxResponse struct {
	response *msgqpb.MailboxStreamResponse
	queuedAt time.Time
}

type grpcMailboxSession struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	stream   *grpcMailboxStream
	header   SessionHeader
	onRevoke func()

	// 接入与关闭确认独立于下行队列；错误结束后仍可重试精确控制。
	opened     chan error
	closed     chan error
	terminated chan error
	items      chan mailboxResponse

	controlMu     sync.Mutex
	controlStream *grpcMailboxStream
	detached      bool

	// 回调和本地结束各执行一次，不持有目录锁调用外部逻辑。
	revokeOnce sync.Once
	once       sync.Once
	done       chan struct{}

	mu  sync.Mutex
	err error
}

func newGRPCMailboxStream(streamCtx context.Context, cancel context.CancelFunc, client msgqpb.MailboxServiceClient) (*grpcMailboxStream, error) {
	clientStream, err := client.MailboxStream(streamCtx)
	if err != nil {
		cancel()
		return nil, mailboxClientError(err)
	}
	stream := &grpcMailboxStream{
		ctx: streamCtx, cancel: cancel, client: clientStream,
		requests: make(chan *msgqpb.MailboxStreamRequest, 256), done: make(chan struct{}),
		sessions: make(map[SessionHeader]*grpcMailboxSession),
	}
	go stream.writeLoop()
	go stream.readLoop()
	return stream, nil
}

func (c *grpcConnection) getMailboxStream(ctx context.Context) (*grpcMailboxStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.streamMu.Lock()
	if c.closed {
		c.streamMu.Unlock()
		return nil, ErrSenderClosed
	}
	if c.mailbox != nil {
		select {
		case <-c.mailbox.done:
			c.mailbox = nil
		default:
			stream := c.mailbox
			c.streamMu.Unlock()
			return stream, nil
		}
	}
	if c.closedCh == nil {
		c.closedCh = make(chan struct{})
	}
	closed := c.closedCh
	opening := c.mailboxOpening
	if opening == nil {
		streamCtx, cancel := context.WithCancel(context.Background())
		opening = &mailboxStreamOpening{done: make(chan struct{}), cancel: cancel}
		c.mailboxOpening = opening
		go c.openMailboxStream(streamCtx, opening)
	}
	c.streamMu.Unlock()
	// 建流属于连接生命周期，任一调用取消都只结束自己的等待。
	select {
	case <-opening.done:
		return opening.stream, opening.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-closed:
		return nil, ErrSenderClosed
	}
}

func (c *grpcConnection) openMailboxStream(ctx context.Context, opening *mailboxStreamOpening) {
	stream, err := newGRPCMailboxStream(ctx, opening.cancel, c.client)
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	c.mailboxOpening = nil
	if c.closed {
		err = ErrSenderClosed
		if stream != nil {
			stream.fail(err)
			stream = nil
		}
	}
	if err == nil {
		stream.reopen = c.getMailboxStream
		c.mailbox = stream
	}
	opening.stream, opening.err = stream, err
	close(opening.done)
}

func (stream *grpcMailboxStream) open(ctx context.Context, request InitRequest, onRevoke func()) (MailboxStreamSession, error) {
	if a := request.Admission; a != nil && a.Policy != RejectExisting && a.Policy != ReplaceExisting {
		return nil, ErrInvalidSession
	}
	header := request.Header
	sessionCtx, cancel := context.WithCancelCause(ctx)
	session := &grpcMailboxSession{ctx: sessionCtx, cancel: cancel, stream: stream, header: header, onRevoke: onRevoke, items: make(chan mailboxResponse, 64), done: make(chan struct{}), opened: make(chan error, 1), closed: make(chan error, 1), terminated: make(chan error, 1)}
	stream.mu.Lock()
	if err := stream.ctx.Err(); err != nil {
		stream.mu.Unlock()
		cancel(err)
		return nil, err
	}
	if _, exists := stream.sessions[header]; exists {
		stream.mu.Unlock()
		cancel(ErrSessionChanged)
		return nil, ErrSessionChanged
	}
	stream.sessions[header] = session
	stream.mu.Unlock()
	err := stream.send(ctx, &msgqpb.MailboxStreamRequest{Header: headerToProto(header), Kind: msgqpb.MailboxStreamRequest_KIND_OPEN, Max: 1, Init: initToProto(request)})
	if err == nil {
		select {
		case err = <-session.opened:
		case <-ctx.Done():
			err = ctx.Err()
		case <-stream.done:
			err = ErrRouteUnavailable
		}
	}
	if err != nil {
		// Open 返回成功前调用方尚未开放上行，直接撤销候选，不抢先关闭用于返回错误的 socket。
		session.finish(err)
		return nil, errors.Join(err, session.Close())
	}
	return session, nil
}

func (stream *grpcMailboxStream) send(ctx context.Context, request *msgqpb.MailboxStreamRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := stream.ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stream.done:
		return errors.New("msgq: mailbox stream is closed")
	case stream.requests <- request:
		return stream.ctx.Err()
	}
}

func (stream *grpcMailboxStream) writeLoop() {
	for {
		select {
		case <-stream.ctx.Done():
			return
		case item := <-stream.requests:
			if err := stream.client.Send(item); err != nil {
				stream.fail(mailboxClientError(err))
				return
			}
		}
	}
}

func (stream *grpcMailboxStream) readLoop() {
	for {
		response, err := stream.client.Recv()
		if err != nil {
			stream.fail(mailboxClientError(err))
			return
		}
		header := headerFromProto(response.GetHeader())
		stream.mu.Lock()
		session := stream.sessions[header]
		stream.mu.Unlock()
		if session == nil {
			continue
		}
		var responseErr error
		if response.GetErrorMessage() != "" {
			responseErr = mailboxClientError(status.Error(codes.Code(response.GetErrorCode()), response.GetErrorMessage()))
		}
		switch response.GetKind() {
		case msgqpb.MailboxStreamResponse_KIND_OPENED:
			select {
			case session.opened <- responseErr:
			default:
			}
			continue
		case msgqpb.MailboxStreamResponse_KIND_CLOSED:
			select {
			case session.closed <- responseErr:
			default:
			}
			continue
		case msgqpb.MailboxStreamResponse_KIND_TERMINATED:
			select {
			case session.terminated <- responseErr:
			default:
			}
			continue
		case msgqpb.MailboxStreamResponse_KIND_REVOKE:
			go func() {
				session.closeIngress()
				session.finish(ErrSessionChanged)
				controlCtx, cancel := context.WithTimeout(context.Background(), mailboxControlTimeout)
				defer cancel()
				_ = stream.send(controlCtx, &msgqpb.MailboxStreamRequest{Header: headerToProto(header), Kind: msgqpb.MailboxStreamRequest_KIND_DETACHED})
			}()
			continue
		}
		if responseErr != nil {
			session.finish(responseErr)
			continue
		}
		if response.GetMessage() == nil || response.GetMessage().GetSeq() <= 0 {
			session.finish(ErrInvalidMailboxResponse)
			continue
		}
		item := mailboxResponse{response: response}
		stream.receiveSamples++
		if stream.receiveSamples&63 == 0 {
			item.queuedAt = time.Now()
		}
		select {
		case session.items <- item:
		case <-session.done:
		case <-stream.done:
			return
		default:
			// 单个慢会话退出后按 Ack 恢复，不阻塞其他会话，也不跳过缺失下行。
			session.finish(ErrMailboxBackpressure)
		}
	}
}

func (stream *grpcMailboxStream) fail(err error) {
	stream.once.Do(func() {
		stream.mu.Lock()
		stream.cancel()
		close(stream.done)
		sessions := stream.sessions
		stream.sessions = make(map[SessionHeader]*grpcMailboxSession)
		stream.mu.Unlock()
		for _, session := range sessions {
			session.finish(err)
		}
	})
}

// ErrMailboxBackpressure 表示单个会话未及时消费本地有界下行队列。
var ErrMailboxBackpressure = errors.New("msgq: mailbox stream session is too slow")

func (session *grpcMailboxSession) Context() context.Context { return session.ctx }

func (session *grpcMailboxSession) receiveError() error {
	if err := context.Cause(session.ctx); err != nil {
		return err
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.err
}

func (session *grpcMailboxSession) Recv() (FetchResult, error) {
	if err := session.receiveError(); err != nil {
		return FetchResult{}, err
	}
	select {
	case item := <-session.items:
		if err := session.receiveError(); err != nil {
			return FetchResult{}, err
		}
		if !item.queuedAt.IsZero() {
			txmetrics.RecordMailboxReceiveQueue(time.Since(item.queuedAt))
		}
		message := item.response.GetMessage()
		return FetchResult{Seq: message.GetSeq(), Payload: message.GetPayload()}, nil
	case <-session.ctx.Done():
		return FetchResult{}, context.Cause(session.ctx)
	case <-session.done:
		return FetchResult{}, session.receiveError()
	}
}

func (session *grpcMailboxSession) TryRecv() (FetchResult, bool, error) {
	if err := session.receiveError(); err != nil {
		return FetchResult{}, false, err
	}
	select {
	case item := <-session.items:
		if err := session.receiveError(); err != nil {
			return FetchResult{}, false, err
		}
		if !item.queuedAt.IsZero() {
			txmetrics.RecordMailboxReceiveQueue(time.Since(item.queuedAt))
		}
		message := item.response.GetMessage()
		return FetchResult{Seq: message.GetSeq(), Payload: message.GetPayload()}, true, nil
	default:
		return FetchResult{}, false, nil
	}
}

func (session *grpcMailboxSession) Ack(ctx context.Context, ack int64) error {
	session.mu.Lock()
	err := session.err
	session.mu.Unlock()
	if err != nil {
		return err
	}
	return session.stream.send(ctx, &msgqpb.MailboxStreamRequest{
		Header: headerToProto(session.header), Kind: msgqpb.MailboxStreamRequest_KIND_ACK,
		Ack: ack,
	})
}

func (session *grpcMailboxSession) Renew(ctx context.Context) error {
	session.mu.Lock()
	err := session.err
	session.mu.Unlock()
	if err != nil {
		return err
	}
	return session.stream.send(ctx, &msgqpb.MailboxStreamRequest{Header: headerToProto(session.header), Kind: msgqpb.MailboxStreamRequest_KIND_RENEW})
}

const mailboxControlTimeout = 5 * time.Second

func (session *grpcMailboxSession) closeIngress() {
	session.revokeOnce.Do(func() {
		if session.onRevoke != nil {
			session.onRevoke()
		}
	})
}

// Close 等待 MsgQ 记录本连接已关闭；失败保留精确接入以便重试。
// 调用方必须先停止上行；流断开不能伪装为关闭确认。
func (session *grpcMailboxSession) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), mailboxControlTimeout)
	defer cancel()
	session.controlMu.Lock()
	defer session.controlMu.Unlock()
	session.finish(context.Canceled)
	if session.detached {
		return nil
	}
	if err := session.control(ctx, msgqpb.MailboxStreamRequest_KIND_CLOSE, session.closed); err != nil {
		return err
	}
	session.detached = true
	session.remove()
	return nil
}

func (session *grpcMailboxSession) Terminate(ctx context.Context) error {
	session.controlMu.Lock()
	defer session.controlMu.Unlock()
	if err := session.control(ctx, msgqpb.MailboxStreamRequest_KIND_TERMINATE, session.terminated); err != nil {
		return err
	}
	session.detached = true
	session.finish(context.Canceled)
	session.remove()
	return nil
}

func (session *grpcMailboxSession) control(ctx context.Context, kind msgqpb.MailboxStreamRequest_Kind, result <-chan error) error {
	stream := session.stream
	select {
	case <-stream.done:
		if stream.reopen == nil {
			return ErrRouteUnavailable
		}
		var err error
		stream, err = stream.reopen(ctx)
		if err != nil {
			return err
		}
	default:
	}
	stream.mu.Lock()
	if current := stream.sessions[session.header]; current != nil && current != session {
		stream.mu.Unlock()
		return ErrSessionChanged
	}
	stream.sessions[session.header] = session
	stream.mu.Unlock()
	session.controlStream = stream
	// 已解除的旧句柄仍可发送精确控制，但不能因拒绝结果重新常驻流目录。
	if session.detached {
		defer session.remove()
	}
	if err := stream.send(ctx, &msgqpb.MailboxStreamRequest{Header: headerToProto(session.header), Kind: kind}); err != nil {
		return err
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-stream.done:
		return ErrRouteUnavailable
	}
}
func (session *grpcMailboxSession) remove() {
	for _, stream := range []*grpcMailboxStream{session.stream, session.controlStream} {
		if stream != nil {
			stream.mu.Lock()
			if stream.sessions[session.header] == session {
				delete(stream.sessions, session.header)
			}
			stream.mu.Unlock()
		}
	}
}

func (session *grpcMailboxSession) finish(err error) {
	session.once.Do(func() {
		session.mu.Lock()
		session.err = err
		session.mu.Unlock()
		session.cancel(err)
		close(session.done)
	})
}

type localMailboxSession struct {
	mailbox *Server
	header  SessionHeader
	ctx     context.Context
	cancel  context.CancelCauseFunc
	items   chan FetchResult
}

func (session *localMailboxSession) Context() context.Context { return session.ctx }

func (session *localMailboxSession) Recv() (FetchResult, error) {
	select {
	case result := <-session.items:
		if err := context.Cause(session.ctx); err != nil {
			return FetchResult{}, err
		}
		return result, nil
	case <-session.ctx.Done():
		return FetchResult{}, context.Cause(session.ctx)
	}
}

func (session *localMailboxSession) TryRecv() (FetchResult, bool, error) {
	if err := context.Cause(session.ctx); err != nil {
		return FetchResult{}, false, err
	}
	select {
	case result := <-session.items:
		if err := context.Cause(session.ctx); err != nil {
			return FetchResult{}, false, err
		}
		return result, true, nil
	default:
		return FetchResult{}, false, nil
	}
}

func (session *localMailboxSession) Ack(ctx context.Context, ack int64) error {
	return session.mailbox.Confirm(ctx, ConfirmRequest{Header: session.header, Ack: ack})
}

func (session *localMailboxSession) Renew(ctx context.Context) error {
	return session.mailbox.Touch(ctx, TouchRequest{Header: session.header})
}

func (session *localMailboxSession) Close() error {
	session.cancel(context.Canceled)
	ctx, cancel := context.WithTimeout(context.Background(), mailboxControlTimeout)
	defer cancel()
	return session.mailbox.detachAttachment(ctx, session.header)
}
func (session *localMailboxSession) Terminate(ctx context.Context) error {
	session.cancel(context.Canceled)
	if err := session.mailbox.detachAttachment(ctx, session.header); err != nil {
		return err
	}
	return session.mailbox.terminateAttachedSession(ctx, session.header)
}

func openLocalMailboxSession(ctx context.Context, mailbox *Server, request InitRequest, onRevoke func()) (MailboxStreamSession, error) {
	// 先登记可撤销接入，初始化成功后才启动下行读取。
	sessionCtx, cancel := context.WithCancelCause(ctx)
	session := &localMailboxSession{mailbox: mailbox, header: request.Header, ctx: sessionCtx, cancel: cancel, items: make(chan FetchResult, 1)}
	attachment := &mailboxAttachment{header: request.Header, restore: request.Restore, detached: make(chan struct{})}
	var revokeOnce sync.Once
	closure := make(chan struct{})
	attachment.revoke = func(opCtx context.Context) error {
		revokeOnce.Do(func() {
			go func() {
				if onRevoke != nil {
					onRevoke()
				}
				session.cancel(ErrSessionChanged)
				close(closure)
			}()
		})
		select {
		case <-closure:
			return mailbox.detachAttachment(opCtx, request.Header)
		case <-opCtx.Done():
			return opCtx.Err()
		}
	}
	if err := mailbox.openAttachment(ctx, request, attachment); err != nil {
		cancel(err)
		controlCtx, stop := context.WithTimeout(context.Background(), mailboxControlTimeout)
		defer stop()
		return nil, errors.Join(err, mailbox.detachFailedAttachment(controlCtx, attachment))
	}
	go session.fetchLoop()
	return session, nil
}
func (session *localMailboxSession) fetchLoop() {
	for {
		batch, err := session.mailbox.FetchSome(session.ctx, FetchSomeRequest{Header: session.header, Max: 1})
		if err != nil {
			session.cancel(err)
			return
		}
		select {
		case session.items <- batch.Messages[0]:
		case <-session.ctx.Done():
			return
		}
	}
}
