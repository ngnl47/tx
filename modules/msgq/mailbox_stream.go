package msgq

import (
	"context"
	"errors"
	"sync"
	"time"

	txmetrics "github.com/ngnl47/tx/metrics"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
)

// MailboxStreamSession 是共享 MailboxStream 上的一条逻辑会话。
type MailboxStreamSession interface {
	// Context 随逻辑会话结束取消，Cause 保留终止原因；不影响同流的其他会话。
	Context() context.Context
	Recv() (FetchResult, error)
	Ack(context.Context, int64) error
	Renew(context.Context) error
	Close() error
}

type grpcMailboxStream struct {
	ctx            context.Context
	cancel         context.CancelFunc
	client         grpc.BidiStreamingClient[msgqpb.MailboxStreamRequest, msgqpb.MailboxStreamResponse]
	requests       chan *msgqpb.MailboxStreamRequest
	done           chan struct{}
	mu             sync.Mutex
	sessions       map[int64]*grpcMailboxSession
	once           sync.Once
	receiveSamples uint64
}

type mailboxResponse struct {
	response *msgqpb.MailboxStreamResponse
	queuedAt time.Time
}

type grpcMailboxSession struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	stream *grpcMailboxStream
	header SessionHeader
	items  chan mailboxResponse
	done   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	err    error
}

func newGRPCMailboxStream(client msgqpb.MailboxServiceClient) (*grpcMailboxStream, error) {
	streamCtx, cancel := context.WithCancel(context.Background())
	clientStream, err := client.MailboxStream(streamCtx)
	if err != nil {
		cancel()
		return nil, mailboxClientError(err)
	}
	stream := &grpcMailboxStream{
		ctx: streamCtx, cancel: cancel, client: clientStream,
		requests: make(chan *msgqpb.MailboxStreamRequest, 256), done: make(chan struct{}),
		sessions: make(map[int64]*grpcMailboxSession),
	}
	go stream.writeLoop()
	go stream.readLoop()
	return stream, nil
}

func (stream *grpcMailboxStream) open(ctx context.Context, header SessionHeader) (MailboxStreamSession, error) {
	sessionCtx, cancel := context.WithCancelCause(ctx)
	session := &grpcMailboxSession{ctx: sessionCtx, cancel: cancel, stream: stream, header: header, items: make(chan mailboxResponse, 64), done: make(chan struct{})}
	stream.mu.Lock()
	if err := stream.ctx.Err(); err != nil {
		stream.mu.Unlock()
		cancel(err)
		return nil, err
	}
	if _, exists := stream.sessions[header.SessionID]; exists {
		stream.mu.Unlock()
		cancel(ErrSessionChanged)
		return nil, ErrSessionChanged
	}
	stream.sessions[header.SessionID] = session
	stream.mu.Unlock()
	if err := stream.send(ctx, &msgqpb.MailboxStreamRequest{Header: headerToProto(header), Kind: msgqpb.MailboxStreamRequest_KIND_OPEN, Max: 1}); err != nil {
		stream.mu.Lock()
		if stream.sessions[header.SessionID] == session {
			delete(stream.sessions, header.SessionID)
		}
		stream.mu.Unlock()
		cancel(err)
		return nil, err
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
		session := stream.sessions[header.SessionID]
		stream.mu.Unlock()
		if session == nil || session.header != header {
			continue
		}
		if message := response.GetErrorMessage(); message != "" {
			session.finish(errors.New(message))
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
		stream.sessions = make(map[int64]*grpcMailboxSession)
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

// Close 总是先释放本地会话；远端控制队列拥堵时由会话超时回收，不阻塞调用方。
func (session *grpcMailboxSession) Close() error {
	session.stream.mu.Lock()
	current := session.stream.sessions[session.header.SessionID] == session
	if current {
		delete(session.stream.sessions, session.header.SessionID)
	}
	session.stream.mu.Unlock()
	session.finish(context.Canceled)
	if !current {
		return nil
	}
	select {
	case session.stream.requests <- &msgqpb.MailboxStreamRequest{Header: headerToProto(session.header), Kind: msgqpb.MailboxStreamRequest_KIND_CLOSE}:
		return nil
	case <-session.stream.done:
		return nil
	default:
		return ErrMailboxBackpressure
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
	once    sync.Once
}

func newLocalMailboxSession(ctx context.Context, mailbox *Server, header SessionHeader) MailboxStreamSession {
	sessionCtx, cancel := context.WithCancelCause(ctx)
	session := &localMailboxSession{mailbox: mailbox, header: header, ctx: sessionCtx, cancel: cancel, items: make(chan FetchResult, 1)}
	go func() {
		for {
			batch, err := mailbox.FetchSome(sessionCtx, FetchSomeRequest{Header: header, Max: 1})
			if err != nil {
				cancel(err)
				return
			}
			select {
			case session.items <- batch.Messages[0]:
			case <-sessionCtx.Done():
				return
			}
		}
	}()
	return session
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
	session.once.Do(func() { session.cancel(context.Canceled) })
	return nil
}
