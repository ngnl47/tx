package msgq

import (
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type streamAttachment struct {
	header SessionHeader
	cancel context.CancelFunc

	// 接入协程写入结果，关闭协程等待 opened 后读取。
	opened     chan struct{}
	attachment *mailboxAttachment
	openErr    error
}

type mailboxDownlink struct {
	response *msgqpb.MailboxStreamResponse
	queuedAt time.Time
}

// mailboxStreamServer 拥有一条共享流的收发与接入记录，不拥有逻辑邮箱。
type mailboxStreamServer struct {
	mailbox *Server
	stream  grpc.BidiStreamingServer[msgqpb.MailboxStreamRequest, msgqpb.MailboxStreamResponse]
	ctx     context.Context
	cancel  context.CancelFunc

	// 发送和接收各由一个协程执行；队列不随单个 Session 关闭。
	downlinks     chan mailboxDownlink
	sendResult    chan error
	requests      chan *msgqpb.MailboxStreamRequest
	receiveResult chan error
	samples       atomic.Uint64

	// 主循环登记/查找接入，关闭协程确认成功后条件删除。
	mu       sync.Mutex
	sessions map[SessionHeader]*streamAttachment
}

// MailboxStream 管理一条 Agent↔MsgQ 双向流，复用多个 Session。
//
// 执行流程：
//
//	建立上下文、发送队列和 Session 接入表
//	├─ 发送协程：统一写入 stream
//	├─ 接收协程：读取请求，交给主循环分派
//	├─ 请求分派
//	│  ├─ OPEN：登记 → 异步接纳 → OPENED → 持续推送下行
//	│  ├─ CLOSE：解除接入，保留邮箱供重连
//	│  ├─ DETACHED：确认旧接入关闭，推进顶号协调
//	│  ├─ TERMINATE：解除接入并终止逻辑会话
//	│  ├─ ACK：裁剪已确认下行
//	│  └─ RENEW：续期
//	└─ 流退出：取消本流的会话工作和收发任务
//
// 只有发送协程调用 stream.Send；接入和关闭等待不能阻塞请求分派，
// 否则可能阻塞顶号所需的 DETACHED。流退出不等同于主动退出登录，
// 取消也不替代解除接入确认；底层 gRPC 接收随本方法返回而结束。
func (s *mailboxService) MailboxStream(stream grpc.BidiStreamingServer[msgqpb.MailboxStreamRequest, msgqpb.MailboxStreamResponse]) error {
	if err := s.mailbox.checkStartup(); err != nil {
		return mailboxStatusError(err)
	}
	ctx, cancel := context.WithCancel(stream.Context())
	connection := &mailboxStreamServer{
		mailbox: s.mailbox, stream: stream, ctx: ctx, cancel: cancel,
		downlinks:     make(chan mailboxDownlink, 128),
		sendResult:    make(chan error, 1),
		requests:      make(chan *msgqpb.MailboxStreamRequest),
		receiveResult: make(chan error, 1),
		sessions:      make(map[SessionHeader]*streamAttachment),
	}
	defer connection.close()
	return connection.run()
}

func (connection *mailboxStreamServer) run() error {
	go connection.writeLoop()
	go connection.readLoop()
	for {
		select {
		case <-connection.ctx.Done():
			select {
			case err := <-connection.sendResult:
				return err
			default:
				return connection.ctx.Err()
			}
		case err := <-connection.receiveResult:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case request := <-connection.requests:
			header := headerFromProto(request.GetHeader())
			if header.validateConnection() != nil {
				return status.Error(codes.InvalidArgument, "invalid mailbox stream header")
			}
			switch request.GetKind() {
			case msgqpb.MailboxStreamRequest_KIND_OPEN:
				if err := connection.openSession(header, request); err != nil {
					return err
				}
			case msgqpb.MailboxStreamRequest_KIND_CLOSE, msgqpb.MailboxStreamRequest_KIND_DETACHED, msgqpb.MailboxStreamRequest_KIND_TERMINATE:
				connection.mu.Lock()
				active := connection.sessions[header]
				connection.mu.Unlock()
				go connection.closeSession(header, request.GetKind(), active)
			case msgqpb.MailboxStreamRequest_KIND_ACK:
				if err := connection.mailbox.Confirm(connection.ctx, ConfirmRequest{Header: header, Ack: request.GetAck()}); err != nil && !errors.Is(err, ErrSessionOffline) && !errors.Is(err, ErrSessionChanged) {
					txlog.ErrorContext(connection.ctx, "mailbox acknowledgement failed", "sessionID", header.SessionID, "err", err)
				}
			case msgqpb.MailboxStreamRequest_KIND_RENEW:
				if err := connection.mailbox.Touch(connection.ctx, TouchRequest{Header: header}); err != nil && !errors.Is(err, ErrSessionOffline) && !errors.Is(err, ErrSessionChanged) {
					txlog.ErrorContext(connection.ctx, "mailbox lease renewal failed", "sessionID", header.SessionID, "err", err)
				}
			default:
				return status.Error(codes.InvalidArgument, "unknown mailbox stream operation")
			}
		}
	}
}

func (connection *mailboxStreamServer) send(ctx context.Context, response *msgqpb.MailboxStreamResponse) error {
	item := mailboxDownlink{response: response}
	if response.Message != nil && connection.samples.Add(1)&63 == 0 {
		item.queuedAt = time.Now()
	}
	select {
	case connection.downlinks <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-connection.ctx.Done():
		return connection.ctx.Err()
	}
}

func (connection *mailboxStreamServer) writeLoop() {
	for {
		select {
		case item := <-connection.downlinks:
			started := time.Now()
			err := connection.stream.Send(item.response)
			if !item.queuedAt.IsZero() {
				txmetrics.RecordMailboxSend(started.Sub(item.queuedAt), time.Since(started))
			}
			if err != nil {
				connection.sendResult <- err
				connection.cancel()
				return
			}
		case <-connection.ctx.Done():
			return
		}
	}
}

func (connection *mailboxStreamServer) readLoop() {
	for {
		request, err := connection.stream.Recv()
		if err != nil {
			connection.receiveResult <- err
			return
		}
		select {
		case connection.requests <- request:
		case <-connection.ctx.Done():
			return
		}
	}
}

func (connection *mailboxStreamServer) respond(header SessionHeader, kind msgqpb.MailboxStreamResponse_Kind, err error) {
	response := &msgqpb.MailboxStreamResponse{Header: headerToProto(header), Kind: kind}
	if err != nil {
		wire := mailboxStatusError(err)
		response.ErrorMessage = wire.Error()
		response.ErrorCode = int32(status.Code(wire))
	}
	_ = connection.send(connection.ctx, response)
}

func (connection *mailboxStreamServer) openSession(header SessionHeader, request *msgqpb.MailboxStreamRequest) error {
	if request.GetInit() == nil || headerFromProto(request.GetInit().GetHeader()) != header || request.GetMax() < 0 {
		return status.Error(codes.InvalidArgument, "invalid mailbox stream open")
	}
	ctx, cancel := context.WithCancel(connection.ctx)
	active := &streamAttachment{header: header, cancel: cancel, opened: make(chan struct{})}
	connection.mu.Lock()
	_, exists := connection.sessions[header]
	if !exists {
		connection.sessions[header] = active
	}
	connection.mu.Unlock()
	if exists {
		cancel()
		connection.respond(header, msgqpb.MailboxStreamResponse_KIND_OPENED, ErrSessionChanged)
		return nil
	}
	// 先登记再启动工作协程，紧随 OPEN 的关闭请求才能找到初始化中的接入。
	go connection.runSession(ctx, request, active)
	return nil
}

func (connection *mailboxStreamServer) runSession(ctx context.Context, request *msgqpb.MailboxStreamRequest, active *streamAttachment) {
	header := active.header
	attachment := &mailboxAttachment{header: header, restore: request.GetInit().GetRestore(), detached: make(chan struct{})}
	active.attachment = attachment
	attachment.revoke = func(opCtx context.Context) error {
		if err := connection.send(opCtx, &msgqpb.MailboxStreamResponse{Header: headerToProto(header), Kind: msgqpb.MailboxStreamResponse_KIND_REVOKE}); err != nil {
			return err
		}
		select {
		case <-attachment.detached:
			return nil
		case <-opCtx.Done():
			return opCtx.Err()
		case <-connection.ctx.Done():
			return ErrRouteUnavailable
		}
	}
	openCtx, finishOpen := context.WithTimeout(ctx, mailboxControlTimeout)
	err := connection.mailbox.openAttachment(openCtx, initFromProto(request.GetInit()), attachment)
	finishOpen()
	active.openErr = err
	close(active.opened)
	connection.respond(header, msgqpb.MailboxStreamResponse_KIND_OPENED, err)
	if err != nil {
		return
	}
	defer active.cancel()
	connection.forwardDown(ctx, header, int(request.GetMax()))
}

func (connection *mailboxStreamServer) forwardDown(ctx context.Context, header SessionHeader, max int) {
	for {
		batch, err := connection.mailbox.FetchSome(ctx, FetchSomeRequest{Header: header, Max: max})
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				connection.respond(header, msgqpb.MailboxStreamResponse_KIND_DATA, err)
			}
			return
		}
		for _, message := range batch.Messages {
			if connection.send(ctx, &msgqpb.MailboxStreamResponse{Header: headerToProto(header), Message: fetchResultToProto(message)}) != nil {
				return
			}
		}
	}
}

func (connection *mailboxStreamServer) closeSession(header SessionHeader, operation msgqpb.MailboxStreamRequest_Kind, active *streamAttachment) {
	ctx, cancel := context.WithTimeout(connection.ctx, mailboxControlTimeout)
	defer cancel()
	// CLOSE/TERMINATE 已由当前 Agent 确认停止上行，不再反向撤销它的 socket。
	if active != nil {
		active.cancel()
		select {
		case <-active.opened:
		case <-ctx.Done():
			return
		}
	}
	var err error
	if active != nil && active.openErr != nil {
		err = connection.mailbox.detachFailedAttachment(ctx, active.attachment)
	} else {
		err = connection.mailbox.detachAttachment(ctx, header)
	}
	kind := msgqpb.MailboxStreamResponse_KIND_CLOSED
	if operation == msgqpb.MailboxStreamRequest_KIND_TERMINATE {
		kind = msgqpb.MailboxStreamResponse_KIND_TERMINATED
		if err == nil {
			err = connection.mailbox.terminateAttachedSession(ctx, header)
		}
	}
	if err == nil {
		connection.mu.Lock()
		if connection.sessions[header] == active {
			delete(connection.sessions, header)
		}
		connection.mu.Unlock()
	}
	connection.respond(header, kind, err)
}

func (connection *mailboxStreamServer) close() {
	connection.mu.Lock()
	for _, active := range connection.sessions {
		active.cancel()
	}
	connection.mu.Unlock()
	connection.cancel()
}

func (s *Server) terminateAttachedSession(ctx context.Context, header SessionHeader) error {
	if store, ok := s.store.(AdmissionStore); ok {
		finish, err := s.beginAdmission(ctx, header.RoleID)
		if err != nil {
			return err
		}
		defer finish()
		record, err := store.LoadAdmission(ctx, header.RoleID)
		if err != nil {
			return err
		}
		if record.SessionID == header.SessionID {
			if record.Header != header {
				return ErrSessionChanged
			}
			return s.terminateLogicalSession(ctx, header.RoleID, header.SessionID)
		}
	}
	return s.TerminateSession(ctx, header)
}
