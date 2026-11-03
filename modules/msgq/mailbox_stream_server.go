package msgq

import (
	"context"
	"errors"
	txlog "github.com/ngnl47/tx/log"
	txmetrics "github.com/ngnl47/tx/metrics"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type streamAttachment struct {
	header     SessionHeader
	cancel     context.CancelFunc
	opened     chan struct{}
	attachment *mailboxAttachment
	openErr    error
}

type mailboxDownlink struct {
	response *msgqpb.MailboxStreamResponse
	queuedAt time.Time
}

func (s *mailboxService) MailboxStream(stream grpc.BidiStreamingServer[msgqpb.MailboxStreamRequest, msgqpb.MailboxStreamResponse]) error {
	if err := s.mailbox.checkStartup(); err != nil {
		return mailboxStatusError(err)
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	downlinks := make(chan mailboxDownlink, 128)
	var samples atomic.Uint64
	send := func(opCtx context.Context, response *msgqpb.MailboxStreamResponse) error {
		item := mailboxDownlink{response: response}
		if response.Message != nil && samples.Add(1)&63 == 0 {
			item.queuedAt = time.Now()
		}
		select {
		case downlinks <- item:
			return nil
		case <-opCtx.Done():
			return opCtx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	sendResult := make(chan error, 1)
	go func() {
		for {
			select {
			case item := <-downlinks:
				started := time.Now()
				err := stream.Send(item.response)
				if !item.queuedAt.IsZero() {
					txmetrics.RecordMailboxSend(started.Sub(item.queuedAt), time.Since(started))
				}
				if err != nil {
					sendResult <- err
					cancel()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	// 接收循环只分派低频控制；OPEN/终止等待另行执行，不挡住旧端点的 DETACHED。
	requests := make(chan *msgqpb.MailboxStreamRequest)
	receiveResult := make(chan error, 1)
	go func() {
		for {
			request, err := stream.Recv()
			if err != nil {
				receiveResult <- err
				return
			}
			select {
			case requests <- request:
			case <-ctx.Done():
				return
			}
		}
	}()
	var mu sync.Mutex
	sessions := make(map[SessionHeader]*streamAttachment)
	respond := func(header SessionHeader, kind msgqpb.MailboxStreamResponse_Kind, err error) {
		response := &msgqpb.MailboxStreamResponse{Header: headerToProto(header), Kind: kind}
		if err != nil {
			wire := mailboxStatusError(err)
			response.ErrorMessage = wire.Error()
			response.ErrorCode = int32(status.Code(wire))
		}
		_ = send(ctx, response)
	}
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		for _, active := range sessions {
			active.cancel()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			select {
			case err := <-sendResult:
				return err
			default:
				return ctx.Err()
			}
		case err := <-receiveResult:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case request := <-requests:
			header := headerFromProto(request.GetHeader())
			if header.validateConnection() != nil {
				return status.Error(codes.InvalidArgument, "invalid mailbox stream header")
			}
			switch request.GetKind() {
			case msgqpb.MailboxStreamRequest_KIND_OPEN:
				if request.GetInit() == nil || headerFromProto(request.GetInit().GetHeader()) != header || request.GetMax() < 0 {
					return status.Error(codes.InvalidArgument, "invalid mailbox stream open")
				}
				sessionCtx, stop := context.WithCancel(ctx)
				active := &streamAttachment{header: header, cancel: stop, opened: make(chan struct{})}
				mu.Lock()
				_, exists := sessions[header]
				if !exists {
					sessions[header] = active
				}
				mu.Unlock()
				if exists {
					stop()
					respond(header, msgqpb.MailboxStreamResponse_KIND_OPENED, ErrSessionChanged)
					continue
				}
				go func() {
					attachment := &mailboxAttachment{header: header, restore: request.GetInit().GetRestore(), detached: make(chan struct{})}
					active.attachment = attachment
					attachment.revoke = func(opCtx context.Context) error {
						if err := send(opCtx, &msgqpb.MailboxStreamResponse{Header: headerToProto(header), Kind: msgqpb.MailboxStreamResponse_KIND_REVOKE}); err != nil {
							return err
						}
						select {
						case <-attachment.detached:
							return nil
						case <-opCtx.Done():
							return opCtx.Err()
						case <-ctx.Done():
							return ErrRouteUnavailable
						}
					}
					openCtx, finishOpen := context.WithTimeout(sessionCtx, mailboxControlTimeout)
					err := s.mailbox.openAttachment(openCtx, initFromProto(request.GetInit()), attachment)
					finishOpen()
					active.openErr = err
					close(active.opened)
					respond(header, msgqpb.MailboxStreamResponse_KIND_OPENED, err)
					if err != nil {
						return
					}
					defer stop()
					for {
						batch, err := s.mailbox.FetchSome(sessionCtx, FetchSomeRequest{Header: header, Max: int(request.GetMax())})
						if err != nil {
							if !errors.Is(err, context.Canceled) {
								respond(header, msgqpb.MailboxStreamResponse_KIND_DATA, err)
							}
							return
						}
						for _, message := range batch.Messages {
							if send(sessionCtx, &msgqpb.MailboxStreamResponse{Header: headerToProto(header), Message: fetchResultToProto(message)}) != nil {
								return
							}
						}
					}
				}()
			case msgqpb.MailboxStreamRequest_KIND_CLOSE, msgqpb.MailboxStreamRequest_KIND_DETACHED, msgqpb.MailboxStreamRequest_KIND_TERMINATE:
				mu.Lock()
				active := sessions[header]
				mu.Unlock()
				// CLOSE/TERMINATE 已由当前 Agent 确认停止上行，不再反向撤销它的 socket。
				go func() {
					controlCtx, finishControl := context.WithTimeout(ctx, mailboxControlTimeout)
					defer finishControl()
					if active != nil {
						active.cancel()
						select {
						case <-active.opened:
						case <-controlCtx.Done():
							return
						}
					}
					var err error
					if active != nil && active.openErr != nil {
						err = s.mailbox.detachFailedAttachment(controlCtx, active.attachment)
					} else {
						err = s.mailbox.detachAttachment(controlCtx, header)
					}
					kind := msgqpb.MailboxStreamResponse_KIND_CLOSED
					if request.GetKind() == msgqpb.MailboxStreamRequest_KIND_TERMINATE {
						kind = msgqpb.MailboxStreamResponse_KIND_TERMINATED
						if err == nil {
							err = s.mailbox.terminateAttachedSession(controlCtx, header)
						}
					}
					if err == nil {
						mu.Lock()
						if sessions[header] == active {
							delete(sessions, header)
						}
						mu.Unlock()
					}
					respond(header, kind, err)
				}()
			case msgqpb.MailboxStreamRequest_KIND_ACK:
				if err := s.mailbox.Confirm(ctx, ConfirmRequest{Header: header, Ack: request.GetAck()}); err != nil && !errors.Is(err, ErrSessionOffline) && !errors.Is(err, ErrSessionChanged) {
					txlog.ErrorContext(ctx, "mailbox acknowledgement failed", "sessionID", header.SessionID, "err", err)
				}
			case msgqpb.MailboxStreamRequest_KIND_RENEW:
				if err := s.mailbox.Touch(ctx, TouchRequest{Header: header}); err != nil && !errors.Is(err, ErrSessionOffline) && !errors.Is(err, ErrSessionChanged) {
					txlog.ErrorContext(ctx, "mailbox lease renewal failed", "sessionID", header.SessionID, "err", err)
				}
			default:
				return status.Error(codes.InvalidArgument, "unknown mailbox stream operation")
			}
		}
	}
}

func (s *Server) terminateAttachedSession(ctx context.Context, header SessionHeader) error {
	if store, ok := s.store.(AdmissionStore); ok {
		record, err := store.LoadAdmission(ctx, header.RoleID)
		if err != nil {
			return err
		}
		if record.SessionID == header.SessionID {
			if record.Header != header {
				return ErrSessionChanged
			}
			return s.TerminateLogicalSession(ctx, header.RoleID, header.SessionID)
		}
	}
	return s.TerminateSession(ctx, header)
}
