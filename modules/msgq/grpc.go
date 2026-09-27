package msgq

import (
	"context"
	"errors"
	"fmt"
	"io"

	txlog "github.com/ngnl47/tx/log"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegisterMailboxService 将进程内邮箱注册为独立的 gRPC 服务。
func RegisterMailboxService(registrar grpc.ServiceRegistrar, mailbox *Server) error {
	if registrar == nil || mailbox == nil {
		return ErrServerNil
	}
	msgqpb.RegisterMailboxServiceServer(registrar, &mailboxService{mailbox: mailbox})
	return nil
}

type mailboxService struct {
	msgqpb.UnimplementedMailboxServiceServer
	mailbox *Server
}

func (s *mailboxService) Init(ctx context.Context, request *msgqpb.InitRequest) (*msgqpb.InitResponse, error) {
	result, err := s.mailbox.Init(ctx, InitRequest{Header: headerFromProto(request.GetHeader()), Restore: request.GetRestore(), Ack: request.GetAck()})
	if err != nil {
		return nil, mailboxStatusError(err)
	}
	return &msgqpb.InitResponse{ReceivedSeq: result.UpMsgSeq}, nil
}

func (s *mailboxService) PubSession(ctx context.Context, request *msgqpb.PubSessionRequest) (*msgqpb.PubSessionResponse, error) {
	result, err := s.mailbox.PubSession(ctx, request.GetSessionId(), request.GetPayload())
	if err != nil {
		return nil, mailboxStatusError(err)
	}
	return &msgqpb.PubSessionResponse{Seq: result.Seq}, nil
}

func (s *mailboxService) PubRole(ctx context.Context, request *msgqpb.PubRoleRequest) (*msgqpb.BroadcastResponse, error) {
	result, err := s.mailbox.PubRole(ctx, RoleBinding{
		RoleID: request.GetRoleId(), InstanceID: request.GetInstanceId(), Generation: request.GetGeneration(),
	}, request.GetPayload())
	return broadcastResponse(result, err)
}

func (s *mailboxService) MailboxStream(stream grpc.BidiStreamingServer[msgqpb.MailboxStreamRequest, msgqpb.MailboxStreamResponse]) error {
	type incoming struct {
		request *msgqpb.MailboxStreamRequest
		err     error
	}
	type activeStreamSession struct {
		header SessionHeader
		cancel context.CancelFunc
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	requests := make(chan incoming, 1)
	downlinks := make(chan *msgqpb.MailboxStreamResponse, 128)
	// 一条流只由一个协程写出；流控等待不占用会话控制循环。
	sendResult := make(chan error, 1)
	go func() {
		for {
			select {
			case response := <-downlinks:
				if err := stream.Send(response); err != nil {
					sendResult <- err
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		defer close(requests)
		for {
			request, err := stream.Recv()
			select {
			case requests <- incoming{request: request, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	sessions := make(map[int64]*activeStreamSession)
	finished := make(chan *activeStreamSession, 128)
	defer func() {
		for _, session := range sessions {
			session.cancel()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case item, ok := <-requests:
			if !ok || errors.Is(item.err, io.EOF) {
				return nil
			}
			if item.err != nil {
				return item.err
			}
			request := item.request
			header := headerFromProto(request.GetHeader())
			switch request.GetKind() {
			case msgqpb.MailboxStreamRequest_KIND_OPEN:
				if request.GetHeader() == nil || request.GetMax() < 0 {
					return status.Error(codes.InvalidArgument, "invalid mailbox stream open")
				}
				if err := s.mailbox.Touch(ctx, TouchRequest{Header: header}); err != nil {
					select {
					case downlinks <- &msgqpb.MailboxStreamResponse{Header: request.GetHeader(), ErrorMessage: mailboxStatusError(err).Error()}:
					default:
						return status.Error(codes.ResourceExhausted, "mailbox stream response queue full")
					}
					continue
				}
				if previous, found := sessions[header.SessionID]; found {
					previous.cancel()
				}
				sessionCtx, stop := context.WithCancel(ctx)
				active := &activeStreamSession{header: header, cancel: stop}
				sessions[header.SessionID] = active
				max := int(request.GetMax())
				go func() {
					defer func() {
						stop()
						select {
						case finished <- active:
						case <-ctx.Done():
						}
					}()
					for {
						batch, err := s.mailbox.FetchSome(sessionCtx, FetchSomeRequest{Header: header, Max: max})
						if err != nil {
							if !errors.Is(err, context.Canceled) {
								select {
								case downlinks <- &msgqpb.MailboxStreamResponse{Header: headerToProto(header), ErrorMessage: mailboxStatusError(err).Error()}:
								case <-sessionCtx.Done():
								}
							}
							return
						}
						for _, message := range batch.Messages {
							response := &msgqpb.MailboxStreamResponse{Header: headerToProto(header), Message: fetchResultToProto(message)}
							select {
							case downlinks <- response:
							case <-sessionCtx.Done():
								return
							}
						}
					}
				}()
			case msgqpb.MailboxStreamRequest_KIND_CLOSE:
				if active, found := sessions[header.SessionID]; found && active.header == header {
					active.cancel()
					delete(sessions, header.SessionID)
				}
			case msgqpb.MailboxStreamRequest_KIND_ACK:
				var err error
				if request.GetReceivedSeq() > 0 {
					_, err = s.mailbox.Rec(ctx, RecRequest{Header: header, Seq: request.GetReceivedSeq(), Ack: request.GetAck()})
				} else {
					err = s.mailbox.Confirm(ctx, ConfirmRequest{Header: header, Ack: request.GetAck()})
				}
				if err != nil && !errors.Is(err, ErrSessionOffline) && !errors.Is(err, ErrSessionChanged) {
					txlog.ErrorContext(ctx, "mailbox stream acknowledgement failed", "sessionID", header.SessionID, "ack", request.GetAck(), "err", err)
				}
			case msgqpb.MailboxStreamRequest_KIND_RENEW:
				if err := s.mailbox.Touch(ctx, TouchRequest{Header: header}); err != nil && !errors.Is(err, ErrSessionOffline) && !errors.Is(err, ErrSessionChanged) {
					txlog.ErrorContext(ctx, "mailbox stream lease renewal failed", "sessionID", header.SessionID, "err", err)
				}
			default:
				return status.Error(codes.InvalidArgument, "unknown mailbox stream operation")
			}
		case ended := <-finished:
			if sessions[ended.header.SessionID] == ended {
				delete(sessions, ended.header.SessionID)
			}
		case err := <-sendResult:
			return err
		}
	}
}

func (s *mailboxService) PublishStream(stream grpc.BidiStreamingServer[msgqpb.PublishRequest, msgqpb.PublishRoleRejection]) error {
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch request.GetKind() {
		case msgqpb.PublishRequest_KIND_SESSION:
			if _, err := s.mailbox.PubSession(stream.Context(), request.GetSessionId(), request.GetPayload()); err != nil {
				txlog.ErrorContext(stream.Context(), "async session publish failed", "sessionID", request.GetSessionId(), "err", err)
			}
		case msgqpb.PublishRequest_KIND_ROLE:
			binding := RoleBinding{RoleID: request.GetRoleId(), InstanceID: request.GetInstanceId(), Generation: request.GetGeneration()}
			if _, err := s.mailbox.PubRole(stream.Context(), binding, request.GetPayload()); err != nil {
				s.mailbox.mu.RLock()
				local, found := s.mailbox.roleLeases[binding.RoleID]
				s.mailbox.mu.RUnlock()
				txlog.ErrorContext(stream.Context(), "async role publish failed",
					"roleID", binding.RoleID,
					"routedInstanceID", binding.InstanceID,
					"routedGeneration", binding.Generation,
					"localInstanceID", s.mailbox.instanceID,
					"localLeaseFound", found,
					"localGeneration", local.binding.Generation,
					"err", err,
				)
				if errors.Is(err, ErrRoleBindingChanged) {
					if err := stream.Send(&msgqpb.PublishRoleRejection{
						RoleId: binding.RoleID, InstanceId: binding.InstanceID, Generation: binding.Generation,
					}); err != nil {
						return err
					}
				}
			}
		default:
			txlog.ErrorContext(stream.Context(), "async publish has invalid kind", "kind", request.GetKind())
		}
	}
}

func (s *mailboxService) JoinTopics(ctx context.Context, request *msgqpb.TopicRequest) (*msgqpb.Empty, error) {
	err := s.mailbox.JoinTopics(ctx, TopicRequest{Header: headerFromProto(request.GetHeader()), Topics: request.GetTopics()})
	return emptyResponse(err)
}

func (s *mailboxService) LeaveTopics(ctx context.Context, request *msgqpb.TopicRequest) (*msgqpb.Empty, error) {
	err := s.mailbox.LeaveTopics(ctx, TopicRequest{Header: headerFromProto(request.GetHeader()), Topics: request.GetTopics()})
	return emptyResponse(err)
}

func (s *mailboxService) Topics(ctx context.Context, request *msgqpb.TopicRequest) (*msgqpb.TopicsResponse, error) {
	result, err := s.mailbox.Topics(ctx, TopicRequest{Header: headerFromProto(request.GetHeader())})
	if err != nil {
		return nil, mailboxStatusError(err)
	}
	return &msgqpb.TopicsResponse{Topics: result.Topics}, nil
}

func (s *mailboxService) BroadcastTopic(ctx context.Context, request *msgqpb.BroadcastTopicRequest) (*msgqpb.BroadcastResponse, error) {
	result, err := s.mailbox.BroadcastTopic(ctx, BroadcastTopicRequest{Topic: request.GetTopic(), Payload: request.GetPayload()})
	return broadcastResponse(result, err)
}

func broadcastResponse(result BroadcastResult, err error) (*msgqpb.BroadcastResponse, error) {
	response := &msgqpb.BroadcastResponse{
		Attempted:  int32(result.Attempted),
		Delivered:  int32(result.Delivered),
		Skipped:    int32(result.Skipped),
		Overflowed: int32(result.Overflowed),
	}
	if err != nil {
		// gRPC 不会发送非 nil error 对应的普通响应，部分投递统计必须随状态详情返回。
		failure, detailErr := status.Convert(mailboxStatusError(err)).WithDetails(response)
		if detailErr != nil {
			return nil, mailboxStatusError(detailErr)
		}
		return nil, failure.Err()
	}
	return response, nil
}

func (s *mailboxService) TerminateSession(ctx context.Context, request *msgqpb.TerminateSessionRequest) (*msgqpb.Empty, error) {
	return emptyResponse(s.mailbox.TerminateSession(ctx, headerFromProto(request.GetHeader())))
}

func emptyResponse(err error) (*msgqpb.Empty, error) {
	if err != nil {
		return nil, mailboxStatusError(err)
	}
	return &msgqpb.Empty{}, nil
}

func headerFromProto(header *msgqpb.SessionHeader) SessionHeader {
	if header == nil {
		return SessionHeader{}
	}
	return SessionHeader{RoleID: header.GetRoleId(), SessionID: header.GetSessionId(), ConnectionID: header.GetConnectionId()}
}

func fetchResultToProto(result FetchResult) *msgqpb.FetchResponse {
	return &msgqpb.FetchResponse{Seq: result.Seq, Payload: result.Payload}
}

func mailboxStatusError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	switch {
	case errors.Is(err, ErrInvalidHeader), errors.Is(err, ErrUpMsgSequence), errors.Is(err, ErrDownMsgAcknowledgement), errors.Is(err, ErrInvalidTopic):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrSessionOffline):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrRoleBindingChanged):
		st, _ := status.New(codes.Aborted, err.Error()).WithDetails(&errdetails.ErrorInfo{Reason: "ROLE_BINDING_CHANGED", Domain: "tx.msgq"})
		return st.Err()
	case errors.Is(err, ErrSessionChanged):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, ErrDownMsgOverflow):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, ErrRouteUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, ErrServerNil), errors.Is(err, ErrSessionStoreNil), errors.Is(err, ErrLifecycleNil):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, fmt.Sprintf("msgq request failed: %v", err))
	}
}

func mailboxClientError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch status.Code(err) {
	case codes.Canceled:
		return context.Canceled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	case codes.InvalidArgument:
		return fmt.Errorf("%w: %v", ErrInvalidHeader, err)
	case codes.NotFound:
		return fmt.Errorf("%w: %v", ErrSessionOffline, err)
	case codes.Aborted:
		for _, detail := range status.Convert(err).Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Domain == "tx.msgq" && info.Reason == "ROLE_BINDING_CHANGED" {
				return fmt.Errorf("%w: %v", ErrRoleBindingChanged, err)
			}
		}
		return fmt.Errorf("%w: %v", ErrSessionChanged, err)
	case codes.ResourceExhausted:
		return fmt.Errorf("%w: %v", ErrDownMsgOverflow, err)
	case codes.Unavailable:
		return fmt.Errorf("%w: %v", ErrRouteUnavailable, err)
	default:
		return err
	}
}
