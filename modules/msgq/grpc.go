package msgq

import (
	"context"
	"errors"
	"fmt"

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

func (s *mailboxService) Rec(ctx context.Context, request *msgqpb.RecRequest) (*msgqpb.RecResponse, error) {
	result, err := s.mailbox.Rec(ctx, RecRequest{Header: headerFromProto(request.GetHeader()), Seq: request.GetSeq(), Ack: request.GetAck()})
	if err != nil {
		return nil, mailboxStatusError(err)
	}
	return &msgqpb.RecResponse{ReceivedSeq: result.UpMsgSeq, Ack: result.Ack}, nil
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

func (s *mailboxService) Fetch(ctx context.Context, request *msgqpb.FetchRequest) (*msgqpb.FetchResponse, error) {
	result, err := s.mailbox.Fetch(ctx, FetchRequest{Header: headerFromProto(request.GetHeader())})
	if err != nil {
		return nil, mailboxStatusError(err)
	}
	return fetchResultToProto(result), nil
}

func (s *mailboxService) FetchSome(ctx context.Context, request *msgqpb.FetchSomeRequest) (*msgqpb.FetchSomeResponse, error) {
	result, err := s.mailbox.FetchSome(ctx, FetchSomeRequest{Header: headerFromProto(request.GetHeader()), Max: int(request.GetMax())})
	if err != nil {
		return nil, mailboxStatusError(err)
	}
	return fetchSomeToProto(result), nil
}

func (s *mailboxService) StreamFetch(request *msgqpb.FetchSomeRequest, stream grpc.ServerStreamingServer[msgqpb.FetchSomeResponse]) error {
	for {
		result, err := s.mailbox.FetchSome(stream.Context(), FetchSomeRequest{Header: headerFromProto(request.GetHeader()), Max: int(request.GetMax())})
		if err != nil {
			return mailboxStatusError(err)
		}
		if err := stream.Send(fetchSomeToProto(result)); err != nil {
			return err
		}
	}
}

func fetchSomeToProto(result FetchSomeResult) *msgqpb.FetchSomeResponse {
	messages := make([]*msgqpb.FetchResponse, len(result.Messages))
	for index, message := range result.Messages {
		messages[index] = fetchResultToProto(message)
	}
	return &msgqpb.FetchSomeResponse{Messages: messages}
}

func (s *mailboxService) Confirm(ctx context.Context, request *msgqpb.ConfirmRequest) (*msgqpb.Empty, error) {
	err := s.mailbox.Confirm(ctx, ConfirmRequest{Header: headerFromProto(request.GetHeader()), Ack: request.GetAck()})
	return emptyResponse(err)
}

func (s *mailboxService) Touch(ctx context.Context, request *msgqpb.TouchRequest) (*msgqpb.Empty, error) {
	return emptyResponse(s.mailbox.Touch(ctx, TouchRequest{Header: headerFromProto(request.GetHeader())}))
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
