package msgq

import (
	"context"
	"errors"
	"fmt"

	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
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
	return &msgqpb.InitResponse{ReceivedSeq: result.ReceivedSeq}, nil
}

func (s *mailboxService) Rec(ctx context.Context, request *msgqpb.RecRequest) (*msgqpb.RecResponse, error) {
	result, err := s.mailbox.Rec(ctx, RecRequest{Header: headerFromProto(request.GetHeader()), Seq: request.GetSeq(), Ack: request.GetAck()})
	if err != nil {
		return nil, mailboxStatusError(err)
	}
	return &msgqpb.RecResponse{ReceivedSeq: result.ReceivedSeq, Ack: result.Ack}, nil
}

func (s *mailboxService) Pub(ctx context.Context, request *msgqpb.PubRequest) (*msgqpb.PubResponse, error) {
	result, err := s.mailbox.Pub(ctx, PubRequest{Header: headerFromProto(request.GetHeader()), Message: request.GetMessage()})
	if err != nil {
		return nil, mailboxStatusError(err)
	}
	return &msgqpb.PubResponse{Seq: result.Seq}, nil
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
	messages := make([]*msgqpb.FetchResponse, len(result.Messages))
	for index, message := range result.Messages {
		messages[index] = fetchResultToProto(message)
	}
	return &msgqpb.FetchSomeResponse{Messages: messages}, nil
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

func (s *mailboxService) BroadcastTopic(ctx context.Context, request *msgqpb.BroadcastTopicRequest) (*msgqpb.BroadcastTopicResponse, error) {
	result, err := s.mailbox.BroadcastTopic(ctx, BroadcastTopicRequest{Topic: request.GetTopic(), Message: request.GetMessage()})
	response := &msgqpb.BroadcastTopicResponse{
		Attempted:  int32(result.Attempted),
		Delivered:  int32(result.Delivered),
		Skipped:    int32(result.Skipped),
		Overflowed: int32(result.Overflowed),
	}
	if err != nil {
		return response, mailboxStatusError(err)
	}
	return response, nil
}

func (s *mailboxService) Close(ctx context.Context, request *msgqpb.CloseRequest) (*msgqpb.Empty, error) {
	return emptyResponse(s.mailbox.Close(ctx, headerFromProto(request.GetHeader())))
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
	return &msgqpb.FetchResponse{Seq: result.Seq, Message: result.Message}
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
	case errors.Is(err, ErrInvalidHeader), errors.Is(err, ErrInboundSequence), errors.Is(err, ErrInvalidTopic):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrSessionOffline):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrSessionChanged):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, ErrOutboundOverflow):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, ErrRouteUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, ErrServerNil), errors.Is(err, ErrSessionStoreNil), errors.Is(err, ErrLifecycleNil):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, fmt.Sprintf("msgq request failed: %v", err))
	}
}
