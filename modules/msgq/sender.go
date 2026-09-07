package msgq

import (
	"context"
	"errors"
	"sync"

	"github.com/ngnl47/tx/modules/discovery"
	msgqpb "github.com/ngnl47/tx/modules/msgq/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var (
	ErrDialerNil               = errors.New("msgq: dialer is nil")
	ErrSessionRouterNil        = errors.New("msgq: session router is nil")
	ErrRoleRouterNil           = errors.New("msgq: role router is nil")
	ErrTransportCredentialsNil = errors.New("msgq: transport credentials are nil")
	ErrSenderClosed            = errors.New("msgq: sender is closed")
	ErrInvalidMailboxResponse  = errors.New("msgq: invalid mailbox response")
)

// Connection 向一个已解析的 msgq 实例投递下行消息。
type Connection interface {
	PubSession(context.Context, int64, []byte) (PubResult, error)
	PubRole(context.Context, RoleBinding, []byte) (BroadcastResult, error)
	JoinTopics(context.Context, TopicRequest) error
	BroadcastTopic(context.Context, BroadcastTopicRequest) (BroadcastResult, error)
	Close() error
}

// Dialer 建立到一个 msgq 实例的连接。
type Dialer interface {
	Dial(context.Context, discovery.Instance) (Connection, error)
}

// GRPCDialer 通过显式配置的凭证建立 msgq gRPC 连接。
type GRPCDialer struct {
	credentials credentials.TransportCredentials
	options     []grpc.DialOption
}

// NewGRPCDialer 创建 msgq gRPC dialer。调用方必须显式选择 TLS 或 insecure 凭证。
func NewGRPCDialer(transportCredentials credentials.TransportCredentials, options ...grpc.DialOption) (*GRPCDialer, error) {
	if transportCredentials == nil {
		return nil, ErrTransportCredentialsNil
	}
	return &GRPCDialer{
		credentials: transportCredentials,
		options:     append([]grpc.DialOption(nil), options...),
	}, nil
}

// Dial 打开到实例 endpoint 的 gRPC 连接。
func (d *GRPCDialer) Dial(_ context.Context, instance discovery.Instance) (Connection, error) {
	return d.dial(instance)
}

// DialSession 打开到实例 endpoint 的会话 gRPC 客户端。
func (d *GRPCDialer) DialSession(_ context.Context, instance discovery.Instance) (SessionClient, error) {
	return d.dial(instance)
}

func (d *GRPCDialer) dial(instance discovery.Instance) (*grpcConnection, error) {
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
		client:     msgqpb.NewMailboxServiceClient(connection),
	}, nil
}

type grpcConnection struct {
	connection *grpc.ClientConn
	client     msgqpb.MailboxServiceClient
}

func (c *grpcConnection) PubSession(ctx context.Context, sessionID int64, payload []byte) (PubResult, error) {
	response, err := c.client.PubSession(ctx, &msgqpb.PubSessionRequest{SessionId: sessionID, Payload: payload})
	if err != nil {
		return PubResult{}, mailboxClientError(err)
	}
	if response == nil || response.GetSeq() <= 0 {
		return PubResult{}, ErrInvalidMailboxResponse
	}
	return PubResult{Seq: response.GetSeq()}, nil
}

func (c *grpcConnection) JoinTopics(ctx context.Context, request TopicRequest) error {
	response, err := c.client.JoinTopics(ctx, &msgqpb.TopicRequest{
		Header: headerToProto(request.Header),
		Topics: request.Topics,
	})
	if err != nil {
		return mailboxClientError(err)
	}
	if response == nil {
		return ErrInvalidMailboxResponse
	}
	return nil
}

func (c *grpcConnection) BroadcastTopic(ctx context.Context, request BroadcastTopicRequest) (BroadcastResult, error) {
	response, err := c.client.BroadcastTopic(ctx, &msgqpb.BroadcastTopicRequest{
		Topic:   request.Topic,
		Payload: request.Payload,
	})
	return readBroadcastResponse(response, err)
}

func (c *grpcConnection) PubRole(ctx context.Context, binding RoleBinding, payload []byte) (BroadcastResult, error) {
	response, err := c.client.PubRole(ctx, &msgqpb.PubRoleRequest{
		RoleId: binding.RoleID, InstanceId: binding.InstanceID, Generation: binding.Generation, Payload: payload,
	})
	return readBroadcastResponse(response, err)
}

// readBroadcastResponse 对角色投递和 Topic 广播统一保留部分入队统计，不自动重试。
func readBroadcastResponse(response *msgqpb.BroadcastResponse, err error) (BroadcastResult, error) {
	if err != nil {
		for _, detail := range status.Convert(err).Details() {
			if result, ok := detail.(*msgqpb.BroadcastResponse); ok {
				response = result
				break
			}
		}
		if response == nil {
			// 未收到服务端统计时结果未知，不能把零值当成没有发生投递。
			return BroadcastResult{}, mailboxClientError(err)
		}
	}
	if response == nil || response.GetAttempted() < 0 || response.GetDelivered() < 0 || response.GetSkipped() < 0 || response.GetOverflowed() < 0 {
		return BroadcastResult{}, ErrInvalidMailboxResponse
	}
	if int64(response.GetDelivered())+int64(response.GetSkipped())+int64(response.GetOverflowed()) > int64(response.GetAttempted()) {
		return BroadcastResult{}, ErrInvalidMailboxResponse
	}
	return BroadcastResult{
		Attempted:  int(response.GetAttempted()),
		Delivered:  int(response.GetDelivered()),
		Skipped:    int(response.GetSkipped()),
		Overflowed: int(response.GetOverflowed()),
	}, mailboxClientError(err)
}

func (c *grpcConnection) Init(ctx context.Context, request InitRequest) (InitResult, error) {
	response, err := c.client.Init(ctx, &msgqpb.InitRequest{Header: headerToProto(request.Header), Restore: request.Restore, Ack: request.Ack, Primary: request.Primary})
	if err != nil {
		return InitResult{}, mailboxClientError(err)
	}
	if response == nil || response.GetReceivedSeq() < 0 {
		return InitResult{}, ErrInvalidMailboxResponse
	}
	return InitResult{UpMsgSeq: response.GetReceivedSeq()}, nil
}

func (c *grpcConnection) Rec(ctx context.Context, request RecRequest) (RecResult, error) {
	response, err := c.client.Rec(ctx, &msgqpb.RecRequest{Header: headerToProto(request.Header), Seq: request.Seq, Ack: request.Ack})
	if err != nil {
		return RecResult{}, mailboxClientError(err)
	}
	if response == nil || response.GetReceivedSeq() < request.Seq || response.GetAck() != request.Ack {
		return RecResult{}, ErrInvalidMailboxResponse
	}
	return RecResult{UpMsgSeq: response.GetReceivedSeq(), Ack: response.GetAck()}, nil
}

func (c *grpcConnection) FetchSome(ctx context.Context, request FetchSomeRequest) (FetchSomeResult, error) {
	response, err := c.client.FetchSome(ctx, &msgqpb.FetchSomeRequest{Header: headerToProto(request.Header), Max: int32(request.Max)})
	if err != nil {
		return FetchSomeResult{}, mailboxClientError(err)
	}
	if response == nil || len(response.GetMessages()) == 0 {
		return FetchSomeResult{}, ErrInvalidMailboxResponse
	}
	messages := response.GetMessages()
	result := FetchSomeResult{Messages: make([]FetchResult, len(messages))}
	var previousSeq int64
	for index, message := range messages {
		if message == nil || message.GetSeq() <= previousSeq {
			return FetchSomeResult{}, ErrInvalidMailboxResponse
		}
		result.Messages[index] = FetchResult{Seq: message.GetSeq(), Payload: message.GetPayload()}
		previousSeq = message.GetSeq()
	}
	return result, nil
}

func (c *grpcConnection) Confirm(ctx context.Context, request ConfirmRequest) error {
	response, err := c.client.Confirm(ctx, &msgqpb.ConfirmRequest{Header: headerToProto(request.Header), Ack: request.Ack})
	if err != nil {
		return mailboxClientError(err)
	}
	if response == nil {
		return ErrInvalidMailboxResponse
	}
	return nil
}

func (c *grpcConnection) TerminateSession(ctx context.Context, header SessionHeader) error {
	response, err := c.client.TerminateSession(ctx, &msgqpb.TerminateSessionRequest{Header: headerToProto(header)})
	if err != nil {
		return mailboxClientError(err)
	}
	if response == nil {
		return ErrInvalidMailboxResponse
	}
	return nil
}

func (c *grpcConnection) Close() error {
	return c.connection.Close()
}

func headerToProto(header SessionHeader) *msgqpb.SessionHeader {
	return &msgqpb.SessionHeader{
		RoleId:       header.RoleID,
		SessionId:    header.SessionID,
		ConnectionId: header.ConnectionID,
	}
}

// Sender 按当前会话 owner 路由下行消息，并复用每个 endpoint 的连接直到 Close。
type Sender struct {
	router *SessionRouter
	roles  *RoleRouter
	dialer Dialer

	mu          sync.Mutex
	connections map[string]Connection
	closed      bool
}

// NewSender 创建按 SessionID 或 RoleID 显式路由的 msgq 发送器。
func NewSender(router *SessionRouter, roles *RoleRouter, dialer Dialer) (*Sender, error) {
	if router == nil {
		return nil, ErrSessionRouterNil
	}
	if roles == nil {
		return nil, ErrRoleRouterNil
	}
	if dialer == nil {
		return nil, ErrDialerNil
	}
	return &Sender{
		router:      router,
		roles:       roles,
		dialer:      dialer,
		connections: make(map[string]Connection),
	}, nil
}

// PubSession 向精确 SessionID 投递 BON 编码载荷。
func (s *Sender) PubSession(ctx context.Context, sessionID int64, payload []byte) (PubResult, error) {
	if s == nil {
		return PubResult{}, ErrSenderClosed
	}
	target, err := s.router.ResolveSession(ctx, sessionID)
	if err != nil {
		return PubResult{}, err
	}
	return s.publish(ctx, target, payload)
}

// PubRole 向 RoleID 归属 MsgQ 的全部角色邮箱投递，热路由不查询 Redis。
// 明确拒绝旧归属时只清理对应缓存，留待下一次调用重新寻址；任何错误均不自动重发。
func (s *Sender) PubRole(ctx context.Context, roleID int64, payload []byte) (BroadcastResult, error) {
	if s == nil {
		return BroadcastResult{}, ErrSenderClosed
	}
	target, err := s.roles.Resolve(ctx, roleID)
	if err != nil {
		return BroadcastResult{}, err
	}
	connection, err := s.connection(ctx, target.Instance)
	if err != nil {
		return BroadcastResult{}, err
	}
	result, err := connection.PubRole(ctx, target.Binding, payload)
	if errors.Is(err, ErrSessionChanged) {
		s.roles.Invalidate(target.Binding)
	}
	return result, err
}

// JoinTopics 将 ownerSessionID 的当前连接加入其 MsgQ 实例的本地 Topic 索引。
func (s *Sender) JoinTopics(ctx context.Context, ownerSessionID int64, topics ...string) error {
	if s == nil {
		return ErrSenderClosed
	}
	target, err := s.router.ResolveSession(ctx, ownerSessionID)
	if err != nil {
		return err
	}
	connection, err := s.connection(ctx, target.Instance)
	if err != nil {
		return err
	}
	return connection.JoinTopics(ctx, TopicRequest{
		Header: SessionHeader{
			RoleID:       target.Binding.RoleID,
			SessionID:    target.Binding.SessionID,
			ConnectionID: target.Binding.ConnectionID,
		},
		Topics: topics,
	})
}

// BroadcastTopic 向 ownerSessionID 当前绑定的 MsgQ 实例执行本地 Topic 广播。
// 该方法不会扫描其他 MsgQ 实例；跨实例分发必须由业务层显式完成。
func (s *Sender) BroadcastTopic(ctx context.Context, ownerSessionID int64, request BroadcastTopicRequest) (BroadcastResult, error) {
	if s == nil {
		return BroadcastResult{}, ErrSenderClosed
	}
	target, err := s.router.ResolveSession(ctx, ownerSessionID)
	if err != nil {
		return BroadcastResult{}, err
	}
	connection, err := s.connection(ctx, target.Instance)
	if err != nil {
		return BroadcastResult{}, err
	}
	return connection.BroadcastTopic(ctx, request)
}

func (s *Sender) publish(ctx context.Context, target SessionTarget, payload []byte) (PubResult, error) {
	connection, err := s.connection(ctx, target.Instance)
	if err != nil {
		return PubResult{}, err
	}
	return connection.PubSession(ctx, target.Binding.SessionID, payload)
}

// Close 关闭所有缓存 endpoint 的连接。
func (s *Sender) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	connections := s.connections
	s.connections = nil
	s.mu.Unlock()

	var errs []error
	for _, connection := range connections {
		errs = append(errs, connection.Close())
	}
	return errors.Join(errs...)
}

func (s *Sender) connection(ctx context.Context, instance discovery.Instance) (Connection, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrSenderClosed
	}
	if connection := s.connections[instance.Endpoint]; connection != nil {
		s.mu.Unlock()
		return connection, nil
	}
	s.mu.Unlock()

	created, err := s.dialer.Dial(ctx, instance)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = created.Close()
		return nil, ErrSenderClosed
	}
	if connection := s.connections[instance.Endpoint]; connection != nil {
		_ = created.Close()
		return connection, nil
	}
	s.connections[instance.Endpoint] = created
	return created, nil
}
