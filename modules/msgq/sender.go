package msgq

import (
	"context"
	"errors"
	"sync"

	"github.com/ngnl47/tx/internal/instanceconn"
	txlog "github.com/ngnl47/tx/log"
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
	SendSession(context.Context, int64, []byte) error
	SendRole(context.Context, RoleBinding, []byte) error
	OnRoleRejection(func(context.Context, RoleBinding, []byte))
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
	result := &grpcConnection{
		connection: connection,
		client:     msgqpb.NewMailboxServiceClient(connection),
	}
	return result, nil
}

type grpcConnection struct {
	connection  *grpc.ClientConn
	client      msgqpb.MailboxServiceClient
	mailboxMu   sync.Mutex
	mailbox     *grpcMailboxStream
	publisherMu sync.Mutex
	publisher   *grpcPublishStream
	rejectRole  func(context.Context, RoleBinding, []byte)
}

// OnRoleRejection 在连接开始发布前设置明确未入邮箱的角色消息回调。
func (c *grpcConnection) OnRoleRejection(reject func(context.Context, RoleBinding, []byte)) {
	c.rejectRole = reject
}

func (c *grpcConnection) OpenMailboxStream(ctx context.Context, header SessionHeader) (MailboxStreamSession, error) {
	c.mailboxMu.Lock()
	stream := c.mailbox
	if stream != nil {
		select {
		case <-stream.done:
			stream = nil
		default:
		}
	}
	if stream == nil {
		var err error
		stream, err = newGRPCMailboxStream(c.client)
		if err != nil {
			c.mailboxMu.Unlock()
			return nil, err
		}
		c.mailbox = stream
	}
	c.mailboxMu.Unlock()
	return stream.open(ctx, header)
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
	response, err := c.client.Init(ctx, &msgqpb.InitRequest{Header: headerToProto(request.Header), Restore: request.Restore, Ack: request.Ack})
	if err != nil {
		return InitResult{}, mailboxClientError(err)
	}
	if response == nil || response.GetReceivedSeq() < 0 {
		return InitResult{}, ErrInvalidMailboxResponse
	}
	return InitResult{UpMsgSeq: response.GetReceivedSeq()}, nil
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
	c.mailboxMu.Lock()
	mailbox := c.mailbox
	c.mailbox = nil
	c.mailboxMu.Unlock()
	if mailbox != nil {
		mailbox.fail(ErrSenderClosed)
	}
	if err := c.closePublisher(); err != nil {
		_ = c.connection.Close()
		return err
	}
	return c.connection.Close()
}

func headerToProto(header SessionHeader) *msgqpb.SessionHeader {
	return &msgqpb.SessionHeader{
		RoleId:       header.RoleID,
		SessionId:    header.SessionID,
		ConnectionId: header.ConnectionID,
	}
}

// Sender 按当前会话 owner 路由下行消息，并复用当前实例的连接。
type Sender struct {
	router *SessionRouter
	roles  *RoleRouter
	dialer Dialer

	connections *instanceconn.Cache[Connection]
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
		connections: instanceconn.New[Connection](router.directory),
	}, nil
}

// PubSession 向精确 SessionID 投递 BON 编码载荷，热邮箱路由不查询 Redis。
// 明确邮箱失效时清缓存；任何投递错误都不自动重发。
func (s *Sender) PubSession(ctx context.Context, sessionID int64, payload []byte) (PubResult, error) {
	if s == nil {
		return PubResult{}, ErrSenderClosed
	}
	instance, err := s.router.ResolveMailbox(ctx, sessionID)
	if err != nil {
		return PubResult{}, err
	}
	connection, err := s.connection(ctx, instance)
	if err != nil {
		return PubResult{}, err
	}
	result, err := connection.PubSession(ctx, sessionID, payload)
	if errors.Is(err, ErrSessionOffline) || errors.Is(err, ErrSessionChanged) {
		s.router.invalidateMailbox(sessionID, instance.ID)
	}
	return result, err
}

// SendSession 将下行载荷交给实际邮箱 owner 的本地有界发布队列，不等待写流或 MsgQ 逐项回执。
func (s *Sender) SendSession(ctx context.Context, sessionID int64, payload []byte) error {
	if s == nil {
		return ErrSenderClosed
	}
	instance, err := s.router.ResolveMailbox(ctx, sessionID)
	if err != nil {
		return err
	}
	connection, err := s.connection(ctx, instance)
	if err != nil {
		return err
	}
	return connection.SendSession(ctx, sessionID, payload)
}

// PubRole 向 RoleID 归属 MsgQ 的全部角色邮箱投递，热路由不查询 Redis。
// 仅在明确尚未入队的归属拒绝后重新寻址并重试一次；其他错误不重发。
func (s *Sender) PubRole(ctx context.Context, roleID int64, payload []byte) (BroadcastResult, error) {
	if s == nil {
		return BroadcastResult{}, ErrSenderClosed
	}
	for attempt := 0; ; attempt++ {
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
		if attempt == 0 && errors.Is(err, ErrRoleBindingChanged) {
			continue
		}
		return result, err
	}
}

// SendRole 将下行载荷交给角色归属 MsgQ 的本地有界发布队列，不等待写流或逐项回执。
// MsgQ 明确拒绝旧归属且确认未入邮箱时，后台重新寻址并单次投递该载荷。
func (s *Sender) SendRole(ctx context.Context, roleID int64, payload []byte) error {
	if s == nil {
		return ErrSenderClosed
	}
	target, err := s.roles.Resolve(ctx, roleID)
	if err != nil {
		return err
	}
	connection, err := s.connection(ctx, target.Instance)
	if err != nil {
		return err
	}
	return connection.SendRole(ctx, target.Binding, payload)
}

func (s *Sender) readdressRejectedRole(ctx context.Context, rejected RoleBinding, payload []byte) {
	s.roles.Invalidate(rejected)
	if len(payload) == 0 {
		txlog.ErrorContext(ctx, "async role publish rejection lacks payload", "roleID", rejected.RoleID)
		return
	}
	target, err := s.roles.Resolve(ctx, rejected.RoleID)
	if err == nil {
		var connection Connection
		connection, err = s.connection(ctx, target.Instance)
		if err == nil {
			_, err = connection.PubRole(ctx, target.Binding, payload)
		}
	}
	if err != nil {
		txlog.ErrorContext(ctx, "async role publish readdress failed", "roleID", rejected.RoleID, "err", err)
	}
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

// Close 关闭所有缓存的实例连接。
func (s *Sender) Close() error {
	if s == nil {
		return nil
	}
	return s.connections.Close()
}

func (s *Sender) connection(ctx context.Context, instance discovery.Instance) (Connection, error) {
	connection, err := s.connections.Get(ctx, instance, func(ctx context.Context, instance discovery.Instance) (Connection, error) {
		connection, err := s.dialer.Dial(ctx, instance)
		if err == nil {
			connection.OnRoleRejection(s.readdressRejectedRole)
		}
		return connection, err
	})
	if errors.Is(err, instanceconn.ErrClosed) {
		return nil, ErrSenderClosed
	}
	if errors.Is(err, instanceconn.ErrStaleInstance) {
		return nil, ErrRouteUnavailable
	}
	return connection, err
}
