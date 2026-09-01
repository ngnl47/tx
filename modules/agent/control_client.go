package agent

import (
	"context"
	"errors"
	"sync"

	agentpb "github.com/ngnl47/tx/modules/agent/pb"
	"github.com/ngnl47/tx/modules/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	ErrSessionControlDirectoryNil = errors.New("agent: session control directory is nil")
	ErrSessionControlDialerNil    = errors.New("agent: session control dialer is nil")
	ErrSessionControlClientNil    = errors.New("agent: session control client is nil")
	ErrSessionControlResponse     = errors.New("agent: invalid session control response")
)

const agentServiceName = "agent"

// SessionControlClient 控制一个指定 Agent 实例中的本地 Session。
type SessionControlClient interface {
	DisconnectSession(context.Context, int64, string) (SessionControlResult, error)
	TerminateSession(context.Context, int64, string) (SessionControlResult, error)
	Close() error
}

// SessionControlDialer 建立到一个指定 Agent 实例的控制客户端。
type SessionControlDialer interface {
	DialSessionControl(context.Context, discovery.Instance) (SessionControlClient, error)
}

// GRPCSessionControlDialer 使用显式凭据建立 Agent 控制连接。
type GRPCSessionControlDialer struct {
	credentials credentials.TransportCredentials
	options     []grpc.DialOption
}

// NewGRPCSessionControlDialer 创建 Agent 控制 gRPC dialer。
func NewGRPCSessionControlDialer(transportCredentials credentials.TransportCredentials, options ...grpc.DialOption) (*GRPCSessionControlDialer, error) {
	if transportCredentials == nil {
		return nil, ErrSessionControlDialerNil
	}
	return &GRPCSessionControlDialer{
		credentials: transportCredentials,
		options:     append([]grpc.DialOption(nil), options...),
	}, nil
}

func (dialer *GRPCSessionControlDialer) DialSessionControl(_ context.Context, instance discovery.Instance) (SessionControlClient, error) {
	if dialer == nil || dialer.credentials == nil {
		return nil, ErrSessionControlDialerNil
	}
	options := make([]grpc.DialOption, 0, len(dialer.options)+1)
	options = append(options, grpc.WithTransportCredentials(dialer.credentials))
	options = append(options, dialer.options...)
	connection, err := grpc.NewClient("passthrough:///"+instance.Endpoint, options...)
	if err != nil {
		return nil, err
	}
	return &grpcSessionControlClient{connection: connection, client: agentpb.NewAgentControlServiceClient(connection)}, nil
}

type grpcSessionControlClient struct {
	connection *grpc.ClientConn
	client     agentpb.AgentControlServiceClient
}

func (client *grpcSessionControlClient) DisconnectSession(ctx context.Context, sessionID int64, reason string) (SessionControlResult, error) {
	response, err := client.client.DisconnectSession(ctx, &agentpb.SessionControlRequest{SessionId: sessionID, Reason: reason})
	return sessionControlResultFromProto(sessionID, response, err)
}

func (client *grpcSessionControlClient) TerminateSession(ctx context.Context, sessionID int64, reason string) (SessionControlResult, error) {
	response, err := client.client.TerminateSession(ctx, &agentpb.SessionControlRequest{SessionId: sessionID, Reason: reason})
	return sessionControlResultFromProto(sessionID, response, err)
}

func (client *grpcSessionControlClient) Close() error {
	if client == nil || client.connection == nil {
		return nil
	}
	return client.connection.Close()
}

func sessionControlResultFromProto(sessionID int64, response *agentpb.SessionControlResponse, err error) (SessionControlResult, error) {
	if err != nil {
		return SessionControlResult{SessionID: sessionID}, err
	}
	if response == nil || response.GetSessionId() != sessionID || response.GetConnectionId() < 0 || (response.GetHandled() && response.GetConnectionId() <= 0) {
		return SessionControlResult{SessionID: sessionID}, ErrSessionControlResponse
	}
	return SessionControlResult{
		SessionID:    response.GetSessionId(),
		ConnectionID: response.GetConnectionId(),
		Handled:      response.GetHandled(),
	}, nil
}

// SessionControlInstanceResult 保留一个 Agent 实例的控制结果。
type SessionControlInstanceResult struct {
	InstanceID string
	SessionControlResult
	Err error
}

// SessionControlBroadcastResult 保留一次控制广播的逐实例结果。
type SessionControlBroadcastResult struct {
	Instances []SessionControlInstanceResult
}

// Handled 报告是否至少一个 Agent 命中了目标 Session。
func (result SessionControlBroadcastResult) Handled() bool {
	for _, instance := range result.Instances {
		if instance.Handled {
			return true
		}
	}
	return false
}

// Uncertain 报告是否没有实例确认命中，且至少一个实例调用失败。
func (result SessionControlBroadcastResult) Uncertain() bool {
	if result.Handled() {
		return false
	}
	for _, instance := range result.Instances {
		if instance.Err != nil {
			return true
		}
	}
	return false
}

// SessionControlBroadcaster 向服务发现中的全部运行中 Agent 并行发送 Session 控制请求。
type SessionControlBroadcaster struct {
	directory discovery.Directory
	dialer    SessionControlDialer
}

// NewSessionControlBroadcaster 创建不访问 Redis 的 Agent 控制广播器。
func NewSessionControlBroadcaster(directory discovery.Directory, dialer SessionControlDialer) (*SessionControlBroadcaster, error) {
	if directory == nil {
		return nil, ErrSessionControlDirectoryNil
	}
	if dialer == nil {
		return nil, ErrSessionControlDialerNil
	}
	return &SessionControlBroadcaster{directory: directory, dialer: dialer}, nil
}

// DisconnectSession 向全部运行中 Agent 广播物理断连请求。
func (broadcaster *SessionControlBroadcaster) DisconnectSession(ctx context.Context, sessionID int64, reason string) SessionControlBroadcastResult {
	return broadcaster.broadcast(ctx, sessionID, reason, func(ctx context.Context, client SessionControlClient, sessionID int64, reason string) (SessionControlResult, error) {
		return client.DisconnectSession(ctx, sessionID, reason)
	})
}

// TerminateSession 向全部运行中 Agent 广播逻辑终止请求。
func (broadcaster *SessionControlBroadcaster) TerminateSession(ctx context.Context, sessionID int64, reason string) SessionControlBroadcastResult {
	return broadcaster.broadcast(ctx, sessionID, reason, func(ctx context.Context, client SessionControlClient, sessionID int64, reason string) (SessionControlResult, error) {
		return client.TerminateSession(ctx, sessionID, reason)
	})
}

type sessionControlCall func(context.Context, SessionControlClient, int64, string) (SessionControlResult, error)

func (broadcaster *SessionControlBroadcaster) broadcast(ctx context.Context, sessionID int64, reason string, call sessionControlCall) SessionControlBroadcastResult {
	instances := broadcaster.directory.RunningInstances(agentServiceName)
	result := SessionControlBroadcastResult{Instances: make([]SessionControlInstanceResult, len(instances))}
	var wait sync.WaitGroup
	wait.Add(len(instances))
	for index, instance := range instances {
		go func() {
			defer wait.Done()
			current := SessionControlInstanceResult{InstanceID: instance.ID, SessionControlResult: SessionControlResult{SessionID: sessionID}}
			client, err := broadcaster.dialer.DialSessionControl(ctx, instance)
			if err != nil {
				current.Err = err
				result.Instances[index] = current
				return
			}
			if client == nil {
				current.Err = ErrSessionControlClientNil
				result.Instances[index] = current
				return
			}
			defer func() { _ = client.Close() }()
			current.SessionControlResult, current.Err = call(ctx, client, sessionID, reason)
			result.Instances[index] = current
		}()
	}
	wait.Wait()
	return result
}
