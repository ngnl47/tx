package rpc

import (
	"context"
	"errors"

	"google.golang.org/grpc"
)

// Access 提供进程共享的协议注册与路由客户端；连接生命周期由框架管理。
// Client 需要同时启用 Discovery，不经过 Actor 寻址或邮箱。
type Access interface {
	Registrar
	Client(Route) grpc.ClientConnInterface
}

var ErrGRPCUnsupported = errors.New("rpc: connection does not support generated gRPC clients")

// Client 将生成的 gRPC 客户端接到既有实例选择与连接缓存，不向业务暴露 Close。
// 每次新调用解析当前实例；已经建立的流固定在原连接，不自动重放或迁移。
func (s *Sender) Client(route Route) grpc.ClientConnInterface {
	return routedClient{sender: s, route: route}
}

type routedClient struct {
	sender *Sender
	route  Route
}

func (c routedClient) connection(ctx context.Context) (grpc.ClientConnInterface, error) {
	if c.sender == nil {
		return nil, ErrDirectoryNil
	}
	instance, err := Resolve(c.sender.directory, c.route)
	if err != nil {
		return nil, err
	}
	connection, err := c.sender.connection(ctx, instance)
	if err != nil {
		return nil, err
	}
	client, ok := connection.(grpc.ClientConnInterface)
	if !ok {
		return nil, ErrGRPCUnsupported
	}
	return client, nil
}

func (c routedClient) Invoke(ctx context.Context, method string, args, reply any, options ...grpc.CallOption) error {
	connection, err := c.connection(ctx)
	if err != nil {
		return err
	}
	return connection.Invoke(ctx, method, args, reply, options...)
}

func (c routedClient) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, options ...grpc.CallOption) (grpc.ClientStream, error) {
	connection, err := c.connection(ctx)
	if err != nil {
		return nil, err
	}
	return connection.NewStream(ctx, desc, method, options...)
}

func (c *grpcConnection) Invoke(ctx context.Context, method string, args, reply any, options ...grpc.CallOption) error {
	return c.connection.Invoke(ctx, method, args, reply, options...)
}

func (c *grpcConnection) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, options ...grpc.CallOption) (grpc.ClientStream, error) {
	return c.connection.NewStream(ctx, desc, method, options...)
}
