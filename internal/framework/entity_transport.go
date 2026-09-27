package framework

import (
	"errors"

	"github.com/ngnl47/tx/internal/actorhost"
	"github.com/ngnl47/tx/internal/actorrpc"
	"github.com/ngnl47/tx/modules/rpc"
	"google.golang.org/grpc"
)

// startEntityTransport 在服务发现给出本次启动身份后，绑定同一宿主的两个接收入口。
// 由框架启动阶段调用；连接与宿主归框架持有，不能在业务工厂里重复组装。
func (server *Server) startEntityTransport(local *actorhost.Runtime, url, namespace string, maxPending int) error {
	runtime := server.runtime
	instance := runtime.instanceSnapshot().ID
	if instance == "" || runtime.entities != nil {
		return errors.New("framework: entity transport requires a unique startup identity and one host")
	}
	// 先注册尚未监听的共享 gRPC，失败时不创建 NATS 连接。
	if err := runtime.rpc.Register(func(registrar grpc.ServiceRegistrar) error {
		receiver := actorrpc.NewDirectReceiver(instance, local)
		return rpc.RegisterDeliveryService(registrar, receiver, receiver)
	}); err != nil {
		return err
	}
	transport, err := actorrpc.ConnectNATS(url, namespace, instance, local, maxPending)
	if err != nil {
		return err
	}
	runtime.entities = local
	runtime.actorRPC = transport
	return nil
}
