// Package msgq 将 Core 会话邮箱服务注册到 Ant 框架门面。
package msgq

import (
	"context"
	"fmt"
	"sync"

	"github.com/ngnl47/tx/ant"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
	msgqservice "github.com/ngnl47/tx/modules/msgq/service"
)

// Options 是标准会话邮箱的强类型配置。
type Options = msgqservice.Options

// OptionsProvider 只在 msgq 被本轮进程选中时转换项目配置。
type OptionsProvider func() (Options, error)

// Service 保存 msgq 的静态配置提供者和启动后的本地会话客户端入口。
type Service struct {
	provider OptionsProvider

	mu      sync.RWMutex
	runtime *msgqservice.Service
}

// New 使用已经转换完成的配置创建 msgq 注册器。
func New(options Options) *Service {
	return &Service{provider: func() (Options, error) { return options, nil }}
}

// NewConfigured 推迟配置转换，未选中的 msgq 不会校验其业务参数。
func NewConfigured(provider OptionsProvider) (*Service, error) {
	if provider == nil {
		return nil, fmt.Errorf("ant msgq: options provider is nil")
	}
	return &Service{provider: provider}, nil
}

// Registration 使用项目指定的名称返回可交给 ant.Builder.Services 的静态注册信息。
func (service *Service) Registration(name string) ant.ServiceRegistration {
	return ant.ServiceRegistration{
		Spec: ant.ServiceSpec{
			Name:     name,
			Requires: []ant.CModID{ant.CModRPC, ant.CModRedisStorage, ant.CModDiscovery},
		},
		Factory: service.factory,
	}
}

func (service *Service) factory(runtime ant.Context) (ant.Service, error) {
	if service == nil || service.provider == nil {
		return nil, fmt.Errorf("ant msgq: service is nil")
	}
	options, err := service.provider()
	if err != nil {
		return nil, fmt.Errorf("ant msgq: configure: %w", err)
	}
	rpc := runtime.CModRPC()
	redis := runtime.CModRedisStorage()
	core, err := msgqservice.New(options, msgqservice.Dependencies{
		Tasks:    runtime.Tasks(),
		RPC:      rpc,
		Instance: runtime.Instance(),
		Redis:    redis.ScriptRoutes(),
	})
	if err != nil {
		return nil, err
	}
	return &serviceInstance{owner: service, core: core}, nil
}

// SessionClient 返回同进程 Agent 可复用的本地会话客户端。
func (service *Service) SessionClient() (txmsgq.SessionClient, error) {
	if service == nil {
		return nil, fmt.Errorf("ant msgq: local session client is unavailable")
	}
	service.mu.RLock()
	runtime := service.runtime
	service.mu.RUnlock()
	if runtime == nil {
		return nil, fmt.Errorf("ant msgq: local session client is unavailable")
	}
	return runtime.SessionClient()
}

type serviceInstance struct {
	owner *Service
	core  *msgqservice.Service
}

func (instance *serviceInstance) Start(ctx context.Context) error {
	if err := instance.core.Start(ctx); err != nil {
		return err
	}
	instance.owner.mu.Lock()
	instance.owner.runtime = instance.core
	instance.owner.mu.Unlock()
	return nil
}

func (instance *serviceInstance) Stop(ctx context.Context) error {
	instance.owner.mu.Lock()
	if instance.owner.runtime == instance.core {
		instance.owner.runtime = nil
	}
	instance.owner.mu.Unlock()
	return instance.core.Stop(ctx)
}
