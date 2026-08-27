// Package msgq 提供标准的客户端会话邮箱服务 adapter。
package msgq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	txlog "github.com/ngnl47/tx/log"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
	txserver "github.com/ngnl47/tx/server"
	"github.com/ngnl47/tx/server/adapters/msgqredis"
	"google.golang.org/grpc"
)

// ServiceName 是 msgq 在启动参数和服务发现中的稳定名称。
const ServiceName = "msgq"

// Options 描述会话邮箱存储、积压边界和离线回收策略。
type Options struct {
	// InstanceID 覆盖框架实例 ID；空字符串使用服务发现中的当前实例 ID。
	InstanceID string
	// Store 显式提供会话存储；设置后不再从 SessionStore 创建 Redis 实现。
	Store txmsgq.SessionStore
	// SessionStore 配置默认 Redis 会话存储。
	SessionStore msgqredis.Options
	// MaxBacklogMessages 限制单会话未确认下行消息条数。
	MaxBacklogMessages int
	// MaxBacklogBytes 限制单会话未确认下行载荷字节数；零表示不按字节限制。
	MaxBacklogBytes int
	// OfflineTimeout 是离线会话保留时间；零表示不按离线时间回收。
	OfflineTimeout time.Duration
	// SweepInterval 是离线会话扫描间隔；零值使用 msgq 的默认间隔。
	SweepInterval time.Duration
}

// OptionsProvider 在 msgq 被选中时转换项目配置。
type OptionsProvider func() (Options, error)

type Service struct {
	options  Options
	provider OptionsProvider

	mu     sync.RWMutex
	client txmsgq.SessionClient
}

// New 使用已经转换完成的强类型配置创建 msgq 服务。
func New(options Options) (*Service, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	return &Service{options: options}, nil
}

// NewConfigured 将项目配置转换推迟到当前进程选中该服务之后。
// 因此，未选中的服务不会阻止其他有效进程启动。
func NewConfigured(provider OptionsProvider) (*Service, error) {
	if provider == nil {
		return nil, fmt.Errorf("msgq service: options provider is nil")
	}
	return &Service{provider: provider}, nil
}

// Spec 声明 msgq 的 RPC 与 Redis 能力依赖。
func (service *Service) Spec() txserver.Spec {
	return txserver.Spec{
		Name:     ServiceName,
		Requires: []txserver.CModID{txserver.CModRPC, txserver.CModRedisStorage},
	}
}

// Start 创建会话邮箱、注册共享 gRPC 服务并接入进程生命周期。
func (service *Service) Start(runtime txserver.Context) error {
	if runtime == nil || runtime.Tasks() == nil || runtime.RPC() == nil {
		return fmt.Errorf("msgq service: lifecycle and RPC host are required")
	}
	if err := service.configure(); err != nil {
		return err
	}
	instanceID := strings.TrimSpace(service.options.InstanceID)
	if instanceID == "" {
		instanceID = runtime.Instance().ID
	}
	store := service.options.Store
	var err error
	if store == nil {
		advanced := runtime.Advanced()
		if advanced == nil || advanced.Redis() == nil {
			return fmt.Errorf("msgq service: Redis routes are unavailable")
		}
		store, err = msgqredis.New(advanced.Redis(), service.options.SessionStore)
		if err != nil {
			return err
		}
	}
	mailbox, err := txmsgq.NewServer(txmsgq.ServerOptions{
		InstanceID:         instanceID,
		Store:              store,
		MaxBacklogMessages: service.options.MaxBacklogMessages,
		MaxBacklogBytes:    service.options.MaxBacklogBytes,
		OfflineTimeout:     service.options.OfflineTimeout,
		SweepInterval:      service.options.SweepInterval,
	})
	if err != nil {
		return err
	}
	client, err := txmsgq.NewLocalSessionClient(mailbox)
	if err != nil {
		return err
	}
	if err := runtime.RPC().Register(func(registrar grpc.ServiceRegistrar) error {
		return txmsgq.RegisterMailboxService(registrar, mailbox)
	}); err != nil {
		return err
	}
	if err := mailbox.Start(runtime.Tasks()); err != nil {
		return err
	}
	service.mu.Lock()
	service.client = client
	service.mu.Unlock()
	txlog.Info("msgq service initialized", "endpoint", runtime.RPC().Endpoint(), "maxBacklogMessages", service.options.MaxBacklogMessages, "maxBacklogBytes", service.options.MaxBacklogBytes)
	return nil
}

func (service *Service) configure() error {
	if service == nil {
		return fmt.Errorf("msgq service: service is nil")
	}
	if service.provider == nil {
		return validateOptions(service.options)
	}
	options, err := service.provider()
	if err != nil {
		return fmt.Errorf("msgq service: configure: %w", err)
	}
	if err := validateOptions(options); err != nil {
		return err
	}
	service.options = options
	return nil
}

// Stop 清理本地会话客户端引用；邮箱后台任务由根生命周期等待退出。
func (service *Service) Stop(context.Context) error {
	if service == nil {
		return nil
	}
	service.mu.Lock()
	service.client = nil
	service.mu.Unlock()
	return nil
}

// SessionClient 返回同进程 Agent 可复用的本地会话客户端。
func (service *Service) SessionClient() (txmsgq.SessionClient, error) {
	if service == nil {
		return nil, fmt.Errorf("msgq service: local session client is unavailable")
	}
	service.mu.RLock()
	client := service.client
	service.mu.RUnlock()
	if client == nil {
		return nil, fmt.Errorf("msgq service: local session client is unavailable")
	}
	return client, nil
}

func validateOptions(options Options) error {
	if options.Store == nil && (strings.TrimSpace(options.SessionStore.RouteType) == "" || strings.TrimSpace(options.SessionStore.Prefix) == "" || options.SessionStore.TTL <= 0) {
		return fmt.Errorf("msgq service: session store is not configured")
	}
	if options.MaxBacklogMessages <= 0 || options.MaxBacklogBytes < 0 || options.OfflineTimeout < 0 || options.SweepInterval < 0 {
		return fmt.Errorf("msgq service: invalid options")
	}
	return nil
}
