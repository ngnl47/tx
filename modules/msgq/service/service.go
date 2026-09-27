// Package msgqservice 提供不依赖框架门面的标准会话邮箱服务。
package msgqservice

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ngnl47/tx/lifecycle"
	txlog "github.com/ngnl47/tx/log"
	txmetrics "github.com/ngnl47/tx/metrics"
	"github.com/ngnl47/tx/modules/discovery"
	txmsgq "github.com/ngnl47/tx/modules/msgq"
	"github.com/ngnl47/tx/modules/msgq/msgqredis"
	txrpc "github.com/ngnl47/tx/modules/rpc"
	txredis "github.com/ngnl47/tx/storage/redis"
	"google.golang.org/grpc"
)

// Dependencies 是 msgq 启动所需的最小基础能力集合。
type Dependencies struct {
	// Tasks 托管邮箱清理等后台任务。
	Tasks lifecycle.Tasks
	// RPC 注册 MailboxService；RPC Host 的监听生命周期仍由外层框架持有。
	RPC txrpc.Registrar
	// Instance 提供未显式配置时使用的当前物理实例 ID。
	Instance discovery.Instance
	// Redis 在未显式提供 Store 时创建路由会话存储。
	Redis txredis.ScriptRoutes
}

// Options 描述会话邮箱存储、积压边界和离线回收策略。
type Options struct {
	InstanceID                   string
	Store                        txmsgq.SessionStore
	SessionStore                 msgqredis.Options
	MaxBacklogMessages           int
	MaxBacklogBytes              int
	OfflineTimeout               time.Duration
	TerminateDeadSessionInterval time.Duration
}

// Service 承载单个进程内的会话邮箱运行时。
type Service struct {
	options      Options
	dependencies Dependencies

	mu            sync.RWMutex
	client        txmsgq.SessionClient
	mailbox       *txmsgq.Server
	removeMetrics func()
}

// New 校验配置和显式依赖，构造时不启动 goroutine。
func New(options Options, dependencies Dependencies) (*Service, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	if dependencies.Tasks == nil || dependencies.RPC == nil {
		return nil, fmt.Errorf("msgq service: lifecycle and RPC host are required")
	}
	if options.Store == nil && dependencies.Redis == nil {
		return nil, fmt.Errorf("msgq service: Redis routes are unavailable")
	}
	return &Service{options: options, dependencies: dependencies}, nil
}

// Start 创建邮箱、注册 gRPC 服务并启动后台回收任务。
func (service *Service) Start(context.Context) error {
	if service == nil {
		return fmt.Errorf("msgq service: service is nil")
	}
	instanceID := strings.TrimSpace(service.options.InstanceID)
	if instanceID == "" {
		instanceID = service.dependencies.Instance.ID
	}
	store := service.options.Store
	var err error
	if store == nil {
		store, err = msgqredis.New(service.dependencies.Redis, service.options.SessionStore)
		if err != nil {
			return err
		}
	}
	mailbox, err := txmsgq.NewServer(txmsgq.ServerOptions{
		InstanceID:                   instanceID,
		Store:                        store,
		MaxBacklogMessages:           service.options.MaxBacklogMessages,
		MaxBacklogBytes:              service.options.MaxBacklogBytes,
		OfflineTimeout:               service.options.OfflineTimeout,
		TerminateDeadSessionInterval: service.options.TerminateDeadSessionInterval,
	})
	if err != nil {
		return err
	}
	client, err := txmsgq.NewLocalSessionClient(mailbox)
	if err != nil {
		return err
	}
	if err := service.dependencies.RPC.Register(func(registrar grpc.ServiceRegistrar) error {
		return txmsgq.RegisterMailboxService(registrar, mailbox)
	}); err != nil {
		return err
	}
	if err := mailbox.Start(service.dependencies.Tasks); err != nil {
		return err
	}
	removeMetrics := txmetrics.RegisterMailboxSnapshot(mailbox.BacklogSnapshot)
	service.mu.Lock()
	service.client = client
	service.mailbox = mailbox
	service.removeMetrics = removeMetrics
	service.mu.Unlock()
	txlog.Info(
		"msgq service initialized",
		"endpoint", service.dependencies.RPC.Endpoint(),
		"maxBacklogMessages", service.options.MaxBacklogMessages,
		"maxBacklogBytes", service.options.MaxBacklogBytes,
	)
	return nil
}

// Stop 停止邮箱并释放本地角色归属；后台任务由 Tasks 生命周期等待退出。
func (service *Service) Stop(ctx context.Context) error {
	if service == nil {
		return nil
	}
	service.mu.Lock()
	service.client = nil
	mailbox := service.mailbox
	service.mailbox = nil
	removeMetrics := service.removeMetrics
	service.removeMetrics = nil
	service.mu.Unlock()
	if removeMetrics != nil {
		removeMetrics()
	}
	if mailbox != nil {
		return mailbox.Stop(ctx)
	}
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
	if options.MaxBacklogMessages <= 0 || options.MaxBacklogBytes < 0 || options.OfflineTimeout < 0 || options.TerminateDeadSessionInterval < 0 {
		return fmt.Errorf("msgq service: invalid options")
	}
	return nil
}
