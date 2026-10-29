package framework

import (
 "github.com/ngnl47/tx/modules/entitycache"
	"fmt"
	"github.com/ngnl47/tx/modules/tasks"
	"io"
	"maps"
	"strings"
	"time"

	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/txn"
	txstorage "github.com/ngnl47/tx/storage"
	storageconfig "github.com/ngnl47/tx/storage/config"
	txfusion "github.com/ngnl47/tx/storage/fusion"
)

// Config 是项目解析本地配置源后得到的框架配置。
// 私有装配内核不读取 YAML、环境变量或命令行参数。
type Config struct {
	// TaskQueue 绑定可靠任务资源；未选择该能力时不使用。
	TaskQueue tasks.Config
	// AppID 是当前应用的稳定身份，用于隔离同一基础设施上的不同项目。
	AppID string
	// Storage 描述 Redis 和 MongoDB 的连接路由。
	Storage StorageConfig
	// Runtime 描述生命周期、存储调度和事务等进程运行参数。
	Runtime RuntimeConfig
	// Log 描述框架结构化日志的级别、格式和输出目标。
	Log LogConfig
	// Actor 描述可选 Actor CMod 的运行参数；未装配该 CMod 时不会使用。
	Actor ActorConfig
	// Discovery 描述可选服务发现 CMod 的运行参数；未装配该 CMod 时不会使用。
	Discovery DiscoveryConfig
}

// LogConfig 描述框架日志输出策略。
type LogConfig struct {
	// Level 是最低输出级别，例如 debug、info、warn 或 error。
	Level string
	// Format 是日志编码格式，可选 console 或 json；空值使用 console。
	Format string
	// Output 接收框架日志；为 nil 时由装配层选择默认输出。
	Output io.Writer
}

// StorageConfig 描述进程可装配的持久化连接。
type StorageConfig struct {
	// Redis 是按存储名称索引的单节点或云代理连接配置。
	Redis storageconfig.RedisConfig
	// MongoDB 是按 routeType 索引的 MongoDB 连接路由表。
	MongoDB storageconfig.RouteTableConfig
}

// RuntimeConfig 描述进程运行期的通用组件参数。
type RuntimeConfig struct {
 // EntityCache 配置应用共享的实体回收预算；是否接入由生成代码决定。
 EntityCache entitycache.Config
	// Lifecycle 控制进程入口等待和关闭排空时间。
	Lifecycle LifecycleConfig
	// Fusion 控制 Redis 热数据和 MongoDB 冷数据的融合存储行为。
	Fusion txfusion.Config
	// DelayTask 控制 Fusion 延迟写任务的 Redis 存储。
	DelayTask DelayTaskConfig
	// Scheduler 控制延迟写任务的轮询、批量和失败重试。
	Scheduler SchedulerConfig
	// Health 控制外部依赖健康探测的周期和超时。
	Health HealthConfig
	// Transaction 控制可选单机事务 CMod 的运行参数。
	Transaction TransactionConfig
}

// LifecycleConfig 配置进程入口等待和 Fusion 本地登记排空的单阶段上限。
type LifecycleConfig struct {
	// DrainTimeout 是停止阶段等待入口任务和 Fusion 本地登记排空的最长时间；0 使用框架默认值。
	DrainTimeout time.Duration
}

// DelayTaskConfig 描述 Fusion 延迟写任务在 Redis 中的组织方式。
type DelayTaskConfig struct {
	// RouteType 引用 Storage.Redis 中已经配置的路由。
	RouteType string
	// Prefix 隔离当前应用的延迟任务键空间。
	Prefix string
	// Buckets 是延迟任务分桶数；0 使用框架默认值。
	Buckets int
	// Lease 是单批任务的消费租约时长；0 使用框架默认值。
	Lease time.Duration
}

// SchedulerConfig 描述 Fusion 延迟写调度器的消费和重试策略。
type SchedulerConfig struct {
	// BatchSize 是单轮最多领取的延迟写任务数；0 使用框架默认值。
	BatchSize int
	// PollInterval 是无到期任务时的轮询间隔；0 使用框架默认值。
	PollInterval time.Duration
	// RetryInitial 是消费失败后的首次退避时间；0 使用框架默认值。
	RetryInitial time.Duration
	// RetryMultiplier 是连续失败时的退避倍率；0 使用框架默认值。
	RetryMultiplier float64
	// RetryMax 是单次退避的时间上限；0 使用框架默认值。
	RetryMax time.Duration
	// RetryJitter 是退避抖动比例，取值范围为 0 到 1；0 表示不增加抖动。
	RetryJitter float64
}

// HealthConfig 描述外部依赖的周期性健康探测。
type HealthConfig struct {
	// Interval 是相邻两轮探测的时间间隔。
	Interval time.Duration
	// Timeout 是单轮探测的最长等待时间，不能大于 Interval。
	Timeout time.Duration
}

// TransactionConfig 描述可选单机事务运行时。
type TransactionConfig struct {
	// Module 是 txn 模块的调试、锁和执行参数。
	Module txn.Config
}

// ActorConfig 描述可选 Actor 集群运行时。
type ActorConfig struct {
	// Runtime 是 Actor 集群、远程通信和邮箱容量参数。
	Runtime txactor.Config
}

// DiscoveryConfig 描述可选服务发现运行时。
type DiscoveryConfig struct {
	// Etcd 是服务注册、租约和实例目录使用的 etcd 参数。
	Etcd discovery.EtcdConfig
}

// RunOptions 保存进程服务选择和部署期发布参数。这些值通常来自命令行参数或编排系统，而不是应用 YAML。
type RunOptions struct {
	// Services 是本进程需要启动的 L4 服务选择表达式。
	Services []string
	// InstanceVersion 是当前实例发布的代码或镜像版本。
	InstanceVersion string
	// InstanceEndpoint 是发布到服务发现的内部业务 RPC 地址。
	InstanceEndpoint string
	// InstanceEndpoints 是当前实例按稳定名称发布的客户端或项目公开地址。
	InstanceEndpoints map[string]string
	// RPCListen 是进程共享 gRPC Host 的监听地址。
	RPCListen string
	// ControlListen 是健康检查和排水控制面的监听地址。
	ControlListen string
}

func (config Config) Validate() error {
	if strings.TrimSpace(config.AppID) == "" {
		return fmt.Errorf("framework: app ID is required")
	}
	if err := config.Runtime.Lifecycle.validate(); err != nil {
		return err
	}
	return nil
}

func (config LifecycleConfig) validate() error {
	if config.DrainTimeout < 0 {
		return fmt.Errorf("framework: lifecycle drain timeout must be greater than or equal to 0")
	}
	return nil
}

func validateStorageRoutes(name string, routes storageconfig.RouteTableConfig) error {
	if len(routes.Routes) == 0 {
		return fmt.Errorf("framework: %s routes are required", name)
	}
	if err := routes.Validate(); err != nil {
		return fmt.Errorf("framework: %s routes: %w", name, err)
	}
	return nil
}

func (config Config) clone() Config {
	cloned := config
	cloned.Actor.Runtime.IdlePassivation = maps.Clone(config.Actor.Runtime.IdlePassivation)
	cloned.Storage.Redis = storageconfig.NewRedisConfig(config.Storage.Redis.Stores)
	cloned.Storage.MongoDB = storageconfig.NewRouteTableConfig(config.Storage.MongoDB.Routes)
	cloned.Discovery.Etcd.Endpoints = append([]string(nil), config.Discovery.Etcd.Endpoints...)
	return cloned
}

func (config DelayTaskConfig) validate() error {
	if config.Buckets < 0 {
		return fmt.Errorf("framework: delay task buckets must be greater than or equal to 0")
	}
	if config.Lease != 0 {
		if err := txstorage.ValidateMillisecondDuration(config.Lease); err != nil {
			return fmt.Errorf("framework: delay task lease: %w", err)
		}
	}
	return nil
}

func (config SchedulerConfig) validate() error {
	if config.BatchSize < 0 {
		return fmt.Errorf("framework: scheduler batch size must be greater than or equal to 0")
	}
	if config.RetryMultiplier != 0 && config.RetryMultiplier <= 1 {
		return fmt.Errorf("framework: scheduler retry multiplier must be greater than 1")
	}
	if config.RetryJitter < 0 || config.RetryJitter > 1 {
		return fmt.Errorf("framework: scheduler retry jitter must be between 0 and 1")
	}
	if config.RetryInitial > 0 && config.RetryMax > 0 && config.RetryMax < config.RetryInitial {
		return fmt.Errorf("framework: scheduler retry max must not be less than retry initial")
	}
	return nil
}

func (config HealthConfig) validate() error {
	if config.Interval <= 0 || config.Timeout <= 0 {
		return fmt.Errorf("framework: health interval and timeout must be greater than 0")
	}
	if config.Timeout > config.Interval {
		return fmt.Errorf("framework: health timeout must not be greater than interval")
	}
	return nil
}

func (config RuntimeConfig) storageOptions() storageOptions {
	return storageOptions{
		FusionDirtyTTL:           config.Fusion.EffectiveDirtyTTL(),
		DelayTaskRouteType:       config.DelayTask.RouteType,
		DelayTaskPrefix:          config.DelayTask.Prefix,
		DelayTaskBuckets:         config.DelayTask.Buckets,
		DelayTaskLease:           config.DelayTask.Lease,
		SchedulerBatchSize:       config.Scheduler.BatchSize,
		SchedulerPoll:            config.Scheduler.PollInterval,
		SchedulerRetryInitial:    config.Scheduler.RetryInitial,
		SchedulerRetryMultiplier: config.Scheduler.RetryMultiplier,
		SchedulerRetryMax:        config.Scheduler.RetryMax,
		SchedulerRetryJitter:     config.Scheduler.RetryJitter,
	}
}
