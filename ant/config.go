package ant

import (
	"github.com/ngnl47/tx/modules/tasks"
	"io"
	"time"

	framework "github.com/ngnl47/tx/internal/framework"
	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/txn"
	storageconfig "github.com/ngnl47/tx/storage/config"
	txfusion "github.com/ngnl47/tx/storage/fusion"
)

// Config 是项目配置源转换后的 Ant 强类型配置。
type Config struct {
	// TaskQueue 绑定可靠任务资源；未选择该能力时不使用。
	TaskQueue tasks.Config
	// AppID 是当前应用在共享基础设施中的稳定隔离标识。
	AppID string
	// Storage 描述可由本进程选择的 Redis 和 MongoDB 连接路由。
	Storage StorageConfig
	// Runtime 描述生命周期、Fusion、Procedure 和后台调度参数。
	Runtime RuntimeConfig
	// Log 描述框架结构化日志输出。
	Log LogConfig
	// Actor 仅在本进程选择 Actor CMod 时生效。
	Actor ActorConfig
	// Discovery 仅在本进程选择 Discovery CMod 时生效。
	Discovery DiscoveryConfig
}

// LogConfig 描述框架结构化日志输出策略。
type LogConfig struct {
	// Level 是最低输出级别，空值使用框架默认值。
	Level string
	// Format 选择 console 或 json，空值使用 console。
	Format string
	// Output 接收框架日志；nil 表示使用标准输出。
	Output io.Writer
}

// StorageConfig 描述 Redis 与 MongoDB 连接路由。
type StorageConfig struct {
	// Redis 是按 routeType 组织的 Redis 连接路由。
	Redis storageconfig.RouteTableConfig
	// MongoDB 是按 routeType 组织的 MongoDB 连接路由。
	MongoDB storageconfig.RouteTableConfig
}

// RuntimeConfig 描述有状态进程的公共运行参数。
type RuntimeConfig struct {
	// Lifecycle 控制进程排水上限。
	Lifecycle LifecycleConfig
	// Fusion 描述 Redis 热数据与 MongoDB 冷数据的融合存储策略。
	Fusion txfusion.Config
	// DelayTask 描述 Fusion 延迟回写任务的 Redis 存储。
	DelayTask DelayTaskConfig
	// Scheduler 描述 Fusion 延迟回写的消费策略。
	Scheduler SchedulerConfig
	// Health 描述外部存储依赖的后台健康探测。
	Health HealthConfig
	// Transaction 仅在本进程选择 CModProcedure 时生效。
	Transaction TransactionConfig
}

// TransactionConfig 描述 CModProcedure 使用的单机事务运行时。
type TransactionConfig struct {
	// Module 描述锁等待、死锁检测和调试检查参数。
	Module txn.Config
}

// LifecycleConfig 配置进程排水上限。
type LifecycleConfig struct {
	// DrainTimeout 是排水等待上限；0 使用框架默认值。
	DrainTimeout time.Duration
}

// DelayTaskConfig 描述 Fusion 延迟任务在 Redis 中的组织方式。
type DelayTaskConfig struct {
	// RouteType 选择保存延迟任务的 Redis 路由。
	RouteType string
	// Prefix 是延迟任务 Redis key 的命名空间。
	Prefix string
	// Buckets 是延迟任务分桶数；0 使用框架默认值。
	Buckets int
	// Lease 是单次消费租约时长；0 使用框架默认值。
	Lease time.Duration
}

// SchedulerConfig 描述 Fusion 延迟写调度策略。
type SchedulerConfig struct {
	// BatchSize 是单轮最多消费的延迟任务数。
	BatchSize int
	// PollInterval 是无到期任务时的轮询间隔。
	PollInterval time.Duration
	// RetryInitial 是任务失败后的初始重试延迟。
	RetryInitial time.Duration
	// RetryMultiplier 是指数退避倍数。
	RetryMultiplier float64
	// RetryMax 是重试延迟上限。
	RetryMax time.Duration
	// RetryJitter 是用于打散重试峰值的随机比例。
	RetryJitter float64
}

// HealthConfig 描述外部依赖健康探测策略。
type HealthConfig struct {
	// Interval 是后台重新探测不可用后端的间隔。
	Interval time.Duration
	// Timeout 是单次后端探测上限。
	Timeout time.Duration
}

// ActorConfig 描述 Actor 集群运行时。
type ActorConfig struct {
	// Runtime 描述本地实体宿主、Core NATS 传输和邮箱参数。
	Runtime txactor.Config
}

// DiscoveryConfig 描述服务注册、租约和实例目录。
type DiscoveryConfig struct {
	// Etcd 描述服务注册、租约和 Watch 使用的 etcd 连接。
	Etcd discovery.EtcdConfig
}

// RunOptions 保存本次进程服务选择和部署期发布参数。
type RunOptions struct {
	// Services 是本进程启动的 L4 服务表达式。
	Services string
	// InstanceVersion 是发布版本或镜像版本，用于可观测和运维识别。
	InstanceVersion string
	// InstanceEndpoint 是发布到服务发现的业务 RPC 地址。
	InstanceEndpoint string
	// InstanceEndpoints 是当前实例按稳定名称发布的客户端或项目公开地址。
	InstanceEndpoints map[string]string
	// RPCListen 是共享 gRPC Host 的本地监听地址。
	RPCListen string
	// ControlListen 是独立运维控制面的本地监听地址；空值表示禁用。
	ControlListen string
}

// Validate 校验不依赖本轮服务选择的基础配置。
func (config Config) Validate() error {
	return config.toFramework().Validate()
}

func (config Config) toFramework() framework.Config {
	return framework.Config{
		AppID: config.AppID,
		Storage: framework.StorageConfig{
			Redis:   config.Storage.Redis,
			MongoDB: config.Storage.MongoDB,
		},
		Runtime: framework.RuntimeConfig{
			Lifecycle: framework.LifecycleConfig{DrainTimeout: config.Runtime.Lifecycle.DrainTimeout},
			Fusion:    config.Runtime.Fusion,
			DelayTask: framework.DelayTaskConfig{
				RouteType: config.Runtime.DelayTask.RouteType,
				Prefix:    config.Runtime.DelayTask.Prefix,
				Buckets:   config.Runtime.DelayTask.Buckets,
				Lease:     config.Runtime.DelayTask.Lease,
			},
			Scheduler: framework.SchedulerConfig{
				BatchSize:       config.Runtime.Scheduler.BatchSize,
				PollInterval:    config.Runtime.Scheduler.PollInterval,
				RetryInitial:    config.Runtime.Scheduler.RetryInitial,
				RetryMultiplier: config.Runtime.Scheduler.RetryMultiplier,
				RetryMax:        config.Runtime.Scheduler.RetryMax,
				RetryJitter:     config.Runtime.Scheduler.RetryJitter,
			},
			Health: framework.HealthConfig{
				Interval: config.Runtime.Health.Interval,
				Timeout:  config.Runtime.Health.Timeout,
			},
			Transaction: framework.TransactionConfig{Module: config.Runtime.Transaction.Module},
		},
		Log: framework.LogConfig{
			Level:  config.Log.Level,
			Format: config.Log.Format,
			Output: config.Log.Output,
		},
		Actor:     framework.ActorConfig{Runtime: config.Actor.Runtime},
		TaskQueue: config.TaskQueue,
		Discovery: framework.DiscoveryConfig{Etcd: config.Discovery.Etcd},
	}
}

func (options RunOptions) toFramework() framework.RunOptions {
	return framework.RunOptions{
		Services:          options.Services,
		InstanceVersion:   options.InstanceVersion,
		InstanceEndpoint:  options.InstanceEndpoint,
		InstanceEndpoints: options.InstanceEndpoints,
		RPCListen:         options.RPCListen,
		ControlListen:     options.ControlListen,
	}
}
