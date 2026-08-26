package server

import (
	"fmt"
	"io"
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
// server 包不读取 YAML、环境变量或命令行参数。
type Config struct {
	AppID     string
	Storage   StorageConfig
	Runtime   RuntimeConfig
	Log       LogConfig
	Actor     ActorConfig
	Discovery DiscoveryConfig
}

type LogConfig struct {
	Level  string
	Format string
	Output io.Writer
}

type StorageConfig struct {
	Redis   storageconfig.RouteTableConfig
	MongoDB storageconfig.RouteTableConfig
	WAL     WALConfig
}

// WALBackend 标识 WAL 的持久化实现。
type WALBackend string

// WALBackendMongo 使用项目已经装配的 Mongo route table 和连接。
const WALBackendMongo WALBackend = "mongo"

// WALConfig 配置可选的预写日志。当前仅实现 Mongo backend。
type WALConfig struct {
	Backend WALBackend
	Mongo   WALMongoConfig
}

// WALMongoConfig 指定 WAL 使用的 Mongo routeType 和独立 collection。
type WALMongoConfig struct {
	RouteType  string
	Collection string
}

type RuntimeConfig struct {
	Lifecycle   LifecycleConfig
	Fusion      txfusion.Config
	DelayTask   DelayTaskConfig
	Scheduler   SchedulerConfig
	Health      HealthConfig
	Transaction TransactionConfig
}

// LifecycleConfig 配置进程入口等待和 Fusion 本地登记排空的单阶段上限。
type LifecycleConfig struct {
	DrainTimeout time.Duration
}

type DelayTaskConfig struct {
	RouteType string
	Prefix    string
	Buckets   int
	Lease     time.Duration
}

type SchedulerConfig struct {
	BatchSize       int
	PollInterval    time.Duration
	RetryInitial    time.Duration
	RetryMultiplier float64
	RetryMax        time.Duration
	RetryJitter     float64
}

type HealthConfig struct {
	Interval time.Duration
	Timeout  time.Duration
}

type TransactionConfig struct {
	Module txn.Config
}

type ActorConfig struct {
	Runtime txactor.Config
}

type DiscoveryConfig struct {
	Etcd discovery.EtcdConfig
}

// RunOptions 保存部署身份和进程服务选择。这些值通常来自命令行参数或编排系统，而不是应用 YAML。
type RunOptions struct {
	Services         string
	InstanceID       string
	InstanceVersion  string
	InstanceEndpoint string
	LogicalSlot      string
	RPCListen        string
	ControlListen    string
}

func (config Config) Validate() error {
	if strings.TrimSpace(config.AppID) == "" {
		return fmt.Errorf("server: app ID is required")
	}
	if err := config.Runtime.Lifecycle.validate(); err != nil {
		return err
	}
	return nil
}

func (config LifecycleConfig) validate() error {
	if config.DrainTimeout < 0 {
		return fmt.Errorf("server: lifecycle drain timeout must be greater than or equal to 0")
	}
	return nil
}

func validateStorageRoutes(name string, routes storageconfig.RouteTableConfig) error {
	if len(routes.Routes) == 0 {
		return fmt.Errorf("server: %s routes are required", name)
	}
	if err := routes.Validate(); err != nil {
		return fmt.Errorf("server: %s routes: %w", name, err)
	}
	return nil
}

func (config Config) clone() Config {
	cloned := config
	cloned.Storage.Redis = storageconfig.NewRouteTableConfig(config.Storage.Redis.Routes)
	cloned.Storage.MongoDB = storageconfig.NewRouteTableConfig(config.Storage.MongoDB.Routes)
	cloned.Actor.Runtime.Etcd.Endpoints = append([]string(nil), config.Actor.Runtime.Etcd.Endpoints...)
	cloned.Discovery.Etcd.Endpoints = append([]string(nil), config.Discovery.Etcd.Endpoints...)
	return cloned
}

func (config WALConfig) validate(routes storageconfig.RouteTableConfig) error {
	backend := WALBackend(strings.TrimSpace(string(config.Backend)))
	if config.Backend != "" && backend != config.Backend {
		return fmt.Errorf("server: WAL backend must not contain surrounding whitespace")
	}
	if backend != "" && backend != WALBackendMongo {
		return fmt.Errorf("server: unsupported WAL backend %q", config.Backend)
	}
	if backend == "" {
		return fmt.Errorf("server: WAL backend is required")
	}
	routeType := strings.TrimSpace(config.Mongo.RouteType)
	if routeType == "" || routeType != config.Mongo.RouteType {
		return fmt.Errorf("server: WAL Mongo route type is empty or contains surrounding whitespace")
	}
	if _, ok := routes.Routes[routeType]; !ok {
		return fmt.Errorf("server: WAL Mongo route %q is not configured", routeType)
	}
	collection := strings.TrimSpace(config.Mongo.Collection)
	if collection == "" || collection != config.Mongo.Collection {
		return fmt.Errorf("server: WAL Mongo collection is empty or contains surrounding whitespace")
	}
	return nil
}

func (config DelayTaskConfig) validate() error {
	if config.Buckets < 0 {
		return fmt.Errorf("server: delay task buckets must be greater than or equal to 0")
	}
	if config.Lease != 0 {
		if err := txstorage.ValidateMillisecondDuration(config.Lease); err != nil {
			return fmt.Errorf("server: delay task lease: %w", err)
		}
	}
	return nil
}

func (config SchedulerConfig) validate() error {
	if config.BatchSize < 0 {
		return fmt.Errorf("server: scheduler batch size must be greater than or equal to 0")
	}
	if config.RetryMultiplier != 0 && config.RetryMultiplier <= 1 {
		return fmt.Errorf("server: scheduler retry multiplier must be greater than 1")
	}
	if config.RetryJitter < 0 || config.RetryJitter > 1 {
		return fmt.Errorf("server: scheduler retry jitter must be between 0 and 1")
	}
	if config.RetryInitial > 0 && config.RetryMax > 0 && config.RetryMax < config.RetryInitial {
		return fmt.Errorf("server: scheduler retry max must not be less than retry initial")
	}
	return nil
}

func (config HealthConfig) validate() error {
	if config.Interval <= 0 || config.Timeout <= 0 {
		return fmt.Errorf("server: health interval and timeout must be greater than 0")
	}
	if config.Timeout > config.Interval {
		return fmt.Errorf("server: health timeout must not be greater than interval")
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
