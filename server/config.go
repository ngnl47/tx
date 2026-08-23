package server

import (
	"fmt"
	"io"
	"strings"
	"time"

	txactor "github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/modules/discovery"
	"github.com/ngnl47/tx/modules/txn"
	storageconfig "github.com/ngnl47/tx/storage/config"
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
}

type RuntimeConfig struct {
	BackgroundWorkers bool
	DelayTask         DelayTaskConfig
	Scheduler         SchedulerConfig
	Health            HealthConfig
	Transaction       TransactionConfig
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
	Module  txn.Config
	Runtime txn.RuntimeConfig
}

type ActorConfig struct {
	Enabled bool
	Runtime txactor.Config
}

type DiscoveryConfig struct {
	Enabled bool
	Etcd    discovery.EtcdConfig
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
	if err := config.Storage.Redis.Validate(); err != nil {
		return fmt.Errorf("server: redis routes: %w", err)
	}
	if err := config.Storage.MongoDB.Validate(); err != nil {
		return fmt.Errorf("server: mongo routes: %w", err)
	}
	if err := config.Runtime.validate(); err != nil {
		return err
	}
	if config.Actor.Enabled {
		if _, err := config.Actor.Runtime.Normalize(); err != nil {
			return fmt.Errorf("server: actor config: %w", err)
		}
	}
	if config.Discovery.Enabled {
		if _, err := config.Discovery.Etcd.Normalize(); err != nil {
			return fmt.Errorf("server: discovery config: %w", err)
		}
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

func (config RuntimeConfig) validate() error {
	if config.DelayTask.Buckets < 0 {
		return fmt.Errorf("server: delay task buckets must be greater than or equal to 0")
	}
	if config.DelayTask.Lease < 0 || config.DelayTask.Lease > 0 && config.DelayTask.Lease < time.Millisecond {
		return fmt.Errorf("server: delay task lease must be zero or at least 1ms")
	}
	if config.Scheduler.BatchSize < 0 {
		return fmt.Errorf("server: scheduler batch size must be greater than or equal to 0")
	}
	if config.Scheduler.RetryMultiplier != 0 && config.Scheduler.RetryMultiplier <= 1 {
		return fmt.Errorf("server: scheduler retry multiplier must be greater than 1")
	}
	if config.Scheduler.RetryJitter < 0 || config.Scheduler.RetryJitter > 1 {
		return fmt.Errorf("server: scheduler retry jitter must be between 0 and 1")
	}
	if config.Scheduler.RetryInitial > 0 && config.Scheduler.RetryMax > 0 && config.Scheduler.RetryMax < config.Scheduler.RetryInitial {
		return fmt.Errorf("server: scheduler retry max must not be less than retry initial")
	}
	if config.Health.Interval <= 0 || config.Health.Timeout <= 0 {
		return fmt.Errorf("server: health interval and timeout must be greater than 0")
	}
	if config.Health.Timeout > config.Health.Interval {
		return fmt.Errorf("server: health timeout must not be greater than interval")
	}
	return nil
}

func (config RuntimeConfig) storageOptions() storageOptions {
	return storageOptions{
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
		HealthProbeInterval:      config.Health.Interval,
		HealthProbeTimeout:       config.Health.Timeout,
	}
}
