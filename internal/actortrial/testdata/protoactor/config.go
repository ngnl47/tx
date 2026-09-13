// Package actor 为项目提供可选的 ProtoActor 集群装配。
package actor

import (
	"errors"
	"strings"
	"time"
)

const (
	defaultEtcdPrefix      = "/tx/actor"
	DefaultMailboxCapacity = 1024
	DefaultMaxPendingCalls = 4096
)

// LogLevel 控制 ProtoActor 内部日志的最低输出级别，不改变项目全局日志级别。
type LogLevel string

const (
	// LogLevelDebug 输出 Gossip、心跳和邮箱等高频内部状态，仅用于专项排查。
	LogLevelDebug LogLevel = "debug"
	// LogLevelInfo 输出成员加入、远程通信启动等生命周期信息，是默认级别。
	LogLevelInfo LogLevel = "info"
	// LogLevelWarn 仅输出 ProtoActor 的警告和错误。
	LogLevelWarn LogLevel = "warn"
	// LogLevelError 仅输出 ProtoActor 的错误。
	LogLevelError LogLevel = "error"
)

var ErrInvalidConfig = errors.New("actor: invalid config")

// Config 描述一个 Actor Cluster 成员。项目代码负责解析配置文件后构造该值。
type Config struct {
	// ClusterName 是当前 Actor 集群的稳定名称。
	ClusterName string
	// Host 是当前 Actor 实例向其他集群成员发布的可达主机地址。
	Host string
	// Port 是当前 Actor 实例远程通信端口；0 允许运行时选择空闲端口。
	Port int
	// MailboxCapacity 是单个 Actor 的默认有界邮箱容量；0 使用框架默认值。
	MailboxCapacity int
	// MaxPendingCalls 限制当前进程同时等待业务结果的 Future 数量；0 使用框架默认值。
	MaxPendingCalls int
	// LogLevel 是 ProtoActor 内部日志的最低级别；空值使用 info。
	LogLevel LogLevel
	// IdlePassivation 按 Kind 名称配置实体 Actor 的空闲钝化时间。
	// 显式 KindOption 优先于该运行时映射；空 map 表示不追加运行时策略。
	IdlePassivation map[string]time.Duration
	// Etcd 描述 Actor Cluster Provider 使用的 etcd 连接。
	Etcd EtcdConfig
}

// EtcdConfig 保存 Actor Cluster 的静态发现配置。
type EtcdConfig struct {
	// Endpoints 是可连接的 etcd 节点列表。
	Endpoints []string
	// Prefix 隔离当前 Actor Cluster 的成员和 placement 数据；空值使用框架默认前缀。
	Prefix string
	// DialTimeout 是建立 etcd 连接的最长等待时间；非正值使用框架默认值。
	DialTimeout time.Duration
}

func (c Config) Normalize() (Config, error) {
	c.ClusterName = strings.TrimSpace(c.ClusterName)
	c.Host = strings.TrimSpace(c.Host)
	c.LogLevel = LogLevel(strings.ToLower(strings.TrimSpace(string(c.LogLevel))))
	if c.LogLevel == "" {
		c.LogLevel = LogLevelInfo
	}
	if c.ClusterName == "" || c.Host == "" || c.Port < 0 || c.Port > 65535 || c.MailboxCapacity < 0 || c.MaxPendingCalls < 0 {
		return Config{}, ErrInvalidConfig
	}
	switch c.LogLevel {
	case LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError:
	default:
		return Config{}, ErrInvalidConfig
	}
	if c.MailboxCapacity == 0 {
		c.MailboxCapacity = DefaultMailboxCapacity
	}
	if c.MaxPendingCalls == 0 {
		c.MaxPendingCalls = DefaultMaxPendingCalls
	}
	if len(c.IdlePassivation) > 0 {
		idlePassivation := make(map[string]time.Duration, len(c.IdlePassivation))
		for name, timeout := range c.IdlePassivation {
			name = strings.TrimSpace(name)
			if name == "" || timeout < time.Millisecond {
				return Config{}, ErrInvalidConfig
			}
			if _, exists := idlePassivation[name]; exists {
				return Config{}, ErrInvalidConfig
			}
			idlePassivation[name] = timeout
		}
		c.IdlePassivation = idlePassivation
	}
	etcd, err := c.Etcd.Normalize()
	if err != nil {
		return Config{}, err
	}
	c.Etcd = etcd
	return c, nil
}

func (c EtcdConfig) Normalize() (EtcdConfig, error) {
	if len(c.Endpoints) == 0 {
		return EtcdConfig{}, ErrInvalidConfig
	}
	c.Endpoints = append([]string(nil), c.Endpoints...)
	for index, endpoint := range c.Endpoints {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			return EtcdConfig{}, ErrInvalidConfig
		}
		c.Endpoints[index] = endpoint
	}
	prefix := strings.Trim(strings.TrimSpace(c.Prefix), "/")
	if prefix == "" {
		c.Prefix = defaultEtcdPrefix
	} else {
		c.Prefix = "/" + prefix
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}
	return c, nil
}
