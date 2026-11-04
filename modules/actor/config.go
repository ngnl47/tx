// Package actor 提供实体行为与调用契约；连接、发现和生命周期由框架统一装配。
//
// Type 声明行为、放置与空闲卸载策略；Messenger 按 Identity 投递业务，
// Controller 和 WaitReady 提供独立的激活控制与观察。Runtime 组合这些借用能力，
// 不转移宿主关闭权。普通调用同实例进入本地邮箱，跨实例使用 Core NATS；
// 原生 gRPC 服务注册与实例调用使用 rpc 包，不经过 Actor 邮箱。
// Actor Ready 表示通信入口完成初始化，不保证独立业务对象存在或可用。
package actor

import (
	"errors"
	"strings"
	"time"
)

const DefaultMailboxCapacity = 1024
const DefaultMaxPendingCalls = 4096

var ErrInvalidConfig = errors.New("actor: invalid config")

// Config 配置实体传输和本地宿主，不另建成员发现。
type Config struct {
	// NATSURL 是 Core NATS 的连接地址。
	NATSURL string
	// Namespace 隔离同一 NATS 上的应用主题；空值由框架使用 AppID。
	Namespace       string
	MailboxCapacity int
	MaxPendingCalls int
	// CallbackTimeout 限制业务处理、卸载和权威查询的上下文，零值使用 30 秒。
	// 不限制业务初始化时间，也不替代调用方等待或框架排水期限。
	CallbackTimeout time.Duration
	IdlePassivation map[string]time.Duration
	Capacity        Capacity
}

func (c Config) Normalize() (Config, error) {
	c.NATSURL = strings.TrimSpace(c.NATSURL)
	c.Namespace = strings.TrimSpace(c.Namespace)
	if c.NATSURL == "" || c.MailboxCapacity < 0 || c.MaxPendingCalls < 0 || c.CallbackTimeout < 0 || c.Capacity.Maximum < 0 || c.Capacity.Initializations < 0 {
		return Config{}, ErrInvalidConfig
	}
	if c.Capacity.Scope == "" {
		c.Capacity.Scope = CapacityActivations
	}
	if c.Capacity.Scope != CapacityActivations && c.Capacity.Scope != CapacityAssignments {
		return Config{}, ErrInvalidConfig
	}
	if c.MailboxCapacity == 0 {
		c.MailboxCapacity = DefaultMailboxCapacity
	}
	if c.MaxPendingCalls == 0 {
		c.MaxPendingCalls = DefaultMaxPendingCalls
	}
	if c.CallbackTimeout == 0 {
		c.CallbackTimeout = 30 * time.Second
	}
	idle := make(map[string]time.Duration, len(c.IdlePassivation))
	for name, timeout := range c.IdlePassivation {
		name = strings.TrimSpace(name)
		if name == "" || timeout < time.Millisecond {
			return Config{}, ErrInvalidConfig
		}
		if _, exists := idle[name]; exists {
			return Config{}, ErrInvalidConfig
		}
		idle[name] = timeout
	}
	c.IdlePassivation = idle
	return c, nil
}
