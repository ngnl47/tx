// Package actor 提供实体行为与调用契约；连接、发现和生命周期由框架统一装配。
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
}

func (c Config) Normalize() (Config, error) {
	c.NATSURL = strings.TrimSpace(c.NATSURL)
	c.Namespace = strings.TrimSpace(c.Namespace)
	if c.NATSURL == "" || c.MailboxCapacity < 0 || c.MaxPendingCalls < 0 || c.CallbackTimeout < 0 {
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
