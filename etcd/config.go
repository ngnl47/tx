package etcd

import (
	"errors"
	"strings"
	"time"
)

const defaultDialTimeout = 5 * time.Second

var ErrInvalidConfig = errors.New("etcd: invalid config")

// Config 描述 etcd 客户端连接参数。
type Config struct {
	// Endpoints 是可连接的 etcd 节点列表。
	Endpoints []string
	// DialTimeout 是建立连接的最长等待时间；非正值使用默认值。
	DialTimeout time.Duration
}

// Normalize 校验配置并返回不共享 Endpoints 底层数组的副本。
func (c Config) Normalize() (Config, error) {
	if len(c.Endpoints) == 0 {
		return Config{}, ErrInvalidConfig
	}
	c.Endpoints = append([]string(nil), c.Endpoints...)
	for index, endpoint := range c.Endpoints {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			return Config{}, ErrInvalidConfig
		}
		c.Endpoints[index] = endpoint
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = defaultDialTimeout
	}
	return c, nil
}
