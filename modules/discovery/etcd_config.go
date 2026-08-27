package discovery

import (
	"errors"
	"strings"
	"time"
)

const defaultEtcdPrefix = "/tx/discovery/instances"

var ErrInvalidEtcdConfig = errors.New("discovery: invalid etcd config")

// EtcdConfig 描述普通 RPC 服务注册和实例目录使用的 etcd 连接。
type EtcdConfig struct {
	// Endpoints 是可连接的 etcd 节点列表。
	Endpoints []string
	// Prefix 隔离当前应用的服务实例注册键；空值使用框架默认前缀。
	Prefix string
	// LeaseTTL 是实例停止续约后注册信息继续存活的时间，必须为整秒且不少于一秒。
	LeaseTTL time.Duration
	// DialTimeout 是建立 etcd 连接的最长等待时间；非正值使用框架默认值。
	DialTimeout time.Duration
}

func (c EtcdConfig) Normalize() (EtcdConfig, error) {
	if len(c.Endpoints) == 0 || c.LeaseTTL < time.Second || c.LeaseTTL%time.Second != 0 {
		return EtcdConfig{}, ErrInvalidEtcdConfig
	}
	c.Endpoints = append([]string(nil), c.Endpoints...)
	for index, endpoint := range c.Endpoints {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			return EtcdConfig{}, ErrInvalidEtcdConfig
		}
		c.Endpoints[index] = endpoint
	}
	prefix := strings.Trim(strings.TrimSpace(c.Prefix), "/")
	if prefix == "" {
		c.Prefix = defaultEtcdPrefix
	} else if strings.HasSuffix(prefix, "/instances") {
		c.Prefix = "/" + prefix
	} else {
		c.Prefix = "/" + prefix + "/instances"
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}
	return c, nil
}

func (c EtcdConfig) ValidateInstance(instance Instance) error {
	if err := instance.Validate(); err != nil || strings.Contains(instance.ID, "/") {
		return ErrInvalidInstance
	}
	return nil
}
