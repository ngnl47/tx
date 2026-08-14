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
)

var ErrInvalidConfig = errors.New("actor: invalid config")

// Config 描述一个 Actor Cluster 成员。项目代码负责解析配置文件后构造该值。
type Config struct {
	ClusterName     string
	Host            string
	Port            int
	MailboxCapacity int
	Etcd            EtcdConfig
}

// EtcdConfig 保存 Actor Cluster 的静态发现配置。
type EtcdConfig struct {
	Endpoints   []string
	Prefix      string
	DialTimeout time.Duration
}

func (c Config) Normalize() (Config, error) {
	c.ClusterName = strings.TrimSpace(c.ClusterName)
	c.Host = strings.TrimSpace(c.Host)
	if c.ClusterName == "" || c.Host == "" || c.Port < 0 || c.Port > 65535 || c.MailboxCapacity < 0 {
		return Config{}, ErrInvalidConfig
	}
	if c.MailboxCapacity == 0 {
		c.MailboxCapacity = DefaultMailboxCapacity
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
