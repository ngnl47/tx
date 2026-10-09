package config

import (
	"fmt"
	"maps"
	"strings"

	"gopkg.in/yaml.v3"
)

// RedisConfig 将存储名称映射到单节点或云代理 URI；不表达节点、槽位和应用层分片。
type RedisConfig struct {
	Stores map[string]string `yaml:",inline" json:"stores"`
}

// NewRedisConfig 复制名称映射，连接池仍由运行时统一管理。
func NewRedisConfig(stores map[string]string) RedisConfig {
	return RedisConfig{Stores: maps.Clone(stores)}
}

func (c *RedisConfig) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("redis config: stores must be a mapping")
	}
	var stores map[string]string
	if err := value.Decode(&stores); err != nil {
		return err
	}
	c.Stores = stores
	return c.Validate()
}

// Validate 拒绝空名称和空 URI；旧的分片数组由 YAML 解码直接拒绝。
func (c RedisConfig) Validate() error {
	for name, endpoint := range c.Stores {
		if name == "" || strings.TrimSpace(name) != name {
			return fmt.Errorf("redis config: invalid store name %q", name)
		}
		if err := validateEndpoint(endpoint); err != nil {
			return fmt.Errorf("redis config: store %q: %w", name, err)
		}
	}
	return nil
}

// Endpoints 返回去重后的 URI，多个名称可复用同一连接池。
func (c RedisConfig) Endpoints() map[string]string {
	endpoints := make(map[string]string, len(c.Stores))
	for _, endpoint := range c.Stores {
		endpoints[endpoint] = endpoint
	}
	return endpoints
}
