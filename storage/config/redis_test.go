package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestRedisConfigNamesShareEndpoint(t *testing.T) {
	var config RedisConfig
	if err := yaml.Unmarshal([]byte("main: &endpoint redis://localhost:6379\nrank: *endpoint\n"), &config); err != nil {
		t.Fatal(err)
	}
	if config.Stores["rank"] != "redis://localhost:6379" || len(config.Endpoints()) != 1 {
		t.Fatalf("config = %+v", config)
	}
	copied := NewRedisConfig(config.Stores)
	config.Stores["rank"] = "redis://changed"
	if copied.Stores["rank"] != "redis://localhost:6379" {
		t.Fatal("配置副本共享可变映射")
	}
}

func TestRedisConfigRejectsApplicationSharding(t *testing.T) {
	for _, body := range []string{
		"rank: [redis://a, redis://b]",
		"rank: [{min: 1, shards: [redis://a]}]",
		"rank: ''",
		"rank: redis://a\nrank: redis://b",
	} {
		t.Run(body, func(t *testing.T) {
			var config RedisConfig
			if err := yaml.Unmarshal([]byte(body), &config); err == nil {
				t.Fatal("旧分片或无效配置应被拒绝")
			}
		})
	}
}
