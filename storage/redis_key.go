package storage

import "fmt"

type RedisKeyBuilder struct {
	namespace string
	config    RedisConfig
}

func (cfg RedisConfig) KeyBuilder(namespace string) RedisKeyBuilder {
	return RedisKeyBuilder{namespace: namespace, config: cfg}
}

func (b RedisKeyBuilder) Build(values map[string]string) (string, error) {
	id, ok := values[b.config.ID]
	if !ok || id == "" {
		return "", fmt.Errorf("%w: redis id %s", ErrMissingValue, b.config.ID)
	}
	shardBy := b.config.Route.By
	if shardBy == "" {
		shardBy = b.config.ID
	}
	shardValue := values[shardBy]
	if shardValue == "" {
		return "", fmt.Errorf("%w: redis shard %s", ErrMissingValue, shardBy)
	}
	if b.config.Key == "" {
		return "", fmt.Errorf("%w: redis key", ErrMissingValue)
	}
	if shardBy == b.config.ID {
		return fmt.Sprintf("%s:%s:{%s}", b.namespace, b.config.Key, id), nil
	}
	return fmt.Sprintf("%s:%s:{%s}:%s", b.namespace, b.config.Key, shardValue, id), nil
}
