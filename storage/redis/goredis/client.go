package goredis

import (
	"context"
	"fmt"
	"time"

	redis "github.com/redis/go-redis/v9"
)

// Client 将 go-redis 的通用命令接口适配为 tx Redis runtime 使用的最小接口。
type Client struct {
	client redis.Cmdable
}

func NewClient(client redis.Cmdable) Client {
	return Client{client: client}
}

func (c Client) Get(ctx context.Context, key string) (string, bool, error) {
	value, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func (c Client) Set(ctx context.Context, key string, value string) error {
	return c.client.Set(ctx, key, value, 0).Err()
}

func (c Client) SetWithTTL(ctx context.Context, key string, value string, ttl time.Duration) error {
	return c.client.Set(ctx, key, value, ttl).Err()
}

func (c Client) Eval(ctx context.Context, script string, keys []string, args ...any) (string, error) {
	return c.client.Eval(ctx, script, keys, args...).Text()
}

func (c Client) MGet(ctx context.Context, keys ...string) ([]string, error) {
	values, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]string, len(values))
	for i, value := range values {
		if value == nil {
			continue
		}
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("redis mget key %s returned %T", keys[i], value)
		}
		out[i] = text
	}
	return out, nil
}
