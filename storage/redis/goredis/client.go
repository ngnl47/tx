package goredis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	redis "github.com/redis/go-redis/v9"

	"github.com/ngnl47/tx/storage"
)

var errRedisClientNotConfigured = errors.New("redis client is not configured")

// Client 将 go-redis 的通用命令接口适配为 tx Redis runtime 使用的最小接口。
type Client struct {
	client redis.Cmdable
}

func NewClient(client redis.Cmdable) Client {
	return Client{client: client}
}

func (c Client) Get(ctx context.Context, key string) (string, bool, error) {
	if c.client == nil {
		return "", false, errRedisClientNotConfigured
	}
	value, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, redisBackendError("get", err)
	}
	return value, true, nil
}

func (c Client) Set(ctx context.Context, key string, value string) error {
	if c.client == nil {
		return errRedisClientNotConfigured
	}
	return redisBackendError("set", c.client.Set(ctx, key, value, 0).Err())
}

func (c Client) SetWithTTL(ctx context.Context, key string, value string, ttl time.Duration) error {
	if c.client == nil {
		return errRedisClientNotConfigured
	}
	return redisBackendError("set with ttl", c.client.Set(ctx, key, value, ttl).Err())
}

func (c Client) Eval(ctx context.Context, script string, keys []string, args ...any) (string, error) {
	if c.client == nil {
		return "", errRedisClientNotConfigured
	}
	value, err := c.client.Eval(ctx, script, keys, args...).Result()
	if err != nil {
		return "", redisBackendError("eval", err)
	}
	switch value := value.(type) {
	case string:
		return value, nil
	case int64:
		return strconv.FormatInt(value, 10), nil
	default:
		return "", fmt.Errorf("redis eval returned %T", value)
	}
}

func (c Client) MGet(ctx context.Context, keys ...string) ([]string, error) {
	if c.client == nil {
		return nil, errRedisClientNotConfigured
	}
	values, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, redisBackendError("mget", err)
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

func redisBackendError(operation string, err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var serverErr redis.Error
	if errors.As(err, &serverErr) {
		return err
	}
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, redis.ErrClosed) ||
		errors.Is(err, redis.ErrPoolExhausted) ||
		errors.Is(err, redis.ErrPoolTimeout) {
		return storage.NewBackendError(storage.BackendRedis, operation, err)
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) {
		return storage.NewBackendError(storage.BackendRedis, operation, err)
	}
	return err
}
