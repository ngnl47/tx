package msgq

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

var (
	ErrSessionRedisClientNil = errors.New("msgq: session redis client is nil")
	ErrInvalidSessionOptions = errors.New("msgq: invalid session redis options")
	ErrSessionStoreResponse  = errors.New("msgq: invalid session store response")
)

const bindSessionScript = `
local previous = redis.call('GET', KEYS[1])
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return previous or ''
`

const refreshSessionScript = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])
`

const unbindSessionScript = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
return redis.call('DEL', KEYS[1])
`

// SessionRedisClient 是会话绑定使用的 Redis 命令子集。
type SessionRedisClient interface {
	Get(ctx context.Context, key string) (value string, found bool, err error)
	Eval(ctx context.Context, script string, keys []string, args ...any) (string, error)
}

// SessionRedisResolver 按角色选择会话绑定所在的 Redis 客户端。
// 路由键固定为 RoleID，确保 Bind、Find、Refresh 和 Unbind 始终落到同一分片。
type SessionRedisResolver interface {
	ResolveSessionRedis(context.Context, int64) (SessionRedisClient, error)
}

// SessionRedisOptions 配置一个短期会话绑定命名空间。
type SessionRedisOptions struct {
	Prefix string
	TTL    time.Duration
}

// RedisSessionStore 在 Redis 中保存角色到实例的绑定。
type RedisSessionStore struct {
	resolve func(context.Context, int64) (SessionRedisClient, error)
	prefix  string
	ttl     time.Duration
}

// NewRedisSessionStore 创建 Redis 会话绑定存储。
func NewRedisSessionStore(client SessionRedisClient, options SessionRedisOptions) (*RedisSessionStore, error) {
	if client == nil {
		return nil, ErrSessionRedisClientNil
	}
	return newRedisSessionStore(func(context.Context, int64) (SessionRedisClient, error) { return client, nil }, options)
}

// NewRoutedRedisSessionStore 创建按角色路由 Redis 分片的会话绑定存储。
func NewRoutedRedisSessionStore(resolver SessionRedisResolver, options SessionRedisOptions) (*RedisSessionStore, error) {
	if resolver == nil {
		return nil, ErrSessionRedisClientNil
	}
	return newRedisSessionStore(resolver.ResolveSessionRedis, options)
}

func newRedisSessionStore(resolve func(context.Context, int64) (SessionRedisClient, error), options SessionRedisOptions) (*RedisSessionStore, error) {
	if options.Prefix == "" || options.Prefix != strings.TrimSpace(options.Prefix) || strings.ContainsAny(options.Prefix, "{}") || options.TTL < time.Millisecond {
		return nil, ErrInvalidSessionOptions
	}
	return &RedisSessionStore{resolve: resolve, prefix: options.Prefix, ttl: options.TTL}, nil
}

// Bind 设置当前绑定，并在覆盖时返回旧绑定。
func (s *RedisSessionStore) Bind(ctx context.Context, binding SessionBinding) (SessionBinding, bool, error) {
	if err := binding.Validate(); err != nil {
		return SessionBinding{}, false, err
	}
	encoded, err := encodeSessionBinding(binding)
	if err != nil {
		return SessionBinding{}, false, err
	}
	client, err := s.client(ctx, binding.RoleID)
	if err != nil {
		return SessionBinding{}, false, err
	}
	previous, err := client.Eval(ctx, bindSessionScript, []string{s.key(binding.RoleID)}, encoded, s.ttl.Milliseconds())
	if err != nil {
		return SessionBinding{}, false, err
	}
	if previous == "" {
		return SessionBinding{}, false, nil
	}
	decoded, err := decodeSessionBinding(binding.RoleID, previous)
	if err != nil {
		return SessionBinding{}, false, err
	}
	return decoded, true, nil
}

// Find 返回 roleID 的当前绑定。
func (s *RedisSessionStore) Find(ctx context.Context, roleID int64) (SessionBinding, bool, error) {
	if roleID <= 0 {
		return SessionBinding{}, false, ErrInvalidSession
	}
	client, err := s.client(ctx, roleID)
	if err != nil {
		return SessionBinding{}, false, err
	}
	value, found, err := client.Get(ctx, s.key(roleID))
	if err != nil || !found {
		return SessionBinding{}, found, err
	}
	binding, err := decodeSessionBinding(roleID, value)
	if err != nil {
		return SessionBinding{}, false, err
	}
	return binding, true, nil
}

// Refresh 仅在 Redis 仍保存完全相同会话时延长绑定 TTL。
func (s *RedisSessionStore) Refresh(ctx context.Context, binding SessionBinding) (bool, error) {
	if err := binding.Validate(); err != nil {
		return false, err
	}
	encoded, err := encodeSessionBinding(binding)
	if err != nil {
		return false, err
	}
	return s.evalConditional(ctx, refreshSessionScript, binding, encoded)
}

// Unbind 仅在 Redis 仍保存完全相同会话时删除绑定。
func (s *RedisSessionStore) Unbind(ctx context.Context, binding SessionBinding) (bool, error) {
	if err := binding.Validate(); err != nil {
		return false, err
	}
	encoded, err := encodeSessionBinding(binding)
	if err != nil {
		return false, err
	}
	client, err := s.client(ctx, binding.RoleID)
	if err != nil {
		return false, err
	}
	result, err := client.Eval(ctx, unbindSessionScript, []string{s.key(binding.RoleID)}, encoded)
	if err != nil {
		return false, err
	}
	return conditionalResult(result)
}

func (s *RedisSessionStore) evalConditional(ctx context.Context, script string, binding SessionBinding, encoded string) (bool, error) {
	client, err := s.client(ctx, binding.RoleID)
	if err != nil {
		return false, err
	}
	result, err := client.Eval(ctx, script, []string{s.key(binding.RoleID)}, encoded, s.ttl.Milliseconds())
	if err != nil {
		return false, err
	}
	return conditionalResult(result)
}

func (s *RedisSessionStore) client(ctx context.Context, roleID int64) (SessionRedisClient, error) {
	if s == nil || s.resolve == nil {
		return nil, ErrSessionRedisClientNil
	}
	client, err := s.resolve(ctx, roleID)
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, ErrSessionRedisClientNil
	}
	return client, nil
}

func (s *RedisSessionStore) key(roleID int64) string {
	return s.prefix + ":session:{" + strconv.FormatInt(roleID, 10) + "}"
}

type sessionRedisRecord struct {
	InstanceID   string `json:"instanceId"`
	SessionID    int64  `json:"sessionId"`
	ConnectionID int64  `json:"connectionId"`
}

func encodeSessionBinding(binding SessionBinding) (string, error) {
	encoded, err := json.Marshal(sessionRedisRecord{
		InstanceID:   binding.InstanceID,
		SessionID:    binding.SessionID,
		ConnectionID: binding.ConnectionID,
	})
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func decodeSessionBinding(roleID int64, value string) (SessionBinding, error) {
	var record sessionRedisRecord
	if err := json.Unmarshal([]byte(value), &record); err != nil {
		return SessionBinding{}, err
	}
	binding := SessionBinding{
		RoleID:       roleID,
		SessionID:    record.SessionID,
		ConnectionID: record.ConnectionID,
		InstanceID:   record.InstanceID,
	}
	if err := binding.Validate(); err != nil {
		return SessionBinding{}, ErrSessionStoreResponse
	}
	return binding, nil
}

func conditionalResult(result string) (bool, error) {
	switch result {
	case "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, ErrSessionStoreResponse
	}
}
