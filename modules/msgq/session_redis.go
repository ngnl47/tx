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

// SessionRedisOptions 配置一个短期会话绑定命名空间。
type SessionRedisOptions struct {
	Prefix string
	TTL    time.Duration
}

// RedisSessionStore 在 Redis 中保存角色到实例的绑定。
type RedisSessionStore struct {
	client SessionRedisClient
	prefix string
	ttl    time.Duration
}

// NewRedisSessionStore 创建 Redis 会话绑定存储。
func NewRedisSessionStore(client SessionRedisClient, options SessionRedisOptions) (*RedisSessionStore, error) {
	if client == nil {
		return nil, ErrSessionRedisClientNil
	}
	if options.Prefix == "" || options.Prefix != strings.TrimSpace(options.Prefix) || strings.ContainsAny(options.Prefix, "{}") || options.TTL <= 0 {
		return nil, ErrInvalidSessionOptions
	}
	return &RedisSessionStore{client: client, prefix: options.Prefix, ttl: options.TTL}, nil
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
	previous, err := s.client.Eval(ctx, bindSessionScript, []string{s.key(binding.RoleID)}, encoded, s.ttl.Milliseconds())
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
	value, found, err := s.client.Get(ctx, s.key(roleID))
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
	result, err := s.client.Eval(ctx, unbindSessionScript, []string{s.key(binding.RoleID)}, encoded)
	if err != nil {
		return false, err
	}
	return conditionalResult(result)
}

func (s *RedisSessionStore) evalConditional(ctx context.Context, script string, binding SessionBinding, encoded string) (bool, error) {
	result, err := s.client.Eval(ctx, script, []string{s.key(binding.RoleID)}, encoded, s.ttl.Milliseconds())
	if err != nil {
		return false, err
	}
	return conditionalResult(result)
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
