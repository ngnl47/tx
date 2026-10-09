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

const createSessionScript = `
local created = redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX')
if created then return 1 end
return 0
`

const rebindSessionScript = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1
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

// RedisSessionStore 在 Redis 中保存精确会话绑定和角色 MsgQ 归属。
type RedisSessionStore struct {
	redis  SessionRedisClient
	prefix string
	ttl    time.Duration
}

func (s *RedisSessionStore) leaseRefreshInterval() time.Duration {
	if s == nil {
		return 0
	}
	return s.ttl / 2
}

// NewRedisSessionStore 创建 Redis 会话绑定存储。
func NewRedisSessionStore(client SessionRedisClient, options SessionRedisOptions) (*RedisSessionStore, error) {
	if client == nil {
		return nil, ErrSessionRedisClientNil
	}
	if options.Prefix == "" || options.Prefix != strings.TrimSpace(options.Prefix) || strings.ContainsAny(options.Prefix, "{}") || options.TTL < time.Millisecond {
		return nil, ErrInvalidSessionOptions
	}
	return &RedisSessionStore{redis: client, prefix: options.Prefix, ttl: options.TTL}, nil
}

// Create 仅在 SessionID 尚未占用时创建精确绑定。
func (s *RedisSessionStore) Create(ctx context.Context, binding SessionBinding) (bool, error) {
	if err := binding.Validate(); err != nil {
		return false, err
	}
	encoded, err := encodeSessionBinding(binding)
	if err != nil {
		return false, err
	}
	client, err := s.client()
	if err != nil {
		return false, err
	}
	result, err := client.Eval(ctx, createSessionScript, []string{s.sessionKey(binding.SessionID)}, encoded, s.ttl.Milliseconds())
	if err != nil {
		return false, err
	}
	return conditionalResult(result)
}

// Rebind 仅在完整旧绑定匹配时更新同一会话的 ConnectionID。
func (s *RedisSessionStore) Rebind(ctx context.Context, expected, replacement SessionBinding) (bool, error) {
	if err := expected.Validate(); err != nil {
		return false, err
	}
	if err := replacement.Validate(); err != nil {
		return false, err
	}
	if expected.RoleID != replacement.RoleID || expected.SessionID != replacement.SessionID || expected.InstanceID != replacement.InstanceID {
		return false, ErrInvalidSession
	}
	expectedEncoded, err := encodeSessionBinding(expected)
	if err != nil {
		return false, err
	}
	replacementEncoded, err := encodeSessionBinding(replacement)
	if err != nil {
		return false, err
	}
	client, err := s.client()
	if err != nil {
		return false, err
	}
	result, err := client.Eval(ctx, rebindSessionScript, []string{s.sessionKey(expected.SessionID)}, expectedEncoded, replacementEncoded, s.ttl.Milliseconds())
	if err != nil {
		return false, err
	}
	return conditionalResult(result)
}

// Find 返回 SessionID 的精确绑定。
func (s *RedisSessionStore) Find(ctx context.Context, sessionID int64) (SessionBinding, bool, error) {
	if sessionID <= 0 {
		return SessionBinding{}, false, ErrInvalidSession
	}
	client, err := s.client()
	if err != nil {
		return SessionBinding{}, false, err
	}
	value, found, err := client.Get(ctx, s.sessionKey(sessionID))
	if err != nil || !found {
		return SessionBinding{}, found, err
	}
	binding, err := decodeSessionBinding(sessionID, value)
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
	client, err := s.client()
	if err != nil {
		return false, err
	}
	result, err := client.Eval(ctx, unbindSessionScript, []string{s.sessionKey(binding.SessionID)}, encoded)
	if err != nil {
		return false, err
	}
	return conditionalResult(result)
}

func (s *RedisSessionStore) evalConditional(ctx context.Context, script string, binding SessionBinding, encoded string) (bool, error) {
	client, err := s.client()
	if err != nil {
		return false, err
	}
	result, err := client.Eval(ctx, script, []string{s.sessionKey(binding.SessionID)}, encoded, s.ttl.Milliseconds())
	if err != nil {
		return false, err
	}
	return conditionalResult(result)
}

func (s *RedisSessionStore) client() (SessionRedisClient, error) {
	if s == nil || s.redis == nil {
		return nil, ErrSessionRedisClientNil
	}
	return s.redis, nil
}

func (s *RedisSessionStore) sessionKey(sessionID int64) string {
	return s.prefix + ":session:v2:{" + strconv.FormatInt(sessionID, 10) + "}"
}

type sessionRedisRecord struct {
	RoleID       int64  `json:"roleId"`
	SessionID    int64  `json:"sessionId"`
	ConnectionID int64  `json:"connectionId"`
	InstanceID   string `json:"instanceId"`
}

func encodeSessionBinding(binding SessionBinding) (string, error) {
	encoded, err := json.Marshal(sessionRedisRecord{
		RoleID:       binding.RoleID,
		SessionID:    binding.SessionID,
		ConnectionID: binding.ConnectionID,
		InstanceID:   binding.InstanceID,
	})
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func decodeSessionBinding(sessionID int64, value string) (SessionBinding, error) {
	var record sessionRedisRecord
	if err := json.Unmarshal([]byte(value), &record); err != nil {
		return SessionBinding{}, ErrSessionStoreResponse
	}
	binding := SessionBinding{
		RoleID:       record.RoleID,
		SessionID:    record.SessionID,
		ConnectionID: record.ConnectionID,
		InstanceID:   record.InstanceID,
	}
	if err := binding.Validate(); err != nil || binding.SessionID != sessionID {
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
