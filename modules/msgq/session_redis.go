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

const bindPrimaryScript = `
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return 1
`

const refreshPrimaryScript = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])
`

const unbindPrimaryScript = `
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

// SessionRedisResolver 按索引身份选择会话数据所在的 Redis 客户端。
// 精确绑定使用 SessionID，主会话投影使用 RoleID。
type SessionRedisResolver interface {
	ResolveSessionRedis(context.Context, int64) (SessionRedisClient, error)
}

// SessionRedisOptions 配置一个短期会话绑定命名空间。
type SessionRedisOptions struct {
	Prefix string
	TTL    time.Duration
}

// RedisSessionStore 在 Redis 中保存精确会话绑定和角色主会话投影。
type RedisSessionStore struct {
	resolve func(context.Context, int64) (SessionRedisClient, error)
	prefix  string
	ttl     time.Duration
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
	return newRedisSessionStore(func(context.Context, int64) (SessionRedisClient, error) { return client, nil }, options)
}

// NewRoutedRedisSessionStore 创建按索引身份路由 Redis 分片的会话存储。
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

// Create 仅在 SessionID 尚未占用时创建精确绑定。
func (s *RedisSessionStore) Create(ctx context.Context, binding SessionBinding) (bool, error) {
	if err := binding.Validate(); err != nil {
		return false, err
	}
	encoded, err := encodeSessionBinding(binding)
	if err != nil {
		return false, err
	}
	client, err := s.client(ctx, binding.SessionID)
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
	client, err := s.client(ctx, expected.SessionID)
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
	client, err := s.client(ctx, sessionID)
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
	client, err := s.client(ctx, binding.SessionID)
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
	client, err := s.client(ctx, binding.SessionID)
	if err != nil {
		return false, err
	}
	result, err := client.Eval(ctx, script, []string{s.sessionKey(binding.SessionID)}, encoded, s.ttl.Milliseconds())
	if err != nil {
		return false, err
	}
	return conditionalResult(result)
}

// BindPrimary 将角色的主会话替换为 sessionID。
func (s *RedisSessionStore) BindPrimary(ctx context.Context, roleID, sessionID int64) error {
	if roleID <= 0 || sessionID <= 0 {
		return ErrInvalidSession
	}
	client, err := s.client(ctx, roleID)
	if err != nil {
		return err
	}
	result, err := client.Eval(ctx, bindPrimaryScript, []string{s.primaryKey(roleID)}, strconv.FormatInt(sessionID, 10), s.ttl.Milliseconds())
	if err != nil {
		return err
	}
	_, err = conditionalResult(result)
	return err
}

// FindPrimary 返回角色当前唯一主 SessionID。
func (s *RedisSessionStore) FindPrimary(ctx context.Context, roleID int64) (int64, bool, error) {
	if roleID <= 0 {
		return 0, false, ErrInvalidSession
	}
	client, err := s.client(ctx, roleID)
	if err != nil {
		return 0, false, err
	}
	value, found, err := client.Get(ctx, s.primaryKey(roleID))
	if err != nil || !found {
		return 0, found, err
	}
	sessionID, err := strconv.ParseInt(value, 10, 64)
	if err != nil || sessionID <= 0 {
		return 0, false, ErrSessionStoreResponse
	}
	return sessionID, true, nil
}

// RefreshPrimary 仅在角色仍指向 sessionID 时续期主会话投影。
func (s *RedisSessionStore) RefreshPrimary(ctx context.Context, roleID, sessionID int64) (bool, error) {
	return s.evalPrimaryConditional(ctx, refreshPrimaryScript, roleID, sessionID, true)
}

// UnbindPrimary 仅在角色仍指向 sessionID 时删除主会话投影。
func (s *RedisSessionStore) UnbindPrimary(ctx context.Context, roleID, sessionID int64) (bool, error) {
	return s.evalPrimaryConditional(ctx, unbindPrimaryScript, roleID, sessionID, false)
}

func (s *RedisSessionStore) evalPrimaryConditional(ctx context.Context, script string, roleID, sessionID int64, refresh bool) (bool, error) {
	if roleID <= 0 || sessionID <= 0 {
		return false, ErrInvalidSession
	}
	client, err := s.client(ctx, roleID)
	if err != nil {
		return false, err
	}
	args := []any{strconv.FormatInt(sessionID, 10)}
	if refresh {
		args = append(args, s.ttl.Milliseconds())
	}
	result, err := client.Eval(ctx, script, []string{s.primaryKey(roleID)}, args...)
	if err != nil {
		return false, err
	}
	return conditionalResult(result)
}

func (s *RedisSessionStore) client(ctx context.Context, shardKey int64) (SessionRedisClient, error) {
	if s == nil || s.resolve == nil {
		return nil, ErrSessionRedisClientNil
	}
	client, err := s.resolve(ctx, shardKey)
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, ErrSessionRedisClientNil
	}
	return client, nil
}

func (s *RedisSessionStore) sessionKey(sessionID int64) string {
	return s.prefix + ":session:v2:{" + strconv.FormatInt(sessionID, 10) + "}"
}

func (s *RedisSessionStore) primaryKey(roleID int64) string {
	return s.prefix + ":primary:v1:{" + strconv.FormatInt(roleID, 10) + "}"
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
