package msgq

import (
	"context"
	"encoding/json"
	"strconv"
)

const claimRoleScript = `
local current = redis.call('GET', KEYS[1])
if current then return current end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return ARGV[1]
`

// ClaimRole 原子建立角色归属，已有归属原样返回且不会因查询而续期。
// candidate.Generation 由调用方为本次申请生成唯一令牌，不能复用已释放代际。
func (s *RedisSessionStore) ClaimRole(ctx context.Context, candidate RoleBinding) (RoleBinding, error) {
	if err := candidate.Validate(); err != nil {
		return RoleBinding{}, err
	}
	client, err := s.client(ctx, candidate.RoleID)
	if err != nil {
		return RoleBinding{}, err
	}
	encoded, err := json.Marshal(candidate)
	if err != nil {
		return RoleBinding{}, err
	}
	value, err := client.Eval(ctx, claimRoleScript, []string{s.roleKey(candidate.RoleID)}, string(encoded), s.ttl.Milliseconds())
	if err != nil {
		return RoleBinding{}, err
	}
	return decodeRoleBinding(candidate.RoleID, value)
}

// FindRole 按 RoleID 读取唯一 MsgQ 归属，不读取 Session 或主会话投影。
func (s *RedisSessionStore) FindRole(ctx context.Context, roleID int64) (RoleBinding, bool, error) {
	if roleID <= 0 {
		return RoleBinding{}, false, ErrInvalidSession
	}
	client, err := s.client(ctx, roleID)
	if err != nil {
		return RoleBinding{}, false, err
	}
	value, found, err := client.Get(ctx, s.roleKey(roleID))
	if err != nil || !found {
		return RoleBinding{}, false, err
	}
	binding, err := decodeRoleBinding(roleID, value)
	return binding, err == nil, err
}

// RefreshRole 仅续期完整匹配的归属。由 MsgQ 低频维护，不在消息投递热路径调用。
func (s *RedisSessionStore) RefreshRole(ctx context.Context, expected RoleBinding) (bool, error) {
	return s.mutateRole(ctx, refreshSessionScript, expected)
}

// ReleaseRole 仅释放完整匹配的归属，旧实例或旧代际不能删除后续归属。
func (s *RedisSessionStore) ReleaseRole(ctx context.Context, expected RoleBinding) (bool, error) {
	return s.mutateRole(ctx, unbindSessionScript, expected)
}

func (s *RedisSessionStore) mutateRole(ctx context.Context, script string, expected RoleBinding) (bool, error) {
	if err := expected.Validate(); err != nil {
		return false, err
	}
	client, err := s.client(ctx, expected.RoleID)
	if err != nil {
		return false, err
	}
	encoded, err := json.Marshal(expected)
	if err != nil {
		return false, err
	}
	value, err := client.Eval(ctx, script, []string{s.roleKey(expected.RoleID)}, string(encoded), s.ttl.Milliseconds())
	if err != nil {
		return false, err
	}
	return conditionalResult(value)
}

func (s *RedisSessionStore) roleKey(roleID int64) string {
	return s.prefix + ":role:{" + strconv.FormatInt(roleID, 10) + "}"
}

func decodeRoleBinding(roleID int64, encoded string) (RoleBinding, error) {
	var binding RoleBinding
	if err := json.Unmarshal([]byte(encoded), &binding); err != nil || binding.RoleID != roleID || binding.Validate() != nil {
		return RoleBinding{}, ErrSessionStoreResponse
	}
	return binding, nil
}
