package msgq

import (
	"context"
	"time"

	txmetrics "github.com/ngnl47/tx/metrics"
)

// BroadcastRole 仅向本实例内绑定 roleID 的邮箱分发，不执行 Redis 寻址或跨实例广播。
// 调用开始即交接只读 payload；即使返回错误，成功邮箱也可能已持有该载荷。
// 分布式调用使用 PubRole，并提供完整归属版本。
func (s *Server) BroadcastRole(ctx context.Context, roleID int64, payload []byte) (BroadcastResult, error) {
	return s.broadcastRole(ctx, roleID, nil, payload)
}

// PubRole 核对本地归属版本后向该角色所有邮箱投递，不查询 Redis，也不跨实例转发。
// 返回部分成功时不可重发整个载荷；每个邮箱独立分配 Seq，不返回共同 Seq。
// 与 BroadcastRole 一样，调用开始即交接只读载荷，错误返回不代表所有邮箱均未持有载荷。
func (s *Server) PubRole(ctx context.Context, binding RoleBinding, payload []byte) (BroadcastResult, error) {
	if binding.Validate() != nil {
		return BroadcastResult{}, ErrInvalidHeader
	}
	return s.broadcastRole(ctx, binding.RoleID, &binding, payload)
}

func (s *Server) broadcastRole(ctx context.Context, roleID int64, expected *RoleBinding, payload []byte) (result BroadcastResult, err error) {
	started := time.Now()
	defer func() {
		txmetrics.RecordMsgQPublish(txmetrics.PublishRole, result.Delivered, time.Since(started), err)
	}()
	if s == nil {
		return BroadcastResult{}, ErrServerNil
	}
	if err := s.checkStartup(); err != nil {
		return BroadcastResult{}, err
	}
	if roleID <= 0 {
		return BroadcastResult{}, ErrInvalidHeader
	}
	s.mu.RLock()
	lease, found := s.roleLeases[roleID]
	if (expected != nil && (!found || lease.binding != *expected)) || (found && !time.Now().Before(lease.expires)) {
		s.mu.RUnlock()
		if expected != nil {
			return BroadcastResult{}, ErrRoleBindingChanged
		}
		return BroadcastResult{}, ErrSessionChanged
	}
	indexed := s.roles[roleID]
	// 版本校验与邮箱快照使用同一把读锁；归属换代后的新邮箱不会混入旧请求。
	sessions := make([]*mailboxSession, 0, len(indexed))
	for session := range indexed {
		sessions = append(sessions, session)
	}
	s.mu.RUnlock()
	return s.broadcastSessions(ctx, sessions, payload)
}

// addRoleLocked 在持有 session.mu 且初始化成功后发布本地索引。
func (s *Server) addRoleLocked(session *mailboxSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sessions := s.roles[session.roleID]
	if sessions == nil {
		sessions = make(map[*mailboxSession]struct{})
		s.roles[session.roleID] = sessions
	}
	sessions[session] = struct{}{}
}
