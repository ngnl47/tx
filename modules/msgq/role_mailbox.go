package msgq

import "context"

// BroadcastRole 仅向本实例内绑定 roleID 的邮箱分发，不执行 Redis 寻址或跨实例广播。
// 调用开始即交接只读 payload；即使返回错误，成功邮箱也可能已持有该载荷。
// 分布式入口必须先确定并校验角色归属，不能把此本地方法当作集群投递接口。
func (s *Server) BroadcastRole(ctx context.Context, roleID int64, payload []byte) (BroadcastResult, error) {
	if s == nil {
		return BroadcastResult{}, ErrServerNil
	}
	if roleID <= 0 {
		return BroadcastResult{}, ErrInvalidHeader
	}
	s.mu.RLock()
	indexed := s.roles[roleID]
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
