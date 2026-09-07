package msgq

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

type roleLease struct {
	binding   RoleBinding
	expires   time.Time
	refreshAt time.Time
}

// ensureRole 与维护循环共用角色初始化锁，防止最后邮箱释放与新登录交错。
func (s *Server) ensureRole(ctx context.Context, roleID int64) error {
	started := time.Now()
	s.mu.RLock()
	lease, exists := s.roleLeases[roleID]
	s.mu.RUnlock()
	if exists {
		if !started.Before(lease.expires) {
			return ErrSessionChanged
		}
		return nil
	}
	binding, err := s.store.ClaimRole(ctx, RoleBinding{RoleID: roleID, InstanceID: s.instanceID, Generation: uuid.NewString()})
	if err != nil {
		return err
	}
	if binding.Validate() != nil || binding.RoleID != roleID {
		return ErrSessionStoreResponse
	}
	if binding.InstanceID != s.instanceID {
		return ErrSessionChanged
	}
	// ClaimRole 可能返回临近过期的既有绑定，只有条件续期成功才获得完整本地租期。
	refreshed, err := s.store.RefreshRole(ctx, binding)
	if err != nil {
		return err
	}
	if !refreshed {
		return ErrSessionChanged
	}
	lease = s.newRoleLease(binding, started)
	if !time.Now().Before(lease.expires) {
		return ErrSessionChanged
	}
	s.mu.Lock()
	s.roleLeases[roleID] = lease
	s.mu.Unlock()
	return nil
}

func (s *Server) newRoleLease(binding RoleBinding, started time.Time) roleLease {
	ttl := s.store.RoleLeaseTTL()
	return roleLease{binding: binding, expires: started.Add(ttl), refreshAt: started.Add(ttl / 2)}
}

func (s *Server) roleCurrent(roleID int64, now time.Time) bool {
	s.mu.RLock()
	lease, found := s.roleLeases[roleID]
	s.mu.RUnlock()
	return found && now.Before(lease.expires)
}

// maintainRoles 只遍历本地角色归属；错误不会延长本地有效期，过期后拒绝旧邮箱活动。
func (s *Server) maintainRoles(ctx context.Context, now time.Time) error {
	s.mu.RLock()
	ids := make([]int64, 0, len(s.roleLeases))
	for id := range s.roleLeases {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	var errs []error
	for _, id := range ids {
		lock := &s.initLocks[uint64(id)%initLockShards]
		lock.Lock()
		if err := s.maintainRole(ctx, id, now); err != nil {
			errs = append(errs, err)
		}
		lock.Unlock()
	}
	return errors.Join(errs...)
}

func (s *Server) maintainRole(ctx context.Context, roleID int64, now time.Time) error {
	s.mu.RLock()
	lease, found := s.roleLeases[roleID]
	count := len(s.roles[roleID])
	s.mu.RUnlock()
	if !found {
		return nil
	}
	if count == 0 || !now.Before(lease.expires) {
		s.retireRole(lease)
		_, err := s.store.ReleaseRole(ctx, lease.binding)
		return err
	}
	if now.Before(lease.refreshAt) {
		return nil
	}
	started := time.Now()
	refreshed, err := s.store.RefreshRole(ctx, lease.binding)
	if err != nil {
		return err
	}
	if !refreshed {
		s.retireRole(lease)
		return ErrSessionChanged
	}
	updated := s.newRoleLease(lease.binding, started)
	if !time.Now().Before(updated.expires) {
		s.retireRole(lease)
		return ErrSessionChanged
	}
	s.mu.Lock()
	s.roleLeases[roleID] = updated
	s.mu.Unlock()
	return nil
}

// retireRole 使归属下所有旧邮箱失效；Redis 精确 Session 记录由自身 TTL 清理。
func (s *Server) retireRole(lease roleLease) {
	s.mu.Lock()
	delete(s.roleLeases, lease.binding.RoleID)
	sessions := make([]*mailboxSession, 0, len(s.roles[lease.binding.RoleID]))
	for session := range s.roles[lease.binding.RoleID] {
		sessions = append(sessions, session)
	}
	s.mu.Unlock()
	for _, session := range sessions {
		session.mu.Lock()
		s.removeLocked(session)
		session.mu.Unlock()
	}
}
