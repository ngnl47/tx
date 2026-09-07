package msgq

import (
	"context"
	"sync"
	"time"
)

// roleTestStore 是会话测试共用的原子归属存根；TTL 行为由真实 Redis 测试验证。
type roleTestStore struct {
	roleMu        sync.Mutex
	owners        map[int64]RoleBinding
	roleRefreshes int
}

func (*roleTestStore) RoleLeaseTTL() time.Duration { return time.Hour }
func (s *roleTestStore) ClaimRole(_ context.Context, candidate RoleBinding) (RoleBinding, error) {
	s.roleMu.Lock()
	defer s.roleMu.Unlock()
	if s.owners == nil {
		s.owners = make(map[int64]RoleBinding)
	}
	if current, found := s.owners[candidate.RoleID]; found {
		return current, nil
	}
	s.owners[candidate.RoleID] = candidate
	return candidate, nil
}
func (s *roleTestStore) FindRole(_ context.Context, id int64) (RoleBinding, bool, error) {
	s.roleMu.Lock()
	defer s.roleMu.Unlock()
	current, found := s.owners[id]
	return current, found, nil
}
func (s *roleTestStore) RefreshRole(_ context.Context, b RoleBinding) (bool, error) {
	s.roleMu.Lock()
	defer s.roleMu.Unlock()
	s.roleRefreshes++
	current, found := s.owners[b.RoleID]
	return found && current == b, nil
}
func (s *roleTestStore) ReleaseRole(_ context.Context, b RoleBinding) (bool, error) {
	s.roleMu.Lock()
	defer s.roleMu.Unlock()
	if current, found := s.owners[b.RoleID]; !found || current != b {
		return false, nil
	}
	delete(s.owners, b.RoleID)
	return true, nil
}
