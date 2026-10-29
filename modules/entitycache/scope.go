package entitycache

import (
	"context"
	"errors"
	"sync"

	"github.com/ngnl47/tx/modules/txn"
)

var ErrScopeRequired = errors.New("entity cache: operation scope is required")
var ErrScopeClosed = errors.New("entity cache: operation scope is closed")

type scopeKey struct{}
type scope struct {
	mu       sync.Mutex
	closed   bool
	releases map[any]func()
}

// Begin 建立对象使用期；end 必须在工作实际退出时调用，不能用 ctx 取消代替。
// 它不创建事务、不获取数据锁，也不撤销普通修改。
func Begin(ctx context.Context) (context.Context, func()) {
	s := &scope{releases: make(map[any]func())}
	return context.WithValue(ctx, scopeKey{}, s), s.end
}

func (s *scope) end() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	releases := s.releases
	s.releases = nil
	s.mu.Unlock()
	for _, release := range releases {
		release()
	}
}

// Find 返回本操作使用期内的唯一对象；使用期结束后不得继续保留可变引用。
func (cache *Cache[K, V]) Find(ctx context.Context, key K) (value V, exists bool, err error) {
	s, _ := ctx.Value(scopeKey{}).(*scope)
	tx, active := txn.FromContext(ctx)
	if tx != nil && !active {
		return value, false, txn.ErrInactive
	}
	if s == nil && !active {
		return value, false, ErrScopeRequired
	}
	if s != nil {
		s.mu.Lock()
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return value, false, ErrScopeClosed
		}
	}
	entry, exists, err := cache.acquireLoaded(ctx, key)
	if err != nil || !exists {
		return value, exists, err
	}
	release := func() { cache.mu.Lock(); entry.users--; cache.mu.Unlock() }
	// 事务优先：使用权覆盖锁释放及提交/回滚回调，不能随嵌套操作提前结束。
	if active {
		tx.OnFinish(release)
	} else {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			release()
			return value, false, ErrScopeClosed
		}
		if _, ok := s.releases[entry]; ok {
			release()
		} else {
			s.releases[entry] = release
		}
		s.mu.Unlock()
	}
	return entry.value, true, nil
}
