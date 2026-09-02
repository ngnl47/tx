package etcd

import (
	"context"
	"errors"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	ErrLeaseLost    = errors.New("etcd: lease lost")
	ErrLeaseStopped = errors.New("etcd: lease stopped")
)

// Lease 是由 Client 续租并报告最终状态的 etcd 租约。
type Lease struct {
	client *Client
	id     clientv3.LeaseID
	cancel context.CancelFunc
	done   chan struct{}

	mu       sync.Mutex
	doneOnce sync.Once
	doneErr  error

	revokeOnce sync.Once
	revokeErr  error
}

// Grant 创建租约并在返回前启动续租。
func (c *Client) Grant(ctx context.Context, ttl time.Duration) (*Lease, error) {
	response, err := c.raw.Grant(ctx, int64(ttl/time.Second))
	if err != nil {
		return nil, err
	}
	keepAliveCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	responses, err := c.raw.KeepAlive(keepAliveCtx, response.ID)
	if err != nil {
		cancel()
		_, _ = c.raw.Revoke(ctx, response.ID)
		return nil, err
	}
	lease := &Lease{
		client: c,
		id:     response.ID,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go lease.keepAlive(responses)
	return lease, nil
}

// ID 返回 etcd 租约编号。
func (l *Lease) ID() int64 {
	return int64(l.id)
}

// Done 在主动停止、撤销或续租丢失时关闭。
func (l *Lease) Done() <-chan struct{} {
	return l.done
}

// Err 返回租约生命周期的最终结束原因；运行期间返回 nil。
func (l *Lease) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.doneErr
}

// Put 写入一个绑定当前租约的键。
func (l *Lease) Put(ctx context.Context, key string, value []byte) error {
	return l.client.Put(ctx, key, value, int64(l.id))
}

// Create 仅在键不存在时创建一个绑定当前租约的键。
func (l *Lease) Create(ctx context.Context, key string, value []byte) (KeyValue, bool, error) {
	return l.client.Create(ctx, key, value, int64(l.id))
}

// Stop 停止续租，让关联键在 TTL 后自然过期。
func (l *Lease) Stop() error {
	l.finish(ErrLeaseStopped)
	return nil
}

// Revoke 停止续租并主动撤销租约。
func (l *Lease) Revoke(ctx context.Context) error {
	l.finish(ErrLeaseStopped)
	l.revokeOnce.Do(func() {
		_, l.revokeErr = l.client.raw.Revoke(ctx, l.id)
	})
	return l.revokeErr
}

func (l *Lease) keepAlive(responses <-chan *clientv3.LeaseKeepAliveResponse) {
	for range responses {
	}
	l.finish(ErrLeaseLost)
}

func (l *Lease) finish(err error) {
	l.doneOnce.Do(func() {
		l.mu.Lock()
		l.doneErr = err
		l.mu.Unlock()
		l.cancel()
		close(l.done)
	})
}
