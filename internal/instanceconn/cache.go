// Package instanceconn 统一管理服务实例连接的复用和重启轮换。
package instanceconn

import (
	"context"
	"errors"
	"sync"

	"github.com/ngnl47/tx/modules/discovery"
)

var (
	ErrClosed        = errors.New("instance connection cache is closed")
	ErrStaleInstance = errors.New("instance connection identity is stale")
)

type entry[T interface{ Close() error }] struct {
	instanceID string
	connection T
}

// Cache 按 endpoint 复用当前实例连接；同址重启时关闭旧实例连接。
type Cache[T interface{ Close() error }] struct {
	directory   discovery.Directory
	mu          sync.Mutex
	connections map[string]entry[T]
	closed      bool
}

func New[T interface{ Close() error }](directory discovery.Directory) *Cache[T] {
	return &Cache[T]{directory: directory, connections: make(map[string]entry[T])}
}

// Get 只在拨号后的冷路径复核身份，不为每条热路径消息增加目录查询。
func (c *Cache[T]) Get(ctx context.Context, instance discovery.Instance, dial func(context.Context, discovery.Instance) (T, error)) (T, error) {
	var zero T
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return zero, ErrClosed
	}
	if cached, found := c.connections[instance.Endpoint]; found && cached.instanceID == instance.ID {
		c.mu.Unlock()
		return cached.connection, nil
	}
	c.mu.Unlock()

	created, err := dial(ctx, instance)
	if err != nil {
		return zero, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = created.Close()
		return zero, ErrClosed
	}
	if cached, found := c.connections[instance.Endpoint]; found && cached.instanceID == instance.ID {
		c.mu.Unlock()
		_ = created.Close()
		return cached.connection, nil
	}
	current, found := c.directory.LookupInstance(instance.ID)
	if !found || current.Endpoint != instance.Endpoint {
		c.mu.Unlock()
		_ = created.Close()
		return zero, ErrStaleInstance
	}
	previous, hadPrevious := c.connections[instance.Endpoint]
	c.connections[instance.Endpoint] = entry[T]{instanceID: instance.ID, connection: created}
	c.mu.Unlock()
	if hadPrevious {
		_ = previous.connection.Close()
	}
	return created, nil
}

func (c *Cache[T]) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	connections := c.connections
	c.connections = nil
	c.mu.Unlock()
	var errs []error
	for _, connection := range connections {
		errs = append(errs, connection.connection.Close())
	}
	return errors.Join(errs...)
}
