package sessionid

import (
	"context"
	"errors"
	"fmt"
)

// RevisionWriter 提供单次成功写入的 etcd 修订版本。
type RevisionWriter interface {
	PutRevision(ctx context.Context, key string, value []byte, leaseID int64) (int64, error)
}

// EtcdAllocator 使用每次独立写入的修订版本发号，不持有连接所有权。
type EtcdAllocator struct {
	writer RevisionWriter
	key    string
}

var _ Allocator = (*EtcdAllocator)(nil)

// NewEtcdAllocator 借用写入器，在指定键上覆盖固定值以取得新修订版本。
func NewEtcdAllocator(writer RevisionWriter, key string) (*EtcdAllocator, error) {
	if writer == nil {
		return nil, errors.New("sessionid: revision writer is nil")
	}
	if key == "" {
		return nil, errors.New("sessionid: issuer key is empty")
	}
	return &EtcdAllocator{writer: writer, key: key}, nil
}

// AllocateSessionID 返回一个正数且不超过 JavaScript 安全整数上限的 SessionID。
func (allocator *EtcdAllocator) AllocateSessionID(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	revision, err := allocator.writer.PutRevision(ctx, allocator.key, []byte("1"), 0)
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if revision <= 0 || revision > MaxSessionID {
		return 0, fmt.Errorf("sessionid: revision %d is outside the session ID range", revision)
	}
	return revision, nil
}
