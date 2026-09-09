package mongo

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// OwnedWriter 将已声明所有权的 Mongo 根对象直接写回，不编码日志或持有 GD。
// 每个服务实例使用一个 Writer；一个 Writer 绑定一个固定所有权代次；该代次只能由控制面指定的实例持有。
type OwnedWriter struct {
	next     storage.ChangeApplier
	backend  OwnershipStore
	sequence atomic.Int64
	owner    atomic.Int64
}

// NewOwnedWriter 使用现有 mode-aware Writer 及其 Mongo 所有权 SPI。
func NewOwnedWriter(next storage.ChangeApplier) (*OwnedWriter, error) {
	backend, ok := next.(OwnershipStore)
	if !ok {
		return nil, errors.New("mongo: writer does not support ownership")
	}
	return &OwnedWriter{next: next, backend: backend}, nil
}

// Claim 在控制面确认归属之后声明记录所有权，成功后调用方必须重新加载数据。
func (writer *OwnedWriter) Claim(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, owner int64) error {
	if owner <= 0 {
		return errors.New("mongo: positive owner is required")
	}
	writer.owner.CompareAndSwap(0, owner)
	if writer.owner.Load() != owner {
		return errors.New("mongo: writer is bound to another owner")
	}
	sequence, err := writer.backend.ClaimOwnership(ctx, spec, key, owner)
	if err != nil {
		return err
	}
	for current := writer.sequence.Load(); current < sequence; current = writer.sequence.Load() {
		if writer.sequence.CompareAndSwap(current, sequence) {
			break
		}
	}
	return nil
}

// ApplyChangesWithOptions 在原实体锁内读取变更；失败不清理，后续 Flush 携带全部未确认变更并使用更大序号。
func (writer *OwnedWriter) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, options storage.ApplyOptions) error {
	if spec.Storage.Mode != meta.StorageModeMongo {
		return writer.next.ApplyChangesWithOptions(ctx, spec, key, source, resolver, values, options)
	}
	owner := writer.owner.Load()
	if owner == 0 {
		return errors.New("mongo: record ownership has not been claimed")
	}
	sequence := writer.sequence.Add(1)
	if sequence <= 0 {
		return errors.New("mongo: write sequence exhausted")
	}
	return writer.backend.ApplyOwnedChanges(ctx, spec, key, source, resolver, values, WritePosition{Owner: owner, Sequence: sequence}, options)
}
