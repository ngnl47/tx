package runtime

import (
	"context"
	"fmt"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
	txmongo "github.com/ngnl47/tx/storage/mongo"
)

func (writer storageChangeWriter) ownershipStore(spec meta.TypeSpec) (txmongo.OwnershipStore, error) {
	if writer.storage == nil {
		return nil, ErrNotConfigured
	}
	if spec.Storage.Mode != meta.StorageModeMongo {
		return nil, fmt.Errorf("storage runtime: ownership requires mongo storage mode")
	}
	if err := writer.storage.requireStorageMode(meta.StorageModeMongo); err != nil {
		return nil, err
	}
	backend, ok := writer.mongo.(txmongo.OwnershipStore)
	if !ok {
		return nil, fmt.Errorf("storage runtime: mongo ownership is not configured")
	}
	return backend, nil
}

// ClaimOwnership 复用存储健康闸门，业务不能绕过不可写状态声明所有权。
func (writer storageChangeWriter) ClaimOwnership(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, owner int64) (int64, error) {
	backend, err := writer.ownershipStore(spec)
	if err != nil {
		return 0, err
	}
	sequence, err := backend.ClaimOwnership(ctx, spec, key, owner)
	if err != nil {
		writer.storage.observeStorageError(err)
	}
	return sequence, err
}

// ApplyOwnedChanges 复用现有 Mongo 路由与健康状态，不增加每次写入前的探测 I/O。
func (writer storageChangeWriter) ApplyOwnedChanges(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, position txmongo.WritePosition, options storage.ApplyOptions) error {
	backend, err := writer.ownershipStore(spec)
	if err != nil {
		return err
	}
	err = backend.ApplyOwnedChanges(ctx, spec, key, source, resolver, values, position, options)
	if err != nil {
		writer.storage.observeStorageError(err)
	}
	return err
}
