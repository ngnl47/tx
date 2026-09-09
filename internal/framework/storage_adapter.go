package framework

import (
	"context"

	storageruntime "github.com/ngnl47/tx/storage/runtime"
)

// ErrStorageUnavailable 表示目标存储后端当前不可用。
var ErrStorageUnavailable = storageruntime.ErrStorageUnavailable

type storageRuntime = storageruntime.Runtime
type storageOptions = storageruntime.Config

func newStorageRuntime(ctx context.Context) *storageRuntime {
	return storageruntime.New(ctx)
}
