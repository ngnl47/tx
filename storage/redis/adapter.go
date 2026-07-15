package redis

import (
	"context"
	"fmt"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// HashClient 是 Redis hash 写入所需的最小接口。
//
// go-redis 适配层后续只需要把 HSet/HDel 转成这里的签名即可，runtime
// 基建不直接依赖具体驱动。
type HashClient interface {
	HSet(context.Context, string, map[string]any) error
	HDel(context.Context, string, []string) error
}

// Adapter 把 ChangeSet 应用到 Redis hash。
type Adapter struct {
	client HashClient
}

// NewAdapter 创建 Redis ChangeSet 适配器。
func NewAdapter(client HashClient) *Adapter {
	return &Adapter{client: client}
}

// ApplyChanges 根据 Redis 元数据和记录 ID 写入 hash 变更。
func (a *Adapter) ApplyChanges(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, set change.ChangeSet) error {
	if a.client == nil || set.Empty() {
		return nil
	}
	prefix := redisKeyPrefix(spec, key)
	for hash, fields := range BuildHashChanges(set) {
		if err := a.client.HSet(ctx, prefix+":"+hash, fields); err != nil {
			return err
		}
	}
	for hash, fields := range BuildHashDeletes(set) {
		if err := a.client.HDel(ctx, prefix+":"+hash, fields); err != nil {
			return err
		}
	}
	return nil
}

func redisKeyPrefix(spec meta.TypeSpec, key storage.RecordKey) string {
	base := spec.Type.Ref.Name
	if spec.Redis != nil && spec.Redis.Key != "" {
		base = spec.Redis.Key
	}
	return fmt.Sprintf("%s:%v", base, key.ID)
}
