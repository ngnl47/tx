package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// StringClient 是 Mandarin 风格 Redis string JSON 写入所需的最小接口。
//
// go-redis 适配层后续只需要把 Get/Set 转成这里的签名即可，runtime
// 基建不直接依赖具体驱动。Get 返回第二个 bool 表示 key 是否存在。
type StringClient interface {
	Get(context.Context, string) (string, bool, error)
	Set(context.Context, string, string) error
}

// Adapter 把 ChangeSet 应用到 Redis string JSON 文档。
type Adapter struct {
	client   StringClient
	resolver change.FieldResolver
}

// NewAdapter 创建 Redis ChangeSet 适配器。
func NewAdapter(client StringClient) *Adapter {
	return &Adapter{client: client}
}

// NewAdapterWithResolver 创建带字段编号解析器的 Redis ChangeSet 适配器。
func NewAdapterWithResolver(client StringClient, resolver change.FieldResolver) *Adapter {
	return &Adapter{client: client, resolver: resolver}
}

// ApplyChanges 根据 Redis 元数据和记录 ID 写入 Mandarin 风格 string JSON
// 变更。非 subKey 字段写入主 key；subKey 字段写入 mainKey:subKey。
func (a *Adapter) ApplyChanges(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, set change.ChangeSet) error {
	if a.client == nil || set.Empty() {
		return nil
	}
	prefix := redisKeyPrefix(spec, key)
	for suffix, patch := range BuildDocumentPatchesWithResolver(spec, set, a.resolver) {
		redisKey := prefix
		if suffix != "" {
			redisKey = prefix + ":" + suffix
		}
		doc, err := a.loadDocument(ctx, redisKey)
		if err != nil {
			return err
		}
		ApplyDocumentPatch(doc, patch)
		body, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("redis: marshal document %s: %w", redisKey, err)
		}
		if err := a.client.Set(ctx, redisKey, string(body)); err != nil {
			return err
		}
	}
	return nil
}

func (a *Adapter) loadDocument(ctx context.Context, key string) (map[string]any, error) {
	body, exists, err := a.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !exists || body == "" {
		return map[string]any{}, nil
	}
	doc := map[string]any{}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		return nil, fmt.Errorf("redis: unmarshal document %s: %w", key, err)
	}
	return doc, nil
}

func redisKeyPrefix(spec meta.TypeSpec, key storage.RecordKey) string {
	base := spec.Type.Ref.Name
	if spec.Redis != nil && spec.Redis.Key != "" {
		base = spec.Redis.Key
	}
	return fmt.Sprintf("%s:%v", base, key.ID)
}
