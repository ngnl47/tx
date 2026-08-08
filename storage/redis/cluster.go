package redis

import (
	"context"
	"fmt"
	"reflect"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// Router 按 routeType 和 shardKey 选择 Redis 端点名称。
type Router interface {
	RouteRedis(context.Context, string, any) (string, error)
}

// RouterFunc 允许用函数快速实现 Router。
type RouterFunc func(context.Context, string, any) (string, error)

func (f RouterFunc) RouteRedis(ctx context.Context, routeType string, shardKey any) (string, error) {
	return f(ctx, routeType, shardKey)
}

// Cluster 把 Redis 操作按 routeType 和 RecordKey.ShardKey 路由到具体端点 Adapter。
type Cluster struct {
	router Router
	shards map[string]*Adapter
}

// NewCluster 创建 Redis route/cluster 适配器。
func NewCluster(router Router, shards map[string]ScriptClient) *Cluster {
	cluster := &Cluster{
		router: router,
		shards: make(map[string]*Adapter, len(shards)),
	}
	for name, client := range shards {
		cluster.shards[name] = NewAdapter(client)
	}
	return cluster
}

func (c *Cluster) ApplyDeltasWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, deltas change.DeltaSet, resolver change.FieldNameResolver, values change.DeltaValueResolver, options storage.ApplyOptions) error {
	if len(deltas) == 0 {
		return nil
	}
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return err
	}
	return adapter.ApplyDeltasWithOptions(ctx, spec, key, deltas, resolver, values, options)
}

func (c *Cluster) ReplaceDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, options storage.ApplyOptions) error {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return err
	}
	return adapter.ReplaceDocumentWithOptions(ctx, spec, key, doc, options)
}

func (c *Cluster) ReplaceDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any) error {
	return c.ReplaceDocumentWithOptions(ctx, spec, key, doc, storage.ApplyOptions{Upsert: true})
}

func (c *Cluster) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return err
	}
	return adapter.ApplyDelete(ctx, spec, key)
}

func (c *Cluster) ApplyDeleteWithRevision(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, expectedRevision string) (bool, string, error) {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return false, "", err
	}
	return adapter.ApplyDeleteWithRevision(ctx, spec, key, expectedRevision)
}

func (c *Cluster) ReadRecordRevision(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) (string, error) {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return "", err
	}
	return adapter.ReadRecordRevision(ctx, spec, key)
}

func (c *Cluster) LockRecord(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, options storage.LockOptions) (storage.RecordLock, error) {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return nil, err
	}
	return adapter.LockRecord(ctx, spec, key, options)
}

func (c *Cluster) LoadDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return nil, false, err
	}
	return adapter.LoadDocument(ctx, spec, key, fields...)
}

func (c *Cluster) LoadDocuments(ctx context.Context, spec meta.TypeSpec, keys []storage.RecordKey, fields ...string) ([]map[string]any, []bool, error) {
	return c.LoadDocumentsWithOptions(ctx, spec, keys, LoadOptions{}, fields...)
}

func (c *Cluster) LoadDocumentsWithOptions(ctx context.Context, spec meta.TypeSpec, keys []storage.RecordKey, options LoadOptions, fields ...string) ([]map[string]any, []bool, error) {
	docs := make([]map[string]any, len(keys))
	exists := make([]bool, len(keys))
	if len(keys) == 0 {
		return docs, exists, nil
	}
	groups := make(map[string][]int)
	for i, key := range keys {
		shard, err := c.shardName(ctx, spec, key)
		if err != nil {
			return nil, nil, err
		}
		groups[shard] = append(groups[shard], i)
	}
	for shard, indexes := range groups {
		adapter, ok := c.shards[shard]
		if !ok {
			return nil, nil, fmt.Errorf("redis: shard %q is not configured", shard)
		}
		groupKeys := make([]storage.RecordKey, len(indexes))
		for i, idx := range indexes {
			groupKeys[i] = keys[idx]
		}
		groupDocs, groupExists, err := adapter.LoadDocumentsWithOptions(ctx, spec, groupKeys, options, fields...)
		if err != nil {
			return nil, nil, err
		}
		if len(groupDocs) != len(groupKeys) || len(groupExists) != len(groupKeys) {
			return nil, nil, fmt.Errorf("redis: shard %q returned docs=%d exists=%d for %d keys", shard, len(groupDocs), len(groupExists), len(groupKeys))
		}
		for i, idx := range indexes {
			docs[idx] = groupDocs[i]
			exists[idx] = groupExists[i]
		}
	}
	return docs, exists, nil
}

func (c *Cluster) adapterFor(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) (*Adapter, error) {
	shard, err := c.shardName(ctx, spec, key)
	if err != nil {
		return nil, err
	}
	adapter, ok := c.shards[shard]
	if !ok {
		return nil, fmt.Errorf("redis: shard %q is not configured", shard)
	}
	return adapter, nil
}

func (c *Cluster) shardName(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) (string, error) {
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return "", err
	}
	if c == nil || isNilRouter(c.router) {
		return "", fmt.Errorf("redis: router is not configured")
	}
	shard, err := c.router.RouteRedis(ctx, redisRouteType(spec), redisRecordShardKey(key))
	if err != nil {
		return "", err
	}
	if shard == "" {
		return "", fmt.Errorf("redis: route not found for routeType %q shardKey %v", redisRouteType(spec), redisRecordShardKey(key))
	}
	return shard, nil
}

func isNilRouter(router Router) bool {
	if router == nil {
		return true
	}
	value := reflect.ValueOf(router)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func redisRouteType(spec meta.TypeSpec) string {
	if spec.Redis == nil {
		return ""
	}
	return spec.Redis.Route.Type
}

func redisRecordShardKey(key storage.RecordKey) any {
	if key.ShardKey != nil {
		return key.ShardKey
	}
	return key.ID
}
