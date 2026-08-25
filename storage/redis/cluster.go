package redis

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// Router 按 routeType 和 shardKey 选择 Redis 端点名称。
type Router interface {
	RouteRedis(context.Context, string, any) (string, error)
}

// ScriptRoutes 向需要复用既有 Redis 路由表的基础设施 adapter 暴露路由脚本客户端。
type ScriptRoutes interface {
	ScriptClientFor(context.Context, string, any) (ScriptClient, error)
}

// RouterFunc 允许用函数快速实现 Router。
type RouterFunc func(context.Context, string, any) (string, error)

func (f RouterFunc) RouteRedis(ctx context.Context, routeType string, shardKey any) (string, error) {
	return f(ctx, routeType, shardKey)
}

// Cluster 把 Redis 操作按 routeType 和 RecordKey.ShardKey 路由到具体端点 Adapter。
type Cluster struct {
	router  Router
	shards  map[string]*Adapter
	clients map[string]ScriptClient
	order   []string
}

var (
	_ ScriptRoutes               = (*Cluster)(nil)
	_ storage.DocumentBackfiller = (*Cluster)(nil)
)

// NewCluster 创建 Redis route/cluster 适配器。
func NewCluster(router Router, shards map[string]ScriptClient) (*Cluster, error) {
	if isNilRouter(router) {
		return nil, fmt.Errorf("redis: router is not configured")
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("redis: shards are not configured")
	}
	cluster := &Cluster{
		router:  router,
		shards:  make(map[string]*Adapter, len(shards)),
		clients: make(map[string]ScriptClient, len(shards)),
		order:   make([]string, 0, len(shards)),
	}
	for name, client := range shards {
		if name == "" {
			return nil, fmt.Errorf("redis: shard name is empty")
		}
		adapter, err := NewAdapter(client)
		if err != nil {
			return nil, fmt.Errorf("redis: shard %q: %w", name, err)
		}
		cluster.shards[name] = adapter
		cluster.clients[name] = client
		cluster.order = append(cluster.order, name)
	}
	sort.Strings(cluster.order)
	return cluster, nil
}

// ScriptClientFor 返回 routeType 和 shardKey 对应的原始脚本客户端。
// 仅限项目基础设施需要复用已经装配的 Redis 连接时使用；业务数据读写仍应经过
// Cluster 的类型化存储接口。
func (c *Cluster) ScriptClientFor(ctx context.Context, routeType string, shardKey any) (ScriptClient, error) {
	if c == nil || isNilRouter(c.router) {
		return nil, fmt.Errorf("redis: router is not configured")
	}
	shard, err := c.router.RouteRedis(ctx, routeType, shardKey)
	if err != nil {
		return nil, err
	}
	client, ok := c.clients[shard]
	if !ok || client == nil {
		return nil, fmt.Errorf("redis: shard %q is not configured", shard)
	}
	return client, nil
}

func (c *Cluster) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, options storage.ApplyOptions) error {
	if err := change.ValidateSource(source); err != nil {
		return err
	}
	if !source.HasChanges() {
		return nil
	}
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return err
	}
	return adapter.ApplyChangesWithOptions(ctx, spec, key, source, resolver, values, options)
}

func (c *Cluster) ReplaceDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, options storage.ApplyOptions) error {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return err
	}
	return adapter.ReplaceDocumentWithOptions(ctx, spec, key, doc, options)
}

func (c *Cluster) BackfillDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, options storage.ApplyOptions) (bool, error) {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return false, err
	}
	return adapter.BackfillDocumentWithOptions(ctx, spec, key, doc, options)
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

func (c *Cluster) ReadRecordRevision(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) (string, error) {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return "", err
	}
	return adapter.ReadRecordRevision(ctx, spec, key)
}

func (c *Cluster) ApplyTTLWithRevision(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, expectedRevision string, ttl time.Duration) (bool, string, error) {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return false, "", err
	}
	return adapter.ApplyTTLWithRevision(ctx, spec, key, expectedRevision, ttl)
}

func (c *Cluster) LockRecord(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, options storage.LockOptions) (storage.RecordLock, error) {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return nil, err
	}
	return adapter.LockRecord(ctx, spec, key, options)
}

func (c *Cluster) LoadDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	if err := storage.ValidateLoadFields(spec, fields); err != nil {
		return nil, false, err
	}
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
	if err := storage.ValidateLoadFields(spec, fields); err != nil {
		return nil, nil, err
	}
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
