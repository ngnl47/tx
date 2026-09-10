package mongo

import (
	"context"
	"fmt"
	"reflect"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// Router 按 routeType 和 shardKey 选择 Mongo 分片名称。
type Router interface {
	RouteMongo(context.Context, string, any) (string, error)
}

// DocumentFinder 按显式路由键和 BSON 等值条件查询单条文档，不执行跨分片搜索。
type DocumentFinder interface {
	FindDocument(context.Context, meta.TypeSpec, any, Filter, ...string) (map[string]any, bool, error)
}

// FindDocument 先选择唯一存储目标，再执行联合条件查询。
func (c *Cluster) FindDocument(ctx context.Context, spec meta.TypeSpec, shardKey any, filter Filter, fields ...string) (map[string]any, bool, error) {
	adapter, err := c.adapterForRoute(ctx, mongoRouteType(spec), shardKey)
	if err != nil {
		return nil, false, err
	}
	return adapter.FindDocument(ctx, spec, filter, fields...)
}

// RouterFunc 允许用函数快速实现 Router。
type RouterFunc func(context.Context, string, any) (string, error)

func (f RouterFunc) RouteMongo(ctx context.Context, routeType string, shardKey any) (string, error) {
	return f(ctx, routeType, shardKey)
}

// Cluster 把 Mongo 操作按 routeType 和 RecordKey.ShardKey 路由到具体分片 Adapter。
type Cluster struct {
	router Router
	shards map[string]*Adapter
}

var _ storage.DocumentBackfiller = (*Cluster)(nil)
var _ CounterStore = (*Cluster)(nil)

// NewCluster 创建 Mongo route/cluster 适配器。
func NewCluster(router Router, shards map[string]Client) (*Cluster, error) {
	if isNilRouter(router) {
		return nil, fmt.Errorf("mongo: router is not configured")
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("mongo: shards are not configured")
	}
	cluster := &Cluster{
		router: router,
		shards: make(map[string]*Adapter, len(shards)),
	}
	for name, client := range shards {
		if name == "" {
			return nil, fmt.Errorf("mongo: shard name is empty")
		}
		adapter, err := NewAdapter(client)
		if err != nil {
			return nil, fmt.Errorf("mongo: shard %q: %w", name, err)
		}
		cluster.shards[name] = adapter
	}
	return cluster, nil
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

func (c *Cluster) ReplaceDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any) error {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return err
	}
	return adapter.ReplaceDocument(ctx, spec, key, doc)
}

// BackfillDocumentWithOptions 把 Mongo 条件回填路由到记录所属分片。
func (c *Cluster) BackfillDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, options storage.ApplyOptions) (bool, error) {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return false, err
	}
	return adapter.BackfillDocumentWithOptions(ctx, spec, key, doc, options)
}

// Reserve 按 routeType 和 shardKey 选择分片后原子预留计数区间。
func (c *Cluster) Reserve(ctx context.Context, routeType string, shardKey any, collection, key string, size int64) (int64, error) {
	adapter, err := c.adapterForRoute(ctx, routeType, shardKey)
	if err != nil {
		return 0, err
	}
	return adapter.Reserve(ctx, collection, key, size)
}

func (c *Cluster) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	adapter, err := c.adapterFor(ctx, spec, key)
	if err != nil {
		return err
	}
	return adapter.ApplyDelete(ctx, spec, key)
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
			return nil, nil, fmt.Errorf("mongo: shard %q is not configured", shard)
		}
		groupKeys := make([]storage.RecordKey, len(indexes))
		for i, idx := range indexes {
			groupKeys[i] = keys[idx]
		}
		groupDocs, groupExists, err := adapter.LoadDocuments(ctx, spec, groupKeys, fields...)
		if err != nil {
			return nil, nil, err
		}
		if len(groupDocs) != len(groupKeys) || len(groupExists) != len(groupKeys) {
			return nil, nil, fmt.Errorf("mongo: shard %q returned docs=%d exists=%d for %d keys", shard, len(groupDocs), len(groupExists), len(groupKeys))
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
	return c.adapterByName(shard)
}

func (c *Cluster) adapterForRoute(ctx context.Context, routeType string, shardKey any) (*Adapter, error) {
	shard, err := c.route(ctx, routeType, shardKey)
	if err != nil {
		return nil, err
	}
	return c.adapterByName(shard)
}

func (c *Cluster) adapterByName(shard string) (*Adapter, error) {
	adapter, ok := c.shards[shard]
	if !ok {
		return nil, fmt.Errorf("mongo: shard %q is not configured", shard)
	}
	return adapter, nil
}

func (c *Cluster) shardName(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) (string, error) {
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return "", err
	}
	return c.route(ctx, mongoRouteType(spec), recordShardKey(key))
}

func (c *Cluster) route(ctx context.Context, routeType string, shardKey any) (string, error) {
	if c == nil || isNilRouter(c.router) {
		return "", fmt.Errorf("mongo: router is not configured")
	}
	shard, err := c.router.RouteMongo(ctx, routeType, shardKey)
	if err != nil {
		return "", err
	}
	if shard == "" {
		return "", fmt.Errorf("mongo: route not found for routeType %q shardKey %v", routeType, shardKey)
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

func mongoRouteType(spec meta.TypeSpec) string {
	if spec.Mongo == nil {
		return ""
	}
	return spec.Mongo.Route.Type
}

func recordShardKey(key storage.RecordKey) any {
	if key.ShardKey != nil {
		return key.ShardKey
	}
	return key.ID
}
