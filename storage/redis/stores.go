package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// Stores 按元数据中的存储名称选择 Redis，不按记录身份选择节点。
type Stores struct {
	adapters map[string]*Adapter
}

var _ storage.DocumentBackfiller = (*Stores)(nil)

// NewStores 装配具名存储；连接、节点拓扑和关闭由调用方管理。
func NewStores(clients map[string]LuaClient) (*Stores, error) {
	if len(clients) == 0 {
		return nil, fmt.Errorf("redis: stores are not configured")
	}
	stores := &Stores{adapters: make(map[string]*Adapter, len(clients))}
	for name, client := range clients {
		if name == "" {
			return nil, fmt.Errorf("redis: store name is empty")
		}
		adapter, err := NewAdapter(client)
		if err != nil {
			return nil, fmt.Errorf("redis: store %q: %w", name, err)
		}
		stores.adapters[name] = adapter
	}
	return stores, nil
}

func (c *Stores) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, options storage.ApplyOptions) error {
	if err := change.ValidateSource(source); err != nil {
		return err
	}
	if !source.HasChanges() {
		return nil
	}
	adapter, err := c.adapterFor(spec, key)
	if err != nil {
		return err
	}
	return adapter.ApplyChangesWithOptions(ctx, spec, key, source, resolver, values, options)
}

func (c *Stores) ReplaceDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, options storage.ApplyOptions) error {
	adapter, err := c.adapterFor(spec, key)
	if err != nil {
		return err
	}
	return adapter.ReplaceDocumentWithOptions(ctx, spec, key, doc, options)
}

func (c *Stores) BackfillDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, options storage.ApplyOptions) (bool, error) {
	adapter, err := c.adapterFor(spec, key)
	if err != nil {
		return false, err
	}
	return adapter.BackfillDocumentWithOptions(ctx, spec, key, doc, options)
}

func (c *Stores) ReplaceDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any) error {
	return c.ReplaceDocumentWithOptions(ctx, spec, key, doc, storage.ApplyOptions{Upsert: true})
}

func (c *Stores) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	adapter, err := c.adapterFor(spec, key)
	if err != nil {
		return err
	}
	return adapter.ApplyDelete(ctx, spec, key)
}

func (c *Stores) ReadRecordRevision(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) (string, error) {
	adapter, err := c.adapterFor(spec, key)
	if err != nil {
		return "", err
	}
	return adapter.ReadRecordRevision(ctx, spec, key)
}

func (c *Stores) ApplyTTLWithRevision(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, expectedRevision string, ttl time.Duration) (bool, string, error) {
	adapter, err := c.adapterFor(spec, key)
	if err != nil {
		return false, "", err
	}
	return adapter.ApplyTTLWithRevision(ctx, spec, key, expectedRevision, ttl)
}

func (c *Stores) LockRecord(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, options storage.LockOptions) (storage.RecordLock, error) {
	adapter, err := c.adapterFor(spec, key)
	if err != nil {
		return nil, err
	}
	return adapter.LockRecord(ctx, spec, key, options)
}

func (c *Stores) LoadDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	if err := storage.ValidateLoadFields(spec, fields); err != nil {
		return nil, false, err
	}
	adapter, err := c.adapterFor(spec, key)
	if err != nil {
		return nil, false, err
	}
	return adapter.LoadDocument(ctx, spec, key, fields...)
}

func (c *Stores) LoadDocuments(ctx context.Context, spec meta.TypeSpec, keys []storage.RecordKey, fields ...string) ([]map[string]any, []bool, error) {
	return c.LoadDocumentsWithOptions(ctx, spec, keys, LoadOptions{}, fields...)
}

func (c *Stores) LoadDocumentsWithOptions(ctx context.Context, spec meta.TypeSpec, keys []storage.RecordKey, options LoadOptions, fields ...string) ([]map[string]any, []bool, error) {
	if err := storage.ValidateLoadFields(spec, fields); err != nil {
		return nil, nil, err
	}
	if len(keys) == 0 {
		return []map[string]any{}, []bool{}, nil
	}
	for _, key := range keys {
		if err := storage.ValidateRecordKey(spec, key); err != nil {
			return nil, nil, err
		}
	}
	adapter, err := c.adapterFor(spec, keys[0])
	if err != nil {
		return nil, nil, err
	}
	return adapter.LoadDocumentsWithOptions(ctx, spec, keys, options, fields...)
}

func (c *Stores) adapterFor(spec meta.TypeSpec, key storage.RecordKey) (*Adapter, error) {
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return nil, err
	}
	if c == nil {
		return nil, fmt.Errorf("redis: stores are not configured")
	}
	name := redisStoreName(spec)
	adapter, ok := c.adapters[name]
	if !ok {
		return nil, fmt.Errorf("redis: store %q is not configured", name)
	}
	return adapter, nil
}

func redisStoreName(spec meta.TypeSpec) string {
	if spec.Redis == nil {
		return ""
	}
	return spec.Redis.Route.Type
}

func (c *Stores) MarkDeletion(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	a, err := c.adapterFor(spec, key)
	if err != nil {
		return err
	}
	return a.MarkDeletion(ctx, spec, key)
}
