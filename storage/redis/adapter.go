package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// ScriptClient 是 Redis Lua 写入所需的最小接口。
//
// go-redis 适配层后续只需要把 Eval 转成这里的签名即可，runtime
// 基建不直接依赖具体驱动。
type ScriptClient interface {
	Eval(context.Context, string, []string, ...any) (string, error)
}

// BatchClient 是 Redis 批量 string 读取所需的最小接口。
type BatchClient interface {
	MGet(context.Context, ...string) ([]string, error)
}

// ApplyOptions 控制 Redis 变更写入脚本的尾部参数。
type ApplyOptions struct {
	Upsert bool
	TTL    int64
}

// Adapter 把 ChangeSet 应用到 Redis string JSON 文档。
type Adapter struct {
	client   ScriptClient
	resolver change.FieldResolver
}

// NewAdapter 创建 Redis ChangeSet 适配器。
func NewAdapter(client ScriptClient) *Adapter {
	return &Adapter{client: client}
}

// NewAdapterWithResolver 创建带字段编号解析器的 Redis ChangeSet 适配器。
func NewAdapterWithResolver(client ScriptClient, resolver change.FieldResolver) *Adapter {
	return &Adapter{client: client, resolver: resolver}
}

// ApplyChanges 根据 Redis 元数据和记录 ID 写入 string JSON
// 变更。非 subKey 字段写入主 key；subKey 字段写入 mainKey:subKey。
func (a *Adapter) ApplyChanges(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, set change.ChangeSet) error {
	return a.ApplyChangesWithOptions(ctx, spec, key, set, ApplyOptions{Upsert: true})
}

// ApplyChangesWithOptions 根据指定选项写入 Redis string JSON 变更。
func (a *Adapter) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, set change.ChangeSet, options ApplyOptions) error {
	if a.client == nil || set.Empty() {
		return nil
	}
	prefix := redisKeyPrefix(spec, key)
	patches := BuildDocumentPatchesWithResolver(spec, set, a.resolver)
	args := make([]any, 0, len(patches)*3+2)
	for suffix, patch := range patches {
		body, err := EncodeDocumentPatch(patch)
		if err != nil {
			return err
		}
		args = append(args, suffix, body, "0")
	}
	upsert := "0"
	if options.Upsert {
		upsert = "1"
	}
	args = append(args, upsert, strconv.FormatInt(options.TTL, 10))
	result, err := a.client.Eval(ctx, updateDocumentScript, []string{prefix}, args...)
	if err != nil {
		return fmt.Errorf("redis: apply changes %s: %w", prefix, err)
	}
	if result == "ne" {
		return fmt.Errorf("redis: apply changes %s: %w", prefix, storage.ErrNotExists)
	}
	return nil
}

// LoadDocument 读取并合并 Redis string JSON 主文档和 subKey 文档。
func (a *Adapter) LoadDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	if a.client == nil {
		return nil, false, nil
	}
	prefix := redisKeyPrefix(spec, key)
	keys := redisReadKeys(spec, prefix, fields)
	body, err := a.client.Eval(ctx, findDocumentScript, keys)
	if err != nil {
		return nil, false, fmt.Errorf("redis: load document %s: %w", prefix, err)
	}
	if body == "" {
		return nil, false, nil
	}
	doc, err := decodeDocument(body)
	if err != nil {
		return nil, false, fmt.Errorf("redis: decode document %s: %w", prefix, err)
	}
	return doc, true, nil
}

// LoadDocuments 批量读取并合并 Redis string JSON 文档。
func (a *Adapter) LoadDocuments(ctx context.Context, spec meta.TypeSpec, records []storage.RecordKey, fields ...string) ([]map[string]any, []bool, error) {
	docs := make([]map[string]any, len(records))
	exists := make([]bool, len(records))
	if len(records) == 0 {
		return docs, exists, nil
	}
	client, ok := a.client.(BatchClient)
	if !ok {
		return nil, nil, fmt.Errorf("redis: client does not support MGet")
	}
	plan := redisBatchReadPlan(spec, fields)
	keys := make([]string, 0, len(records)*plan.step())
	for _, record := range records {
		prefix := redisKeyPrefix(spec, record)
		keys = append(keys, prefix)
		for _, suffix := range plan.subKeys {
			keys = append(keys, prefix+":"+suffix)
		}
	}
	values, err := client.MGet(ctx, keys...)
	if err != nil {
		return nil, nil, fmt.Errorf("redis: load documents: %w", err)
	}
	if len(values) != len(keys) {
		return nil, nil, fmt.Errorf("redis: load documents returned %d values for %d keys", len(values), len(keys))
	}
	step := plan.step()
	for i := range records {
		start := i * step
		if values[start] == "" {
			continue
		}
		exists[i] = true
		parts := make([]string, 0, step)
		if plan.includeMain {
			parts = append(parts, values[start])
		}
		for j := 1; j < step; j++ {
			parts = append(parts, values[start+j])
		}
		body := joinDocumentBodies(parts)
		doc, err := decodeDocument(body)
		if err != nil {
			return nil, nil, fmt.Errorf("redis: decode document %s: %w", redisKeyPrefix(spec, records[i]), err)
		}
		docs[i] = doc
	}
	return docs, exists, nil
}

func redisKeyPrefix(spec meta.TypeSpec, key storage.RecordKey) string {
	base := spec.Type.Ref.Name
	if spec.Redis != nil && spec.Redis.Key != "" {
		base = spec.Redis.Key
	}
	return fmt.Sprintf("%s:%v", base, key.ID)
}

func decodeDocument(body string) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(body))
	decoder.UseNumber()
	var doc map[string]any
	if err := decoder.Decode(&doc); err != nil {
		return nil, err
	}
	return doc, nil
}

func joinDocumentBodies(parts []string) string {
	body := "{}"
	for _, part := range parts {
		if len(part) < 2 || part[0] != '{' || part[len(part)-1] != '}' {
			continue
		}
		if body == "{}" {
			body = part
			continue
		}
		body = body[:len(body)-1] + "," + part[1:]
	}
	return body
}
