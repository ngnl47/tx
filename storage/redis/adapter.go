package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage"
)

// ScriptClient 是 Redis Lua 写入所需的最小接口。
//
// runtime 基建不直接依赖具体驱动；官方 go-redis 适配见 storage/redis/goredis。
type ScriptClient interface {
	Eval(context.Context, string, []string, ...any) (string, error)
}

// BatchClient 是 Redis 批量 string 读取所需的最小接口。
type BatchClient interface {
	MGet(context.Context, ...string) ([]string, error)
}

// ApplyOptions 控制 Redis 变更写入脚本的尾部参数。
type ApplyOptions = storage.ApplyOptions

// LoadOptions 控制 Redis 批量读取行为。
type LoadOptions struct {
	BatchSize   int
	ClusterSafe bool
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

// ApplyChangesWithOptions 根据指定选项写入 Redis string JSON 变更。
func (a *Adapter) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, set change.ChangeSet, options ApplyOptions) error {
	if a.client == nil || set.Empty() {
		return nil
	}
	patches := BuildDocumentPatchesWithResolver(spec, set, a.resolver)
	return a.applyDocumentPatchesWithOptions(ctx, spec, key, patches, options)
}

func (a *Adapter) applyDocumentPatchesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, patches map[string]DocumentPatch, options ApplyOptions) error {
	if a.client == nil {
		return nil
	}
	prefix := redisKeyPrefix(spec, key)
	if options.TTL != 0 {
		ensureTTLOnlyPatches(spec, patches)
	}
	args := make([]any, 0, len(patches)*3+2)
	for suffix, patch := range patches {
		mode := patch.Mode
		if mode == "" {
			mode = documentPatchModeMerge
		}
		body := ""
		if mode != documentPatchModeTTLOnly {
			encoded, err := EncodeDocumentPatch(patch)
			if err != nil {
				return err
			}
			body = encoded
		}
		args = append(args, suffix, body, string(mode))
	}
	upsert := scriptFlagFalse
	if options.Upsert {
		upsert = scriptFlagTrue
	}
	args = append(args, upsert, strconv.FormatInt(options.TTL, 10))
	result, err := a.client.Eval(ctx, updateDocumentScript, []string{prefix}, args...)
	if err != nil {
		return fmt.Errorf("redis: apply changes %s: %w", prefix, err)
	}
	if result == scriptResultNotExists {
		return fmt.Errorf("redis: apply changes %s: %w", prefix, storage.ErrNotExists)
	}
	return nil
}

// ReplaceDocumentWithOptions 按 Redis string JSON 文档模型全量替换一条热数据。
func (a *Adapter) ReplaceDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, options ApplyOptions) error {
	if doc == nil {
		return nil
	}
	options.Upsert = true
	return a.applyDocumentPatchesWithOptions(ctx, spec, key, buildReplaceDocumentPatches(spec, doc), options)
}

// ReplaceDocument 按 Redis string JSON 文档模型全量替换一条热数据。
func (a *Adapter) ReplaceDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any) error {
	return a.ReplaceDocumentWithOptions(ctx, spec, key, doc, ApplyOptions{Upsert: true})
}

// ApplyDelete 删除 Redis string JSON 主文档、subKey 文档和版本键。
func (a *Adapter) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	if a.client == nil {
		return nil
	}
	prefix := redisKeyPrefix(spec, key)
	keys := redisDeleteKeys(spec, prefix)
	if _, err := a.client.Eval(ctx, deleteDocumentScript, keys); err != nil {
		return fmt.Errorf("redis: delete document %s: %w", prefix, err)
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
	return a.LoadDocumentsWithOptions(ctx, spec, records, LoadOptions{}, fields...)
}

// LoadDocumentsWithOptions 按指定选项批量读取并合并 Redis string JSON 文档。
func (a *Adapter) LoadDocumentsWithOptions(ctx context.Context, spec meta.TypeSpec, records []storage.RecordKey, options LoadOptions, fields ...string) ([]map[string]any, []bool, error) {
	docs := make([]map[string]any, len(records))
	exists := make([]bool, len(records))
	if len(records) == 0 {
		return docs, exists, nil
	}
	client, ok := a.client.(BatchClient)
	if !ok {
		return a.loadDocumentsByEval(ctx, spec, records, fields...)
	}
	plan := redisBatchReadPlan(spec, fields)
	if options.ClusterSafe {
		return a.loadDocumentsByHashTagMGet(ctx, spec, records, options, plan)
	}
	batchSize := options.BatchSize
	if batchSize <= 0 || batchSize > len(records) {
		batchSize = len(records)
	}
	for start := 0; start < len(records); start += batchSize {
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}
		keys := make([]string, 0, (end-start)*plan.step())
		for _, record := range records[start:end] {
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
		for i := start; i < end; i++ {
			valueStart := (i - start) * step
			if values[valueStart] == "" {
				continue
			}
			exists[i] = true
			parts := make([]string, 0, step)
			if plan.includeMain {
				parts = append(parts, values[valueStart])
			}
			for j := 1; j < step; j++ {
				parts = append(parts, values[valueStart+j])
			}
			body := joinDocumentBodies(parts)
			doc, err := decodeDocument(body)
			if err != nil {
				return nil, nil, fmt.Errorf("redis: decode document %s: %w", redisKeyPrefix(spec, records[i]), err)
			}
			docs[i] = doc
		}
	}
	return docs, exists, nil
}

func (a *Adapter) loadDocumentsByHashTagMGet(ctx context.Context, spec meta.TypeSpec, records []storage.RecordKey, options LoadOptions, plan redisBatchPlan) ([]map[string]any, []bool, error) {
	docs := make([]map[string]any, len(records))
	exists := make([]bool, len(records))
	client := a.client.(BatchClient)
	groups := make(map[string][]int)
	for i, record := range records {
		prefix := redisKeyPrefix(spec, record)
		groups[redisKeyHashTag(prefix)] = append(groups[redisKeyHashTag(prefix)], i)
	}
	for _, indexes := range groups {
		batchSize := options.BatchSize
		if batchSize <= 0 || batchSize > len(indexes) {
			batchSize = len(indexes)
		}
		for start := 0; start < len(indexes); start += batchSize {
			end := start + batchSize
			if end > len(indexes) {
				end = len(indexes)
			}
			keys := make([]string, 0, (end-start)*plan.step())
			for _, index := range indexes[start:end] {
				prefix := redisKeyPrefix(spec, records[index])
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
			for groupOffset, index := range indexes[start:end] {
				valueStart := groupOffset * step
				if values[valueStart] == "" {
					continue
				}
				exists[index] = true
				parts := make([]string, 0, step)
				if plan.includeMain {
					parts = append(parts, values[valueStart])
				}
				for j := 1; j < step; j++ {
					parts = append(parts, values[valueStart+j])
				}
				body := joinDocumentBodies(parts)
				doc, err := decodeDocument(body)
				if err != nil {
					return nil, nil, fmt.Errorf("redis: decode document %s: %w", redisKeyPrefix(spec, records[index]), err)
				}
				docs[index] = doc
			}
		}
	}
	return docs, exists, nil
}

func (a *Adapter) loadDocumentsByEval(ctx context.Context, spec meta.TypeSpec, records []storage.RecordKey, fields ...string) ([]map[string]any, []bool, error) {
	docs := make([]map[string]any, len(records))
	exists := make([]bool, len(records))
	for i, record := range records {
		doc, found, err := a.LoadDocument(ctx, spec, record, fields...)
		if err != nil {
			return nil, nil, err
		}
		docs[i] = doc
		exists[i] = found
	}
	return docs, exists, nil
}

func redisKeyPrefix(spec meta.TypeSpec, key storage.RecordKey) string {
	base := spec.Type.Ref.Name
	if spec.Redis != nil && spec.Redis.Key != "" {
		base = spec.Redis.Key
	}
	if spec.Redis != nil && spec.Redis.Route.By != "" && spec.Redis.IDField != "" && spec.Redis.Route.By != spec.Redis.IDField && key.ShardKey != nil {
		return fmt.Sprintf("%s:{%v}:%v", base, key.ShardKey, key.ID)
	}
	return fmt.Sprintf("%s:{%v}", base, key.ID)
}

func redisKeyHashTag(key string) string {
	start := strings.IndexByte(key, '{')
	if start < 0 {
		return key
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end < 0 || end == 0 {
		return key
	}
	return key[start+1 : start+1+end]
}

func redisDeleteKeys(spec meta.TypeSpec, prefix string) []string {
	keys := []string{prefix}
	for _, suffix := range redisSubKeySuffixes(spec) {
		keys = append(keys, prefix+":"+suffix)
	}
	keys = append(keys, prefix+"@v")
	return keys
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
