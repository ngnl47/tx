package redis

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

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

type recordLock struct {
	client ScriptClient
	key    string
	token  string
}

// LoadOptions 控制 Redis 批量读取行为。
type LoadOptions struct {
	BatchSize   int
	ClusterSafe bool
}

// Adapter 把 DeltaSet 应用到 Redis string JSON 文档。
type Adapter struct {
	client ScriptClient
}

// NewAdapter 创建 Redis DeltaSet 适配器。
func NewAdapter(client ScriptClient) *Adapter {
	return &Adapter{client: client}
}

// ApplyDeltasWithOptions 根据指定选项写入 Redis string JSON Delta 变更。
func (a *Adapter) ApplyDeltasWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, deltas change.DeltaSet, resolver change.FieldNameResolver, values change.DeltaValueResolver, options storage.ApplyOptions) error {
	if len(deltas) == 0 {
		return nil
	}
	patches, err := BuildDocumentPatchesWithDeltas(spec, deltas, resolver, values)
	if err != nil {
		return err
	}
	if len(patches) == 0 {
		return nil
	}
	return a.applyDocumentPatchesWithOptions(ctx, spec, key, patches, options)
}

func (a *Adapter) applyDocumentPatchesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, patches map[string]DocumentPatch, options storage.ApplyOptions) error {
	client, err := a.scriptClient()
	if err != nil {
		return err
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
	result, err := client.Eval(ctx, updateDocumentScript, []string{prefix}, args...)
	if err != nil {
		return fmt.Errorf("redis: apply changes %s: %w", prefix, err)
	}
	if result == scriptResultNotExists {
		return fmt.Errorf("redis: apply changes %s: %w", prefix, storage.ErrNotExists)
	}
	if path, ok := strings.CutPrefix(result, scriptResultPathTypeMismatch+":"); ok {
		return fmt.Errorf("redis: apply changes %s at %s: %w", prefix, path, storage.ErrPathTypeMismatch)
	}
	if result != scriptResultOK {
		return fmt.Errorf("redis: apply changes %s: unexpected script result %q", prefix, result)
	}
	return nil
}

// ReplaceDocumentWithOptions 按 Redis string JSON 文档模型全量替换一条热数据。
func (a *Adapter) ReplaceDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, options storage.ApplyOptions) error {
	if doc == nil {
		return nil
	}
	options.Upsert = true
	return a.applyDocumentPatchesWithOptions(ctx, spec, key, buildReplaceDocumentPatches(spec, doc), options)
}

// ReplaceDocument 按 Redis string JSON 文档模型全量替换一条热数据。
func (a *Adapter) ReplaceDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any) error {
	return a.ReplaceDocumentWithOptions(ctx, spec, key, doc, storage.ApplyOptions{Upsert: true})
}

// ApplyDelete 删除 Redis string JSON 主文档、subKey 文档和版本键。
func (a *Adapter) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	client, err := a.scriptClient()
	if err != nil {
		return err
	}
	prefix := redisKeyPrefix(spec, key)
	keys := redisDeleteKeys(spec, prefix)
	result, err := client.Eval(ctx, deleteDocumentScript, keys)
	if err != nil {
		return fmt.Errorf("redis: delete document %s: %w", prefix, err)
	}
	if result != scriptResultOK {
		return fmt.Errorf("redis: delete document %s: unexpected script result %q", prefix, result)
	}
	return nil
}

// ApplyDeleteWithRevision 仅当 Redis 生命周期版本仍为 expectedRevision 时删除热数据。
func (a *Adapter) ApplyDeleteWithRevision(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, expectedRevision string) (bool, string, error) {
	client, err := a.scriptClient()
	if err != nil {
		return false, "", err
	}
	prefix := redisKeyPrefix(spec, key)
	if !isRecordRevision(expectedRevision) {
		return false, "", fmt.Errorf("redis: delete document %s: missing record revision", prefix)
	}
	keys := redisDeleteKeys(spec, prefix)
	result, err := client.Eval(ctx, deleteDocumentScript, keys, expectedRevision)
	if err != nil {
		return false, "", fmt.Errorf("redis: delete document %s: %w", prefix, err)
	}
	if result == scriptResultOK {
		return true, "", nil
	}
	if isRecordRevision(result) {
		return false, result, nil
	}
	return false, "", fmt.Errorf("redis: delete document %s: unexpected script result %q", prefix, result)
}

// ReadRecordRevision 读取 Redis 热数据生命周期版本。
func (a *Adapter) ReadRecordRevision(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) (string, error) {
	client, err := a.scriptClient()
	if err != nil {
		return "", err
	}
	prefix := redisKeyPrefix(spec, key)
	result, err := client.Eval(ctx, readRevisionScript, []string{prefix + "@v"})
	if err != nil {
		return "", fmt.Errorf("redis: read record revision %s: %w", prefix, err)
	}
	if !isRecordRevision(result) {
		return "", fmt.Errorf("redis: read record revision %s: invalid record revision %q", prefix, result)
	}
	return result, nil
}

func isRecordRevision(value string) bool {
	if len(value) == 0 || value[0] == '0' {
		return false
	}
	for i := range value {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func (a *Adapter) LockRecord(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, options storage.LockOptions) (storage.RecordLock, error) {
	client, err := a.scriptClient()
	if err != nil {
		return nil, err
	}
	ttl := options.TTL
	if ttl <= 0 {
		return nil, fmt.Errorf("redis: lock ttl must be greater than 0")
	}
	wait := options.Wait
	lockKey := redisKeyPrefix(spec, key) + "@lock"
	token := newLockToken()
	deadline := time.Now().Add(wait)
	for {
		result, err := client.Eval(ctx, lockRecordScript, []string{lockKey}, token, ttl.Milliseconds())
		if err != nil {
			return nil, fmt.Errorf("redis: lock record %s: %w", lockKey, err)
		}
		if result == scriptResultOK {
			return &recordLock{client: client, key: lockKey, token: token}, nil
		}
		if result != scriptResultBusy {
			return nil, fmt.Errorf("redis: lock record %s: unexpected script result %q", lockKey, result)
		}
		if wait <= 0 || time.Now().After(deadline) {
			return nil, fmt.Errorf("redis: lock timeout %s", lockKey)
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (l *recordLock) Unlock(ctx context.Context) error {
	if l == nil {
		return fmt.Errorf("redis: record lock is nil")
	}
	if isNilScriptClient(l.client) {
		return ErrClientNotConfigured
	}
	result, err := l.client.Eval(ctx, unlockRecordScript, []string{l.key}, l.token)
	if err != nil {
		return fmt.Errorf("redis: unlock record %s: %w", l.key, err)
	}
	if result != scriptResultOK && result != scriptResultStale {
		return fmt.Errorf("redis: unlock record %s: unexpected script result %q", l.key, result)
	}
	return nil
}

// LoadDocument 读取并合并 Redis string JSON 主文档和 subKey 文档。
func (a *Adapter) LoadDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, fields ...string) (map[string]any, bool, error) {
	client, err := a.scriptClient()
	if err != nil {
		return nil, false, err
	}
	prefix := redisKeyPrefix(spec, key)
	keys := redisReadKeys(spec, prefix, fields)
	if len(keys) == 0 {
		return nil, false, nil
	}
	body, err := client.Eval(ctx, findDocumentScript, keys)
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
	scriptClient, err := a.scriptClient()
	if err != nil {
		return nil, nil, err
	}
	client, ok := scriptClient.(BatchClient)
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
			if plan.includeMain {
				keys = append(keys, prefix)
			}
			for _, suffix := range plan.subKeys {
				keys = append(keys, prefix+":"+suffix)
			}
		}
		if len(keys) == 0 {
			continue
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
			parts := make([]string, 0, step)
			valueOffset := valueStart
			if plan.includeMain {
				parts = append(parts, values[valueOffset])
				valueOffset++
			}
			for j := range plan.subKeys {
				parts = append(parts, values[valueOffset+j])
			}
			if !hasDocumentPart(parts) {
				continue
			}
			exists[i] = true
			doc, err := joinDocumentBodies(parts)
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
	scriptClient, err := a.scriptClient()
	if err != nil {
		return nil, nil, err
	}
	client, ok := scriptClient.(BatchClient)
	if !ok {
		return nil, nil, fmt.Errorf("redis: cluster-safe batch load requires BatchClient")
	}
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
				if plan.includeMain {
					keys = append(keys, prefix)
				}
				for _, suffix := range plan.subKeys {
					keys = append(keys, prefix+":"+suffix)
				}
			}
			if len(keys) == 0 {
				continue
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
				parts := make([]string, 0, step)
				valueOffset := valueStart
				if plan.includeMain {
					parts = append(parts, values[valueOffset])
					valueOffset++
				}
				for j := range plan.subKeys {
					parts = append(parts, values[valueOffset+j])
				}
				if !hasDocumentPart(parts) {
					continue
				}
				exists[index] = true
				doc, err := joinDocumentBodies(parts)
				if err != nil {
					return nil, nil, fmt.Errorf("redis: decode document %s: %w", redisKeyPrefix(spec, records[index]), err)
				}
				docs[index] = doc
			}
		}
	}
	return docs, exists, nil
}

func (a *Adapter) scriptClient() (ScriptClient, error) {
	if a == nil || isNilScriptClient(a.client) {
		return nil, ErrClientNotConfigured
	}
	return a.client, nil
}

func isNilScriptClient(client ScriptClient) bool {
	if client == nil {
		return true
	}
	value := reflect.ValueOf(client)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
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

func hasDocumentPart(parts []string) bool {
	for _, part := range parts {
		if part != "" {
			return true
		}
	}
	return false
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

func newLockToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	return strconv.FormatInt(time.Now().UnixNano(), 10)
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
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("invalid JSON document: multiple values")
		}
		return nil, fmt.Errorf("invalid JSON document: %w", err)
	}
	if doc == nil {
		return nil, fmt.Errorf("expected JSON object")
	}
	return doc, nil
}

func joinDocumentBodies(parts []string) (map[string]any, error) {
	body := make(map[string]any)
	for _, part := range parts {
		if part == "" {
			continue
		}
		doc, err := decodeDocument(part)
		if err != nil {
			return nil, err
		}
		for key, value := range doc {
			body[key] = value
		}
	}
	return body, nil
}
