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

// LuaClient 是 Redis Lua 写入所需的最小接口。
//
// runtime 基建不直接依赖具体驱动；官方 go-redis 适配见 storage/redis/goredis。
type LuaClient interface {
	Eval(context.Context, string, []string, ...any) (string, error)
}

// ScriptCall 描述一条可进入同一 pipeline 的 Lua 调用。
type ScriptCall struct {
	Script string
	Keys   []string
	Args   []any
}

// BatchScriptClient 为固定 key 批量探测提供 pipeline 能力。
type BatchScriptClient interface {
	EvalBatch(context.Context, []ScriptCall) ([]string, error)
}

// BatchClient 是 Redis 批量 string 读取所需的最小接口。
type BatchClient interface {
	MGet(context.Context, ...string) ([]string, error)
}

type recordLock struct {
	client LuaClient
	key    string
	token  string
}

// LoadOptions 控制 Redis 批量读取行为。
type LoadOptions struct {
	BatchSize   int
	ClusterSafe bool
}

// Adapter 把规范化变更流应用到 Redis string JSON 文档。
type Adapter struct {
	client LuaClient
}

var _ storage.DocumentBackfiller = (*Adapter)(nil)

// NewAdapter 创建 Redis 变更适配器。
func NewAdapter(client LuaClient) (*Adapter, error) {
	if isNilScriptClient(client) {
		return nil, ErrClientNotConfigured
	}
	return &Adapter{client: client}, nil
}

// ApplyChangesWithOptions 根据指定选项写入 Redis string JSON 变更。
func (a *Adapter) ApplyChangesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, options storage.ApplyOptions) error {
	if err := change.ValidateSource(source); err != nil {
		return err
	}
	if !source.HasChanges() {
		return nil
	}
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return err
	}
	patches, err := buildDocumentPatches(spec, source, resolver, values)
	if err != nil {
		return err
	}
	if len(patches) == 0 {
		return storage.ErrNoEffectiveChanges
	}
	return a.applyDocumentPatchesWithOptions(ctx, spec, key, patches, options)
}

func (a *Adapter) applyDocumentPatchesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, patches map[string]DocumentPatch, options storage.ApplyOptions) error {
	result, prefix, err := a.evalDocumentPatchesWithOptions(ctx, spec, key, patches, options)
	if err != nil {
		return err
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

func (a *Adapter) evalDocumentPatchesWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, patches map[string]DocumentPatch, options storage.ApplyOptions) (string, string, error) {
	client, err := a.scriptClient()
	if err != nil {
		return "", "", err
	}
	prefix := redisKeyPrefix(spec, key)
	ttlMilliseconds, err := applyTTLMilliseconds(options.TTL)
	if err != nil {
		return "", prefix, fmt.Errorf("redis: apply changes %s: %w", prefix, err)
	}
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
				return "", prefix, err
			}
			body = encoded
		}
		args = append(args, suffix, body, string(mode))
	}
	upsert := scriptFlagFalse
	if options.Upsert {
		upsert = scriptFlagTrue
	}
	args = append(args, upsert, strconv.FormatInt(ttlMilliseconds, 10))
	result, err := client.Eval(ctx, updateDocumentScript, []string{prefix}, args...)
	if err != nil {
		return "", prefix, fmt.Errorf("redis: apply changes %s: %w", prefix, err)
	}
	return result, prefix, nil
}

// ReplaceDocumentWithOptions 按 Redis string JSON 文档模型全量替换一条热数据。
func (a *Adapter) ReplaceDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, options storage.ApplyOptions) error {
	if doc == nil {
		return storage.ErrNilDocument
	}
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return err
	}
	options.Upsert = true
	return a.applyDocumentPatchesWithOptions(ctx, spec, key, buildReplaceDocumentPatches(spec, doc), options)
}

// BackfillDocumentWithOptions 仅当 Redis 主热文档不存在时原子回填完整文档。
func (a *Adapter) BackfillDocumentWithOptions(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any, options storage.ApplyOptions) (bool, error) {
	if doc == nil {
		return false, storage.ErrNilDocument
	}
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return false, err
	}
	client, err := a.scriptClient()
	if err != nil {
		return false, err
	}
	prefix := redisKeyPrefix(spec, key)
	ttlMilliseconds, err := applyTTLMilliseconds(options.TTL)
	if err != nil {
		return false, fmt.Errorf("redis: backfill document %s: %w", prefix, err)
	}
	patches := buildReplaceDocumentPatches(spec, doc)
	args := make([]any, 0, len(patches)*3+1)
	for suffix, patch := range patches {
		if patch.Mode != documentPatchModeReplace && patch.Mode != documentPatchModeKeepEmpty {
			return false, fmt.Errorf("redis: backfill document %s: invalid patch mode %q", prefix, patch.Mode)
		}
		body, err := EncodeDocumentPatch(patch)
		if err != nil {
			return false, err
		}
		args = append(args, suffix, body, string(patch.Mode))
	}
	args = append(args, strconv.FormatInt(ttlMilliseconds, 10))
	result, err := client.Eval(ctx, backfillDocumentScript, []string{prefix}, args...)
	if err != nil {
		return false, fmt.Errorf("redis: backfill document %s: %w", prefix, err)
	}
	switch result {
	case scriptResultOK:
		return true, nil
	case scriptResultExists:
		return false, nil
	default:
		return false, fmt.Errorf("redis: backfill document %s: unexpected script result %q", prefix, result)
	}
}

// ReplaceDocument 按 Redis string JSON 文档模型全量替换一条热数据。
func (a *Adapter) ReplaceDocument(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, doc map[string]any) error {
	return a.ReplaceDocumentWithOptions(ctx, spec, key, doc, storage.ApplyOptions{Upsert: true})
}

// ApplyDelete 删除 Redis string JSON 主文档、subKey 文档和版本键。
func (a *Adapter) ApplyDelete(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) error {
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return err
	}
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

// ReadRecordRevision 读取 Redis 热数据生命周期版本。
func (a *Adapter) ReadRecordRevision(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey) (string, error) {
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return "", err
	}
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

// ApplyTTLWithRevision 仅当 Redis 生命周期版本未变化时，原子更新主文档、
// subKey 文档和版本键的 TTL。
func (a *Adapter) ApplyTTLWithRevision(ctx context.Context, spec meta.TypeSpec, key storage.RecordKey, expectedRevision string, ttl time.Duration) (bool, string, error) {
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return false, "", err
	}
	client, err := a.scriptClient()
	if err != nil {
		return false, "", err
	}
	prefix := redisKeyPrefix(spec, key)
	if !isRecordRevision(expectedRevision) {
		return false, "", fmt.Errorf("redis: update document TTL %s: missing record revision", prefix)
	}
	ttlMilliseconds, err := positiveTTLMilliseconds(ttl)
	if err != nil {
		return false, "", fmt.Errorf("redis: update document TTL %s: %w", prefix, err)
	}
	result, err := client.Eval(
		ctx,
		expireDocumentScript,
		redisDeleteKeys(spec, prefix),
		expectedRevision,
		ttlMilliseconds,
	)
	if err != nil {
		return false, "", fmt.Errorf("redis: update document TTL %s: %w", prefix, err)
	}
	if result == scriptResultOK {
		return true, "", nil
	}
	if isRecordRevision(result) {
		return false, result, nil
	}
	return false, "", fmt.Errorf("redis: update document TTL %s: unexpected script result %q", prefix, result)
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
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return nil, err
	}
	client, err := a.scriptClient()
	if err != nil {
		return nil, err
	}
	ttl := options.TTL
	if ttl <= 0 {
		return nil, fmt.Errorf("redis: lock ttl must be greater than 0")
	}
	ttlMilliseconds, err := positiveTTLMilliseconds(ttl)
	if err != nil {
		return nil, fmt.Errorf("redis: lock record: %w", err)
	}
	wait := options.Wait
	lockKey := redisKeyPrefix(spec, key) + "@lock"
	token := newLockToken()
	deadline := time.Now().Add(wait)
	for {
		result, err := client.Eval(ctx, lockRecordScript, []string{lockKey}, token, ttlMilliseconds)
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

func applyTTLMilliseconds(ttl time.Duration) (int64, error) {
	if ttl < 0 {
		return -1, nil
	}
	if ttl == 0 {
		return 0, nil
	}
	return positiveTTLMilliseconds(ttl)
}

func positiveTTLMilliseconds(ttl time.Duration) (int64, error) {
	if err := storage.ValidateExpiringTTL(ttl); err != nil {
		return 0, err
	}
	return ttl.Milliseconds(), nil
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
	if err := storage.ValidateRecordKey(spec, key); err != nil {
		return nil, false, err
	}
	if err := storage.ValidateLoadFields(spec, fields); err != nil {
		return nil, false, err
	}
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
	if err := storage.ValidateLoadFields(spec, fields); err != nil {
		return nil, nil, err
	}
	if len(records) == 0 {
		return docs, exists, nil
	}
	if err := storage.ValidateRecordKeys(spec, records); err != nil {
		return nil, nil, err
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

func (a *Adapter) scriptClient() (LuaClient, error) {
	if a == nil || isNilScriptClient(a.client) {
		return nil, ErrClientNotConfigured
	}
	return a.client, nil
}

func isNilScriptClient(client LuaClient) bool {
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
