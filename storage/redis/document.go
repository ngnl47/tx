// Package redis 提供 DeltaSet 到 Redis string JSON 写入模型的转换。
package redis

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
)

const maxLuaJSONSafeInteger uint64 = 1<<53 - 1

// DocumentPatch 描述一次 Redis JSON 文档 patch。
type DocumentPatch struct {
	Set        []PathValue
	Replace    []PathValue
	Clear      []change.Path
	Mode       documentPatchMode
	fullUpdate bool
	diffUpdate bool
	operations []documentPatchOperation
}

// PathValue 描述 JSON 文档内一个路径和值。
type PathValue struct {
	Path  change.Path
	Value any
}

type documentPatchOperation struct {
	Kind  string      `json:"kind"`
	Path  change.Path `json:"path"`
	Value any         `json:"value"`
}

// BuildDocumentPatchesWithDeltas 按 Redis string/subKey 模型把 DeltaSet
// 分组成 JSON 文档 patch。Redis 边界当前选择 JSON 字段名。
func BuildDocumentPatchesWithDeltas(spec meta.TypeSpec, deltas change.DeltaSet, resolver change.FieldNameResolver, values change.DeltaValueResolver) (map[string]DocumentPatch, error) {
	out := map[string]DocumentPatch{}
	for _, delta := range deltas.Clone().Compact() {
		if delta.Path.Depth == 0 {
			continue
		}
		goPath := delta.Path.FullPath(resolver, change.NameBackendGo)
		if len(goPath) == 0 {
			return nil, fmt.Errorf("redis: cannot resolve Go field path for delta %#v", delta.Path)
		}
		jsonPath := delta.Path.FullPath(resolver, change.NameBackendJSON)
		if len(jsonPath) == 0 {
			return nil, fmt.Errorf("redis: cannot resolve JSON field path for Go path %s", goPath)
		}
		goField := goPath[0]
		suffix, ignore := redisDocumentSuffix(spec, goField)
		if ignore {
			continue
		}
		patch := out[suffix]
		if patch.Mode == "" {
			patch.Mode = documentPatchModeMerge
		}
		// 深路径通过 operation 顺序执行。merge JSON 对象无法表达数组下标，
		// 而 operation 可在已有数组内按稳定 index 定位元素。
		if len(jsonPath) > 1 {
			patch.Mode = documentPatchModeOperations
		}
		switch delta.Kind {
		case change.DeltaClear:
			patch = appendDocumentPatchSet(patch, jsonPath, emptyDocumentValue(spec, goField), "clear")
			patch.diffUpdate = true
		case change.DeltaDelete:
			patch.Clear = append(patch.Clear, jsonPath)
			patch.operations = append(patch.operations, documentPatchOperation{Kind: "delete", Path: jsonPath})
			patch.diffUpdate = true
		case change.DeltaKindReplace:
			value, err := resolveDeltaValue(values, delta.Path, jsonPath)
			if err != nil {
				return nil, err
			}
			if redisFullUpdateField(spec, goField) && suffix != "" && len(jsonPath) == 1 {
				patch = appendDocumentPatchValue(patch, jsonPath, value, goField, suffix, spec)
			} else {
				patch.Replace = append(patch.Replace, PathValue{Path: jsonPath, Value: value})
				patch.operations = append(patch.operations, documentPatchOperation{Kind: "replace", Path: jsonPath, Value: value})
				patch.Mode = documentPatchModeOperations
			}
		default:
			value, err := resolveDeltaValue(values, delta.Path, jsonPath)
			if err != nil {
				return nil, err
			}
			patch = appendDocumentPatchValue(patch, jsonPath, value, goField, suffix, spec)
		}
		out[suffix] = patch
	}
	return out, nil
}

func resolveDeltaValue(values change.DeltaValueResolver, path change.DeltaPath, display change.Path) (any, error) {
	if values == nil {
		return nil, fmt.Errorf("redis: cannot resolve current value for path %s", display)
	}
	value, ok := values.DeltaValue(path)
	if !ok {
		return nil, fmt.Errorf("redis: cannot resolve current value for path %s", display)
	}
	return value, nil
}

func appendDocumentPatchSet(patch DocumentPatch, path change.Path, value any, kind string) DocumentPatch {
	patch.Set = append(patch.Set, PathValue{Path: path, Value: value})
	patch.operations = append(patch.operations, documentPatchOperation{Kind: kind, Path: path, Value: value})
	return patch
}

func appendDocumentPatchValue(patch DocumentPatch, path change.Path, value any, field, suffix string, spec meta.TypeSpec) DocumentPatch {
	patch = appendDocumentPatchSet(patch, path, value, "set")
	if patch.Mode == documentPatchModeOperations {
		return patch
	}
	if redisFullUpdateField(spec, field) && suffix != "" && len(path) == 1 {
		patch.fullUpdate = true
		if patch.diffUpdate {
			patch.Mode = documentPatchModePartialReplace
		} else {
			patch.Mode = documentPatchModeReplace
		}
		return patch
	}
	patch.diffUpdate = true
	if patch.fullUpdate {
		patch.Mode = documentPatchModePartialReplace
	}
	return patch
}

func buildReplaceDocumentPatches(spec meta.TypeSpec, doc map[string]any) map[string]DocumentPatch {
	out := map[string]DocumentPatch{}
	for field, value := range doc {
		goField := redisGoFieldForCanonicalName(spec, field)
		suffix, ignore := redisDocumentSuffix(spec, goField)
		if ignore {
			continue
		}
		patch := out[suffix]
		patch.Mode = documentPatchModeReplace
		patch.Set = append(patch.Set, PathValue{Path: redisPath(spec, change.Path{goField}), Value: value})
		out[suffix] = patch
	}
	if _, ok := out[""]; !ok {
		// 主文档是 Redis 热记录的存在标记；即使业务字段都拆到 subKey，也要保留空对象。
		out[""] = DocumentPatch{Mode: documentPatchModeKeepEmpty}
	}
	for _, suffix := range redisSubKeySuffixes(spec) {
		if _, ok := out[suffix]; !ok {
			// 完整替换未包含的已知 subKey 必须删除，避免旧子文档在后续读取时回流。
			out[suffix] = DocumentPatch{Mode: documentPatchModeReplace}
		}
	}
	return out
}

func redisGoFieldForCanonicalName(spec meta.TypeSpec, name string) string {
	field, ok := spec.FieldByJSONName(name)
	if !ok {
		return name
	}
	return field.Name
}

func redisPath(spec meta.TypeSpec, path change.Path) change.Path {
	if len(path) == 0 {
		return nil
	}
	out := append(change.Path(nil), path...)
	out[0] = spec.RedisName(path[0])
	return out
}

// ApplyDocumentPatch 把 patch 合并进 JSON 文档。
func ApplyDocumentPatch(doc map[string]any, patch DocumentPatch) {
	for _, item := range patch.Set {
		setPathValue(doc, item.Path, item.Value)
	}
	for _, item := range patch.Replace {
		setPathValue(doc, item.Path, item.Value)
	}
	for _, path := range patch.Clear {
		clearPathValue(doc, path)
	}
}

// EncodeDocumentPatch 把 patch 编码成 Lua merge 脚本可消费的 JSON 文档。
func EncodeDocumentPatch(patch DocumentPatch) (string, error) {
	if patch.Mode == documentPatchModeOperations {
		return encodeDocumentPatchOperations(patch.operations)
	}
	doc := map[string]any{}
	for _, item := range patch.Set {
		if err := validateLuaJSONValue(item.Value); err != nil {
			return "", err
		}
		setPathValue(doc, item.Path, item.Value)
	}
	for _, path := range patch.Clear {
		setPathValue(doc, path, nil)
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("redis: marshal patch: %w", err)
	}
	return string(body), nil
}

func encodeDocumentPatchOperations(operations []documentPatchOperation) (string, error) {
	for _, operation := range operations {
		if operation.Kind == "delete" {
			continue
		}
		if err := validateLuaJSONValue(operation.Value); err != nil {
			return "", err
		}
	}
	body, err := json.Marshal(struct {
		Operations []documentPatchOperation `json:"operations"`
	}{Operations: operations})
	if err != nil {
		return "", fmt.Errorf("redis: marshal path operations: %w", err)
	}
	return string(body), nil
}

func validateLuaJSONValue(value any) error {
	return validateLuaJSONValueAt(reflect.ValueOf(value), "")
}

func validateLuaJSONValueAt(value reflect.Value, path string) error {
	if !value.IsValid() {
		return nil
	}
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := value.Int()
		if n > int64(maxLuaJSONSafeInteger) || n < -int64(maxLuaJSONSafeInteger) {
			return fmt.Errorf("redis: unsafe integer for Lua JSON at %s: %d", valuePath(path), n)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n := value.Uint()
		if n > maxLuaJSONSafeInteger {
			return fmt.Errorf("redis: unsafe integer for Lua JSON at %s: %d", valuePath(path), n)
		}
	case reflect.Map:
		for _, key := range value.MapKeys() {
			childPath := appendValuePath(path, fmt.Sprint(key.Interface()))
			if err := validateLuaJSONValueAt(value.MapIndex(key), childPath); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := validateLuaJSONValueAt(value.Index(i), appendValuePath(path, fmt.Sprint(i))); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if field.PkgPath != "" {
				continue
			}
			if err := validateLuaJSONValueAt(value.Field(i), appendValuePath(path, field.Name)); err != nil {
				return err
			}
		}
	}
	return nil
}

func valuePath(path string) string {
	if path == "" {
		return "<root>"
	}
	return path
}

func appendValuePath(path string, elem string) string {
	if path == "" {
		return elem
	}
	return path + "." + elem
}

func redisDocumentSuffix(spec meta.TypeSpec, field string) (string, bool) {
	fieldSpec, ok := spec.Fields[field]
	if !ok || fieldSpec.Redis == nil {
		return "", false
	}
	if fieldSpec.Redis.Ignore {
		return "", true
	}
	return fieldSpec.Redis.SubKey, false
}

func redisFullUpdateField(spec meta.TypeSpec, field string) bool {
	fieldSpec, ok := spec.Fields[field]
	return ok && fieldSpec.Redis != nil && fieldSpec.Redis.FullUpdate
}

func emptyDocumentValue(spec meta.TypeSpec, field string) any {
	fieldSpec, ok := spec.FieldByName(field)
	if !ok {
		return map[string]any{}
	}
	switch {
	case strings.HasPrefix(fieldSpec.Type, "[]"), strings.HasPrefix(fieldSpec.Type, "["):
		return []any{}
	default:
		return map[string]any{}
	}
}

func ensureTTLOnlyPatches(spec meta.TypeSpec, patches map[string]DocumentPatch) {
	if _, ok := patches[""]; !ok {
		patches[""] = DocumentPatch{Mode: documentPatchModeTTLOnly}
	}
	for _, suffix := range redisSubKeySuffixes(spec) {
		if _, ok := patches[suffix]; !ok {
			patches[suffix] = DocumentPatch{Mode: documentPatchModeTTLOnly}
		}
	}
}

func redisSubKeySuffixes(spec meta.TypeSpec) []string {
	subKeys := map[string]struct{}{}
	for field, fieldSpec := range spec.Fields {
		if field == "" || fieldSpec.Redis == nil || fieldSpec.Redis.Ignore || fieldSpec.Redis.SubKey == "" {
			continue
		}
		subKeys[fieldSpec.Redis.SubKey] = struct{}{}
	}
	suffixes := make([]string, 0, len(subKeys))
	for suffix := range subKeys {
		suffixes = append(suffixes, suffix)
	}
	sort.Strings(suffixes)
	return suffixes
}

func redisReadKeys(spec meta.TypeSpec, prefix string, fields []string) []string {
	keys := []string{}
	includeMain := len(fields) == 0
	subKeys := map[string]struct{}{}
	if len(fields) == 0 {
		for field, fieldSpec := range spec.Fields {
			if field == "" || fieldSpec.Redis == nil || fieldSpec.Redis.Ignore || fieldSpec.Redis.SubKey == "" {
				continue
			}
			subKeys[fieldSpec.Redis.SubKey] = struct{}{}
		}
	} else {
		for _, field := range fields {
			suffix, ignore := redisDocumentSuffix(spec, field)
			if ignore {
				continue
			}
			if suffix == "" {
				includeMain = true
				continue
			}
			subKeys[suffix] = struct{}{}
		}
	}
	if includeMain {
		keys = append(keys, prefix)
	}
	suffixes := make([]string, 0, len(subKeys))
	for suffix := range subKeys {
		suffixes = append(suffixes, suffix)
	}
	sort.Strings(suffixes)
	for _, suffix := range suffixes {
		keys = append(keys, prefix+":"+suffix)
	}
	return keys
}

type redisBatchPlan struct {
	includeMain bool
	subKeys     []string
}

func (p redisBatchPlan) step() int {
	if p.includeMain {
		return 1 + len(p.subKeys)
	}
	return len(p.subKeys)
}

func redisBatchReadPlan(spec meta.TypeSpec, fields []string) redisBatchPlan {
	includeMain := len(fields) == 0
	subKeys := map[string]struct{}{}
	if len(fields) == 0 {
		for field, fieldSpec := range spec.Fields {
			if field == "" || fieldSpec.Redis == nil || fieldSpec.Redis.Ignore || fieldSpec.Redis.SubKey == "" {
				continue
			}
			subKeys[fieldSpec.Redis.SubKey] = struct{}{}
		}
	} else {
		for _, field := range fields {
			suffix, ignore := redisDocumentSuffix(spec, field)
			if ignore {
				continue
			}
			if suffix == "" {
				includeMain = true
				continue
			}
			subKeys[suffix] = struct{}{}
		}
	}
	suffixes := make([]string, 0, len(subKeys))
	for suffix := range subKeys {
		suffixes = append(suffixes, suffix)
	}
	sort.Strings(suffixes)
	return redisBatchPlan{includeMain: includeMain, subKeys: suffixes}
}

func setPathValue(doc map[string]any, path change.Path, value any) {
	if len(path) == 0 {
		return
	}
	if len(path) == 1 {
		doc[path[0]] = value
		return
	}
	current := doc
	for _, elem := range path[:len(path)-1] {
		next, ok := current[elem].(map[string]any)
		if !ok {
			next = map[string]any{}
			current[elem] = next
		}
		current = next
	}
	current[path[len(path)-1]] = value
}

func clearPathValue(doc map[string]any, path change.Path) {
	if len(path) == 0 {
		return
	}
	if len(path) == 1 {
		delete(doc, path[0])
		return
	}
	current := doc
	for _, elem := range path[:len(path)-1] {
		next, ok := current[elem].(map[string]any)
		if !ok {
			return
		}
		current = next
	}
	delete(current, path[len(path)-1])
}
