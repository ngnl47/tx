// Package redis 提供变更集合到 Redis string JSON 写入模型的转换。
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
	Clear      []change.Path
	Mode       documentPatchMode
	fullUpdate bool
	diffUpdate bool
}

// PathValue 描述 JSON 文档内一个路径和值。
type PathValue struct {
	Path  change.Path
	Value any
}

// BuildDocumentPatches 按 string/subKey 模型把字段变更分组成 Redis
// JSON 文档 patch。返回 map 的 key 是 Redis key 后缀：空字符串表示主 key，
// 非空字符串表示 mainKey:<suffix>。
func BuildDocumentPatches(spec meta.TypeSpec, set change.ChangeSet) map[string]DocumentPatch {
	return BuildDocumentPatchesWithResolver(spec, set, nil)
}

// BuildDocumentPatchesWithResolver 按 string/subKey 模型把字段变更
// 分组成 Redis JSON 文档 patch，并用 resolver 把 FieldID 解析为字段别名。
func BuildDocumentPatchesWithResolver(spec meta.TypeSpec, set change.ChangeSet, resolver change.FieldResolver) map[string]DocumentPatch {
	out := map[string]DocumentPatch{}
	for _, fieldChange := range set.Changes() {
		path := fieldChange.FullPath(resolver)
		if len(path) == 0 {
			continue
		}
		goField := path[0]
		suffix, ignore := redisDocumentSuffix(spec, goField)
		if ignore {
			continue
		}
		path = redisPath(spec, path)
		patch := out[suffix]
		if patch.Mode == "" {
			patch.Mode = documentPatchModeMerge
		}
		switch fieldChange.Kind {
		case change.KindClear:
			patch.Clear = append(patch.Clear, path)
			patch.diffUpdate = true
		default:
			patch.Set = append(patch.Set, PathValue{Path: path, Value: fieldChange.Value})
			if fieldChange.Kind == change.KindReplace && redisFullUpdateField(spec, goField) && suffix != "" && len(path) == 1 {
				patch.fullUpdate = true
				if patch.diffUpdate {
					patch.Mode = documentPatchModePartialReplace
				} else {
					patch.Mode = documentPatchModeReplace
				}
			} else {
				patch.diffUpdate = true
				if patch.fullUpdate {
					patch.Mode = documentPatchModePartialReplace
				}
			}
		}
		out[suffix] = patch
	}
	return out
}

// BuildDocumentPatchesWithDeltas 按 Redis string/subKey 模型把 DeltaSet
// 分组成 JSON 文档 patch。Redis 边界当前选择 JSON 字段名。
func BuildDocumentPatchesWithDeltas(spec meta.TypeSpec, deltas change.DeltaSet, resolver change.FieldNameResolver, values change.DeltaValueResolver) map[string]DocumentPatch {
	out := map[string]DocumentPatch{}
	for _, delta := range deltas.Compact() {
		goPath := delta.Path.FullPath(resolver, change.NameBackendGo)
		if len(goPath) == 0 {
			continue
		}
		jsonPath := delta.Path.FullPath(resolver, change.NameBackendJSON)
		if len(jsonPath) == 0 {
			continue
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
		switch delta.Kind {
		case change.DeltaClear:
			patch.Set = append(patch.Set, PathValue{Path: jsonPath, Value: emptyDocumentValue(spec, goField)})
			patch.diffUpdate = true
		case change.DeltaDelete:
			patch.Clear = append(patch.Clear, jsonPath)
			patch.diffUpdate = true
		default:
			if values != nil {
				resolved, ok := values.DeltaValue(delta.Path)
				if ok {
					patch.Set = append(patch.Set, PathValue{Path: jsonPath, Value: resolved})
					if redisFullUpdateField(spec, goField) && suffix != "" && len(jsonPath) == 1 {
						patch.fullUpdate = true
						if patch.diffUpdate {
							patch.Mode = documentPatchModePartialReplace
						} else {
							patch.Mode = documentPatchModeReplace
						}
					} else {
						patch.diffUpdate = true
						if patch.fullUpdate {
							patch.Mode = documentPatchModePartialReplace
						}
					}
					break
				}
			}
			patch.Set = append(patch.Set, PathValue{Path: jsonPath, Value: nil})
			if redisFullUpdateField(spec, goField) && suffix != "" && len(jsonPath) == 1 {
				patch.fullUpdate = true
				if patch.diffUpdate {
					patch.Mode = documentPatchModePartialReplace
				} else {
					patch.Mode = documentPatchModeReplace
				}
			} else {
				patch.diffUpdate = true
				if patch.fullUpdate {
					patch.Mode = documentPatchModePartialReplace
				}
			}
		}
		out[suffix] = patch
	}
	return out
}

func buildReplaceDocumentPatches(spec meta.TypeSpec, doc map[string]any) map[string]DocumentPatch {
	out := map[string]DocumentPatch{}
	for field, value := range doc {
		suffix, ignore := redisDocumentSuffix(spec, field)
		if ignore {
			continue
		}
		patch := out[suffix]
		patch.Mode = documentPatchModeReplace
		patch.Set = append(patch.Set, PathValue{Path: redisPath(spec, change.Path{field}), Value: value})
		out[suffix] = patch
	}
	return out
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
	for _, path := range patch.Clear {
		clearPathValue(doc, path)
	}
}

// EncodeDocumentPatch 把 patch 编码成 Lua merge 脚本可消费的 JSON 文档。
func EncodeDocumentPatch(patch DocumentPatch) (string, error) {
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
