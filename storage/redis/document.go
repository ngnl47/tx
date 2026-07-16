// Package redis 提供 ChangeSet 到 Redis string JSON 写入模型的转换。
package redis

import (
	"encoding/json"
	"fmt"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
)

// DocumentPatch 描述一次 Redis JSON 文档 patch。
type DocumentPatch struct {
	Set   []PathValue
	Unset []change.Path
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
		suffix, ignore := redisDocumentSuffix(spec, path[0])
		if ignore {
			continue
		}
		patch := out[suffix]
		switch fieldChange.Kind {
		case change.KindUnset:
			patch.Unset = append(patch.Unset, path)
		default:
			patch.Set = append(patch.Set, PathValue{Path: path, Value: fieldChange.Value})
		}
		out[suffix] = patch
	}
	return out
}

// ApplyDocumentPatch 把 patch 合并进 JSON 文档。
func ApplyDocumentPatch(doc map[string]any, patch DocumentPatch) {
	for _, item := range patch.Set {
		setPathValue(doc, item.Path, item.Value)
	}
	for _, path := range patch.Unset {
		unsetPathValue(doc, path)
	}
}

// EncodeDocumentPatch 把 patch 编码成 Lua merge 脚本可消费的 JSON 文档。
func EncodeDocumentPatch(patch DocumentPatch) (string, error) {
	doc := map[string]any{}
	for _, item := range patch.Set {
		setPathValue(doc, item.Path, item.Value)
	}
	for _, path := range patch.Unset {
		setPathValue(doc, path, nil)
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("redis: marshal patch: %w", err)
	}
	return string(body), nil
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

func unsetPathValue(doc map[string]any, path change.Path) {
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
