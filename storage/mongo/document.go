// Package mongo 提供 DeltaSet 到 MongoDB 更新文档的转换。
package mongo

import (
	"fmt"
	"strings"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
)

// BuildUpdateDocumentWithDeltas 把 DeltaSet 转换成 MongoDB update document。
// Mongo 边界当前选择 BSON 字段名。
func BuildUpdateDocumentWithDeltas(spec meta.TypeSpec, deltas change.DeltaSet, resolver change.FieldNameResolver, values change.DeltaValueResolver) (map[string]any, error) {
	setDoc := map[string]any{}
	unsetDoc := map[string]any{}
	for _, delta := range normalizeMongoDeltas(deltas) {
		valuePath := delta.Path
		goPath := valuePath.FullPath(resolver, change.NameBackendGo)
		if len(goPath) == 0 {
			continue
		}
		bsonPath := valuePath.FullPath(resolver, change.NameBackendBSON)
		if len(bsonPath) == 0 {
			continue
		}
		if bsonPath[0] == "_id" {
			return nil, fmt.Errorf("mongo: cannot update immutable _id field %s", goPath[0])
		}
		switch delta.Kind {
		case change.DeltaDelete:
			unsetDoc[bsonPath.String()] = ""
		case change.DeltaClear:
			setDoc[bsonPath.String()] = emptyMongoValue(spec, goPath[0])
		default:
			resolved, err := resolveMongoDeltaValue(values, delta.Path, bsonPath)
			if err != nil {
				return nil, err
			}
			setDoc[bsonPath.String()] = resolved
		}
	}
	update := map[string]any{}
	if len(setDoc) > 0 {
		update["$set"] = setDoc
	}
	if len(unsetDoc) > 0 {
		update["$unset"] = unsetDoc
	}
	return update, nil
}

// normalizeMongoDeltas 在 Mongo update 边界消除不可表达的 string key 路径。
// 整体替换会读取该 map 的最终值，因此同一 map 下的其他子路径无需再写入。
func normalizeMongoDeltas(deltas change.DeltaSet) change.DeltaSet {
	compacted := deltas.Compact()
	fallbacks := make([]change.DeltaPath, 0)
	for _, delta := range compacted {
		fallback, ok := fallbackMongoUnsafeStringKeyPath(delta.Path)
		if !ok || containsMongoFallbackPath(fallbacks, fallback) {
			continue
		}
		fallbacks = append(fallbacks, fallback)
	}
	if len(fallbacks) == 0 {
		return compacted
	}
	out := make(change.DeltaSet, 0, len(compacted))
	for _, delta := range compacted {
		if mongoFallbackCoversPath(fallbacks, delta.Path) {
			continue
		}
		out = append(out, delta)
	}
	for _, fallback := range fallbacks {
		out = append(out, change.DeltaEntry{Path: fallback, Kind: change.DeltaKindReplace})
	}
	return out.Compact()
}

func containsMongoFallbackPath(paths []change.DeltaPath, target change.DeltaPath) bool {
	for _, path := range paths {
		if path.Equal(target) {
			return true
		}
	}
	return false
}

func mongoFallbackCoversPath(fallbacks []change.DeltaPath, path change.DeltaPath) bool {
	for _, fallback := range fallbacks {
		if mongoPathCovers(fallback, path) {
			return true
		}
	}
	return false
}

func mongoPathCovers(parent, child change.DeltaPath) bool {
	if parent.Depth == 0 || parent.Depth > child.Depth {
		return false
	}
	for i := 0; i < int(parent.Depth); i++ {
		parentNode := parent.Nodes[i]
		childNode := child.Nodes[i]
		if parentNode.Field != childNode.Field {
			return false
		}
		if parentNode.Key.Kind == change.KeyNone {
			continue
		}
		if !parentNode.Key.Equal(childNode.Key) {
			return false
		}
	}
	return true
}

func resolveMongoDeltaValue(values change.DeltaValueResolver, path change.DeltaPath, display change.Path) (any, error) {
	if values == nil {
		return nil, fmt.Errorf("mongo: cannot resolve current value for path %s", display)
	}
	value, ok := values.DeltaValue(path)
	if !ok {
		return nil, fmt.Errorf("mongo: cannot resolve current value for path %s", display)
	}
	return value, nil
}

func fallbackMongoUnsafeStringKeyPath(path change.DeltaPath) (change.DeltaPath, bool) {
	if path.Depth == 0 {
		return change.DeltaPath{}, false
	}
	for i := 0; i < int(path.Depth); i++ {
		key := path.Nodes[i].Key
		if key.Kind == change.KeyString && unsafeMongoPathKey(key.Str) {
			// Mongo dot path 无法安全定位该 map entry，改为整体替换最近的 map 字段。
			fallback := path
			fallback.Depth = uint8(i + 1)
			fallback.Nodes[i].Key = change.PathKey{}
			for j := i + 1; j < len(fallback.Nodes); j++ {
				fallback.Nodes[j] = change.PathNode{}
			}
			return fallback, true
		}
	}
	return change.DeltaPath{}, false
}

func unsafeMongoPathKey(key string) bool {
	return key == "" || strings.ContainsAny(key, ".$")
}

func emptyMongoValue(spec meta.TypeSpec, field string) any {
	fieldSpec, ok := spec.FieldByName(field)
	if !ok {
		return map[string]any{}
	}
	switch {
	case len(fieldSpec.Type) > 0 && fieldSpec.Type[0] == '[':
		return []any{}
	default:
		return map[string]any{}
	}
}

func mongoProjectionFields(spec meta.TypeSpec, fields []string) []string {
	if len(fields) == 0 {
		return nil
	}
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		out = append(out, spec.BSONName(field))
	}
	return out
}
