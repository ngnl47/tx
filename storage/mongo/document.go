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
	for _, delta := range deltas.Compact() {
		valuePath := delta.Path
		unsafeStringKey := false
		if fallbackPath, fallback := fallbackMongoUnsafeStringKeyPath(delta.Path); fallback {
			valuePath = fallbackPath
			unsafeStringKey = true
		}
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
		if unsafeStringKey {
			if values != nil {
				resolved, ok := values.DeltaValue(valuePath)
				if ok {
					setDoc[bsonPath.String()] = resolved
					continue
				}
			}
			setDoc[bsonPath.String()] = nil
			continue
		}
		switch delta.Kind {
		case change.DeltaDelete:
			unsetDoc[bsonPath.String()] = ""
		case change.DeltaClear:
			setDoc[bsonPath.String()] = emptyMongoValue(spec, goPath[0])
		default:
			if values != nil {
				resolved, ok := values.DeltaValue(delta.Path)
				if ok {
					setDoc[bsonPath.String()] = resolved
					continue
				}
			}
			setDoc[bsonPath.String()] = nil
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

func fallbackMongoUnsafeStringKeyPath(path change.DeltaPath) (change.DeltaPath, bool) {
	if path.Depth == 0 {
		return change.DeltaPath{}, false
	}
	for i := 0; i < int(path.Depth); i++ {
		key := path.Nodes[i].Key
		if key.Kind == change.KeyString && unsafeMongoPathKey(key.Str) {
			return change.DeltaPath{
				Nodes: [change.MaxDeltaDepth]change.PathNode{{Field: path.Nodes[0].Field}},
				Depth: 1,
			}, true
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
