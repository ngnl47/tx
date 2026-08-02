// Package mongo 提供 ChangeSet 到 MongoDB 更新文档的转换。
package mongo

import (
	"fmt"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
)

// BuildUpdateDocument 把字段变更转换成 MongoDB update document。
//
// KindSet 和 KindReplace 进入 `$set`，KindClear 进入 `$unset`。
// 返回值使用 map 以避免在 runtime 基建阶段引入 MongoDB driver 类型。
func BuildUpdateDocument(set change.ChangeSet) map[string]any {
	return BuildUpdateDocumentWithResolver(set, nil)
}

// BuildUpdateDocumentWithResolver 把字段变更转换成 MongoDB update document，
// 并用 resolver 把 FieldID 解析为字段别名。
func BuildUpdateDocumentWithResolver(set change.ChangeSet, resolver change.FieldResolver) map[string]any {
	update, _ := buildUpdateDocument(set, resolver, nil)
	return update
}

// BuildUpdateDocumentWithSpecAndResolver 把字段变更转换成 MongoDB update document，
// 并按 TypeSpec 将 Go 字段名转换为 BSON 字段名。
func BuildUpdateDocumentWithSpecAndResolver(spec meta.TypeSpec, set change.ChangeSet, resolver change.FieldResolver) (map[string]any, error) {
	return buildUpdateDocument(set, resolver, func(path change.Path) (change.Path, error) {
		return mongoPath(spec, path)
	})
}

// BuildUpdateDocumentWithDeltas 把 DeltaSet 转换成 MongoDB update document。
// Mongo 边界当前选择 BSON 字段名。
func BuildUpdateDocumentWithDeltas(spec meta.TypeSpec, deltas change.DeltaSet, resolver change.FieldNameResolver, values change.DeltaValueResolver) (map[string]any, error) {
	setDoc := map[string]any{}
	unsetDoc := map[string]any{}
	for _, delta := range deltas.Compact() {
		goPath := delta.Path.FullPath(resolver, change.NameBackendGo)
		if len(goPath) == 0 {
			continue
		}
		bsonPath := delta.Path.FullPath(resolver, change.NameBackendBSON)
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

func buildUpdateDocument(set change.ChangeSet, resolver change.FieldResolver, mapPath func(change.Path) (change.Path, error)) (map[string]any, error) {
	setDoc := map[string]any{}
	clearDoc := map[string]any{}
	for _, fieldChange := range set.Changes() {
		path := fieldChange.FullPath(resolver)
		if len(path) == 0 {
			continue
		}
		if mapPath != nil {
			mapped, err := mapPath(path)
			if err != nil {
				return nil, err
			}
			path = mapped
		}
		switch fieldChange.Kind {
		case change.KindSet, change.KindReplace:
			setDoc[path.String()] = fieldChange.Value
		case change.KindClear:
			clearDoc[path.String()] = ""
		}
	}
	update := map[string]any{}
	if len(setDoc) > 0 {
		update["$set"] = setDoc
	}
	if len(clearDoc) > 0 {
		update["$unset"] = clearDoc
	}
	return update, nil
}

func mongoPath(spec meta.TypeSpec, path change.Path) (change.Path, error) {
	if len(path) == 0 {
		return nil, nil
	}
	out := append(change.Path(nil), path...)
	goField := out[0]
	bsonName := spec.BSONName(goField)
	if bsonName == "_id" {
		return nil, fmt.Errorf("mongo: cannot update immutable _id field %s", goField)
	}
	out[0] = bsonName
	return out, nil
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
