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
