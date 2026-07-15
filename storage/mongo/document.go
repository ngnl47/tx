// Package mongo 提供 ChangeSet 到 MongoDB 更新文档的转换。
package mongo

import "github.com/ngnl47/tx/change"

// BuildUpdateDocument 把字段变更转换成 MongoDB update document。
//
// KindSet 和 KindReplace 进入 `$set`，KindUnset 进入 `$unset`。
// 返回值使用 map 以避免在 runtime 基建阶段引入 MongoDB driver 类型。
func BuildUpdateDocument(set change.ChangeSet) map[string]any {
	return BuildUpdateDocumentWithResolver(set, nil)
}

// BuildUpdateDocumentWithResolver 把字段变更转换成 MongoDB update document，
// 并用 resolver 把 FieldID 解析为字段别名。
func BuildUpdateDocumentWithResolver(set change.ChangeSet, resolver change.FieldResolver) map[string]any {
	doc := set.DocumentWithResolver(resolver)
	setDoc := map[string]any{}
	unsetDoc := map[string]any{}
	for path, value := range doc.Set {
		setDoc[path] = value
	}
	for path := range doc.Unset {
		unsetDoc[path] = ""
	}
	update := map[string]any{}
	if len(setDoc) > 0 {
		update["$set"] = setDoc
	}
	if len(unsetDoc) > 0 {
		update["$unset"] = unsetDoc
	}
	return update
}
