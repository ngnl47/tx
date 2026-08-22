// Package mongo 提供生成式变更到 MongoDB 更新文档的转换。
package mongo

import (
	"fmt"
	"strings"

	"github.com/ngnl47/tx/change"
	"github.com/ngnl47/tx/meta"
)

// BuildUpdateDocument 把规范化变更流转换成 MongoDB update document。
// Mongo 边界当前选择 BSON 字段名。
func BuildUpdateDocument(spec meta.TypeSpec, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver) (map[string]any, error) {
	return buildUpdateDocument(spec, source, resolver, values, false)
}

func buildUpdateDocument(spec meta.TypeSpec, source change.Source, resolver change.FieldNameResolver, values change.ValueResolver, skipUpsertID bool) (map[string]any, error) {
	if source == nil {
		return nil, change.ErrNilSource
	}
	setDoc := map[string]any{}
	unsetDoc := map[string]any{}
	fallbacks := make([]change.Path, 0)
	err := source.VisitChanges(change.VisitorFunc(func(valuePath change.Path, op change.Op) error {
		if valuePath.Depth == 0 {
			return nil
		}
		if !op.Valid() {
			return fmt.Errorf("change: unknown operation %d", op)
		}
		if mongoFallbackCoversPath(fallbacks, valuePath) {
			return nil
		}
		if fallback, ok := fallbackMongoUnsafeStringKeyPath(valuePath); ok {
			fallbackPath := fallback.FullPath(resolver, change.FieldNameBSON)
			if len(fallbackPath) == 0 {
				return fmt.Errorf("mongo: cannot resolve BSON fallback path for change %#v", valuePath)
			}
			if fallbackPath[0] == "_id" {
				return fmt.Errorf("mongo: cannot update immutable _id field")
			}
			resolved, err := resolveMongoChangeValue(values, fallback, fallbackPath)
			if err != nil {
				return err
			}
			name := fallbackPath.String()
			removeMongoDescendantWrites(setDoc, name)
			removeMongoDescendantWrites(unsetDoc, name)
			setDoc[name] = resolved
			delete(unsetDoc, name)
			fallbacks = append(fallbacks, fallback)
			return nil
		}
		goPath := valuePath.FullPath(resolver, change.FieldNameGo)
		if len(goPath) == 0 {
			return fmt.Errorf("mongo: cannot resolve Go field path for change %#v", valuePath)
		}
		bsonPath := valuePath.FullPath(resolver, change.FieldNameBSON)
		if len(bsonPath) == 0 {
			return fmt.Errorf("mongo: cannot resolve BSON field path for Go path %s", goPath)
		}
		if bsonPath[0] == "_id" {
			if skipUpsertID && len(bsonPath) == 1 && (op == change.OpSet || op == change.OpReplace) {
				return nil
			}
			return fmt.Errorf("mongo: cannot update immutable _id field %s", goPath[0])
		}
		name := bsonPath.String()
		switch op {
		case change.OpDelete:
			delete(setDoc, name)
			unsetDoc[name] = ""
		case change.OpClear:
			delete(unsetDoc, name)
			setDoc[name] = emptyMongoValue(spec, goPath[0])
		case change.OpSet, change.OpReplace:
			resolved, err := resolveMongoChangeValue(values, valuePath, bsonPath)
			if err != nil {
				return err
			}
			delete(unsetDoc, name)
			setDoc[name] = resolved
		}
		return nil
	}))
	if err != nil {
		return nil, err
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

func removeMongoDescendantWrites(document map[string]any, parent string) {
	prefix := parent + "."
	for path := range document {
		if path == parent || strings.HasPrefix(path, prefix) {
			delete(document, path)
		}
	}
}

func mongoFallbackCoversPath(fallbacks []change.Path, path change.Path) bool {
	for _, fallback := range fallbacks {
		if mongoPathCovers(fallback, path) {
			return true
		}
	}
	return false
}

func mongoPathCovers(parent, child change.Path) bool {
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

func resolveMongoChangeValue(values change.ValueResolver, path change.Path, display change.DocumentPath) (any, error) {
	if values == nil {
		return nil, fmt.Errorf("mongo: cannot resolve current value for path %s", display)
	}
	value, ok := values.ResolveValue(path)
	if !ok {
		return nil, fmt.Errorf("mongo: cannot resolve current value for path %s", display)
	}
	return value, nil
}

func fallbackMongoUnsafeStringKeyPath(path change.Path) (change.Path, bool) {
	if path.Depth == 0 {
		return change.Path{}, false
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
	return change.Path{}, false
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
