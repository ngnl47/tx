package change

import (
	"fmt"
	"reflect"
)

// Document 是 DeltaSet 的后端无关文档形式。
//
// Set 对应字段设置、整体替换或容器清空；Clear 对应字段删除。Mongo
// 适配器会把 Clear 转成 `$unset`，Redis 适配器会把 Clear 转成 JSON 字段移除。
type Document struct {
	Set   map[string]any
	Clear map[string]struct{}
}

// DeltaValueResolver 按 DeltaPath 从当前对象读取最终值。
type DeltaValueResolver interface {
	DeltaValue(DeltaPath) (any, bool)
}

// DeltaValueResolverFunc 允许用函数快速声明 Delta 值读取器。
type DeltaValueResolverFunc func(DeltaPath) (any, bool)

// DeltaValue 实现 DeltaValueResolver。
func (f DeltaValueResolverFunc) DeltaValue(path DeltaPath) (any, bool) {
	return f(path)
}

// DocumentWithResolver 把 DeltaSet 转成指定命名体系下的 Set/Clear 文档。
//
// 每个 Set 或 Replace Delta 都必须能从 values 读取当前值。路径或值无法解析时
// 返回错误，避免把未知状态伪造成 null 字段。
func (ds DeltaSet) DocumentWithResolver(resolver FieldNameResolver, backend NameBackend, values DeltaValueResolver) (Document, error) {
	if err := ds.ValidateKinds(); err != nil {
		return Document{}, err
	}
	doc := Document{
		Set:   map[string]any{},
		Clear: map[string]struct{}{},
	}
	for _, delta := range ds.Clone().Compact() {
		path := delta.Path.FullPath(resolver, backend)
		if len(path) == 0 {
			return Document{}, fmt.Errorf("change: cannot resolve %s field path for delta %#v", backend, delta.Path)
		}
		switch delta.Kind {
		case DeltaDelete:
			doc.Clear[path.String()] = struct{}{}
		case DeltaClear:
			value, err := emptyDeltaValueFor(values, delta.Path, path)
			if err != nil {
				return Document{}, err
			}
			doc.Set[path.String()] = value
		case DeltaKindSet, DeltaKindReplace:
			if values == nil {
				return Document{}, fmt.Errorf("change: cannot resolve current value for path %s", path)
			}
			resolved, ok := values.DeltaValue(delta.Path)
			if !ok {
				return Document{}, fmt.Errorf("change: cannot resolve current value for path %s", path)
			}
			doc.Set[path.String()] = resolved
		}
	}
	return doc, nil
}

func emptyDeltaValueFor(values DeltaValueResolver, path DeltaPath, display Path) (any, error) {
	if values == nil {
		return nil, fmt.Errorf("change: cannot resolve current value for path %s", display)
	}
	value, ok := values.DeltaValue(path)
	if !ok {
		return nil, fmt.Errorf("change: cannot resolve current value for path %s", display)
	}
	valueType := reflect.TypeOf(value)
	if valueType == nil {
		return map[string]any{}, nil
	}
	switch valueType.Kind() {
	case reflect.Map:
		return reflect.MakeMapWithSize(valueType, 0).Interface(), nil
	case reflect.Slice:
		return reflect.MakeSlice(valueType, 0, 0).Interface(), nil
	case reflect.Array:
		return reflect.Zero(valueType).Interface(), nil
	default:
		return map[string]any{}, nil
	}
}
