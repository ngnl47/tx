package change

import (
	"fmt"
	"reflect"
)

// Document 是 DeltaSet 的后端无关文档形式。
//
// Set 对应字段设置、整体替换或容器清空；Delete 对应字段删除。Mongo
// 适配器会把 Delete 转成 `$unset`，Redis 适配器会把 Delete 转成 JSON 字段移除。
type Document struct {
	Set    map[string]any
	Delete map[string]struct{}
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

// DocumentBuilder 把同步投影的 Delta 直接写入后端无关文档。
// 生成 Store 保证输入已按自身脏状态收敛，因此这里不再构造或压缩 DeltaSet。
type DocumentBuilder struct {
	doc      Document
	resolver FieldNameResolver
	kind     FieldNameKind
	values   DeltaValueResolver
}

// NewDocumentBuilder 创建一个流式变更文档构建器。
func NewDocumentBuilder(resolver FieldNameResolver, kind FieldNameKind, values DeltaValueResolver) *DocumentBuilder {
	return &DocumentBuilder{
		doc: Document{
			Set:    map[string]any{},
			Delete: map[string]struct{}{},
		},
		resolver: resolver,
		kind:     kind,
		values:   values,
	}
}

// ProjectDocument 按指定字段命名体系把规范化脏状态投影为变更文档。
func ProjectDocument(projector DeltaProjector, resolver FieldNameResolver, kind FieldNameKind, values DeltaValueResolver) (Document, error) {
	if projector == nil {
		return Document{}, ErrNilDeltaProjector
	}
	if !kind.Valid() {
		return Document{}, fmt.Errorf("change: unknown field name kind %q", kind)
	}
	builder := NewDocumentBuilder(resolver, kind, values)
	if err := projector.ProjectDeltas(builder); err != nil {
		return Document{}, err
	}
	return builder.Document(), nil
}

// VisitDelta 实现 DeltaVisitor。
func (b *DocumentBuilder) VisitDelta(path DeltaPath, op DeltaOp) error {
	if b == nil {
		return ErrNilDeltaVisitor
	}
	if !op.Valid() {
		return fmt.Errorf("change: unknown delta operation %d", op)
	}
	display := path.FullPath(b.resolver, b.kind)
	if len(display) == 0 {
		return fmt.Errorf("change: cannot resolve %s field path for delta %#v", b.kind, path)
	}
	name := display.String()
	switch op {
	case OpDelete:
		delete(b.doc.Set, name)
		b.doc.Delete[name] = struct{}{}
	case OpClear:
		value, err := emptyDeltaValueFor(b.values, path, display)
		if err != nil {
			return err
		}
		delete(b.doc.Delete, name)
		b.doc.Set[name] = value
	case OpSet, OpReplace:
		if b.values == nil {
			return fmt.Errorf("change: cannot resolve current value for path %s", display)
		}
		value, ok := b.values.DeltaValue(path)
		if !ok {
			return fmt.Errorf("change: cannot resolve current value for path %s", display)
		}
		delete(b.doc.Delete, name)
		b.doc.Set[name] = value
	}
	return nil
}

// Document 返回已同步构建的文档。调用方在继续 VisitDelta 前不应修改其 map。
func (b *DocumentBuilder) Document() Document {
	if b == nil {
		return Document{}
	}
	return b.doc
}

// DocumentWithResolver 把 DeltaSet 转成指定命名体系下的 Set/Delete 文档。
//
// 每个 Set 或 Replace Delta 都必须能从 values 读取当前值。路径或值无法解析时
// 返回错误，避免把未知状态伪造成 null 字段。
func (ds DeltaSet) DocumentWithResolver(resolver FieldNameResolver, kind FieldNameKind, values DeltaValueResolver) (Document, error) {
	if !kind.Valid() {
		return Document{}, fmt.Errorf("change: unknown field name kind %q", kind)
	}
	if err := ds.ValidateOps(); err != nil {
		return Document{}, err
	}
	doc := Document{
		Set:    map[string]any{},
		Delete: map[string]struct{}{},
	}
	for _, delta := range ds.Clone().Compact() {
		path := delta.Path.FullPath(resolver, kind)
		if len(path) == 0 {
			return Document{}, fmt.Errorf("change: cannot resolve %s field path for delta %#v", kind, delta.Path)
		}
		switch delta.Op {
		case OpDelete:
			doc.Delete[path.String()] = struct{}{}
		case OpClear:
			value, err := emptyDeltaValueFor(values, delta.Path, path)
			if err != nil {
				return Document{}, err
			}
			doc.Set[path.String()] = value
		case OpSet, OpReplace:
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
