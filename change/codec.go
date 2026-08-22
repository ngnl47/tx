package change

import (
	"fmt"
	"reflect"
)

// Document 是规范化变更的后端无关文档形式。
//
// Set 对应字段设置、整体替换或容器清空；Delete 对应字段删除。Mongo
// 适配器会把 Delete 转成 `$unset`，Redis 适配器会把 Delete 转成 JSON 字段移除。
type Document struct {
	Set    map[string]any
	Delete map[string]struct{}
}

// ValueResolver 按结构化 Path 从当前对象读取最终值。
type ValueResolver interface {
	ResolveValue(Path) (any, bool)
}

// ValueResolverFunc 允许用函数快速声明当前值读取器。
type ValueResolverFunc func(Path) (any, bool)

// ResolveValue 实现 ValueResolver。
func (f ValueResolverFunc) ResolveValue(path Path) (any, bool) {
	return f(path)
}

// DocumentBuilder 把同步访问到的变更直接写入后端无关文档。
// 生成 Store 保证输入已按自身脏状态收敛，因此这里直接构建输出文档。
type DocumentBuilder struct {
	doc      Document
	resolver FieldNameResolver
	kind     FieldNameKind
	values   ValueResolver
}

// NewDocumentBuilder 创建一个流式变更文档构建器。
func NewDocumentBuilder(resolver FieldNameResolver, kind FieldNameKind, values ValueResolver) *DocumentBuilder {
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

// BuildDocument 按指定字段命名体系把规范化脏状态构建为变更文档。
func BuildDocument(source Source, resolver FieldNameResolver, kind FieldNameKind, values ValueResolver) (Document, error) {
	if source == nil {
		return Document{}, ErrNilSource
	}
	if !kind.Valid() {
		return Document{}, fmt.Errorf("change: unknown field name kind %q", kind)
	}
	builder := NewDocumentBuilder(resolver, kind, values)
	if err := source.VisitChanges(builder); err != nil {
		return Document{}, err
	}
	return builder.Document(), nil
}

// VisitChange 实现 Visitor。
func (b *DocumentBuilder) VisitChange(path Path, op Op) error {
	if b == nil {
		return ErrNilVisitor
	}
	if !op.Valid() {
		return fmt.Errorf("change: unknown operation %d", op)
	}
	display := path.FullPath(b.resolver, b.kind)
	if len(display) == 0 {
		return fmt.Errorf("change: cannot resolve %s field path for change %#v", b.kind, path)
	}
	name := display.String()
	switch op {
	case OpDelete:
		delete(b.doc.Set, name)
		b.doc.Delete[name] = struct{}{}
	case OpClear:
		value, err := emptyChangeValueFor(b.values, path, display)
		if err != nil {
			return err
		}
		delete(b.doc.Delete, name)
		b.doc.Set[name] = value
	case OpSet, OpReplace:
		if b.values == nil {
			return fmt.Errorf("change: cannot resolve current value for path %s", display)
		}
		value, ok := b.values.ResolveValue(path)
		if !ok {
			return fmt.Errorf("change: cannot resolve current value for path %s", display)
		}
		delete(b.doc.Delete, name)
		b.doc.Set[name] = value
	}
	return nil
}

// Document 返回已同步构建的文档。调用方在继续 VisitChange 前不应修改其 map。
func (b *DocumentBuilder) Document() Document {
	if b == nil {
		return Document{}
	}
	return b.doc
}

func emptyChangeValueFor(values ValueResolver, path Path, display DocumentPath) (any, error) {
	if values == nil {
		return nil, fmt.Errorf("change: cannot resolve current value for path %s", display)
	}
	value, ok := values.ResolveValue(path)
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
