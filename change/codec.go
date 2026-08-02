package change

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
func (ds DeltaSet) DocumentWithResolver(resolver FieldNameResolver, backend NameBackend, values DeltaValueResolver) Document {
	doc := Document{
		Set:   map[string]any{},
		Clear: map[string]struct{}{},
	}
	for _, delta := range ds.Clone().Compact() {
		path := delta.Path.FullPath(resolver, backend)
		if len(path) == 0 {
			continue
		}
		switch delta.Kind {
		case DeltaDelete:
			doc.Clear[path.String()] = struct{}{}
		case DeltaClear:
			doc.Set[path.String()] = emptyDeltaValue
		default:
			if values != nil {
				resolved, ok := values.DeltaValue(delta.Path)
				if ok {
					doc.Set[path.String()] = resolved
					continue
				}
			}
			doc.Set[path.String()] = nil
		}
	}
	return doc
}

var emptyDeltaValue = map[string]any{}
