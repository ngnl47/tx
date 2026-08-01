package change

// Filter 判断一条字段变更是否应该保留。
//
// 生成 Store 或存储适配器可以用它实现 ignoreClient、redis ignore、
// mongo ignore 等过滤规则。
type Filter interface {
	Keep(FieldChange) bool
}

// FilterFunc 允许用函数快速声明过滤规则。
type FilterFunc func(FieldChange) bool

// Keep 实现 Filter。
func (f FilterFunc) Keep(change FieldChange) bool {
	return f(change)
}

// Document 是 ChangeSet 的后端无关文档形式。
//
// Set 对应字段设置或整体替换；Clear 对应字段清空。Mongo 适配器会把
// Clear 转成 `$unset`，Redis 适配器会把 Clear 转成 JSON 字段移除。
type Document struct {
	Set   map[string]any
	Clear map[string]struct{}
}

// Filter 返回只包含过滤器保留项的新 ChangeSet。
func (s ChangeSet) Filter(filter Filter) ChangeSet {
	if filter == nil {
		return s
	}
	var out ChangeSet
	for _, fieldChange := range s.Changes() {
		if filter.Keep(fieldChange) {
			_ = out.Add(fieldChange)
		}
	}
	return out
}

// Document 把 ChangeSet 转成 Set/Clear 两段文档。
//
// 该方法只解析完整 Path；如果 ChangeSet 使用 FieldID，请调用
// DocumentWithResolver。
func (s ChangeSet) Document() Document {
	return s.DocumentWithResolver(nil)
}

// DocumentWithResolver 把 ChangeSet 转成 Set/Clear 两段文档，并用 resolver
// 把 FieldID 解析为字段别名。
func (s ChangeSet) DocumentWithResolver(resolver FieldResolver) Document {
	doc := Document{
		Set:   map[string]any{},
		Clear: map[string]struct{}{},
	}
	for _, fieldChange := range s.Changes() {
		path := fieldChange.FullPath(resolver)
		if len(path) == 0 {
			continue
		}
		switch fieldChange.Kind {
		case KindSet, KindReplace:
			doc.Set[path.String()] = fieldChange.Value
		case KindClear:
			doc.Clear[path.String()] = struct{}{}
		}
	}
	return doc
}
