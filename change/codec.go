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
// Set 对应字段设置或整体替换；Unset 对应字段删除。Mongo 适配器可以直接
// 转成 `$set/$unset`，Redis 适配器可以据此拆分 HSET/HDEL。
type Document struct {
	Set   map[string]any
	Unset map[string]struct{}
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

// Document 把 ChangeSet 转成 Set/Unset 两段文档。
func (s ChangeSet) Document() Document {
	doc := Document{
		Set:   map[string]any{},
		Unset: map[string]struct{}{},
	}
	for _, fieldChange := range s.Changes() {
		switch fieldChange.Kind {
		case KindSet, KindReplace:
			doc.Set[fieldChange.Path.String()] = fieldChange.Value
		case KindUnset:
			doc.Unset[fieldChange.Path.String()] = struct{}{}
		}
	}
	return doc
}
