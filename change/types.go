package change

import "strings"

// Kind 描述字段变更的语义，用于后续转换成 Redis/Mongo 写入命令。
type Kind string

const (
	// KindSet 表示把字段设置为 Value。
	KindSet Kind = "set"
	// KindUnset 表示删除字段或把字段从持久化文档中移除。
	KindUnset Kind = "unset"
	// KindReplace 表示字段整体替换，常用于 map、slice、struct 指针等复合值。
	KindReplace Kind = "replace"
)

// Path 是从根对象到变更字段的别名路径。
//
// 例如角色名字段是 Path{"name"}，物品数量可以表示为
// Path{"items", "1001", "quantity"}。
type Path []string

// String 返回点分隔路径，便于转换为 Mongo `$set` 字段或日志输出。
func (p Path) String() string {
	return strings.Join(p, ".")
}

// Equal 判断两个路径是否逐段相等。
func (p Path) Equal(other Path) bool {
	if len(p) != len(other) {
		return false
	}
	for i := range p {
		if p[i] != other[i] {
			return false
		}
	}
	return true
}

// FieldChange 记录一次字段变更。
type FieldChange struct {
	Path  Path
	Kind  Kind
	Value any
}

// ChangeSet 是一次保存周期内的字段变更集合。
//
// 同一路径重复写入时，后一次变更覆盖前一次变更，符合 Store setter
// 多次调用后只关心最终落地值的需求。
type ChangeSet struct {
	changes []FieldChange
}

// Add 加入或覆盖一条字段变更。
func (s *ChangeSet) Add(change FieldChange) error {
	if len(change.Path) == 0 {
		return ErrEmptyPath
	}
	copied := FieldChange{
		Path:  append(Path(nil), change.Path...),
		Kind:  change.Kind,
		Value: change.Value,
	}
	for i, existing := range s.changes {
		if existing.Path.Equal(copied.Path) {
			s.changes[i] = copied
			return nil
		}
	}
	s.changes = append(s.changes, copied)
	return nil
}

// Remove 移除指定路径上的变更记录。
func (s *ChangeSet) Remove(path Path) {
	for i, change := range s.changes {
		if change.Path.Equal(path) {
			s.changes = append(s.changes[:i], s.changes[i+1:]...)
			return
		}
	}
}

// Changes 返回变更快照，调用方不能修改内部切片。
func (s ChangeSet) Changes() []FieldChange {
	out := make([]FieldChange, len(s.changes))
	copy(out, s.changes)
	return out
}

// Empty 判断集合中是否没有字段级变更。
func (s ChangeSet) Empty() bool {
	return len(s.changes) == 0
}
