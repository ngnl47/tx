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

// FieldID 是由 txgen 为结构体字段生成的稳定编号。
//
// 普通字段变更只需要记录 FieldID，避免在热路径上反复保存字段名字符串；
// map、slice 等动态成员继续用 Path 保存字段内部的尾路径。
type FieldID uint16

const (
	// FieldNone 表示未使用字段编号，此时 FieldChange 通过 Path 表示完整目标。
	FieldNone FieldID = 0
)

// FieldResolver 把 FieldID 解析为字段持久化别名。
//
// 生成代码通常为每个 GD* 类型提供一个 resolver。例如字段编号 2 解析为
// "name"，再和尾路径 Path{"1001"} 组合成 "name.1001"。
type FieldResolver interface {
	FieldName(FieldID) string
}

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
//
// Field 优先表示结构体静态字段；Path 在 Field 为 FieldNone 时表示完整路径，
// 在 Field 非零时表示字段内部的动态尾路径。
type FieldChange struct {
	Field FieldID
	Path  Path
	Kind  Kind
	Value any
}

// FullPath 返回适配器落地时使用的完整字段路径。
//
// 示例：
//   - Field=2，resolver(2)="name"，Path 为空，结果为 Path{"name"}。
//   - Field=3，resolver(3)="items"，Path={"1001"}，结果为 Path{"items", "1001"}。
//   - Field=FieldNone 时直接返回 Path 的拷贝，用于兼容手写完整路径。
func (c FieldChange) FullPath(resolver FieldResolver) Path {
	if c.Field == FieldNone {
		return append(Path(nil), c.Path...)
	}
	if resolver == nil {
		return nil
	}
	name := resolver.FieldName(c.Field)
	if name == "" {
		return nil
	}
	out := make(Path, 0, 1+len(c.Path))
	out = append(out, name)
	out = append(out, c.Path...)
	return out
}

// ChangeSet 是一次保存周期内的字段变更集合。
//
// 同一目标重复写入时，后一次变更覆盖前一次变更，符合 Store setter 多次
// 调用后只关心最终落地值的需求。目标可以是 FieldID，也可以是完整 Path。
type ChangeSet struct {
	changes []FieldChange
}

// Add 加入或覆盖一条字段变更。
func (s *ChangeSet) Add(change FieldChange) error {
	if change.Field == FieldNone && len(change.Path) == 0 {
		return ErrEmptyPath
	}
	copied := FieldChange{
		Field: change.Field,
		Path:  append(Path(nil), change.Path...),
		Kind:  change.Kind,
		Value: change.Value,
	}
	if parentFieldTargetExists(s.changes, copied) {
		return nil
	}
	if parentPathTargetExists(s.changes, copied) {
		return nil
	}
	for i, existing := range s.changes {
		if sameTarget(existing, copied) {
			s.changes[i] = copied
			if isParentFieldTarget(copied) {
				s.removeChildFieldTargets(copied.Field)
			}
			if isPathTarget(copied) {
				s.removeChildPathTargets(copied.Path)
			}
			return nil
		}
	}
	if isParentFieldTarget(copied) {
		s.removeChildFieldTargets(copied.Field)
	}
	if isPathTarget(copied) {
		s.removeChildPathTargets(copied.Path)
	}
	s.changes = append(s.changes, copied)
	return nil
}

// Remove 移除指定路径上的变更记录。
func (s *ChangeSet) Remove(path Path) {
	s.remove(FieldNone, path)
}

// RemoveField 移除指定字段编号和尾路径上的变更记录。
func (s *ChangeSet) RemoveField(field FieldID, tail Path) {
	s.remove(field, tail)
}

func (s *ChangeSet) remove(field FieldID, path Path) {
	for i, change := range s.changes {
		if sameTarget(change, FieldChange{Field: field, Path: path}) {
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

func (s *ChangeSet) removeChildFieldTargets(field FieldID) {
	out := s.changes[:0]
	for _, change := range s.changes {
		if change.Field == field && len(change.Path) > 0 {
			continue
		}
		out = append(out, change)
	}
	s.changes = out
}

func (s *ChangeSet) removeChildPathTargets(path Path) {
	out := s.changes[:0]
	for _, change := range s.changes {
		if change.Field == FieldNone && pathParentOf(path, change.Path) {
			continue
		}
		out = append(out, change)
	}
	s.changes = out
}

func parentFieldTargetExists(changes []FieldChange, change FieldChange) bool {
	if change.Field == FieldNone || len(change.Path) == 0 {
		return false
	}
	for _, existing := range changes {
		if existing.Field == change.Field && len(existing.Path) == 0 {
			return true
		}
	}
	return false
}

func isParentFieldTarget(change FieldChange) bool {
	return change.Field != FieldNone && len(change.Path) == 0
}

func isPathTarget(change FieldChange) bool {
	return change.Field == FieldNone && len(change.Path) > 0
}

func parentPathTargetExists(changes []FieldChange, change FieldChange) bool {
	if change.Field != FieldNone || len(change.Path) == 0 {
		return false
	}
	for _, existing := range changes {
		if existing.Field == FieldNone && pathParentOf(existing.Path, change.Path) {
			return true
		}
	}
	return false
}

func pathParentOf(parent Path, child Path) bool {
	if len(parent) == 0 || len(parent) >= len(child) {
		return false
	}
	for i := range parent {
		if parent[i] != child[i] {
			return false
		}
	}
	return true
}

func sameTarget(left FieldChange, right FieldChange) bool {
	return left.Field == right.Field && left.Path.Equal(right.Path)
}
