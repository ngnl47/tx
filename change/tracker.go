package change

// Tracker 是生成 Store 嵌入或持有的运行时脏标记记录器。
//
// Store setter 调用 Touch/Replace 记录具体字段路径；AllTouch 用于整对象
// 需要全量落地的场景。
type Tracker struct {
	changes  ChangeSet
	allDirty bool
}

// Touch 标记字段发生普通 set 变更。
func (t *Tracker) Touch(path Path, value any) error {
	return t.changes.Add(FieldChange{Path: path, Kind: KindSet, Value: value})
}

// TouchField 标记结构体静态字段发生普通 set 变更。
func (t *Tracker) TouchField(field FieldID, value any) error {
	return t.TouchFieldPath(field, nil, value)
}

// TouchFieldPath 标记结构体静态字段内部的动态路径发生普通 set 变更。
func (t *Tracker) TouchFieldPath(field FieldID, tail Path, value any) error {
	return t.changes.Add(FieldChange{Field: field, Path: tail, Kind: KindSet, Value: value})
}

// UnTouch 取消字段变更标记。
func (t *Tracker) UnTouch(path Path) {
	t.changes.Remove(path)
}

// UnTouchField 取消结构体静态字段的变更标记。
func (t *Tracker) UnTouchField(field FieldID) {
	t.UnTouchFieldPath(field, nil)
}

// UnTouchFieldPath 取消结构体静态字段内部动态路径的变更标记。
func (t *Tracker) UnTouchFieldPath(field FieldID, tail Path) {
	t.changes.RemoveField(field, tail)
}

// Replace 标记字段发生整体替换。
func (t *Tracker) Replace(path Path, value any) error {
	return t.changes.Add(FieldChange{Path: path, Kind: KindReplace, Value: value})
}

// ReplaceFieldPath 标记结构体静态字段或其内部动态路径发生整体替换。
func (t *Tracker) ReplaceFieldPath(field FieldID, tail Path, value any) error {
	return t.changes.Add(FieldChange{Field: field, Path: tail, Kind: KindReplace, Value: value})
}

// Unset 标记字段应从持久化文档中删除。
func (t *Tracker) Unset(path Path) error {
	return t.changes.Add(FieldChange{Path: path, Kind: KindUnset})
}

// UnsetFieldPath 标记结构体静态字段或其内部动态路径应从持久化文档中删除。
func (t *Tracker) UnsetFieldPath(field FieldID, tail Path) error {
	return t.changes.Add(FieldChange{Field: field, Path: tail, Kind: KindUnset})
}

// AllTouch 标记整个对象需要全量落地。
func (t *Tracker) AllTouch() {
	t.allDirty = true
	t.changes = ChangeSet{}
}

// AllUnTouch 清空全量和字段级变更标记。
func (t *Tracker) AllUnTouch() {
	t.allDirty = false
	t.changes = ChangeSet{}
}

// AllDirty 返回当前对象是否需要全量落地。
func (t *Tracker) AllDirty() bool {
	return t.allDirty
}

// Changes 返回当前字段级变更集合。
func (t *Tracker) Changes() ChangeSet {
	return t.changes
}

// Clone 返回一份可独立修改的脏标记记录器副本。
func (t Tracker) Clone() Tracker {
	return Tracker{
		changes:  t.changes.Clone(),
		allDirty: t.allDirty,
	}
}
