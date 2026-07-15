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

// UnTouch 取消字段变更标记。
func (t *Tracker) UnTouch(path Path) {
	t.changes.Remove(path)
}

// Replace 标记字段发生整体替换。
func (t *Tracker) Replace(path Path, value any) error {
	return t.changes.Add(FieldChange{Path: path, Kind: KindReplace, Value: value})
}

// Unset 标记字段应从持久化文档中删除。
func (t *Tracker) Unset(path Path) error {
	return t.changes.Add(FieldChange{Path: path, Kind: KindUnset})
}

// AllTouch 标记整个对象需要全量落地。
func (t *Tracker) AllTouch() {
	t.allDirty = true
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
