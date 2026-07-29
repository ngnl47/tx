package change

// Mode 描述一个数据对象当前变更周期使用的持久化模式。
type Mode uint8

const (
	// ModeNone 表示对象当前没有进入持久化变更周期。
	ModeNone Mode = iota
	// ModeSnapshot 表示对象使用原生字段修改加快照 diff 的方式保存。
	ModeSnapshot
	// ModeStore 表示对象使用生成 Store 的拦截式脏标记方式保存。
	ModeStore
)
