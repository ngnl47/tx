package change

// UndoFunc 恢复一次受控数据修改前的状态。
//
// 它只由可选单机事务模块在失败路径倒序执行。普通 Store 不会创建或保存 UndoFunc。
type UndoFunc func()

// UndoRecorder 接收生成 Store 在受控修改前登记的反向操作。
//
// change 只定义这一最小契约，避免 Store 对可选事务模块产生反向依赖。
type UndoRecorder interface {
	RecordUndo(UndoFunc)
}

// UndoBindable 由生成 Store 实现，用于在一次事务期间临时绑定 undo recorder。
// 绑定结束后必须传入 nil 解除关联。
type UndoBindable interface {
	SetUndoRecorder(UndoRecorder)
}

// DeltaInspectable 由生成 Store 实现，用于在调试场景检查是否存在待落地变更。
//
// 该接口不包含存储元数据，避免可选事务模块依赖具体存储实现。
type DeltaInspectable interface {
	Deltas() DeltaSet
}
