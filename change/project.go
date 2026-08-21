package change

import "errors"

var (
	// ErrNilDeltaProjector 表示没有可输出规范化脏状态的投影源。
	ErrNilDeltaProjector = errors.New("change: delta projector is nil")
	// ErrNilDeltaVisitor 表示流式投影没有可接收变更的目标。
	ErrNilDeltaVisitor = errors.New("change: delta visitor is nil")
	// ErrDeltaValueMismatch 表示两个 Store 没有装饰同一个数据对象，不能只合并脏元数据。
	ErrDeltaValueMismatch = errors.New("change: stores refer to different values")
)

// DeltaVisitor 同步消费一条变更路径。
//
// 生成 Store 在调用返回后会立即复用内部路径状态，visitor 不应保留对调用过程
// 中临时数据的引用。DeltaPath 当前按值传递，因此 visitor 可以安全保存该值。
type DeltaVisitor interface {
	VisitDelta(DeltaPath, DeltaOp) error
}

// DeltaProjector 描述可把规范化脏状态同步投影给 visitor 的对象。
type DeltaProjector interface {
	ProjectDeltas(DeltaVisitor) error
}

// DeltaVisitorFunc 允许普通函数直接作为 DeltaVisitor 使用。
type DeltaVisitorFunc func(DeltaPath, DeltaOp) error

func (f DeltaVisitorFunc) VisitDelta(path DeltaPath, op DeltaOp) error {
	if f == nil {
		return ErrNilDeltaVisitor
	}
	return f(path, op)
}

// DeltaCollector 把流式投影物化为兼容边界仍可使用的 DeltaSet。
type DeltaCollector struct {
	Deltas DeltaSet
}

func (c *DeltaCollector) VisitDelta(path DeltaPath, op DeltaOp) error {
	if c == nil {
		return ErrNilDeltaVisitor
	}
	c.Deltas = append(c.Deltas, DeltaEntry{Path: path, Op: op})
	return nil
}
