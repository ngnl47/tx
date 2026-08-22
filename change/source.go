package change

import (
	"errors"
	"reflect"
)

var (
	// ErrNilSource 表示没有可访问的变更源。
	ErrNilSource = errors.New("change: source is nil")
	// ErrNilVisitor 表示没有可接收变更的访问器。
	ErrNilVisitor = errors.New("change: visitor is nil")
	// ErrInvalidPath 表示变更路径为空、超过固定深度或包含未知节点。
	ErrInvalidPath = errors.New("change: invalid path")
	// ErrValueMismatch 表示两个 Store 没有装饰同一个数据对象，不能只合并脏元数据。
	ErrValueMismatch = errors.New("change: stores refer to different values")
)

// Visitor 同步消费一条变更路径。
//
// 生成 Store 在调用返回后会立即复用内部路径状态，visitor 不应保留对调用过程
// 中临时数据的引用。Path 当前按值传递，因此 visitor 可以安全保存该值。
type Visitor interface {
	VisitChange(Path, Op) error
}

// Source 描述可供存储边界直接消费的稳定变更源。
//
// HasChanges 必须是无副作用的快速检查；在源状态未被修改时，VisitChanges
// 必须可重复调用并输出相同变更。生成 Store 实现该契约。
type Source interface {
	HasChanges() bool
	VisitChanges(Visitor) error
}

// ValidateSource 拒绝 nil 以及装在接口值中的 typed nil Source。
// 存储和序列化边界应在调用 HasChanges 或 VisitChanges 前执行该校验。
func ValidateSource(source Source) error {
	if source == nil {
		return ErrNilSource
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return ErrNilSource
		}
	}
	return nil
}

// VisitorFunc 允许普通函数直接作为 Visitor 使用。
type VisitorFunc func(Path, Op) error

func (f VisitorFunc) VisitChange(path Path, op Op) error {
	if f == nil {
		return ErrNilVisitor
	}
	return f(path, op)
}
