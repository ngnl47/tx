// Package change 提供生成 Store 使用的变更记录基础模型。
package change

import "errors"

// ErrEmptyPath 表示一次变更没有提供字段路径。
var ErrEmptyPath = errors.New("change: empty path")

// ErrModeConflict 表示同一个数据对象的变更周期内混用了两种持久化模式。
var ErrModeConflict = errors.New("change: mode conflict")

// ErrModeMismatch 表示保存入口和当前数据对象的变更模式不匹配。
var ErrModeMismatch = errors.New("change: mode mismatch")
