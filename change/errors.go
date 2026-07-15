// Package change 提供生成 Store 使用的变更记录基础模型。
package change

import "errors"

// ErrEmptyPath 表示一次变更没有提供字段路径。
var ErrEmptyPath = errors.New("change: empty path")
