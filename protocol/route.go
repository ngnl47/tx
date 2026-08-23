// Package protocol 定义游戏协议共享且与传输方式无关的值。
package protocol

// RouteKeyKind 描述业务路由键原始的 Go 类型。
type RouteKeyKind uint8

const (
	RouteKeyNone RouteKeyKind = iota
	RouteKeyInt64
	RouteKeyUint64
	RouteKeyString
)

// RouteKey 是携带在 BON 载荷之外的不透明强类型路由值。
type RouteKey struct {
	kind    RouteKeyKind
	numeric uint64
	text    string
}

// Int64RouteKey 创建有符号整数路由键。
func Int64RouteKey(value int64) RouteKey {
	return RouteKey{kind: RouteKeyInt64, numeric: uint64(value)}
}

// Uint64RouteKey 创建无符号整数路由键。
func Uint64RouteKey(value uint64) RouteKey {
	return RouteKey{kind: RouteKeyUint64, numeric: value}
}

// StringRouteKey 创建字符串路由键。
func StringRouteKey(value string) RouteKey {
	return RouteKey{kind: RouteKeyString, text: value}
}

// Kind 返回路由键原始的 Go 类型。
func (key RouteKey) Kind() RouteKeyKind {
	return key.kind
}

// Int64 在当前路由键为 int64 时返回有符号整数值。
func (key RouteKey) Int64() (int64, bool) {
	return int64(key.numeric), key.kind == RouteKeyInt64
}

// Uint64 在当前路由键为 uint64 时返回无符号整数值。
func (key RouteKey) Uint64() (uint64, bool) {
	return key.numeric, key.kind == RouteKeyUint64
}

// String 在当前路由键为 string 时返回字符串值。
func (key RouteKey) String() (string, bool) {
	return key.text, key.kind == RouteKeyString
}
