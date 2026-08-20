// Package protocol defines transport-neutral values shared by game protocols.
package protocol

// RouteKeyKind describes the original Go type of a business-route key.
type RouteKeyKind uint8

const (
	RouteKeyNone RouteKeyKind = iota
	RouteKeyInt64
	RouteKeyUint64
	RouteKeyString
)

// RouteKey is an opaque, strongly typed routing value carried outside BON payloads.
type RouteKey struct {
	kind    RouteKeyKind
	numeric uint64
	text    string
}

// Int64RouteKey creates a signed integer route key.
func Int64RouteKey(value int64) RouteKey {
	return RouteKey{kind: RouteKeyInt64, numeric: uint64(value)}
}

// Uint64RouteKey creates an unsigned integer route key.
func Uint64RouteKey(value uint64) RouteKey {
	return RouteKey{kind: RouteKeyUint64, numeric: value}
}

// StringRouteKey creates a string route key.
func StringRouteKey(value string) RouteKey {
	return RouteKey{kind: RouteKeyString, text: value}
}

// Kind returns the route key's original Go type.
func (key RouteKey) Kind() RouteKeyKind {
	return key.kind
}

// Int64 returns the signed key value when this is an int64 route key.
func (key RouteKey) Int64() (int64, bool) {
	return int64(key.numeric), key.kind == RouteKeyInt64
}

// Uint64 returns the unsigned key value when this is a uint64 route key.
func (key RouteKey) Uint64() (uint64, bool) {
	return key.numeric, key.kind == RouteKeyUint64
}

// String returns the key value when this is a string route key.
func (key RouteKey) String() (string, bool) {
	return key.text, key.kind == RouteKeyString
}
