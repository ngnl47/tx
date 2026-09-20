package protocol

import (
	"encoding/json"
	"fmt"
)

// routeKeyJSON 保留路由类型与整数位模式，不经过 float64 中转。
type routeKeyJSON struct {
	Kind    RouteKeyKind `json:"kind"`
	Numeric uint64       `json:"numeric,omitempty"`
	Text    string       `json:"text,omitempty"`
}

// MarshalJSON 使持久任务等 JSON 信封能够携带不透明路由键。
func (key RouteKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(routeKeyJSON{Kind: key.kind, Numeric: key.numeric, Text: key.text})
}

// UnmarshalJSON 恢复强类型路由键；缺省对象表示无路由键。
func (key *RouteKey) UnmarshalJSON(encoded []byte) error {
	var value routeKeyJSON
	if err := json.Unmarshal(encoded, &value); err != nil {
		return err
	}
	switch value.Kind {
	case RouteKeyNone:
		*key = RouteKey{}
	case RouteKeyInt64:
		*key = Int64RouteKey(int64(value.Numeric))
	case RouteKeyUint64:
		*key = Uint64RouteKey(value.Numeric)
	case RouteKeyString:
		*key = StringRouteKey(value.Text)
	default:
		return fmt.Errorf("protocol: unknown route key kind %d", value.Kind)
	}
	return nil
}
