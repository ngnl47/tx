package protocol

import (
	"encoding/json"
	"math"
	"testing"
)

func TestRouteKeyJSONRoundTrip(t *testing.T) {
	for _, key := range []RouteKey{{}, Int64RouteKey(math.MinInt64), Int64RouteKey(math.MaxInt64), Uint64RouteKey(math.MaxUint64), StringRouteKey("世界.490001"), StringRouteKey("")} {
		encoded, err := json.Marshal(key)
		if err != nil {
			t.Fatal(err)
		}
		// 解码覆盖已有值，避免可复用目标残留上一次路由。
		decoded := StringRouteKey("previous")
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded != key {
			t.Errorf("%+v -> %s -> %+v", key, encoded, decoded)
		}
	}
}

func TestRouteKeyJSONRejectsUnknownKind(t *testing.T) {
	var key RouteKey
	if err := json.Unmarshal([]byte(`{"kind":255}`), &key); err == nil {
		t.Fatal("未知路由类型未被拒绝")
	}
}
