package tests

import (
	actor "github.com/ngnl47/tx/modules/actor"
	"testing"
)

// 来源独立于消息名称，经过跨进程序列化后仍保持不变。
func TestDeliveryOriginIndependentOfMessageName(t *testing.T) {
	for _, source := range []actor.DeliveryOrigin{actor.OriginInternal, actor.OriginClient, actor.OriginTask} {
		d := actor.Delivery{MessageType: "custom.business", Origin: source}
		wire, err := d.ToProto()
		if err != nil {
			t.Fatal(err)
		}
		got, err := actor.DeliveryFromProto(wire)
		if err != nil {
			t.Fatal(err)
		}
		if got.Origin != source {
			t.Fatalf("来源丢失: %v", got)
		}
	}
	if _, err := (actor.Delivery{MessageType: "valid", Origin: actor.DeliveryOrigin(99)}).ToProto(); err == nil {
		t.Fatal("未拒绝未知来源")
	}
}
