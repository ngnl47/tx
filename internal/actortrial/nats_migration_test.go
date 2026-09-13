package actortrial

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/ngnl47/tx/internal/actorrpc"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
)

// 保留的试验协议与迁入后的实现双向互通，不通过另一套编码掩盖迁移偏差。
func TestNATSMigrationWireCompatibility(t *testing.T) {
	url, ctx := natsURL(t), trialContext(t)
	ns := nats.NewInbox()
	oldRuntime, newRuntime := echoRuntime(), echoRuntime()
	old := peer(t, url, ns, "old", oldRuntime)
	current, err := actorrpc.ConnectNATS(url, ns, "new", newRuntime, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	defer oldRuntime.Drain(ctx)
	defer newRuntime.Drain(ctx)
	for _, tc := range []struct {
		caller Caller
		target string
	}{{old, "new"}, {current, "old"}} {
		result, err := tc.caller.Call(ctx, Target{tc.target, Identity{"role", "中文/1"}}, &actorpb.Delivery{MessageType: "echo", CommandId: 47, Payload: []byte("payload")})
		if err != nil {
			t.Fatal(err)
		}
		if result.MessageType != "echo" || result.CommandId != 47 || string(result.Payload) != "payload" {
			t.Fatalf("wire mismatch: %+v", result)
		}
	}
}
