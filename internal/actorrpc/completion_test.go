package actorrpc

import (
	"github.com/ngnl47/tx/internal/actorresult"
	actorpb "github.com/ngnl47/tx/modules/actor/pb"
	"testing"
)

func TestCompletionResultHasNoBusinessPayload(t *testing.T) {
	result := actorresult.Result{Completed: true}
	wire := actorresult.ToProto(result)
	if err := ResultError(wire); err != nil {
		t.Fatal(err)
	}
	decoded, err := actorresult.FromProto(wire, nil)
	if err != nil || !decoded.Completed {
		t.Fatalf("完成结果丢失: %+v %v", decoded, err)
	}
	for _, invalid := range []*actorpb.Result{{}, {Completed: true, MessageType: "business"}, {Completed: true, Payload: []byte{1}}, {Completed: true, CommandHash: 1}} {
		if err := ResultError(invalid); err == nil {
			t.Fatalf("接受了歧义结果: %+v", invalid)
		}
	}
}
