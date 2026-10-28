package taskqueue

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ngnl47/tx/modules/actor"
	"github.com/ngnl47/tx/protocol"
)

// 任务信封必须保留身份字节、连接编号及其余投递字段，且不能修改发布者的对象。
func TestTaskJSONPreservesSessionIdentity(t *testing.T) {
	for _, claims := range []string{"", "角色身份", string([]byte{0, 255, 42})} {
		identity := protocol.SessionIdentity{SessionPrincipal: protocol.SessionPrincipal{RoleID: 14, SessionID: 15, Claims: claims}, ConnectionID: 16}
		task := Task{Identity: actor.Identity{Type: "role", ID: "14"}, Delivery: actor.Delivery{
			Origin: actor.OriginClient, MessageType: "reward", CommandHash: 47,
			RouteKey: protocol.Int64RouteKey(9007199254740993), Payload: []byte{0, 255, 42}, Identity: &identity,
		}}
		encoded, err := json.Marshal(task)
		if err != nil {
			t.Fatal(err)
		}
		if identity.Claims != claims {
			t.Fatal("序列化修改了原始身份")
		}
		var restored Task
		if err = json.Unmarshal(encoded, &restored); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(task, restored) {
			t.Fatalf("任务未完整恢复: claims=%x got=%+v", []byte(claims), restored.Delivery.Identity)
		}
	}
	task := Task{Identity: actor.Identity{Type: "role", ID: "14"}, Delivery: actor.Delivery{MessageType: "no-session"}}
	encoded, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	var restored Task
	if err = json.Unmarshal(encoded, &restored); err != nil || restored.Delivery.Identity != nil {
		t.Fatalf("无会话任务被修改: %+v %v", restored, err)
	}
}

func TestTaskJSONRejectsCorruptedClaims(t *testing.T) {
	var task Task
	if err := json.Unmarshal([]byte(`{"Delivery":{"Identity":{"RoleID":14,"SessionID":15,"ConnectionID":16,"Claims":"!"}}}`), &task); err == nil {
		t.Fatal("损坏的 Claims 编码未被拒绝")
	}
}
