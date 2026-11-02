package actorhost

import "github.com/ngnl47/tx/modules/discovery"

// Target 使用启动时唯一实例 ID；路由层负责解析，传输不以 Queue Group 猜测归属。
type Target struct {
	Instance   string
	Identity   Identity
	SlotID     string
	Generation uint64
}

// Assignment 保留本次发送观察到的归属，用于条件失效而非业务重投。
func (t Target) Assignment() discovery.SlotAssignment {
	return discovery.SlotAssignment{SlotID: t.SlotID, RuntimeInstanceID: t.Instance, Generation: t.Generation}
}
