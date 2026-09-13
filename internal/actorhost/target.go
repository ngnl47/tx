package actorhost

// Target 使用启动时唯一实例 ID；路由层负责解析，传输不以 Queue Group 猜测归属。
type Target struct {
	Instance string
	Identity Identity
}
