// Package sessionid 提供基于 etcd revision 的 SessionID 分配能力。
package sessionid

import "context"

// MaxSessionID 保持客户端 JavaScript 安全整数范围。
const MaxSessionID int64 = 1<<53 - 1

// Allocator 分配当前集群内有效 Session 生命周期范围内唯一的 SessionID。
// 分配失败允许留空号；调用方不能依赖连续性或并发返回顺序。
type Allocator interface {
	AllocateSessionID(context.Context) (int64, error)
}
