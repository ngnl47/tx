package actor

import "github.com/ngnl47/tx/internal/actorhost"

// Identity 以 Type 与 ID 标识实体，不携带物理实例或传输地址。
type Identity = actorhost.Identity

// Behavior 是每次激活独立创建的业务行为，未设置的生命周期事件无需空实现。
// Receive 与事件回调串行执行；卸载前事件失败只记录错误，不否决卸载。
type Behavior = actorhost.Behavior

// BehaviorFactory 判断业务加载条件并创建行为；永久拒绝可返回 ErrActivationDenied。
// 工厂及初始化、激活事件不附加统一 deadline，具体加载 I/O 使用自身的超时。
type BehaviorFactory = actorhost.BehaviorFactory

// Context 提供当前回调的上下文、实体身份及本次激活的 Post、After、Deactivate 控制。
// 回调返回后嵌入的 context.Context 结束；实体句柄在卸载前仍可投递。
// After 只将消息送入邮箱，不能用于并发修改业务状态。
type Context = actorhost.Context

// LocalFuture 等待一次本地 Post 的任意类型结果，不开放完成结果的方法。
// RPC Future 则返回协议 Result；不能在同一实体回调内等待自己的 Post。
type LocalFuture = actorhost.LocalFuture

var (
	// ErrActivationDenied 表示业务永久拒绝本次实体加载。
	ErrActivationDenied = actorhost.ErrActivationDenied
	// ErrDeactivated 表示本次激活已关闭准入，旧 Context 不能投递到重建的实体。
	ErrDeactivated = actorhost.ErrDeactivated
	// ErrMailboxFull 表示本地邮箱已满，消息尚未入队。
	ErrMailboxFull = actorhost.ErrMailboxFull
)

// SlotMapping 是稳定分服、分战区或独立世界的归属映射 SPI；零值使用 1024 分区。
// 自定义槽位应带业务命名空间，不能使用保留的 actor-v1. 前缀。
// 同一槽位内的实体共用 owner，仍有独立状态与邮箱；不得在线变更分组来模拟热迁移。
type SlotMapping = actorhost.SlotMapping
