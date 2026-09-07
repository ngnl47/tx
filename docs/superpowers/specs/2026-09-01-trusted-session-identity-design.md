# 可信会话身份贯穿设计

## 目标与阶段边界

本阶段建立 Login 票据进入 Agent 后的稳定身份模型，并把该身份从 Agent 可信边界贯穿到 Game/Actor Handler：

```text
Login ticket
  -> Agent IdentityResolver
  -> SessionPrincipal
  -> Agent 生成 ConnectionID
  -> SessionIdentity
  -> Actor Delivery
  -> 项目 HandlerContext
```

本阶段只改变内部身份契约，不实现 Login 服务、票据签名、账号映射、Borrow 协议、创建角色业务、MsgQ 的 SessionID 主键迁移、Game/Actor 直接发布响应或上行处理完成确认。

## 身份模型

`protocol.SessionPrincipal` 是 Login 票据经过项目校验后得到的逻辑主体：

```go
type SessionPrincipal struct {
	AccountID       int64
	BorrowAccountID int64
	ServerID        int64
	RoleID          int64
	SessionID       int64
	AccessMode      AccessMode
	CanCreateRole   bool
}
```

当前身份契约只要求 `RoleID` 为正，不校验编号来源。后续 Login 与创建角色阶段会在签发创建角色票据前预分配
`RoleID`；预分配只保留编号，只有 `Create_Role` Handler 可以建立持久角色和账号角色关系。

`AccessMode` 只定义当前已经确认的两种模式：

- `AccessModeNormal`：普通可写会话，`BorrowAccountID` 必须为零。
- `AccessModeBorrow`：主账号借用目标账号的独立会话，`BorrowAccountID` 必须为正；读写权限和建角许可由业务决定。

`protocol.SessionIdentity` 在 `SessionPrincipal` 基础上增加正 `ConnectionID`。`ConnectionID` 只能由 Agent 为本次物理连接生成；项目身份解析器不能从票据或客户端数据指定它。

## Agent 边界

`IdentityResolver` 返回 `SessionPrincipal`。Agent 校验主体后，以服务端生成的 `ConnectionID` 构造不可变 `SessionIdentity`，用于：

- Session 与 MsgQ 初始化；
- 连接生命周期和命令日志；
- 每一条 `DispatchRequest`；
- Actor Delivery 的可信身份。

项目 `ActorDispatchResolver` 仍只决定目标 Actor、消息类型、命令号、路由键和载荷。通用 `NewActorDispatcher` 在调用 Messenger 前必须用 `DispatchRequest` 中的身份和上行序号覆盖 Delivery 对应字段，不能接受项目 resolver 或客户端载荷伪造这些值。

## Actor Delivery

`actor.Delivery` 增加可选的 `Identity *protocol.SessionIdentity` 和 `UpSeq int64`：

- Agent 上行投递必须同时携带有效 Identity 与正 UpSeq。
- Actor 内部调用可以同时省略 Identity 与 UpSeq。
- 只出现其中一个时信封无效。

protobuf 使用独立 `SessionIdentity` 消息和 `AccessMode` 枚举。所有 ID 在 Go 与 protobuf 中都使用正 `int64`。

## 项目 HandlerContext

txgen 生成的 `cmd.HandlerContext` 增加：

```go
Identity() txprotocol.SessionIdentity
UpSeq() int64
```

txgame 与 txgameslg 的 `actorcmd.CommandContext` 返回 Delivery 中的只读值。这样业务 Handler 可以直接使用可信 `RoleID` 获取角色基础结构，也能在日志、幂等键和 Borrow 业务鉴权中使用 AccountID、SessionID、ConnectionID、AccessMode 与 UpSeq。

Actor 内部调用未携带会话身份时返回零值；需要玩家身份的 Handler 必须由其业务入口明确要求。框架本阶段不为每个命令增加推测性的身份策略。

## 调试身份

两个示例工程现有 `<roleID>:<sessionID>` 明文调试 Token 保持不变。仅在 debug/test 装配中，将：

- `AccountID` 暂取 `RoleID`；
- `ServerID` 固定为示例服 `1`；
- `AccessMode` 为 `AccessModeNormal`。

release 环境仍必须注入真实票据校验器。调试映射不构成正式账号模型。

## 验收

- Agent 将 SessionPrincipal 与服务端 ConnectionID 组装为 SessionIdentity 后统一校验，拒绝缺失主体和非法模式。
- Agent 生成并覆盖 ConnectionID；resolver 无法伪造 Actor Delivery 的身份或 UpSeq。
- Actor protobuf 往返保留完整身份并复制 Payload。
- txgen 生成 HandlerContext 的新接口，两个示例工程重新生成且 Handler 可读取身份与 UpSeq。
- tx、txgen、txgame/server、txgameslg/server 全量测试通过。
