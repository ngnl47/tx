# MsgQ 精确会话与角色主会话设计

> 后续状态：Game/Actor 直接 Session 下行与 `ProcessedUpSeq` 处理完成确认已在下一阶段实现；本文以下“本阶段”描述保留该次原子迁移的原始边界。

## 目标与阶段边界

本阶段把 MsgQ 的物理邮箱从角色维度迁移到逻辑会话维度，并同时建立角色的唯一主会话投影：

```text
SessionID -> SessionBinding -> MsgQ 实例 -> 精确邮箱
RoleID    -> PrimarySessionID -> SessionBinding -> MsgQ 实例 -> 主会话邮箱
```

公开投递入口明确分为：

- `PubSession(SessionID, payload)`：投递到一个确定的逻辑会话；普通会话和 Borrow 只读会话都可作为目标。
- `PubRole(RoleID, payload)`：先解析该角色当前唯一主会话，再投递到该会话；只面向正常可写会话，不向 Borrow 会话扇出。

计划中的 SessionID 邮箱和角色主会话原本是两个连续步骤。真实调用审计证明，若只删除旧的角色投递，两个示例工程已有的跨角色通知和有状态世界回推会立即失效。因此本阶段将两步作为一个原子迁移，不保留含义模糊的旧 `Pub` 兼容接口。

该原子迁移提交时未同时改变 Game/Actor 业务响应路径、`processedUpSeq` 和 Seq/Ack；后续阶段已经完成这些变更。Login、创建角色和 Borrow 登录入口不属于本文范围。

## 身份和寻址

`SessionID` 全局唯一，标识一次逻辑登录会话；`ConnectionID` 标识该会话下的一次物理连接；`RoleID` 是可信业务身份。三者继续同时存在于 `SessionHeader` 和日志中，但只有 `SessionID` 用于精确邮箱寻址。

`SessionBinding` 保留完整四元组：

```go
type SessionBinding struct {
    RoleID       int64
    SessionID    int64
    ConnectionID int64
    InstanceID   string
}
```

Redis 使用两个独立索引：

- `<prefix>:session:v2:{SessionID}` 保存完整 `SessionBinding`；按 SessionID 选择 Redis 分片。
- `<prefix>:primary:v1:{RoleID}` 保存当前主 `SessionID`；按 RoleID 选择 Redis 分片。

版本段用于避免旧的 RoleID 会话 key 与新 SessionID key 数值碰撞。两个索引不要求位于同一 Redis 分片，也不承诺跨 key 事务；各自的单 key 条件操作是线性化边界。

## Store 契约

精确会话 binding 不再允许无条件覆盖：

- `Create(binding)` 使用 `SET NX PX`。干净登录若重用了现存 SessionID，明确返回冲突。
- `Rebind(expected, replacement)` 只允许完整旧 binding 条件匹配后更新 ConnectionID，用于同 SessionID 恢复。
- `Find(sessionID)`、`Refresh(binding)`、`Unbind(binding)` 全部按 SessionID 定位并校验完整 binding。

角色主会话投影提供：

- `BindPrimary(roleID, sessionID)`：正常会话干净登录时替换角色主会话。
- `FindPrimary(roleID)`：读取唯一主 SessionID。
- `RefreshPrimary(roleID, sessionID)`、`UnbindPrimary(roleID, sessionID)`：仅在当前值仍相同时续期或删除，旧会话不能影响新主会话。

Router 只读解析，不刷新任何 TTL。只有经过当前 ConnectionID 校验的 owner 侧 `Init`、`Rec`、`Fetch`、`Confirm`、`Touch` 等活动才能续期 binding；业务发布不能把已断开的会话伪装成活跃会话。

## 新建、恢复与主会话

### 干净登录

Agent 根据可信 `SessionIdentity.AccessMode` 设置 `InitRequest.Primary`：

- `AccessModeNormal` 设置为 `true`，包括以后允许创建角色的 PendingRole 会话。
- `AccessModeBorrowReadOnly` 设置为 `false`。

MsgQ 按 SessionID 获取初始化锁，执行 `Create` 并安装空邮箱。相同 RoleID 的其他 SessionID 不会被替换、清队列或移除 Topic。

正常会话在精确 binding 创建成功后执行 `BindPrimary`；Borrow 会话不写主会话投影。若主会话写入失败，MsgQ 条件删除本次新建的精确 binding，并且不安装本地邮箱。

### 弱网恢复

Agent 的 owner 选择请求显式携带 `RoleID`、`SessionID` 和 `Restore`：

- `Restore=false` 不查询旧 owner，从运行中的 MsgQ 轮转选择目标，最终由 `Create` 拒绝 SessionID 冲突。
- `Restore=true` 必须按 SessionID 找到原 owner；owner 不可用时直接失败，不能转移到没有内存队列的新 MsgQ。

MsgQ 恢复只接受同 RoleID、同 SessionID、相同主会话属性的本地邮箱。它用完整旧 binding 到新 ConnectionID 的 `Rebind` 保留未确认队列和 Seq/Ack 状态。正常会话还必须仍是该 RoleID 的主 Session；已被新干净登录替换的旧会话不能恢复。Borrow 会话只校验自己的精确 binding。

### 连接活动和清理

所有连接级操作先按 SessionID 找邮箱，再校验 RoleID、SessionID 和 ConnectionID。`touch` 在单个邮箱锁内完成本地校验、Redis 条件续期和最后活动时间更新，避免旧连接与恢复并发时误删新连接。

正常主会话的活动同时条件续期主投影；若投影已经指向其他 SessionID，该旧会话失活并返回 `ErrSessionChanged`。Close、超时回收和积压溢出只条件删除自己的精确 binding，并仅在仍是主会话时删除角色投影，不影响同角色的其他 Session。

## 投递与 Topic

`SessionRouter` 提供两个显式入口：

- `ResolveSession(ctx, sessionID)`：读取精确 binding 和 MsgQ 实例。
- `ResolveRole(ctx, roleID)`：读取主 SessionID，再读取并核对精确 binding 的 RoleID。

`Sender.PubSession` 和 `Sender.PubRole` 最终都调用 MsgQ 的同一个精确 `PubSession` RPC。该 RPC 只携带正 SessionID 与 payload，不携带 ConnectionID；同一 Session 恢复到新连接后仍能收到属于该逻辑会话的消息。

角色主会话切换与并发 `PubRole` 的顺序以 Redis 主投影读取为界：读取发生在切换前的投递属于旧主会话，读取发生在切换后的投递属于新主会话。新 SessionID 永远不会继承旧 SessionID 的队列。

Topic 仍是单个 MsgQ 实例内的本地索引：

- `JoinTopics` 使用当前可信 SessionID 定位并管理当前连接。
- `BroadcastTopic` 使用显式 `ownerSessionID` 选择一个 MsgQ 实例，再在该实例内广播。
- 跨 MsgQ 广播继续由业务分别选择各实例上的一个会话作为锚点，不把 Topic 包装成全局广播。

## 项目接入

两个示例工程的 push 门面同步暴露 `PubSession`、`PubRole`、按 SessionID 的 `JoinTopics` 和 `BroadcastTopic`：

- 跨角色通知和有状态世界回推只知道目标 RoleID，使用 `PubRole`。
- 当前连接订阅 Topic，以及选择当前连接所在 MsgQ 做本地广播，使用 HandlerContext 中可信 Identity 的 SessionID。
- 该原子迁移提交时普通业务响应仍由 Agent 发布；后续阶段已经改为 Game/Actor 直接投递并返回处理完成确认。

客户端线协议不变。真实集群证据读取从 RoleID key 改为 SessionID key，并额外核对 record 中的 RoleID。

## 验收

- 同一 RoleID 的两个不同 SessionID 可同时存在于同一或不同 MsgQ，精确投递、Topic、关闭、回收和溢出互不影响。
- 正常会话会替换角色主投影；Borrow 会话不创建、不替换、不接收 `PubRole`。
- 同 SessionID 恢复保留下行队列并更换 ConnectionID；旧连接不能刷新、确认、管理 Topic 或删除恢复后的连接。
- 干净登录使用新 SessionID，队列和 Seq 从零开始；旧主会话不能再次恢复或删除新主投影。
- Redis Create/Rebind/主投影条件脚本通过真实 Redis 和竞态测试。
- 两个示例工程的跨角色单播、本地 Topic 广播、有状态跨 Actor 推送继续通过。
