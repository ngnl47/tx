# Agent Session 生命周期与集群控制设计

## 目标

为 Agent 增加与业务 Kick 解耦的通用 Session 生命周期控制，并在多 Agent 进程下通过服务发现完成低频控制广播。
同时收敛 MsgQ 的终止语义、失活清理命名和 Redis 续期频率，保证弱网恢复窗口不回退，也不通过 Redis key
扫描维护 Session。

## 固定边界

- `DisconnectSession` 只关闭当前物理连接，保留 MsgQ 邮箱和逻辑 Session，允许相同 `SessionID` 执行
  `Restore=true`。
- `TerminateSession` 终止逻辑 Session，清理 MsgQ 本地邮箱和精确 Session binding，关闭物理连接，相同
  `SessionID` 不再允许恢复。
- tx 不提供 `KickUser`、封禁、顶号等业务接口；项目只把业务策略映射为上述两项基础能力。
- 终止 Session 不主动删除角色主会话投影。新主会话可以覆盖旧投影，旧投影由 Redis TTL 兜底过期。
- Borrow 使用独立 Session，不依赖目标角色在线状态或角色主会话投影。
- Login 不认识 MsgQ。任意 Login 可以向服务发现中的全部 Agent 广播，只有本地持有目标 `SessionID` 的
  Agent 执行操作。
- 广播依赖 tx 服务发现内存快照与 Agent 内部 RPC，不依赖 Kubernetes、外部负载均衡或 Redis owner 查询。
- 禁止 `SCAN`、`KEYS` 或其他 Redis key 遍历。所有 Redis 删除均使用已经持有的完整 Session binding 精确执行。
- 弱网恢复窗口保持 `180s`。

## Agent 本地 Session 注册表

每个 Agent 在内存中维护：

```text
SessionID -> 已激活连接 + 按 ConnectionID 区分的初始化中连接
连接      -> Principal + SessionClient + SessionHeader + cancel
```

握手身份解析并取得 MsgQ client 后、调用 MsgQ `Init` 前登记为初始化中；`Init` 成功后、向客户端发送 Ready 前原子晋升为
已激活。连接退出时使用 `SessionID + ConnectionID` 比较删除，旧连接退出不能移除已经完成恢复的新连接。控制请求同时
覆盖初始化中和已激活连接；初始化中连接收到终止请求后，无论 `Init` 成功还是因连接取消返回不确定结果，都会使用
不受原请求取消影响的有界上下文，按新 ConnectionID 补偿一次精确终止，且禁止发送 Ready。

同一 `SessionID` 的并发终止请求合并为一个本地终止操作。首个请求负责收尾，后续请求等待同一结果；调用方超时只结束
本次等待，不会取消正在执行的终止。终止操作存续期间新登记的初始化连接会加入同一操作，完成全部精确终止后才移除操作。
任一连接的精确终止出现非幂等错误时，整个操作返回不确定错误，不能被更早连接的成功结果掩盖。

同一 `SessionID` 在跨 Agent 恢复边界短暂出现在两个 Agent 本地注册表时，控制广播会命中两处物理连接；MsgQ
完整 binding 校验保证只有当前 `ConnectionID` 可以终止逻辑 Session。

## 双层控制语义

`DisconnectSession` 的本地处理顺序：

1. 查询本地注册表中的已激活连接和初始化中连接。
2. 未命中时幂等返回未处理。
3. 命中时取消连接上下文并关闭传输。
4. 不调用 MsgQ `TerminateSession`。

`TerminateSession` 的本地处理顺序：

1. 创建或加入该 SessionID 的共享终止操作，标记初始化中的连接不得晋升；操作存续期间的新初始化连接自动加入。
2. 未命中时幂等返回未处理。
3. 使用注册时保存的完整 `SessionHeader` 调用 MsgQ `TerminateSession`。
4. MsgQ 精确 binding 不匹配时仍关闭本地旧物理连接，但不能删除新连接的邮箱。
5. 取消连接上下文并关闭传输；调用方提前取消时，后台终止操作继续完成。

Agent 控制协议返回是否命中本地 Session。广播结果保留每个实例的成功、未命中和错误；所有实例均未命中表示
目标当前离线，按幂等成功处理。无命中且存在超时表示结果不确定，由调用方决定是否重试。

## MsgQ 终止与失活终止

公开命名统一为：

```go
TerminateSession(context.Context, SessionHeader) error
TerminateDeadSessions(context.Context, time.Time) (int, error)
TerminateDeadSessionInterval time.Duration
```

显式终止和定时发现失活 Session 共用同一内部终止路径：

- 完整校验 `RoleID + SessionID + ConnectionID + MsgQ InstanceID`。
- 条件删除精确 Session binding。
- 从当前 MsgQ 本地 Session 表和 Topic 索引移除。
- 释放未确认下行队列。
- 不主动删除角色主会话投影。

积压溢出属于同一逻辑终止结果，也使用相同的精确解绑语义。

配置命名：

```yaml
services:
  msgq:
    offlineTimeout: 180s
    terminateDeadSessionInterval: 2s
```

定时任务只遍历 MsgQ 本地内存 Session 表。

## Redis 续期限频

MsgQ 每次合法上行、Ack、Fetch 或心跳仍更新本地 `lastActivity`，但不再每次执行 Redis `PEXPIRE`。

`RedisSessionStore` 根据既有 TTL 提供续期间隔，默认取 `TTL / 2`。邮箱 Session 在 `Create`、`Rebind` 或一次
成功续期后记录下一次允许续期的时间；在此之前的活动只更新本地时间。到达阈值后的第一条活动同时精确续期 binding
和主会话投影。

该策略保证：

- 单个在线 Session 每半个 TTL 最多产生一次 Redis 续期。
- 正常失活 Session 在 180 秒后由本地任务精确解绑。
- MsgQ 异常退出时，遗留 binding 由 Redis TTL 自动淘汰。
- 不需要任何 Redis key 扫描。

## 项目接入顺序

1. tx 完成 MsgQ 生命周期、Agent 本地注册表、控制 RPC 与广播器。
2. txgame 与 txclient 在两个 Agent 实例下验收无状态链路。
3. txgameslg 与 txclientslg 独立验收有状态链路和 Borrow。

AgentPool、票据从实例绑定调整为池绑定、Login 取得旧 SessionID 的业务策略分别留在项目接入阶段实现，不进入
MsgQ 或 Agent 基础生命周期的正确性边界。

## 验收标准

- `DisconnectSession` 关闭连接后，相同 SessionID 可以恢复并保留未确认下行。
- `TerminateSession` 关闭连接后，相同 SessionID 恢复失败，新 SessionID 不继承旧消息。
- 广播到两个 Agent 时仅本地持有目标 Session 的实例报告命中，另一个 Session 不受影响。
- 旧 ConnectionID 不能终止或移除新恢复连接。
- 高频活动不会产生同频 Redis `PEXPIRE`。
- 失活终止、显式终止和积压溢出都不扫描 Redis，也不主动删除主会话投影。
- Bee 与 Ant 装配使用同一 Agent 控制能力。
