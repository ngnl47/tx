# 项目侧 MsgQ 调用点审计：SessionID 邮箱与 `PubSession` 最小影响面

审计日期：2026-09-01  
范围：`txgame`、`txgameslg`、`txclient`、`txclientslg`；同时只读核对了它们本地替换依赖的 `tx/modules/msgq` 与 `tx/modules/agent`。未修改业务代码、生成物或测试。

## 结论

仅把 MsgQ 的**邮箱主键/SessionHeader**改为精确 `SessionID`，并新增或迁移到 `PubSession`，可以保持 Agent Frame、业务响应和 Seq/Ack 原样。但这不能同时安全删除现有 `Sender.Pub(ctx, roleID, payload)`：两套模板均有“只知道目标 RoleID”的主动下行，SLG 还把该 RoleID 穿过了 RoleActor -> WorldActor 的内部调用。

因此最小且不改变现有观察语义的分期是：

1. 本阶段在 `tx` 完成精确 SessionID 邮箱、`PubSession` 及 Agent SessionHeader 路由迁移；Init/Rec/Fetch/Confirm/Touch、Frame 和 Seq/Ack 语义不变。
2. 项目侧只更新因核心接口签名必然失配的 `internal/push` 门面、fake sender 和 cluster-e2e 的 Redis 证据读取。
3. **保留既有 roleID 下行兼容路径**（现有 `Sender.Pub` 或等价的、标明过渡用途的兼容 API），不要在本阶段另行设计/暴露 `PubRole`。它是对既有业务语义的保留，不应被包装成新的长期角色推送能力。
4. 下一阶段先设计“业务如何取得并可信传递目标 SessionID”，再将主动下行全部改为 `PubSession`，届时才能删除 roleID 兼容路径。该阶段会触及协议生成与两套客户端 E2E。

若本阶段直接删除 `Sender.Pub(roleID)` 且不保留兼容路径，则不是单纯编译迁移：会破坏跨会话定向通知、SLG WorldActor 的回推通知，以及当前两套真实 cluster-e2e 所断言的跨 MsgQ owner 单播。

## 现状与边界

当前 `tx` 的语义是 role 归属表：

```text
Agent identity(RoleID, SessionID, ConnectionID)
  -> SessionHeader(RoleID, SessionID, ConnectionID)
  -> Redis key <prefix>:session:{RoleID}
  -> SessionRouter.Resolve(RoleID, expectedSessionID)
  -> Sender.Pub/JoinTopics/BroadcastTopic
  -> owner MsgQ
```

- `SessionHeader` 目前含 `RoleID`、`SessionID`、`ConnectionID`：`tx/modules/msgq/mailbox.go:52`。
- `modules/agent` 在握手后以认证身份构造该 Header，并把同一 Header 用于 Init、Rec、Confirm、Fetch 及同步业务响应 Pub：`tx/modules/agent/session.go:100-105`、`153`、`201`、`223`、`229`。
- `Sender.Pub`、`JoinTopics`、`BroadcastTopic` 都先以 RoleID 调 `SessionRouter.Resolve`：`tx/modules/msgq/sender.go:237-296`；Router 从以 RoleID 为 key 的 store 查 owner：`tx/modules/msgq/session.go:77-110`。
- Topic 是单个 owner MsgQ 实例内索引；`BroadcastTopic` 本身没有目标 Header：`tx/modules/msgq/mailbox.go:553-599`。因此 Topic 广播不能代替精确 Session mailbox 的单播。

两个服务端工程通过 `replace github.com/ngnl47/tx => ../../tx` 使用当前工作区核心（`txgame/server/go.mod:5`、`txgameslg/server/go.mod:5`），核心接口变化会立即传导到它们。

## 本阶段必须处理的项目侧编译点

以下项是“核心去掉/改名 role 型 Sender 方法或改变 Header/Router 签名”时的直接编译影响面。具体 `PubSession` 参数形状应以核心最终 API 为准；表中列出当前接缝而不是假定新签名。

| 工程 | 文件 | 当前接缝 | 本阶段处理 |
| --- | --- | --- | --- |
| txgame | `server/internal/push/push.go:15-19,57-81` | 本地 `Sender` 镜像 `Pub(roleID)`、`JoinTopics(roleID)`、`BroadcastTopic(roleID)` | 与 `txmsgq.Sender` 新接口对齐；若保留 role 兼容层，唯一放在该门面/核心兼容层，业务 Handler 不扩散重构。 |
| txgame | `server/internal/push/push_test.go:10-40` | `recordingSender` 实现旧接口 | 更新 fake 和转发断言。 |
| txgame | `server/services/game/handler/system_test.go:17-44` | `recordingPushSender` 实现旧接口 | 同上；保留 `System_PublishNotifyResp` 的 `Seq`/广播统计断言。 |
| txgame | `server/services/game/service_test.go:35-42` | `serviceSender` 实现旧接口 | 同上。 |
| txgameslg | `server/internal/push/push.go:15-19,56-80` | service-scoped 门面镜像相同旧接口 | 与核心同步；不应给 slggame/slgworld 各造一套 Role resolver。 |
| txgameslg | `server/internal/push/push_test.go:10-27` | `recordingSender` 实现旧接口 | 更新 fake。 |
| txgameslg | `server/services/slgworld/handler_test.go:37-47` | `notifySenderStub` 实现旧接口 | 更新 fake。 |
| 两工程 | `server/internal/app/app.go:67-75` / `:92-100` | 组装 `NewSessionRouter`、`NewSender` | 若精确 SessionID 迁移改变 router/store/constructor，这两处是唯一业务工程生产构造点；若 `NewSender` 构造器保持，仅接口 fake 会编译受影响。 |

`SessionHeader{...}` 在四个项目的非生成业务代码中没有直接字面量；唯一实际 Agent SessionHeader 构造位于核心 `tx/modules/agent/session.go:104`。项目 Agent 服务只提供身份解析和 dispatcher。因此，项目侧不需要另造 Header，也不应复制 Agent 的 Init/Rec/Fetch/Ack 状态机。

### cluster-e2e 必须同步 Redis 证据，但不改线协议

两套 server 编排器当前按 RoleID 直接读取 Redis：

- `txgame/server/internal/clustere2e/dependencies.go:22,273-300`
- `txgameslg/server/internal/clustere2e/dependencies.go:22,382-408`

它们读取 `<prefix>:msgq:session:{roleID}` 并反序列化 `instanceId/sessionId/connectionId`。若新 mailbox/store 改为以 SessionID 为 key，`SessionEvidence` 的输入也必须改为已知 `sessionID`（或同时传 `roleID, sessionID` 仅用于 artifact 展示），并读 `<prefix>:msgq:session:{sessionID}` 的新布局。调用方已经在启动客户端时计算并保存精确 session ID：

- txgame：`server/internal/clustere2e/suite.go:393-404,450-477`。
- txgameslg：`server/internal/clustere2e/suite.go:471-503,523-550`。

所以本阶段只需调整 evidence lookup 与单测 fixture；保留 artifact 中 `RoleID` 作为关联字段，`SessionID` 作为实际查找字段。MsgQ owner 分布、进程故障/重启、Redis/Mongo 验收含义不变。

## 不能本阶段删除的 RoleID 主动下行链路

### 无状态 txgame

`System_PublishNotify` 的 direct 分支调用 `push.Pub(ctx, request.RoleId, payload)`：
`txgame/server/services/game/handler/system.go:47-79`。

这不是总向“当前连接”回包。无状态客户端 E2E 明确从第一个会话发送 `roleId: second.roleID`，并等待第二个会话收到 `GlobalNotify`：
`txclient/src/cli/cluster-e2e.ts:214-223`。因此用 `handlerContext.Identity().SessionID` 简单替换会把 `direct-second` 错投给第一个会话，破坏既有行为。

`SubscribeNotify` 的 `JoinTopics(request.RoleId)` 与 `PublishNotify` 的 `BroadcastTopic(request.RoleId, ...)` 也要依赖 role owner 才能在正确的单个 MsgQ 实例中进行本地 Topic 操作：`txgame/server/services/game/handler/system.go:31-45,69-79`。其中订阅当前连接日后可以改用 HandlerContext 的可信 `(SessionID, ConnectionID)`；广播仍至少需要一个 owner 定位策略，不能借 `PubSession` 直接替代。

### 有状态 txgameslg

`slggame` 有完全相同的 direct/JoinTopics/BroadcastTopic 组合：
`txgameslg/server/services/slggame/notify.go:16-66`。其客户端 E2E 同样从第一个会话定向到第二个会话：
`txclientslg/src/cli/cluster-e2e.ts:161-173`。

更关键的是 `slgworld`：`World_StartMarch` 从 RoleActor 跨 Actor 到 WorldActor 后，只带 `request.RoleId`，再在 WorldActor 中 `push.Pub(... request.RoleId, ...)`：
`txgameslg/server/services/slgworld/handler.go:120-147`。内部 Actor 调用的 `HandlerContext.Identity()` 明确为零值（`services/internal/actorcmd/command_actor.go:73-79`），所以 WorldActor 无法从上下文恢复 SessionID。删除 role 推送会让 `Role_StartMarch` 之后的 `System_Notify` 消失，破坏 `txclientslg` 的真实 E2E 断言：`src/cli/cluster-e2e.ts:206-213`。

## 为什么不应本阶段把 Game 主动下行一律改成显式 SessionID

这条路线在最终设计上合理，但不是“仅 mailbox + PubSession”的最小迁移：

1. `System_PublishNotify` 需要为目标增加受信的 `targetSessionId`（仅 `RoleId` 不能推出哪一次会话）。这会改两个项目的命令定义/生成产物、TypeScript 调用、fake server、E2E 与文档；还需要确定“旧 SessionID 已恢复/已换代”时的错误契约。
2. `Role_StartMarch`/`World_StartMarch` 必须携带 session ID，RoleActor 的可信 Identity 要进入跨 Actor 消息。它会触及 SLG command manifest、两端生成、WorldActor 测试和真实 cluster-e2e；并需定义 WorldActor 延迟处理时 session 已更替应丢弃还是发送到新会话。
3. `SubscribeNotify` 可从可信 HandlerContext 取 `(SessionID, ConnectionID)`，但这会顺带把“请求 RoleId 与认证主体是否必须一致”的现有未显式规定行为变成访问控制变化。它应作为单独决定，而不是暗含在 mailbox 迁移中。
4. Topic 广播必须继续有 owner 选择入口。若新 core 不提供 role resolve，业务必须先持有一个 SessionID/owner 实例；这与 `PubSession` 的精确单播是不同能力。

故建议本阶段**暂留旧 `Sender.Pub(roleID)` 兼容 API**，并严格限制它只服务这些既有链路；不要新增命名为 `PubRole` 的 API，也不要扩大使用面。新的调用使用 `PubSession`，老链路等 SessionID 在业务消息里被显式、可信地传播后再迁移和删除。若产品要求本阶段彻底没有任何 RoleID 下行，则必须把上述协议/Actor 传播列为同一阶段的明确范围，不能宣称响应/Ack 或业务观察行为不变。

## 应保持不变的响应、Ack 与客户端内容

`PubSession` 只更换下行目标定位，不应修改：

- Agent 连接 FrameReady、UpMsg/DownMsg、reconnect 与上行重放：核心在 `tx/modules/agent/session.go:121-174`；客户端 `AgentClient`/`AgentSession` 已共享该线协议。
- `System_PublishNotifyResp` 的 direct `Seq`、broadcast 的 Attempted/Delivered/Skipped/Overflowed；Handler 当前分别在 txgame `system.go:62-79`、SLG `notify.go:49-66` 构造。
- `System_SubscribeNotifyResp` 的 InstanceId/Topic。
- client 在**业务下行成功后**推进 Ack 的规则；本轮不需要改 `txclient/src/protocol/*` 或 `txclientslg/src/protocol/*`。

两客户端目前不 import MsgQ Go API，也不含 `SessionHeader`、`Sender.Pub` 或 `SessionRouter.Resolve` 调用。它们的 `roleID:sessionID` debug token、`AgentSession` 与 E2E 仅在最终“主动下行携带目标 SessionID”的下一阶段才需要改。仅 store key/evidence API 迁移时，`txclient` 与 `txclientslg` 可不改、不重新生成。

## 后续阶段清单（不纳入本阶段）

1. 定义业务级目标会话引用：是 `(roleID, sessionID)` 还是 opaque session handle；并规定 SessionID 过期/换代时的错误和幂等行为。
2. 修改两份 `System_PublishNotify` 生成命令与客户端 E2E，使跨会话 direct 传入目标 SessionID；保留相同响应字段。
3. 修改 SLG `Role_StartMarch` -> `World_StartMarch` 的内部消息，使 WorldActor 获得目标 session 引用；更新 Go/TypeScript 生成物（通过各自 `gen/run.sh`，不手改生成目录）。
4. 决定 Topic owner 的长期 API：显式 owner SessionID、业务维护的 owner handle，或单独的只读 lookup；不得把 local Topic 描述成全局 Pub/Sub。
5. 在核心 role 兼容路径被所有项目迁出后，删除它，并以真实 cluster-e2e 覆盖 direct、Topic、断线恢复和 SLG 跨 Actor 回推。

## 审计覆盖与未发现项

- 已检索四工程（排除 `.git`、依赖目录）的 `SessionHeader`、`Sender.Pub`/`.Pub`、`Resolve`、`JoinTopics`、`BroadcastTopic`、`AgentSession` 与 `cluster-e2e` 调用。
- 除上表和业务主动下行外，未发现额外项目级 MsgQ SessionHeader 字面量、SessionRouter Resolve 调用或客户端对 Go MsgQ API 的调用。
- `Resolve` 的其他命中为 world placement、Promise 等无关 API，未计入。
