# Agent 会话租约替代逐条 Rec：可行性证明

## 结论

Agent 在内存中维护上行游标，确实可以省去每条消息的 `MsgQ.Rec` 往返；但“本地推进、定期同步、恢复时读取 MsgQ 游标”不能保持当前的至多一次上行分派。正常断线重连就能触发重复执行，无须假设进程崩溃。已确认两种不同语义：新建 Session 必须重新登录角色并从空 Seq/Ack 与下行队列开始；恢复原 Session 则保留 SessionID、MsgQ 序号和未确认下行。后续已补齐同址 Agent 重启后的原票据恢复，仍未把租约接入生产路径；跨地址发现与恢复仍待设计。

## 当前代码中的权威边界

- `modules/agent/session.go` 在调用业务 `Dispatcher.Dispatch` **之前**等待 `MsgQ.Rec` 成功；MsgQ 返回的序号决定是否跳过重复消息。业务完成后，Agent 才在客户端帧中推进处理完成 Ack。
- `modules/msgq/mailbox.go` 的 `Rec` 同时校验当前 `ConnectionID`、推进连续上行 Seq、确认客户端捎带的下行 Ack；`Init(Restore=true)` 将新连接绑定到原邮箱并返回该邮箱的已接受上行游标。
- `Rec` 经过 `touch` 维持 MsgQ 会话活动和 Redis binding 租期。移除它后，只有下行读取或独立确认等活动不足以保证纯上行会话持续保活，必须另有活动续期路径。
- 证明时的 `txgame/server/services/login/ticket.go` 把票据绑定到运行 `AgentInstanceID`，恢复仅放宽过期时间。后续同址重启方案已让恢复握手验签旧票据但不校验旧实例目标；新建握手仍校验目标与有效期。每次新登录仍分配新 SessionID，不继承旧邮箱。
- `modules/agent/service/service.go` 的 `Drain` 关闭新连接入口；现有请求准入会排空已接纳的业务，却没有将 Agent 私有的上行游标提交到 MsgQ 的步骤。MsgQ `Stop` 会使本地邮箱失效，旧 Session 不迁移到另一 MsgQ。

## 正常重连反例

假设尝试最薄的租约：Agent A 从 MsgQ 得到游标 9，以本地游标接受 Seq 10，完成购买业务，稍后批量同步游标。

1. A 收到 `UpMsg(Seq=10)`，在本地标记“已接受”，业务扣款成功；MsgQ 的最新游标仍为 9。
2. 客户端正常断线，同一 Agent 上的新连接先向 MsgQ 发出 `Init(Restore=true)`。现有接口只会返回 9。如果此前 `Ack=10` 没有到达客户端，客户端会重放 Seq 10；新连接若据此重建本地游标，就会再次分派，重复扣款。
3. 如果 `Ack=10` 已到达客户端，客户端虽不会重放，但恢复的 `Ready.Ack=9` 违反 `txclient` 对 Ack 不回退的检查。只有 A 保留一个跨物理连接共享的本地会话游标，并让旧连接与新连接通过同一个准入/交接屏障，才能避免这两种结果。

所以，单纯用“最终一致性”替换 `Rec` 不成立。现有 `ConnectionID` 重绑只会挡住旧连接后续的 MsgQ 调用；当分派不再经过 MsgQ 时，它不能单独挡住旧连接的 Game 调用。

## 若要成立，最少需要哪些步骤

| 场景 | 必须成立的边界 |
| --- | --- |
| 同一 Agent 弱网恢复 | 本地接受游标属于逻辑 Session，不属于物理连接；旧连接停止接受新 Seq 与新连接读取游标必须串行。业务调用前推进本地接受游标，维持当前至多一次取舍。 |
| 新建 Session | 客户端重新 Login，取得新目标票据和新 SessionID；新连接由 Agent 生成新 ConnectionID。重新认证与授权，Seq/Ack、未确认下行与旧 Session 无关；不强制重建 Role Actor 或重读 GD。 |
| 同址 Agent 重启后恢复原 Session（已通过真实多进程验收） | 客户端复用已签名旧票据和原地址；新实例验签后以新 ConnectionID 重绑原 MsgQ 邮箱，保留 Seq/Ack 与未确认下行。旧 Agent 必须已停止；原邮箱不存在时明确恢复失败。旧地址变化后的目标发现及并存实例的完整交接尚未实现。 |
| 客户端 Ack | `FrameReady`、独立 `FrameAck` 与下行捎带的上行 Ack 不能超过恢复时可解释的接受进度，或必须让恢复方取得 Agent 的本地进度。单纯异步 Flush 与现有 Ack 契约不兼容。 |
| 下行 Ack 与会话保活 | `Rec` 目前同时确认客户端捎带的下行 Ack 并刷新 MsgQ 会话活动。移除后需要合并/独立的下行确认；纯上行、无下行活动时可复用现有 `Touch` 做低频续期。若逐条改调 `Confirm`，一次 RPC 的节省可能被另一条 RPC 抵消。 |
| MsgQ 排水 | 原 MsgQ 仍持有下行队列；当前 `Stop` 后旧 Session 不迁移。Agent 租约不解决 MsgQ 邮箱迁移，须保持现有“新 Session 干净登录”的边界。 |

这里的“等待业务调用”不能用取消 Context 或超时直接代替：超时不证明 Game 没执行。新登录也不能自动重放旧会话中结果不明的扣款请求。

## Agent 重启后的新登录与恢复边界

重新 Login 请求目标、票据和 SessionID，以 `Restore=false` 建立新会话；新物理连接也取得新的 `ConnectionID`。新 Session 重新认证与授权，上下行序号从头开始，不接续旧 MsgQ 邮箱与未确认下行；Role Actor 或 GD 是否重载取决于各自生命周期与缓存策略。旧邮箱可能仍留在原 MsgQ，直到明确终止、过期或 MsgQ 停止；此时按 RoleID 的 `PubRole` 仍可能覆盖新旧邮箱，但不会把旧邮箱的序号或消息转给新邮箱。

`txclient` 与 `txclientslg` Web 示例在意外断线后限次自动恢复，始终复用原票据、地址、SessionID、序号及待发送队列；手动断开仍由用户触发恢复。新实例复用原公开地址且原 MsgQ 邮箱仍在时可完成恢复；邮箱失效会明确失败，不自动改为新建 Session。旧会话存在未确认上行时，示例仍阻止手动新登录，以免静默丢弃结果不明的请求。地址已变或旧实例与新实例并存时，没有通用的目标发现与交接机制。

## 本轮验证与收益边界

- `go test ./modules/agent -run '^TestAcceptedUpMsgRecoveryBoundary$' -count=1` 通过；`go test ./modules/msgq -run '^(TestRestoreReturnsAcceptedUpSeqBeforeBusinessCompletion|TestInitWithoutRestoreAlwaysCreatesFreshSession)$' -count=1` 通过。这些测试固定当前 `Rec` 先于业务分派和恢复读取 MsgQ 游标的事实，不是租约实现测试。
- 补充验证：`txgame/server` 的 `go test ./services/login ./services/agent -count=1` 通过；`txclient` 的 `node --import tsx --test tests/session.test.ts tests/client.test.ts` 共 20 项通过。它们分别覆盖票据目标与当前客户端重连/Ack 行为，仍不构成跨 Agent 交接验证。
- 本机重跑 `BenchmarkRecTransport` 三次，复用连接的回环 gRPC 中位数为 **110,295 ns/op、9,803 B/op、156 allocs/op**；本地调用中位数为 **468.0 ns/op、127 B/op、2 allocs/op**。回环差额包括 gRPC、序列化、TCP、调度等，不能直接外推云上吞吐，也不能归因于单一 MsgQ 锁。
- [已有同进程正式链路对照](2026-09-24-rec-colocate.md)同时消除了 `Rec`、`StreamFetch`、`Confirm` 的远端传输，不能当作“租约只移除 Rec”的完整链路收益。当前没有租约原型的四机吞吐或 P99 数据。
- 后续真实多进程补验：`txgame/server` 的 `go run ./cmd/cluster-e2e` 完整八进程套件通过。客户端先断线，另一客户端将通知写入原 MsgQ 邮箱；旧 Agent 排水退出，新 Agent 以相同公开地址和新 InstanceID 启动后，原票据与原 AgentClient 恢复同一 SessionID，取回离线通知并继续 Ping。Redis 证据显示 MsgQ owner 未变、ConnectionID 已换；显式终止另一个验收邮箱后，旧会话恢复被拒绝且邮箱没有重新创建。该阶段只证明同址重启和明确失效，不证明 MsgQ 故障时原邮箱可迁移，也没有租约实现或性能收益。完整客户端测试 97 项、构建及集群编排单测通过。证据保存在 `/private/tmp/txgame-cluster-e2e-agent-restart/20260924-181934.246767000/`。

下一项需决定是否做 Agent 租约的受控原型与完整链路对照。若只验证传输收益，可保留 `Rec` 的逐条权威验收和业务前置顺序，单独比较复用 gRPC 流或按目标 MsgQ 合并多 Session 请求。
