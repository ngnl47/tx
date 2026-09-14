# 默认 Actor 后端迁移收口

本轮是当前 feature 分支的功能与依赖核验，不是生产上线批准；不提交 Git。

## 当前版本

| 工程 | 起点提交 |
| --- | --- |
| tx | 46a9348 |
| txgen | 8bc6c97 |
| txgame | 6ab2f20 |
| txgameslg | ab36fef |
| txclient | 5514f2a |
| txclientslg | 4b663cf |

六工程起点工作区干净，本轮仅更新 tx 迁移文档与计划。

## 代码核对

- 两个 server 的 `go list -deps ./cmd/server` 均不含 `github.com/asynkron/protoactor-go` 或 `internal/actortrial`，包含 `internal/actorhost`、`internal/actorrpc` 与 NATS Go 客户端。这证明正式构建的包依赖闭包，不等同于仓库已删除全部历史代码。
- 默认 CModActor 使用 KindFactory、BindSlots 和唯一实体宿主，两条传输共享宿主。Agent 上行走 gRPC，业务下行直接到 MsgQ；Actor 间走 Core NATS。没有永久双后端配置开关。
- 启动时先构造 RPC 能力、初始化 Discovery/Actor 并注册共享入口，业务服务启动后统一启动 RPC 监听和控制面，最后发布 running。当前不依赖独立 ActorEndpoint 端口。
- 普通实体使用默认 1024 稳定分区，自定义 SlotMapping SPI 保留；world、seasonWorld 可使用独立槽位与同一服务池。容量为实例数量配额，初始化、卸载、归属与 GD 缓存边界已有明确契约。
- txgameslg 当前 world Handler 同步调用 Procedure，命令适配同步返回 execute 结果，没有脱离 Receive 返回的世界业务子协程池。因此当前完成计数可以覆盖现有业务；未来若增加异步工作，必须另行接入实际完成和排水，不能提前声称具备任意子协程任务追踪。
- 准备收尾、Stop 并发、确认丢失、多槽部分交接和 CAS 结果未知由既有回归覆盖；本轮重跑相关包。多世界与同会话跨世界验收沿用真实客户端和权威归属证据。

## 保留项与明确延期

- tx 的 ProtoActor 快照与对照基线、两个示例的 protobuf 分派测试适配仍引用 ProtoActor，所以各 go.mod 仍保留依赖。生产包闭包不含它；不能为了删 go.mod 条目破坏现有回归或冻结对照。
- 内部旧 Placement/SetPlacement 及 Discovery 的 ActorEndpoint 查询字段仍供旧夹具使用；正式 CMod 绑定 SlotStore，不调用旧成员快照选址。它们尚未完全从源码隔离，不描述为“所有历史接口已清空”，进一步清理需保持历史证据可复现。
- 自动等待 Pod、选择交接目标与续作发布编排按用户决定后续完善；当前保留显式 PrepareHandoff 与槽位 CAS。自动扩容、故障接管、运行中单实体热迁移不纳入本节点。
- 跨世界验收证明寻址与调用，不代表已经实现侦查玩法。FightRoom 可使用现有 Kind 扩展，不为拓扑示意新增完整房间业务。

## 性能口径

冻结 I2 基线未修改。本轮不改生产链路，也不重新运行 benchmark；既有正式入口验收只作为此前已接受的迁移证据，不冒充本轮性能结果。此前测得的闭环吞吐偏移仍保留，不将功能收口表述为“零性能退化”或生产 SLA。

## 本轮整体验证

以下十项串行执行，全部退出码为 0：

- tx、txgame/server、txgameslg/server 各自 `go test ./... -count=1`。
- txclient 测试 79 项、txclientslg 测试 101 项全部通过，两客户端各自 `npm run build` 通过。
- tx 的 framework、actorhost、actorrpc、control、discovery 五包完整 `-race -count=1` 通过；显式启用本机 NATS/etcd。不是全库 race，不将旧 ProtoActor 历史路径也描述为已通过 race。
- txgame 与 txgameslg 两个真实 cluster-e2e 均通过。SLG 包含满载拒绝、正常交接和双世界同会话访问；本轮未另跑 shutdown-failure 扩展分支。

日志、命令与退出码清单、两个正式依赖图、集群 artifacts 和 SHA256 归档在 `/Users/a47/workspace/tx-artifacts/migration-closeout-46a9348`。所有事实来自本轮代码核验和执行；性能单独沿用上文的历史证据口径。

本轮文档后置 ponytail-review：没有生产代码差异，按纯文档风险简化检查；未引入新抽象或删除必要 SPI/测试，无额外删减建议。`git diff --check` 通过。

结论：I3/I4 与 T4 的默认后端切换范围可以收口；源码层的历史夹具保留、自动运维编排和未来世界异步任务机制不冒充已完成。进入 T5 不代表 JetStream 已经可用。

## 下一阶段

可进入 T5 JetStream 可靠任务的最小闭环验证：独立文件流、共享 durable pull consumer、显式确认，向正式 Actor 入口投递并在业务执行结果成功后 ACK。用隔离测试验证消费停止/重启后续领，复用当前实体路由和生命周期，不增加另一套 actor 归属。

沿用已确认边界：不建成功完成表，不把 ACK 与 GD 异常持久化绑定；结果或 ACK 丢失接受重复窗口并记录可观察错误。永久加载拒绝的死信转存、临时失败重试、MaxDeliver 残留恢复与背压仍是 T5 必须验收的后续项，不能因最小闭环通过就宣称可靠任务整体完成。当前尚未实现 JetStream 任务 API。
