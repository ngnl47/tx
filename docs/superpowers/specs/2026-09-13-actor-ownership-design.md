# Actor 归属组件正式接线设计

**2026-09-13 最新决策：** 用户已接受 I2 的临时业务预算，允许进入 I3 正式接线。原 10% 比较线保留为迁移偏移的排查提示，不再单独阻止本次接线；不等同于生产验收放行。优先保持去掉 ProtoActor 后的上下行完成链路性能，不重复全 NATS 上行测试。固定基线见 [迁移前基线](../../audits/baselines/2026-09-13-uplink.json)。I3 先完成交接准备与 Stop 共享收尾，再迁入实体宿主及适配，最终以相同业务负载复测。下文 I2 未放行表述保留其评审时点，以下最新决策为准。

日期：2026-09-13。代码基线：tx `dd07b29`；本轮开始六工程工作区干净。本文是设计细化，不表示正式运行时已替换，也不把历史测试当作本轮验证。实施顺序见 [接线计划](../plans/2026-09-13-actor-ownership-integration.md)。

## 目标与取舍

保持业务按 Actor 类型和路由键调用。Agent 上行直连 gRPC，Actor 间 RPC 使用 NATS，两者共享唯一的实体归属、加载准入和本地邮箱。沿用 etcd Discovery、SlotStore CAS 和 SlotResolver，不引入第二套成员协议或复制完整第三方集群模块。

前次固定版本源码核验中，ProtoActor disthash 依赖其 Cluster/PID/Remote；Dapr Placement 发布类型宿主表，其交接确认不等价于 tx 的持久化成功许可；GoAkt Grain 的已有 owner 优先策略较接近，但实现依赖其 Grain 引擎、内部注册表和原生通信。三者均非当前约束下可直接替换的独立组件。这是本次候选范围的结论，不是对所有开源方案的排除。

保留源码参考：[ProtoActor disthash](https://github.com/asynkron/protoactor-go/blob/288962e52f3f59533c8f463fc31f98b8d5d39e41/cluster/identitylookup/disthash/manager.go)、[Dapr v1.18.4 交接处理](https://github.com/dapr/dapr/blob/v1.18.4/pkg/actors/internal/placement/loops/disseminator/order.go)、[GoAkt v4.5.5 Grain 引擎](https://github.com/Tochemey/goakt/blob/v4.5.5/actor/grain_engine.go)。本轮不复制上游实现；若后续抽取，逐文件核对版本、许可声明、修改标记和测试，不把上游成熟度当作改造版本的验收。

## 当前能力与正式接线缺口

| 当前事实 | 正式接线要求 |
| --- | --- |
| `internal/actortrial/routing.go` 的 RoutedCaller 读取 SlotResolver；未分配直接返回错误 | 保持高频调用只读缓存；首次分配由低频建服、开房或预热流程显式调用 tx 能力 |
| `Discovery.Runtime.Slots()` 返回 CachedSlotStore，ResolveSlot 只读 Watch；权威存储未从该 Runtime 暴露 | 正式组合根需取得权威存储供首次分配、交接复核和加载准入；不能把现有 Slots() 直接注入 SlotHost，或改变普通读取的缓存语义 |
| `slots.go` 在 ConnectSlotHost 中包装 factory，权威读归属后才加载；直连接收器也使用这个 Runtime | 将加载准入绑定到实体宿主装配，不以创建 NATS 连接作为生效前提；不在两个 Receiver 重复检查或逐消息查 etcd |
| SlotAllocator 已组合 SlotStore、SlotRouter、Directory 与容量探测 | 保留首次分配与交接组合；归属真相仍在 Discovery，不新增通用调度框架 |
| SlotHost.quiesce 排空整个 Runtime，再调用注入的 Flush；现有 Redis 测试写自定义快照 | 接入实际 Procedure/writeback 与 GD；现有测试不能证明正式 GD 收尾完成 |
| `Server.Drain` 等待服务和 Actor 已接纳工作，不停止后台任务，也不最终冲刷 CMod | 控制面的 StateDrained 不能直接作为持久化完成或槽位晋升许可 |
| `Server.shutdown` 停服务任务后调用 CMod Drain；`component.Host` 记住成功阶段，失败从原阶段重试 | 交接复用该收尾顺序和重试进度，不另造完成表或第二个 writeback.Manager |
| I3a 已将 RPC Host 关闭移出 CMod Drain，根生命周期最终取消会唤醒 Serve 退出 | 交接准备可保留共享 RPC；后续 NATS 宿主必须沿用相同资源边界 |
| 当前 CModDiscovery 在 CModActor 之后启动，Discovery 才申请 RuntimeInstanceID | 新宿主先获得租约身份及 Watch 快照，再开放两种入口；修改依赖顺序时消除旧 ActorEndpoint 依赖，不能形成环 |
| `modules/control/SlotPromoter` 执行归属 CAS 和目标就绪检查 | 继续作为高级低层能力；安全交接组合负责先确认源方收尾，不能宣称任意 PromoteSlot 调用都有排水保护 |

当前测试位置：`internal/actortrial/{slots,routing}_test.go`、`internal/framework/drain_test.go`、`tests/writeback_manager_test.go`。它们是待复用的证据入口，本文没有重新执行它们。

## 职责与包边界

| 层 | 承担的职责 | 保留的边界 |
| --- | --- | --- |
| `modules/discovery` | 启动身份、租约、成员目录、持久 assignment、CAS、Watch 缓存与 SlotResolver | 不持有 Actor、不执行 GD 收尾、不实现邮箱 |
| 正式 `modules/actor` | 逻辑身份、单次激活、邮箱、生命周期、容量预留、状态探测；按身份调用的能力 | 不拥有 GD schema 或世界规则；不承诺异常后 RPC 必达 |
| Actor 的 NATS/直连适配 | 编码、请求关联、错误及结果传递；两个入口调用同一实体宿主 | 不各建归属缓存；直连只承接所需 Call，不虚构直连 TellAccepted 的 broker 确认 |
| `internal/framework` | 按 Kind 组装路由、共享 Receiver、实例宿主和控制面；衔接生命周期与持久化收尾 | 业务 Registration 不再手工组装连接、发现和一串依赖检查 |
| `modules/control` | 复用现有运维 gRPC 入口传递交接准备请求与槽位操作 | 协议层不理解具体 GD；低层 CAS SPI 继续可用 |
| game 侧 factory / Procedure / writeback 组件 | 实体存在性与加载规则、处理业务、声明容量估计及卸载策略；遵守既有写回 SPI | 不选 NATS subject、不决定重试改派、不维护第二份 owner |

这是正式迁移的目标位置，不要求将每一行表格做成新包或 Interface。`SlotHost` 当前混合连接、准入和收尾，不能整体搬进公共 API；已有 `RoutedCaller` 的组合足够支撑 Call，不再增加 RouterFactory、通用策略注册表或永久双后端配置。

普通角色、房间使用稳定逻辑分区；分区规则与数量在首次持久 assignment 前固定，不能随实例数取模。重型世界可以各占一个槽位。Kind 到服务池及分区规则在组合根声明一次，两条传输共用；game 与 slgworld 独立伸缩。分区数量及具体 ID 编码是示例接线前的配置决策，本轮不写入任意默认值。

## 正常交接时序

交接单位维持“先排空源实例，再逐槽晋升”，不实现运行中单实体热迁移。I3a 已在正式框架与既有控制端口提供 `PrepareHandoff` 操作；以下完整 Actor 迁移时序仍需后续宿主接线验收：

1. 调用方带预期源 RuntimeInstanceID，接收方验证本次启动身份；端点复用不能获得旧实例的成功确认。
2. 源方关闭服务及实体准入并标记 draining；等待已准入、排队、执行中的工作结束。停止会产生后续业务写入的受管理任务，再进行必要持久化收尾。
3. 卸载前回调仍有机会发送 RPC；错误记录日志，不反向否决卸载。必要持久化走 Procedure/writeback 的 Drain，不能放进一个错误被忽略的 OnDeactivateBefore 回调。
4. 持久化收尾与已接受消息的发送收尾成功后，返回与源启动身份对应的准备成功结果。业务闸门继续关闭，租约、控制连接和收尾所需资源保留到最终 Stop。
5. 交接方持有完整 current assignment，对每个槽位进行 CAS 晋升；目标加载前权威核验归属，按需激活。源实例多个槽位交接可复用成功收尾，不再次执行业务卸载通知或冲刷已完成组件。
6. 所有计划交接完成后停止源实例资源。部分槽位已晋升、后续失败时保留已完成结果；重试重新读取 assignment，不回滚或重放业务。

PrepareHandoff 与 Stop 必须串行共享框架关闭进度，不能在并发操作中提前解绑存储或关闭控制连接。实现优先提取 `shutdown` 已有收尾阶段，复用 `component.Host.drained`；仅增加区分“准备成功、仍保留资源”和最终停止所需的状态，不另建工作完成计数器。现有 Drain/WaitDrained 含义保持明确，相关 GoDoc 不再暗示它已完成持久化。

准备失败或响应未知时不执行下一次 CAS；重试准备操作可查询同一源实例的已完成阶段，但不重新开放业务。租约丢失走现有致命退出策略，不自动把槽位分给新实例；不能将“旧方已不可见”当作“旧方成功收尾”。不承诺网络分区下绝对单活。

GD 收尾失败时，Manager 待刷根对象、writer、存储绑定及其有效 context 必须仍然可用；释放实体引用不等于可释放这些资源。新测试必须验证失败后的重试保存了最后一笔已接受变更。Fusion 仍按既有持久化完成边界验收，不把此次交接变成同步等待全部 Redis 任务写入 Mongo 的新契约。

若 world 在 Receive 中把工作交给子协程后立即返回，Receive 返回不能代表变更已完成。必须核对其受管理任务的完成边界，持久化前等待实际业务完成；优先复用现有 lifecycle 能力，不在本节点开发协程池。未证明这一点时，不宣称已覆盖并行业务的交接。

## API / SPI 迁移清单

以下是正式 T4 的迁移要求，不是本轮删除或改名授权。

| 现有入口 | 目标能力与兼容性 |
| --- | --- |
| `Messenger.VirtualPID` 及四方法的 `*protoactor.PID` 参数 | 普通业务改用逻辑 Identity（Kind、Key），位置由 tx 解析；属于 Go API 变化，不能称为源兼容替换 |
| `NewKind` / `NewFixedKind` 的 ProtoActor producer、`AdaptKind` | Kind 仍由各 factory 声明；承接加载前业务判断、Receive、生命周期和容量配置，保留必要扩展点 |
| `Runtime.ActorSystem()` 与 ProtoActor Context/Props | 原生类型无法在删除依赖后原样保留；逐个实际调用映射到本地消息、定时、卸载及任务能力，存在必要缺口则暂停删除，不保留无需求的整套模拟 ActorSystem |
| `FixedKindPID`、示例 Agent resolver 与 world caller | 正常业务统一身份路由；高级按实例调用仅在控制、诊断等确有需求时保留，并携带启动身份，不能绕过加载准入 |
| `CModActor()` / `ServiceRegistration.ActorKinds` | 保留面向使用者的能力与声明入口；示例不接触底层 NATS 连接和宿主启停 |
| `Advanced.Slots()`、SlotStore、SlotRouter、SlotPromoter | 保留高级 SPI 与现有 CAS 契约；常规调用走 tx 提供的交接组合 |
| `CModProcedureWriteBack` | 保留；复用默认写回组件，不恢复世界专用 WriteBackRuntimeFactory |

生命周期使用 `OnInitAfter`、`OnActivateBefore/After`、`OnDeactivateBefore/After`。房间可在初始化后安排 65 秒卸载，也可根据业务规则显式卸载；结束战斗等会改状态的任务仍进入同一邮箱。卸载后能否再次加载由业务规则决定，不能仅因 ID 可路由就复活已经结束的房间。

四种 NATS 语义沿用分支共识：Tell 本地投递；TellAccepted 确认抵达当前连接的 NATS 服务端；CallFuture 异步等待结果；Call 同步等待结果。都不自动重投，TellAccepted 不证明持久化或目标已经接收。JetStream 的可靠消费另外实施，不混进当前归属组件。

例如：`role/1001` 通知 `world/51` 行军加速，业务只给出这两个逻辑身份。世界已分配时走缓存定位；不存在 assignment 时，先由开世界/预热流程调用分配能力。若 world/51 当前在 A，B 新上线不会使它迁移；明确交接时等 A 全实例收尾成功，再把 world/51 的 assignment 晋升到 B。A 还承载的其他世界也会暂停接收新业务，需由同次滚动计划逐槽处理。

## 验收与决策边界

- 第一节点先在试验区补真实 Procedure/writeback 持久化与 Discovery.Runtime 租约接线，证明双入口共用边界；不直接迁移两个示例。
- 正式迁移还需证明框架启动/关闭顺序、高级 SPI 迁移、普通实体稳定分区，以及两示例真实 GD/多 Session/跨世界链路。
- T4 原有共同负载退化超过 10% 的筛选线尚未改写。[I2 评审](../../audits/2026-09-13-uplink-business-comparison.md) 已完成代表性 GD/Procedure、完整上行、固定到达率和连接字节对照，是否改用业务容量门槛仍待确认。上行也经 NATS 已因转发流量成本退出候选，后续仅评估既有 ProtoActor 基线与上行直连、Actor 间 NATS 的方案。
- 本轮没有新增 Go 代码或依赖，没有性能变化可测；不得用文档完成状态勾选上述运行验收。
