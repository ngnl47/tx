# tx 后续工作

公共 API 与接入方式见 [README](../README.md)，内部职责与可靠性契约见 [设计文档](design.md)，维护检查见 [审计标准](architecture_audit_standard.md)。完成的施工步骤和废弃方案由 Git 历史追溯。

## 当前批次：稳定 Actor 放置与调用

2026-10-29 已完成当前存储版本的 [本地正式包与持续流量滚动验收](local-package-rollout-acceptance.md)：txgame、SLG Role、SLG World 三轮均超过 99.99%，分别核对持久化、排水和 World 重启恢复；验收模板修复与报告尚未提交。本轮不含云端容量或合服执行。

[GD 访问安全与 World 独占运行时](plans-entity-access-safety.md) 第 1～7 批及 EntityCache 管理 API 已提交，完成本地回归与微基准。共享普通读取使用 Snapshot，修改在 Procedure 内 Find；GDUser/GDServer 保留存储权威，World 保留独占路径及 2×2 Monster、50ms 主 tick 和网格保存闭环。[验收报告](entity-access-acceptance.md) 区分完整回归、race、真实依赖、跳过范围及复制开销，不代表云端容量验收。EntityCache 观察与管理 API 已按后续确认范围实现，见 [管理 API](entity-cache-management.md)；本地回归与热路径对照见 [管理验收](entity-cache-management-acceptance.md)；分配次数不变，计时波动和验证边界已记录。

统一 GD 实体访问已实施，实际验收见 [实施记录](plans-entity-access.md)。生成期选择 EntityCache，保留显式 ctx 与现有 Find 入口，创建/删除与 Procedure、WriteBack 协同；不新增普通 Handler 自动互斥或自动事务。

- tx 只提供通用分组放置和稳定槽位归属；业务负责 OriginServerID、MainServerID 等分组规则。合服执行不进入框架。
- txgame 已撤下合服屏障、在线合服协议/运维入口及等级合榜；保留静态 GDServer 目录和两阶段建角。合服示例后置，不能在运行中直接修改目录来代替完整合服流程。
- 通用放置已经具备，不再新增另一套寻址：同服务池、同命名策略、同分组的不同 Actor Type 共用当前槽位 owner，邮箱、状态与生命周期独立。SLG 已将 Role、Legion、ServerActor 接入主服组；GlobalGameActor 和 World 保持独立。
- 本地真实 NATS/etcd 验证三类型同槽、跨实例 Call 和受控交接后缓存继续生效；SLG Agent 上下行及同主服 Actor 调用通过。缓存命中微基准 10,000 身份/P2 约 118～148 ns/op、零分配，不是集群吞吐结论。
- txgame server/gen 回归、受影响业务及放置缓存/交接 race 通过；未跑云集群、在线合服或新吞吐压测。GD 生成查询统一显式 ctx，应用保存入口不提供可选 ctx；Handler 外建立消息操作范围，保留缓存、事务和排水边界，活跃事务拒绝直接持久化。

## 子服务生命周期与模块复用

- 已整理 [子服务、Actor 生命周期与业务模块复用](service-actor-lifecycle.md)，第 1 批子服务阶段与 World 启动衔接已实现。
- 项目模块通过进程固定入口复用资源；Handler 不再绑定 Access，资料订阅保持生产服务所有权，真实操作上下文保留。正式包 Role/Room 生命周期及 World 启动交接的历史验收见下。
- Room 保持直接寻址、按需激活；不增加仅用于转发的 GlobalRoomActor，不改变默认 1024 分区，不自动运行云测试。模块复用规则属于项目，资源所有权和 Actor 邮箱边界必须保留。

## 服务作用域基建访问与示例对齐（2026-10-26）

- 已完成 [基建访问契约](runtime-access.md) 与 [本轮计划及验收](plans-runtime-access.md)：显式 CMod、接收端作用域、三类 Reader、Writer、Procedure 和生成的 ActorAt 入口；未引入全局默认应用或自动事务。
- txgame 的 Fightroom 协议统一为 cmd 单次绑定，并补最小 Legion 查询；txgameslg 补 GlobalGameActor 与等级榜。SLG 保留服务缓存、Procedure、写回和主服放置组，GlobalGameActor 不加入该组。
- 本地全量 Go、两端生成编译及客户端测试/构建通过；真实依赖验证作用域生命周期、Fightroom 两种调用、SLG 寻址、等级榜、周期与关闭写回。作用域纯内存测试通过 race；未启用的可靠任务及其他外部用例不计为通过。本轮没有云端容量或完整滚动发布回测。

## Actor 生命周期与项目 World 启动（2026-10-26）

- 职责与当前实现见 [Actor 生命周期与 World 启动边界](../../txgameslg/docs/actor_lifecycle.md)。txgameslg 已组合现有发现、槽位控制、EnsureActivation/Probe，接入 --world-id 与最小 GDWorld 加载；tx 不新增业务就绪状态或 World 专用策略。
- txconfig Excel 登记 MainServerID → int64 WorldID，txgameslg 按角色 OriginServerID 解析主服后返回配置目标，客户端沿用既有 RouteKey。txgen 只复用现有生成能力。
- 本地回归覆盖正式装配启动、持久记录、owner、Ready 和 Agent 查询链路；不等同于云端压测或滚动发布验收。后续合服继续遵守有状态停服边界，不预设运行中 Actor 热迁移。

## 接入与业务身份边界（2026-10-22）

- Login 流程与票据签发、Agent 票据验证、账号/分服/借号/建角资格由两组业务工程维护。
- tx 仅保留 RoleID、SessionID、ConnectionID 与不可解释的可信身份载荷；同一连接复用载荷，不增加逐消息签票或数据库查询。
- 已调整身份传输和两组工程的共享 session 包，同步生成契约、测试和文档。登录、借号、建角授权、重连与跨 Actor 投递保持原行为。
- 本轮不提交，不实施两阶段建角或合服，不改变既有登录结果和恢复资格。
- 本地验证：tx、txgen、txgame/server、txgameslg/server 全量 Go 测试通过，两组 gen module 回归通过；改动相关的身份、Agent、Actor、Login 和角色入口 race 回归通过。
- 两组生成入口已执行，客户端生成产物未发生变化；txclient 116 项、txclientslg 124 项测试及两端构建通过。
- 新增业务 Principal 经 protobuf 往返的测试，覆盖正常/借号、建角资格、不匹配身份拒绝；生成调用和可靠任务夹具携带不透明 Claims。
- 本机身份信封 ToProto 微基准约 243～395 ns/op、368 B/op、3 allocs/op，仅测本地信封转换，不代表集群吞吐没有回归。未跑云端或完整滚动集群，未显式启用的数据库/任务集成测试不计入通过范围。
- 按 ponytail-review 完成本轮独立只读复核，未发现阻塞问题；核对了票据算法、业务权限校验、内部投递和集成测试迁移，差异检查通过。
- Actor 内部身份字段已改为 bytes Claims，旧字段编号保留；不支持新旧内部身份信封混跑，部署时必须协调升级 Agent 与业务服务。客户端帧和票据 JSON 不变。

## 当前实现与已完成验收

会话链路以 [会话序号与确认契约](design.md#会话序号与确认契约) 为准。以下是 2026-10-17 的实现及验收状态，完成不代表已提交或已部署：

| 独立约束 | 当前状态与核对入口 |
| --- | --- |
| UpSeq 止于 Agent 当前连接 | 已实现；[连接处理](../modules/agent/session.go) 与 [Agent 回归](../tests/agent_session_test.go) 覆盖旧号过滤、跳号及 `TestAgentRestoreOnlyRestoresDownlinkMailbox` |
| 上行不确认、不重放 | 已实现；[写帧回归](../modules/agent/session_writer_test.go) 验证服务端不发上行 Ack；[客户端会话测试](../../txclient/tests/client.test.ts) 验证恢复不重放 |
| 业务与 MsgQ 不接收 UpSeq | Delivery、MessageContext 与 MsgQ 均不接收 UpSeq，MsgQ 上行游标已移除；生成器与两套应用产物已同步，Go/客户端回归已完成 |
| 压测关联与传输序号分离 | 已实现；[ProbeID 回归](../../txclient/tests/cluster-perf.test.ts) 覆盖乱序响应、单条超时后继续且不重投 |
| 本地持续流量滚动发布 | 已完成；[验收记录](../../txgame/docs/acceptance/local-rolling-release.md)：50 连接、180 秒、45,000/45,000 完成，四次 Game 更新/回滚及最终排水通过 |

本地滚动验收覆盖 txgame/txclient；txgameslg/txclientslg 的源码、生成产物和本地单元/构建回归已同步，本次未重跑其真实多进程集群。Agent Rec lease 不是当前待实现事项；MsgQ 会话 TTL 和角色归属租约仍按各自职责存在。

## 投递契约收敛验收

2026-10-17 已完成以下本地验收，本轮变更未提交：

- 来源与完成：`DeliveryOrigin` 统一采用 `OriginInternal`、`OriginClient`、`OriginTask`，区分入口语义而非节点或传输；`Delivery.Origin` 由 Agent/任务入口设置，`Result.Completed` 表达无响应体成功；消息名称不再决定来源和任务确认。两套应用及 txgen 生成接口已同步。
- 身份编码：标准 Actor ID 编解码由 tx 提供，既有 i:/u:/s: 编码保持不变；自定义 ID 不受限制。
- 探针：txgame 额外下行使用 `System_ProbeNotify.ProbeID`；SLG 行军完成使用 `World_MarchCompleted` 的结构化字段；两端产物及关联测试已同步。
- 常规回归：tx、txgen、txgame/server、txgameslg/server 的 `go test ./...` 通过；txclient 115 项、txclientslg 109 项测试及两个客户端构建通过；txbench Handler 测试通过。
- 并发与真实任务：tx 的 taskqueue、actorrpc、agent 包 race 回归通过；txgame 的 `TestQueuedGeneratedCommandRetriesUntilHandlerCompletes` 在本机 NATS/etcd 上以 race 模式通过，验证业务失败保留/重试、完成后才 ACK，并仅清理本轮随机测试资源。
- 独立变更复核未发现实质问题。本轮没有重跑真实多进程集群、完整滚动发布或云端容量测试；其他需显式环境变量的数据库集成测试未启用，不算通过。

## 待开展的部署验收

以下任务需要实际部署参数与专用环境，不自动租赁云实例，也不向生产环境注入故障。

1. **新构建的云端回测与长期稳态**：本地滚动发布已经完成；后续通过 txgame/txclient 及 txgameslg/txclientslg 的集群入口验证持续负载、滚动更新和依赖故障恢复。观察业务完成率、端到端延迟、CPU、内存、队列和客户端计划发起延迟。[七机 20/50 验收](audits/2026-09-28-seven-host-2050-acceptance.md) 仅是三次 180 秒窗口的证据，不代表长期生产容量。
2. **JetStream 生产配置**：按 [部署说明](deployment/jetstream-tasks.md) 确定磁盘、同步策略、副本及物理故障域，验收发布确认、恢复和通知流容量。同机容器故障测试不能替代跨物理机容灾；死信补偿和重投由业务决定。
3. **会话积压容量**：示例继续使用 1024 条/8 MiB 限额；取得慢客户端、离线时长与消息大小分布后再评估容量，不增加无界缓存或触限后静默丢弃机制。

## 需要独立需求才能启动的方向

- 配置中心与进程内配置切换：目前采用外部 JSON、整套校验和滚动重启。后续若启用主动拉取，先明确快照发布、失败保留旧版、在途房间持有原快照及客户端版本协调。结构变化仍需重新生成和构建。
- 控制面认证与 TLS/mTLS：生产身份、权限、证书加载及轮换需独立设计。
- 异常接管、弹性部署与单实体迁移：不从成员过期推导唯一执行者，不在框架内自动重投业务；正常排水和稳定槽位交接沿用现有契约。
- 新存储后端与生成器扩展：以具体应用需求为前提；生成器方向由 [txgen 计划](../../txgen/docs/IMPLEMENTATION_PLAN.md) 维护。

## 维护与验收边界

- tx 保留邮箱顺序、准入、排水、归属 CAS、任务恢复和存储一致性等框架回归；真实 Redis/Mongo/etcd/NATS 测试由明确环境变量启用，跳过不等于通过。
- txgen 保留输入校验、完整生成包编译、生成 API 和配置快照契约，不承接数据库或事务写回实现测试。
- 完整集群编排和端到端容量验收由应用工程维护；无状态与有状态示例分别验收，不互相替代。
- 各库只保留直接测量现行热点的小型 benchmark。新增优化须有可复现瓶颈，不恢复阶段性档位搜索、旧实现 A/B 或固定吞吐采样工具。
- 公共契约变化分别核对字段的产生者、持有者、跨进程传递与释放时机，并逐项记录实现和验收证据；不能用“取消确认”代替“字段不再下传”的核验。同步 README、design、两套应用接入和必要生成产物；普通文档整理不要求重跑集群或重生成未变更产物。
- 有日期的历史设计和压测记录须标明适用版本或已被替代的契约，不作为当前实现依据；原始结果保留，不能改写为新构建已验收。
- 每轮说明实际执行、依赖跳过与未验证的范围，运行差异检查并进行变更复核；未经要求不提交。

## 统一业务协议与独立完成事件验收

2026-10-19 本轮实现，未提交：

- `actor.MessageContext` 显式携带 Actor、Principal 和本次激活的 State；`MessageTable` 按服务一次构造、只读共享，接收端校验 client/remote/local 范围及 Call/Tell 模式。本地信封不经远程 Delivery 调用。
- txgen 的统一协议输入为 `gen/cmd`，同一 DTO 和方法可同时声明多个入口。客户端描述、哈希、BON 和 TypeScript 复用现有生成流程，服务端公开端点使用业务 DTO 别名；生成前拒绝类型/端点冲突与路由键类型不匹配。
- txgameslg 的 World.StartMarch 同时接收客户端与 Role Tell。Role 只校验并转发同一 DTO/可信身份；两条路径都由当前 World 激活的网格、本地完成 Handler 发布最终结果。Handler 不按 Origin 分支，不生成 Accepted，不协调跨节点响应顺序。
- 移除重复行军 cmd/rpc 源、激活级 Handler/注册表、RPCRegistry 与 ForActor 覆盖表。共享依赖在 Service.Init 构造，GridWorld 和 Post 句柄仍按激活隔离；local 发布失败记录业务身份与错误，不重投。
- 既有非行军 cmd 和纯内部 GenerateRPC 接口仍保留；本轮没有全量迁移无关协议。txgame 的独立接入进行了本地回归。
- 本地关闭准入后，尚在执行的网格任务可能无法 Post 最终事件；保留记录错误的既定边界，不把它升级为持久后台任务或同步发布屏障。

验收范围：tx、txgen、txgame/server、txgameslg/server 和 gen 常规 Go 回归，txclientslg 123 项测试与构建，以及 Actor 消息表、统一生成契约、SLG Handler/网格关键路径的 race 检查。真实外部依赖测试未显式启用，跳过不算通过；未运行真实集群、滚动发布或云端吞吐回测。

## 同实例 Actor 投递（2026-10-21）

- Call/CallFuture 沿用本地邮箱路径；Tell/TellAccepted 同 Runtime InstanceID 也直接入邮箱，跨实例仍走 Core NATS。保留消息副本、准入、容量、顺序与排水，不新增队列或执行线程。
- TellAccepted 同实例确认邮箱接纳，跨实例确认 NATS 到达；两者均不确认业务执行或持久化。可靠任务仍先发布 JetStream，消费后才调用 Actor。
- 本地回归覆盖冷启动不阻塞、Tell 后 Call 顺序、邮箱满、排水关闸、消息副本及异步错误；真实 NATS/etcd 与 JetStream 闭环用于验证跨实例路径和任务完成边界。

验收结果：tx、txgame/server、txgameslg/server 全包本地回归通过；显式启用本机 NATS/etcd 的通信与 JetStream 任务闭环通过，actorrpc、actorhost、taskqueue 的 race 检查通过。其他未启用的外部依赖用例跳过不算通过；本轮未运行云端或吞吐回测。

### 子服务生命周期第 1 批（2026-10-27）

已接入必需 Init、可选 Start 和 ServiceStatus；World 在 Start 中完成指定加载，移除 started/AfterStart。Init 全部成功、通信就绪后才执行 Start，全部启动钩子成功后统一开放业务。保留现有 Actor/任务排水顺序，启动失败回收资源并保留错误。Actor 控制入口与跨服务排行榜读取已整理，见 [生命周期边界](service-actor-lifecycle.md)。

本批本地验收：tx、txgame/server、txgameslg/server 全量 `go test ./... -count=1` 通过；子服务状态、启动中业务拒绝、Actor Post/After、排水超时的定向 race 通过。正式双 Runtime 的本地/NATS 启动准入通过；SLG 指定 World 的 GDWorld/Ready/owner 验收通过。最终独立检查发现的 Post/After 与 Sched HTTP 启动闸门遗漏已修复，定时消息等待就绪，任务消费在业务开放后启动。未运行云压测或完整滚动发布。

Claims JSON 缺口已修复：可靠任务信封独立进行标准 Base64 编解码，原始二进制字节完整恢复，业务 string 接口和 Actor protobuf 不变。往返测试覆盖二进制、文本、空 Claims、无身份及损坏编码；真实 `TestJetStreamFormalActorTaskLoop` 已通过持久投递、消费进程退出续领、业务完成确认、死信转存与排水重试。本次 tx 全量 `go test ./... -count=1` 和 taskqueue/protocol 定向 race 通过；仅修改任务序列化与相关测试、文档，未修改两组业务工程。Actor 控制及等待语义进度见下。

## Actor 控制与等待入口（2026-10-27）

Runtime 组合 Messenger 与 Controller；WaitReady 复用 Probe 做低频只读等待，不自动激活/重试，不改变普通消息路径。World 启动使用 EnsureActivation + WaitReady，随后再次核对本机归属。RetryActivation 如实说明无失败时等同 EnsureActivation；ctx.Deactivate 只请求卸载，不同步等待清理。外部卸载及代次控制未纳入本批，Redis/Fusion 路径保持不变。

验证完成：tx、txgame/server、txgameslg/server 的 `go test ./... -count=1` 均通过；启用本地 NATS/etcd 的定向 race 通过，覆盖等待成功、终态失败、取消后继续观测、初始化/清理中重试不重建、旧 Context 重复卸载隔离、同实例与 NATS 跨实例控制，以及正式服务启动准入。未运行云集群或吞吐压测，普通全量中的其他外部依赖验收未单独启用。

本轮独立只读复核无可操作问题。保留固定 20ms 的低频状态轮询，不新增配置；Ready 与 owner 是独立瞬时观测，未承诺激活代次锁定或远程卸载。跨服务排行榜查询及 Scope 收敛进度见下。

## 跨服务榜单读取与 Scope 收敛（历史验收，2026-10-27）

以下记录该批验证；当前模块获取已改为项目进程内单例及启动资源绑定，见两组工程 docs/plans/module-scope.md。

两组工程的 RankService 查询使用本次调用上下文，复用 AppID 与既有 Redis/Mongo 能力；资料订阅由生产服务显式启停。tx 仅补充 AppID 只读访问，不注册业务模块。角色/世界缓存、Sender 等有效宿主资源继续保留 Scope；SLG 事务提交后的资料发布保持不变；txgame 合服屏障已随合服执行示例撤下。

已启用本机真实依赖验证 World 独立启动及 Game/World 组合启动的榜单读取；两组真实 Redis 榜单、资料更新、重复启动/关闭隔离及调用方 AppID 隔离通过。tx、txgame/server、txgameslg/server 全量 `go test ./... -count=1` 通过；运行时访问、资料更新队列、模块隔离和真实 World 读取的定向 `-race` 通过，独立只读复核无可操作问题。没有运行云压测、打包集群或滚动发布；普通全量中未显式启用的其他外部依赖用例不计为通过。

模块入口进一步收敛：两组示例的 `RankService()` 与 `LegionService()` 返回无状态单例，Rank Module 仅构造一次；资料订阅单独持有生产宿主资源。Handler 保留 MessageContext；模块方法按 I/O、事务或宿主定位需要传 ctx，纯计算与无状态获取不加参数。两组 server 全量回归、真实 Redis/World 查询及 Rank 定向 race 通过，独立复核无可操作问题；本次 tx 无代码变更。

## 正式包生命周期验收（2026-10-27）

两组工程均使用正式打包入口，将包解压到源码目录外、删除原构建目录，从独立 cwd 启动业务进程。

- [txgame Role / Room](../../txgame/docs/plans/actor-package-acceptance.md)：Role 空闲卸载后重新加载并保留背包；Room 实际运行 60 秒，最终通知后卸载；五个业务进程正常排水退出。
- [txgameslg World](../../txgameslg/docs/plans/world-startup-acceptance.md)：指定世界 Ready、重复 owner 拒绝、generation 1→2 受控交接、两条行军路径及原 GDWorld 保留均通过。

该验收批只增加示例部署验收与文档，没有修改 tx 运行时代码、生成协议或业务路径。测试证明有限请求下的生命周期正确性；没有重跑吞吐基准、云容量或持续流量滚动发布。其后的提交前复核修复见下；外部卸载等扩展仍需独立需求，不自动进入实施。

## 提交前复核修复（2026-10-27）

- MsgQ 的 Bee/Ant 装配接入统一启动准入，本地与 gRPC 入口在启动完成前拒绝业务；首次开放后保留原邮箱排水语义，不持有长流 Ingress 计数。
- SLG World 验收逐进程限时清理，超时强制回收并等待退出；原超时保留为验收失败，不伪装成正常排水。
- txgame 房间身份统一由项目 `services/internal/roomidentity` 创建和解析，房间模块继续负责持续时间和过期判定。
- 更新 README 的上下文存储访问、Init 装配及统一 cmd 调用示例，收敛失效的独立 Room RPC 接入说明。

本轮先复现启动期间接受 MsgQ 请求及 SIGTERM 超时后残留进程，再修复并验证。tx、txgame/server、txgameslg/server 全量 `go test ./... -count=1` 通过；启动准入、启动失败清理与进程回收定向 `-race` 通过。实际运行了回环 gRPC 和忽略 SIGTERM 的本地子进程；未重新启用真实 Redis/Mongo 集群验收或云压测，依赖开关未开启的用例仍为跳过。

## 项目模块统一访问（历史方案，2026-10-27）

两组工程以 Modules/Access 统一启动构造、Handler 内嵌和 Service 互调；不再通过 context 定位模块，不增加请求包装。I/O、事务与 Actor 上下文保持原契约。txgen 保留业务调用方法名，仅在生成 GoDoc 标明 Call/Tell。当前方案与本轮回归记录见 [txgame 模块装配](../../txgame/docs/plans/module-scope.md) 和 [SLG 模块装配](../../txgameslg/docs/plans/module-scope.md)。本轮 tx 仅同步文档，没有改动框架 API 或运行时代码。


## 进程模块与启动绑定（2026-10-27，本地回归完成）

- Context.Runtime 提供启动绑定 Provider：Setup 访问进程已启用能力，服务绑定保留 Requires，关闭后失效。
- 项目统一 GD/Redis/Actor 资源绑定，模块进程内单例；Handler 匿名内嵌不再注入 Access。
- txgen 生成绑定式 data.Access 与 cmd.Actors，原显式 ctx 接口保留给内部协调、事务和工具；普通调用不重投。
- SLG Procedure/缓存/WriteBack、Actor 激活与排水保持既有职责。
- 本轮 tx、txgen、两组 server 与两组 gen 全量测试通过；tx 启动能力绑定及两组模块/存储/调用入口定向 race 通过。
- 已显式启用本机 Redis 验证两组榜单；World-only 与 Game/World 组合启动读取榜单均实际通过；txgame Fightroom 同进程与独立子进程调用、通知和排水通过。
- 独立跨项目只读复核及工程后置检查无待修问题。未运行云压测、完整滚动发布或部署打包；本轮未提交。


## Procedure 新根创建

本轮补齐 `db.Create(ctx, value)` 的受管事务路径：动态锁、savepoint 回滚、缓存创建占位、无 setter 的首次条件写入和失败保留。复用既有 Store 元数据，不改生成 API，不改两阶段建角或 World 启动回填。实现与边界见 [单机事务](design.md#事务内创建与首次写回)。本地验收覆盖真实 Redis/Mongo/Fusion 条件创建及两组示例回归；不以此声明分布式数据库事务或宕机恢复能力。

### EntityCache 使用期保护

通用 EntityCache 通过 Use 回调覆盖取得对象、等待 Procedure、执行及提交/回滚回调，Role 等共享实体接入；WorldBucket 已在独占运行时批次退出该路径。缓存回收在无使用者时才调用 WriteBack.TryRelease；释放中禁止同键取得旧对象，事务内冲突明确返回 ErrReleasing，避免持锁循环等待。当前生成完整查询已经统一接入应用共享 EntityCache；对象使用期与 Actor 生命周期保持独立，具体契约和最新验收见统一 GD 实体访问计划。

本地验收：tx、txgameslg/server 全量 Go 回归通过；EntityCache、WriteBack 与 slggame/slgworld 针对性 race 通过；本机真实依赖下 Role 周期写回/停服 Flush 和 World Mongo 写回通过。默认关闭空闲过期的缓存命中基准为约 64～67ns/op、0 B/op、0 allocs/op（Intel i9-9880H、Go 1.26.5），旧裸读取约 19～20ns/op；该结果仅说明使用权保护的本机开销，不代表集群吞吐验收。
