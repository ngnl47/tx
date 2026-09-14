# Actor 归属接线实施计划

**2026-09-13 最新决策：** 用户已接受 I2 的临时业务预算，允许进入 I3 正式接线。原 10% 比较线保留为迁移偏移的排查提示，不再单独阻止本次接线；不等同于生产验收放行。优先保持去掉 ProtoActor 后的上下行完成链路性能，不重复全 NATS 上行测试。固定基线见 [迁移前基线](../../audits/baselines/2026-09-13-uplink.json)。I3 先完成交接准备与 Stop 共享收尾，再迁入实体宿主及适配，最终以相同业务负载复测。下文 I2 未放行表述保留其评审时点，以下最新决策为准。

> 使用 superpowers:executing-plans 串行执行，不使用子代理，不提交 Git。设计已提交到 `8de6005`，用户确认继续后完成 I1 与 I2；I3 已获准并开始实施，I4 随正式接线验收推进。

**目标：** 先补真实持久化与实例生命周期证据，再按既定评审门槛迁入正式组件，保持两种传输共用实体与归属。

**架构：** 复用 Discovery、SlotStore CAS、SlotResolver、Procedure/writeback 和框架生命周期。试验区承担缺口验证；正式接线由 framework 组装，game 保留业务 factory 和必要高级 SPI。

**技术栈：** 当前 go.mod 固定的 Go、NATS 客户端、etcd、gRPC 及现有存储适配；不升级依赖，不引入 Dapr/GoAkt 集群组件。

**设计：** [Actor 归属组件正式接线设计](../specs/2026-09-13-actor-ownership-design.md)。原始试验门槛与 T4–T6 范围仍见 [总试验计划](2026-09-12-nats-actor-runtime-trial.md)。

## 执行约束

- 每个节点先检查六工程状态、相关 AGENTS 与实际配置；保留未提交变更，交付累计提交建议。
- 不逐消息查 etcd，不在高频入口自动分配、回退传输或重投业务；失败不宣称未执行。
- 上行固定直连 gRPC，Actor 间 RPC 使用 NATS；用户因转发流量成本排除上行也经 NATS，后续测试仅保留既有 ProtoActor 基线与 hybrid 候选，已采集的全 NATS 数据作为历史对照。
- 先全实例收尾，再逐槽晋升；失败保留资源并有限重试，异常实例处置交给运维。
- 沿用各工程测试资源 ownership 和清理约束；两示例真实集群串行。采集及故障注入只在测试夹具或临时覆盖构建。
- 每节点完成相应验证后执行一次 ponytail-review；不为测试计数、删行数或小幅波动追加工作。

## I1：真实持久化与租约组合验证（已完成）

范围仅 tx 试验与测试。已有 `slots_test.go` 的 Redis 自定义快照、`routing_test.go` 的手工成员目录继续作为定向测试，不把它们改名成真实 GD 验收。

实际文件：新增 `internal/actortrial/ownership_integration_test.go`，从 `slots_test.go` 提取共享 registry 夹具；未发现需要修改 slots.go 或 routing.go 的缺陷。参考 `modules/discovery/runtime.go`、`modules/writeback/manager.go`、`tests/writeback_manager_test.go`。本节点不迁移 `modules/actor` 或两个示例。

接口（I3b 更新）：组合根先用 `Runtime.BindSlots(instanceID, discoveryRuntime.AuthoritativeSlots(), slotID)` 绑定实体加载校验，再开放直连或 NATS 入口；`ConnectSlotHost` 只接入连接与最终 `Flush(context.Context) error`。成员与槽位缓存仍来自 `discovery.NewRuntime`、`Watch`、`WaitReady`、`Directory`、`Slots`。`AuthoritativeSlots` 复用同一个 Runtime 的持久存储连接，普通 `Slots` 不改变缓存读取契约。

- [x] 写组合测试：A 的双入口访问同一实体，事务登记脏根；已接受事务结束前不能 ACK/交接，首次写回失败不晋升，重试成功后 B 加载最后状态。使用实际 txn/writeback/存储适配，不以 `Redis.Set(JSON)` 代替管道。
- [x] 在 tx 内使用最小 Store/Procedure 契约夹具验证管道；涉及生成 GD 的完整契约留在 I4 的独立示例验收，报告明确区分，未 import 示例 internal 包。
- [x] 用真实 Discovery.Runtime 取代组合夹具的手工实例 ID/目录：快照就绪前不发布 running，B 加入不改已存在 assignment；模拟旧端点抵达新实例时拒绝旧身份。租约失效后源方停止接纳新业务，不据此自动晋升。
- [x] 注入第一次写回失败及交接方取消等待，使源方成功结果不再被交接方接收；检查待刷根、writer、有效 context 和控制连接仍可用；成功阶段重试不重复执行退出回调。租约失败与正常排水失败分开断言。
- [x] 运行测试，只补测试组合与共享夹具，复用现有同步边界；正式 API/关闭语义的接线仍交给 I3，不越过 T4 门槛。
- [x] 运行相关基线、tx 全量、显式启用的真实依赖试验包及定向 race；输出 pass/skip/fail、依赖版本和清理记录，详见 [本轮报告](../../audits/2026-09-13-actor-ownership-integration.md)。

命令入口（在 tx 下执行；真实端点从当前环境核对后设置，未设置时产生的 skip 不算通过）：

```bash
go test ./internal/actortrial ./internal/framework ./modules/discovery ./component ./tests -count=1
go test ./internal/actortrial -run 'TestOwnershipIntegration|TestSlotRollingHandoff|TestSharedRouting' -count=1 -v
go test -race ./internal/actortrial -run 'TestOwnershipIntegration|TestSlotRollingHandoff|TestSharedRouting' -count=1
```

`TestOwnershipIntegration` 是本节点新增测试前缀。实际使用现有开关 `TX_ACTORTRIAL_NATS_URL`、`TX_ACTORTRIAL_ETCD_ENDPOINT`、`TX_ACTORTRIAL_REDIS_ADDR`，未新增测试配置开关。

完成标准：真实租约与实际事务持久化组合成立，明确哪些仍是 Store 夹具而非示例生成 GD；失败不能通过关闭依赖或重新选 owner 掩盖。此节点不宣称正式 framework 已接线。

## I2：完整上行与业务负载采用评审（已完成，临时预算已接受）

文件：复用 `internal/actortrial/benchmark_test.go`、`routing_test.go` 与现有隔离测量驱动，新增结果报告到 `docs/audits/`；只按测量需要增加测试夹具，不扩展生产统计。

- [x] 固定载荷、身份数、多 Session 分布、业务处理与持久化完成口径；已对照当前 ProtoActor、Agent 直连 gRPC + Actor NATS、上行也经 NATS。同环境比较成功完成吞吐、固定到达率 P99、错误、CPU、内存和网络字节。完成后用户明确排除第三组，后续不再运行该组。
- [x] 代表负载含 Procedure/GD，不能只用空回显或单向 Pub/Sub；计数/profile 单独构建，禁止混入吞吐样本。
- [x] 列出调用端、业务实例与 broker 每段字节量，按实际部署位置另算计费流量；没有云区域和同区/跨区拓扑时仅给公式及情景，不填写虚构账单。
- [x] 用结果评审 T4。原 10% 筛选线继续有效；若建议改为明确业务容量/P99/成本门槛，给出数字与余量后由用户确认，不把已确认传输方向解释为自动通过性能验收。

完成标准：给出可采用或应保留现状的证据与门槛决策；不继续收益很小的封包或发送协程微优化。

本轮新增隔离的完整 TCP/Session/MsgQ/生成 GD 对照夹具，修复原型两种 Call 信封缺少非零标记及 Tell 沿用旧标记的问题，见 [I2 报告](../../audits/2026-09-13-uplink-business-comparison.md)。60 个正式窗口和独立字节诊断已完成；不将同相位探索批算入最终结果。1 KiB 中位完成吞吐为 ProtoActor 5661/s、hybrid 4642/s、NATS 4264/s，hybrid 未证明满足原 10% 筛选线。1000/s 到达全部完成，3000/s 仍有驱动未发送。后端连接字节 hybrid 比全 NATS 低约 14.6%（反向计算为 NATS 高 17.1%），broker 收发量低约 96.2%。

用户已接受报告中的“单组按 1000 请求/s 投递且全部完成、计划到达 P99 ≤100ms、零错误/未发送、最终 GD 一致”的临时预算进入 I3；这不是生产 SLA。正式 ProtoActor 在必要能力承接之前保留，不继续发送器微优化。

## I3：正式组件与框架接线（临时预算已接受，实施中）

主要文件：`modules/actor/{runtime,kind,delivery}.go` 及迁入的运行时/适配文件，`internal/framework/{assembly,cmod_actor,cmod_discovery,cmod_rpc,context_access,shutdown,drain}.go`，`modules/control/lifecycle.go`、`modules/control/proto/control.proto`；生成物经原生成入口更新。

- [x] I3c：核对 PID、Props、Context、ActorSystem 实际调用清单，记录 Identity、Kind factory、生命周期和高级能力的承接边界，补齐原型的可选空闲卸载。见 [API 核对记录](../../audits/2026-09-13-actor-api-migration-inventory.md)。这是迁入清单完成，不表示所有正式接口已替换；必要能力无替代时不删除 SPI。
- [x] I3b：Discovery.Runtime 提供 AuthoritativeSlots；真实归属集成夹具从自身 Runtime 取得权威存储，加载前只注入 SlotReader。普通 CModSlots/SlotResolver 保持缓存读取，不暴露 etcd 客户端或再创建连接。正式 framework 实体宿主的消费接线仍属下一条。
- [x] I3d：按用户决策将集群归属集中到 Discovery L2：SlotOwnership 负责首次分配与正常交接，CheckSlotOwner 统一权威校验；SlotAllocator 仅绑定 Actor 身份、NATS 执行入口与激活。现有 SlotResolver 缓存和低层 CAS SPI 保留。正式 framework 控制面与真实 etcd 的连续两槽交接已加入回归，见 [L2 基座记录](../../audits/2026-09-13-discovery-ownership-foundation.md)。
- [x] I3e：Discovery Runtime 装配并持有归属组件，Actor 装配接收 Ownership 而非存储/目录；control.NewHandoffClient 统一请求构造和响应身份校验，CLI 与正式交接集成复用。普通 game Context 不增加底层装配入口，见 [封装记录](../../audits/2026-09-13-ownership-composition.md)。
- [x] I3f：本地激活、邮箱、容量、排水与生命周期迁到私有 `internal/actorhost`；试验传输通过别名复用唯一实现，框架后续无需依赖 actortrial。保留既有试验 Placement 供历史对照，正式稳定归属仍使用 L2 SlotOwnership。未切换默认框架运行时，见 [宿主提取记录](../../audits/2026-09-13-local-actor-host.md)。
- [x] I3g：直连 Call/Receiver 和共用信封校验迁入私有 `internal/actorrpc`，复用 RPC Sender/Host 与 actorhost；以真实 gRPC 验证多个 Kind 共用一个 DeliveryService、相同 key 状态隔离及统一排水。正式 Kind 工厂仍返回 ProtoActor Actor，尚未切换默认运行时，见 [直连迁入记录](../../audits/2026-09-13-private-actor-rpc.md)。
- [x] I3h：普通 Kind 声明与 ProtoActor 后端对象创建分离，Runtime 统一装配 Props、生命周期包装和集群 Kind；保留原生高级适配、固定实体及选项优先级，验证集群初始化只发生一次。此节点未替换业务工厂的原生 Actor 返回类型，见 [Kind 装配记录](../../audits/2026-09-13-kind-backend-assembly.md)。
- [x] I3i：两个独立示例的命令执行改为标准 Context + Delivery → Result/error；ProtoActor Receive 仅做信封适配与回包。保留下行发布成功后才返回 ProcessedUpSeq 的边界，未替换公开 Kind factory 或默认宿主，见 [命令执行解耦记录](../../audits/2026-09-13-command-execution-boundary.md)。
- [x] I3j：公开 Behavior/BehaviorFactory、Context、Identity 和只读 LocalFuture，复用唯一 actorhost 实现；直连测试使用公开行为契约，验证回调期限与实体句柄寿命分离。旧 Kind 工厂未切换，不提供无法执行的新 Kind，见 [行为契约记录](../../audits/2026-09-13-public-actor-behavior.md)。
- [x] I3k：用户确认普通 Kind 的 v1 固定 1024 分区；唯一布局函数已被路由、首次分配和加载校验复用，真实依赖测试覆盖同分区的实体隔离和扩容不改派。世界保留独立槽位，并明确后续解除单实例单世界限制。详见 [Kind 切换方案](2026-09-13-actor-kind-cutover.md) 与 [布局验证](../../audits/2026-09-13-actor-partition-layout.md)。正式宿主接线尚未完成。
- [x] I3l：保留稳定分服/分战区 SlotMapping SPI，默认 1024 分区与自定义映射共用唯一解析入口；真实用例验证同战区共用归属、实体状态独立及缺失分组不回退。正式 Kind 声明装配仍待迁入，见 [稳定映射记录](../../audits/2026-09-13-stable-slot-mapping.md)。
- [x] I3m：Core NATS 传输与缓存路由迁入私有 actorrpc，复用唯一宿主；试验包全部转为测试专用，保留基线与 overlay。新传输仅暴露 Flush/Close，排水和写回由框架编排；槽位试验控制面未冒充正式控制面。见 [NATS 迁入记录](../../audits/2026-09-13-private-nats-transport.md)。默认 framework 尚未切换。
本轮排水接线核验（起点 `e26df80`）：私有宿主新增同步 `BeginDrain`，复用原有入队锁和邮箱关闭，`Drain` 继续等待同一 worker 屏障。验证同步关闸后拒绝已有/未加载实体及本地 Post，已接受消息、回包和卸载清理继续完成；取消等待后可重试排水。真实 gRPC 入口在关闸后拒绝调用；真实 NATS 验证已接受请求仍能回包，OnDeactivateBefore 仍能出站 RPC，目标不存在时错误日志不阻止 OnDeactivateAfter。tx 全量回归通过，actorhost/actorrpc 的 race 回归通过（设置本地 NATS 地址，未跳过对应测试）。未运行 benchmark，消息热路径未改动，不作性能收益声明。后置 ponytail-review 无需删除项；正式框架仍未切换，下面接线项保持未完成。

首次分配接线进展：`internal/actorrpc.EntityRouter` 已承接容量预检 → L2 Ownership.Ensure/CAS → 激活状态返回，复用现有 NATS 元数据控制入口，不迁入试验专属 slot-control 订阅或 quiesce 编排。普通 RoutedCaller 与首次分配共用 EntityRouter 的映射，尚未由正式 Kind 自动绑定。真实 etcd/NATS 用例已改用新适配，验证默认分区、自定义战区、同槽独立实体、新成员不改派、容量拒绝不落归属/不加载，以及缺失分组不回退；宿主预检不预留额度，激活仍执行原子容量预留。tx 全量回归及相关 race 通过，未运行 benchmark。后置 ponytail-review 无新增冗余项，测试基线保留。

首次业务调用闭环：EntityRouter.Resolve 优先读缓存，仅 ErrSlotNotFound 进入权威分配/激活；本次使用权威结果转发，不等待 Watch 回显或向缓存写入第二份归属。快照不可用、已有 owner 非 running 或业务 RPC 失败时只返回错误，不分配替代者、不重投。真实用例覆盖冷启动 gRPC 调用、后续缓存路由与缓存失效拒绝；默认布局和业务战区映射均通过。全量与相关 race 验证通过，未运行 benchmark，不声称性能收益。正式 Kind/宿主切换仍待实施。

正式框架交接组合验证：新增 TestActorHostPrepareHandoffRetainsTransportUntilStop，以测试服务挂接新 actorhost 与 actorrpc，使用真实 NATS 和共享 gRPC 验证已接受业务/卸载清理先完成、最终写回失败保留连接、同源重试成功后不重复写回、Stop 才释放传输。tx 全量通过，新增用例 race 连续三次通过。测试仅隔离注入源启动身份，真实租约仍由既有 etcd 交接测试覆盖；未切换默认 CModActor，当时尚未自动执行 NATS Flush（已由下述收尾阶段接线补齐）。本轮仅新增集成验证及计划说明，没有新增生产抽象。后置 ponytail-review 无需删除项，未运行 benchmark。

传输收尾接线：framework 私有持有实体传输 Flush/Close 能力，finishWork 在已接受工作和 CMod 最终写回成功后执行 Flush；失败沿用 errStopDeferred 保留资源，PrepareHandoff 不确认交接，Stop 不释放连接。component.Host 保留成功写回进度，重试不会重新写回；shutdown 在业务服务停止后统一 Close。集成测试已移除测试服务代办 Close/显式 Flush，注入两次 Flush 失败验证 PrepareHandoff 与 Stop 均保留资源，同一真实连接恢复后成功。默认 CModActor 尚未创建并绑定该传输，不能宣称正式后端切换完成。相关框架 race 通过；无新增公共 SPI、后台采集或性能声明。

应用级规格接线：用户确认 Kind 在应用全部注册中唯一，多个 Kind 可共同指向同一服务类型（world、seasonWorld → slgworld）。framework 注册目录保留完整 Kind 声明及服务归属，并在构造时拒绝重名；启动阶段只把选中服务的 Kind 交给本地后端。测试验证 Agent 单独启动可保留远端规格，且不启动远端服务/Actor 工厂。tx、txgame/server、txgameslg/server 全量测试命令通过；修改集中于注册装配，未运行 benchmark。后置 ponytail-review 无新增可删除项。正式 Kind/新宿主仍未自动消费该目录，下一步绑定统一规格，而非要求业务重复声明映射。

新宿主排水所有权接线（起点 `79d10ef`）：framework 私有持有 actorhost.Runtime，Server.Drain 在服务排水前同步关闭实体准入，并在最终写回前等待宿主已接受工作及卸载回调。集成测试已删除测试服务代办宿主 Drain 的逻辑，增加取消等待仍保留业务资源、禁止提前写回和后续重试的断言；真实 NATS 用例 race 连续三次通过。默认 CModActor 尚未创建该宿主，当前只完成框架收尾消费能力，Kind/目录自动装配仍未完成。没有新增业务侧 SPI、后台采集或消息热路径修改；未运行 benchmark。

四种调用的寻址接线：私有 NATSMessenger 将 Tell、TellAccepted、CallFuture、Call 绑定到同一 EntityRouter 和 NATS 连接，业务身份不再需要预先转成物理 Target；直连上行保留仅 Call 的 RoutedCaller。寻址失败直接返回，传输失败不重寻址、不重投。首次寻址可能涉及容量预检、归属分配和激活控制，CallFuture 在寻址完成后返回 Future；不能将冷启动控制开销解释为零等待。真实默认分区与战区映射用例验证四种调用和 gRPC 共用实体状态，race 连续三次通过。未新增订阅、调度 goroutine 或后台采集，未运行 benchmark；公开 Messenger、Kind 工厂与默认 CModActor 的切换仍未完成。

共享 gRPC 注册核对：Agent 注册 ControlService，MsgQ 注册 MailboxService，与 Actor DeliveryService 名称不同；无需增加通用多路分派层。Actor 的多个 Kind 由唯一宿主接收，DeliveryService 仍须由装配层统一注册一次。

- [ ] 将实体宿主迁入正式 framework（I3b 已把原型的加载校验移到 Runtime.BindSlots，独立直连测试无需先创建 NATS）；复用共享 RPC Host 注册 Actor Call，检查同进程所选服务的注册组合，确保 DeliveryService 仅注册一次；两种入口仍共享同一 Runtime。
- [ ] 调整启动顺序：绑定直连端点 → Discovery 启动身份与 Watch 就绪 → 装配 Actor 入口 → 服务与控制面就绪 → 标记 running。移除对旧 ActorEndpoint 的启动依赖，不增加绕环的可空中间对象供业务处理。
- [x] I3a：先写框架生命周期测试，实现正式 PrepareHandoff 与 Stop 共享收尾进度；共享 RPC 和租约保留到 Stop。保留 CMod 逆序收尾、失败阶段重试、fatal 身份丢失分支；StateDrained 不冒充持久化成功。NATS 宿主尚未迁入，待后续绑定同一流程。
- [x] 验证 PrepareHandoff/Stop 并发、持久化失败重试、旧启动身份、成功回复丢失，以及多个槽位部分晋升；业务退出 RPC 失败不能否决卸载。当前测试与故障注入边界见 [交接可靠性证据](../../audits/2026-09-14-handoff-reliability.md)。
- [ ] 核对 world 子协程完成边界：若处理异步返回，实际写入任务必须可被生命周期等待；在此之前不能把 Receive 返回等同于整个业务完成。
- [ ] 运行相关单测、真实依赖回归与必要 race，再更新 README 的正式 API 和 design.md；不在本文设计完成时提前改写公开 API 文档。

完成标准：正式宿主取得框架所有权，原型连接内不再暗藏共享准入；维护成本可解释，无第二份归属、写回管理器或永久后端开关。

I3a 实施起点 `fe8656f`，六工程干净；本节点只改 tx，详见 [正式交接准备与基线记录](../../audits/2026-09-13-framework-handoff-preparation.md)。I3b 从 `f082cea` 继续，已完成权威访问与传输无关的加载绑定，见 [接线记录](../../audits/2026-09-13-actor-ownership-binding.md)。下一节点继续正式实体宿主与高级 API/SPI 的迁入；不重新请求临时预算授权，也不把 I3a 完成等同于已移除 ProtoActor。

## I4：两个独立示例与依赖收敛

多世界准备已推进：txgameslg 去除服务工厂和 Handler 的单世界 logical-slot 绑定，复用逐世界归属校验；一个实例可处理多个已分配世界。单实例双世界真实 Mongo 写入与版本保护回归通过，详见 [Kind 切换方案](2026-09-13-actor-kind-cutover.md)。这不代表 I4 或默认 NATS 后端切换完成，逐世界容量准入和新宿主自动装配仍待接线。

修改 txgame/server 的 `services/{game,agent,internal/actorcmd}`、txgameslg/server 的 `services/{slggame,slgworld,agent,internal/actorcmd}` 及两个工程已有 `internal/writeback` 接线；配置、go.mod、README 与集群测试按实际依赖调整。

- [ ] 普通实体稳定分区先确定编码与数量；世界维持独立槽位，演示多个世界分布在独立 slgworld 实例池，新增实例不自动搬走已存在归属。
- [ ] 先 txgame 角色多 Session，再 txgameslg 世界与跨世界侦查；factory 保持业务加载与生命周期声明，通用装配由 tx 提供。
- [ ] 沿用真实生成 GD 与默认写回组件，验证 Redis/Mongo/Fusion 各自既定持久化边界、失败收尾和有限关闭重试；不修改客户端协议，不将 Fusion 改为同步 Mongo 落盘。
- [ ] 完成 tx、两个 server 的 `go test ./... -count=1`，两客户端的 `npm test`、`npm run build` 及各自 README 指定的真实集群验收；报告三仓版本矩阵。
- [ ] 仅在必要 SPI 和行为有承接、原型对照证据可复现后删除被替代 ProtoActor 生产路径与依赖；若测试仍依赖它，先明确测试基线的保存方式，不宣称已完全移除。

此后才按总试验计划进入 JetStream。etcd 替代、MsgQ 传输整合、运行中单实体热迁移均不随本计划实施。

## 完成态上下行直连验收约定

- Agent 分别通过 gRPC 直达 Game、SLGWorld、FightRoom 等命令所属服务；处理完成 Seq 使用同一次 Call 返回。
- 各业务服务通过 gRPC 直接向 MsgQ 发布下行；Agent 通过会话链路协调 Fetch/Ack 并发送客户端。上下行均不经过 NATS，不强制 Game 中转。
- I4 除角色上行外，须验证扩展服务直连上行与直接下行、多 Session 扇出、可信身份和 Seq/Ack 契约；FightRoom 可先用集成夹具验证，不因拓扑示意额外实现整套战斗业务。
- Core NATS 只承接实体间普通 RPC，JetStream 承接可靠业务任务；任务产生的客户端通知仍走直连下行。图与 design 只表达完成态，本计划保留实现进度与性能验收。

## 框架宿主启动与交接闭环补充

已将新宿主双入口注册与连接所有权移入框架私有装配，并连续补齐启动失败保留资源、Stop/Run 收尾重试、无 deadline 的有限 NATS Flush。真实两个 Server 的 etcd/NATS/gRPC 回归覆盖共享实体状态、源卸载、逐槽晋升、新 owner 懒加载与租约最后注销；不等于公开 Kind 或默认后端已替换。细节见 [Kind 切换方案](2026-09-13-actor-kind-cutover.md)。

## 最新进展：默认后端切换（未提交）

统一 Kind、私有 actorCatalog、L2 稳定归属、本地宿主及 gRPC/NATS 已成为 CModActor 默认实现。两个示例、配置、README 和现有集群验收同步迁移；正式 Bee/Ant 不再导入 ProtoActor，旧后端仅在隔离测试中保留。

三仓全量、相关 race、两客户端测试/build 及真实集群通过；默认后端的 Ping/Pong 单轮同机对照通过。I3/I4 不整体勾完：冻结 I2 在正式入口的固定到达率/GD 回读验收、逐世界容量声明及 world 子协程完成边界仍待完成。详见 [本轮记录](../../audits/2026-09-13-default-actor-cutover.md)。下一项先完成正式入口性能验收，不进入 JetStream。

## 2026-09-14：正式入口 I2 验收完成

默认后端迁移已提交（tx eef61a5、txgame c305eab、txgameslg 2b99d1f）。正式入口五轮 1000/s 共完成 20,000 次，零错误/未发送，计划到达 P99 最差 38.59ms，生成 GD 回读通过。闭环吞吐较冻结 hybrid 下降 10.53%；同二进制三组补充诊断未稳定复现额外装配损失，不宣称无退化。冻结基线保持不变，详见 [正式入口验收](../../audits/2026-09-14-formal-actor-uplink-acceptance.md)。

I3/I4 不整体勾完；下一项为正式 Kind 容量声明、逐世界估算与状态探测 SPI，继续明确业务子协程实际完成边界。暂不进入 JetStream。本次仅修改隔离夹具和证据文档，未提交。

## 2026-09-14：容量与状态正式 API 已接线

复用宿主已有预算和状态机，增加 Kind 估算声明、实例预算与 CModActor 激活控制；Probe 只读权威解析，普通消息仍走缓存。详见 [Kind 进展](2026-09-13-actor-kind-cutover.md#2026-09-14正式容量与激活状态入口)。I3/I4 尚不整体完成：下一项先明确 txgameslg 世界 GD 缓存与 Actor 的独立资源生命周期，再接入世界预算，不提前宣布具备按世界内存调度的能力。

## 2026-09-14 决策修订：数量配额替代字节估算

以本节覆盖此前容量估算方案：删除本轮未提交的 ActivationBudget、WithActivationEstimate 及宿主字节预留。保留三个激活控制 API。容量配置统一为 Maximum / Scope / Initializations：activation 用于临时房间，assignment 用于一世界一稳定槽位的重型世界，多个本地 Kind 共用实例上限。

不预绑定 worldId、不恢复 logical-slot。默认 etcd 复用现有 assignment 与实例规格原子检查数量，不另建配额归属表；卸载不释放世界名额，正常晋升在排水前预检、写入时再次检查，迁出成功后才释放源名额。新增空闲实例可接纳新世界，全部满载报容量不足。普通分区不按槽位数量限制 Actor。

两个示例开放 YAML 数量配置，合并调试默认无限制；独立 slgworld 可配 assignment=1，FightRoom 可配 activation=1000。世界 GD 的独立缓存和直接 Mongo 写回保持原职责，不再以估算 GD 大小作为下一项。自动扩 Pod、世界注销控制、CPU/内存阈值及故障接管不随本次实现。完成项与验证见 [本轮记录](../../audits/2026-09-14-actor-activation-api.md)。

## 2026-09-14：数量配额真实集群与正常世界交接

基于已提交的 tx bbdd202、txgame 6ab2f20、txgameslg c3c76db 继续。SLG 套件通过独立 YAML 发布 assignment=1，首次客户端请求自动分配世界，满载返回 ResourceExhausted 且无归属副作用；保留故障恢复到第二代，再验证 PrepareHandoff 收尾确认、源租约保留、CAS 晋升第三代与客户端重载/Mongo 写回。两个真实集群、SLG server 全量与控制面 race 均通过；未改冻结压测负载。详见 [本轮验收](../../audits/2026-09-14-world-capacity-cluster.md)。

下一项集中核对 I3/I4 剩余交接可靠性证据（并发 Stop、确认回复丢失、多槽部分晋升），先复用现有回归，缺口再补。单世界正常交接不等同于多世界规模验收；不整体勾完 I3/I4，不开始自动扩 Pod 或 JetStream。本轮未提交。

## 2026-09-14：交接可靠性证据收敛

复用已有并发 Stop、写回重试与身份失效回归，增强真实 NATS/etcd 双实例测试中的成功确认丢失和多槽部分晋升，并补齐 L2 晋升已提交但结果未知的存储故障注入。相关四包完整功能与 race 通过，生产实现不变，未提交。详见 [证据记录](../../audits/2026-09-14-handoff-reliability.md)。下一项为 I4 多世界实例池及跨世界访问的正式示例验收，不整体宣称迁移完成。
