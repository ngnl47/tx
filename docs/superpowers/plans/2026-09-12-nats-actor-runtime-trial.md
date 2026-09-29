# NATS 与最小本地 Actor 运行时试验实施计划

> **2026-09-29 收尾状态：** 本分支功能迁移与异步流主线已收口，集群工具重构已提交。当前版本、验证范围与合并前限制以 [feature 收尾记录](../../audits/2026-09-29-feature-closeout.md) 为准。下文阶段性决策、待办与下一项保留历史时点，不作为当前执行队列；生产部署验收仍单独保留。

**2026-09-14 当前状态：默认 Core NATS 后端迁移范围已完成整体验证，T5 JetStream 内部消费适配已落地，框架配置与生命周期接线已落地，游戏示例默认启用尚未完成。历史测试依赖保留，自动 Pod 交接编排延期；以下历史记录以本次 [收口记录](../../audits/2026-09-14-nats-default-closeout.md) 为准。**

**2026-09-13 最新决策：** 用户已接受 I2 的临时业务预算，允许进入 I3 正式接线。原 10% 比较线保留为迁移偏移的排查提示，不再单独阻止本次接线；不等同于生产验收放行。优先保持去掉 ProtoActor 后的上下行完成链路性能，不重复全 NATS 上行测试。固定基线见 [迁移前基线](../../audits/baselines/2026-09-13-uplink.json)。I3 先完成交接准备与 Stop 共享收尾，再迁入实体宿主及适配，最终以相同业务负载复测。下文 I2 未放行表述保留其评审时点，以下最新决策为准。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. 串行执行，不使用子代理，不自动提交。

**Goal:** 在 feature 分支验证 NATS Core + 最小本地实体运行时能否替代 ProtoActor，并以 JetStream 衔接可靠任务；以正确性、维护复杂度和性能决定是否推广。

**Architecture:** 先隔离原型，再逐步接入两个独立 game 示例。本地实体管理负责身份、激活、邮箱及生命周期；Agent 上行直连 gRPC、Actor 间 RPC 使用 NATS，二者共用实体路由；JetStream 负责任务存储与消费进度；etcd 暂留实例身份、租约和槽位交接。上行也经 NATS 已因转发流量成本退出候选，后续不再测试该方案。原型可被否决，不预设必须迁移成功。

**Tech Stack:** 当前仓库锁定的 Go 工具链、NATS 官方 Go 客户端、nats-server、现有 etcd/Redis/Mongo；执行前固定版本和镜像摘要，不用浮动 latest 做结果依据。

**Spec:** [生命周期与可靠任务草案](../specs/2026-09-11-actor-lifecycle-reliable-task-design.md)。本计划补充“试验优先且不预设保留 ProtoActor”的最新方向；不代表已有运行时已变更。

## 当前状态与分支

T3a–T3g 及 T3h 设计已提交，本轮 I1 开始前 tx=8de6005，六工程工作区干净。tx、txgame、txgameslg 均位于 `feature/nats-actor-runtime`。本轮累计完成 I1 事务写回/真实租约组合及 I2 完整上行评审，修复试验区信封标记；正式运行时未修改，T4 正式迁移继续暂停。以下分支创建信息是准备阶段快照。

2026-09-12 检查：六工程均在主分支。tx/main=638d04a4525cfcec15327ac07bfdbaba4a2d59da；txgame/master=b611ebb80af3908d62660d9dd551f434265a5c12；txgameslg/main=0b465210ef878cbc39039a36a228bd7b1dfff085。tx 已有未跟踪的生命周期草案，本轮另更新总计划并新增本文；其余五工程干净。实施起点必须重新核对，以上仅为计划快照。

统一分支名：`feature/nats-actor-runtime`。

| 工程 | 是否先建分支 | 工作边界 |
|---|---|---|
| tx | 是，从当前 main | 原型、本地运行时、通信、JetStream、框架装配、文档及测试 |
| txgame | 是，从当前 master | 无状态角色示例、服务装配、真实集群回归 |
| txgameslg | 是，从当前 main | 世界固定归属、跨世界调用、写回及生命周期示例 |
| txgen | 否，按需 | 仅实际生成契约或模板依赖改变时加入；不优化生成器性能 |
| txclient | 否，按需 | 首先作为现有外部协议验收工具；协议或测试驱动必须变更时加入 |
| txclientslg | 否，按需 | 同上；生成链仍由 txgameslg/gen 管理 |

2026-09-12 已确认三个同名分支创建完成且开始时干净，基线与本批证据见 [试验记录](../../audits/2026-09-12-nats-actor-runtime-trial.md)。以下创建命令仅供尚未准备的其他工作区使用，不在当前分支重复执行。三个同名分支是独立 Git 仓库分支，不会自动固定跨仓库版本。开始时记录所有 HEAD、工作区差异、Go replace/模块解析路径和客户端版本；测试报告必须说明实际使用哪份 tx。已有未提交文档带入 tx 分支，不 reset、clean 或自动提交。只在分支不存在且工作区仍符合记录时执行：

```bash
git -C /Users/a47/workspace/tx switch -c feature/nats-actor-runtime
git -C /Users/a47/workspace/txgame switch -c feature/nats-actor-runtime
git -C /Users/a47/workspace/txgameslg switch -c feature/nats-actor-runtime
```

## 全局边界

- 用户已确认分支并授权实施；按阶段串行推进，示例/正式替换受 T3 评审门槛约束，不预先推广。
- 不保留历史 API 兼容负担，但保留实际需要的高级 SPI；不能把底层组装和空值检查重新推给 game。
- 五个事件名保持 OnInitAfter、OnActivateBefore、OnActivateAfter、OnDeactivateBefore、OnDeactivateAfter。卸载前 RPC 尽力执行，错误记 error 后继续卸载；否决在发起卸载前判断。回调有界且协作取消，不并发释放仍被回调使用的对象。
- RPC 超时不代表未执行，不自动重发。新原型 Tell 只确认本地，TellAccepted 只确认到达 NATS，CallFuture/Call 共用业务结果；历史 T2a 的目标取出确认在 T2c 删除。保留排水的真实工作计数及必要持久化安全收尾。
- 业务明确“无法处理并结束”可以 ACK；永久加载拒绝进入死信，临时加载失败重试。ACK 不等待 GD 刷盘；成功后结果/ACK 丢失只记录可观察错误，不建完成表。
- 默认并发消费不保证业务顺序；可保留产品有序入口，声明保证时必须限定范围和失败边界。
- etcd、GD、Procedure、Redis/Mongo/Fusion 和外部 BON/Frame/Seq/Ack 不随第一阶段替换。消除 etcd 是独立后续决策。
- 不预建通用 Actor 平台、复杂监督树、透明迁移、第二套归属控制面或永久双后端兼容层。
- 所有采集、故障注入与临时对照二进制隔离；失败证据保留。每批验证后执行一次 ponytail-review，仅明确大节点才执行 audit。

## T0：分支、事实清单与共同基线

涉及：三个仓库根、tx/modules/actor、tx/modules/discovery、tx/internal/framework、两示例服务及集群入口；新增证据记录 `tx/docs/audits/2026-09-12-nats-actor-runtime-trial.md`（开始采集时创建）。

- [x] 创建上述分支并保留未提交材料；记录模块解析实际路径，避免新示例误连主线 tx。
- [x] 从现行代码提取必须保持的行为：虚拟/固定寻址、并发激活、Tell/TellAccepted/Call、错误码、排水、实例身份和世界归属；标记目前尚不存在的生命周期能力。
- [x] 固定原实现源码快照或只读 checkout，先编译基线；原型与基线使用相同负载，不直接比较不同机器的历史报告。
- [x] 记录版本、硬件、GOMAXPROCS、消息大小、并发、目标分布、网络/TLS、采样时长与失败计数。NATS 数据目录使用独立临时目录，结束只清理本次拥有的进程和资源。

交付：可复现的对照起点及行为矩阵，不产生性能改善结论。

## T1：仅在 tx 内验证最小本地实体运行时

拟新增：`tx/internal/actortrial/runtime.go`、`runtime_test.go`；这些是试验文件，不是最终公共 API。参考 `modules/actor/{kind,mailbox,lifecycle,fixed_host,passivation}.go`；不复制第三方实现。

- [x] 先写关键状态测试：同身份并发首请求只创建一次、不同实体不互相串行阻塞、准入永久/暂时拒绝可区分、激活失败清理。
- [x] 实现限定的实体映射、有界队列与串行调度；明确锁与回调不互相等待，同步回调不得自调用等待同一邮箱。
- [x] 加入五个生命周期事件、关闸后拒绝、已准入请求排空、重复卸载合并、旧定时器隔离。卸载前错误不得取消卸载。
- [x] 用可控时间完成 60s 战斗结束、65s 卸载与迟到 Buff 的场景；不用真实睡眠替代时序控制。
- [x] 实施最小 panic 清理和等待者错误返回，不直接复制通用监督树；验证没有半激活实例、孤立等待者或明显 goroutine 泄漏。

验证入口：`go test ./internal/actortrial -count=1`，随后单独 `go test -race ./internal/actortrial -count=1`。预期相关状态断言全部通过；原 ProtoActor 的已知 race 不得拿来解释新运行时失败。

退出门槛：若为覆盖现行必需行为必须重建复杂 Actor 框架，则记录原因，转评保留本地 ProtoActor 的备选，不强行推进自建。

## T2：NATS Core RPC 与归属集成

2026-09-12：T2a 真实传输节点完成，T2 整体未完成。已固定 nats.go v1.53.1 与 nats-server 2.14.6，完成真实双进程、断连、响应清理及收尾顺序验证。固定槽位在试验夹具中复用现有 SlotResolver；虚拟角色的成员变化与旧 owner 卸载串联留在 T2b，不把显式实例寻址称为全局归属实现。详见试验记录的 T2a 节。

拟新增：`tx/internal/actortrial/nats.go`、`nats_test.go`；修改 tx/go.mod、go.sum 仅用于固定依赖。参考现有 `modules/actor/{delivery,future}.go` 和 discovery 接口。

- [x] 先验证真实 nats-server 下的双进程调用、目标不在线、连接中断、超时后业务仍可能完成、迟到响应清理。
- [x] 每运行实例设置入口 Subject，载荷携带稳定实体身份和命令；固定世界通过现有 SlotResolver 和加载前校验完成试验交接。
- [x] T2b 本地部分：不可变 Kind 成员快照选择 owner；快照更新与本地准入共用锁，失去归属即关闭旧激活，排空后清理，同进程重获归属不绕过清理占位。
- [x] T2b 决策已收敛：正常排水保持旧归属直至排空和必要写回完成；新增成员不能立即激活原有实体的新副本。采用稳定逻辑归属及显式交接，不推广按成员变化直接重算的 Placement。旧试验继续作为重叠风险证据，不作为新调度实现。
- [x] 初期取出确认验证已由 T2c 新语义取代：TellAccepted 改为服务端到达确认，Call/CallFuture 等业务结果。本地 Call 走相同实体队列但不强制绕 NATS。NATS handler 入队后返回不代表排水已经完成。
- [x] 处理永久准入拒绝、邮箱满和 draining 的显式错误；业务结果编码沿用现行载荷契约，普通业务不直接操作 NATS 连接。
- [x] 验证关闸、在途工作、卸载前 RPC、最终收尾回调与连接关闭次序。复用实体 worker 排水计数，收尾回调失败保留连接并允许重试。
- [ ] T2b/T4：将上述顺序接到真实 GD 写回及实例租约生命周期。当前收尾回调夹具不是 Mongo/Redis/Fusion 持久化验收。

验证：T1 命令加双进程集成入口；真实依赖缺失必须显式失败。测试启动器和配置仅在试验目录，不能依靠开发机已有 NATS 掩盖前置条件。

## T2c：新语义、懒激活状态与容量准入

目标：完成可独立验收的运行时和 NATS 边界，再接跨实例分配。用户已授权串行实施，不自动提交。本节点限定内部原型，不修改正式 ProtoActor API 或生成协议。

1. `internal/actortrial/nats.go`、`nats_test.go`：先用无订阅目标及阻塞初始化复现旧 TellAccepted 的错误边界；再补 Tell、CallFuture，Call 复用 Future。TellAccepted 使用 Publish + Flush，只表示当前连接服务端处理过发布，不作逐条存储确认；即使目标在同一实例，也走 NATS 以保持这项含义。Call 保留本地直达邮箱。
2. `internal/actortrial/runtime.go`、新增 `activation_test.go`：运行时提供不触发业务的状态查询与确保激活入口，区分未加载、初始化中、就绪、关闭中及初始化失败。通过通道阻塞工厂，验证查询不会等待初始化、同身份并发不会重复加载、其他实体仍可执行。
3. 新增 `internal/actortrial/capacity.go` 及相关测试：可选预算配置包含字节上限、并发初始化上限和轻量估算函数。工厂在锁外运行；占位建立、预算预留与准入关闸在同一锁下完成。失败及卸载收尾完成后释放额度；未知世界由业务返回明确错误。压力水位用于新激活准入，不中断已接受业务。
4. `nats.go` 增加运行时探测入口，返回状态快照；远端初始化失败保留可观察结果，不能每次 PING 自动重新执行失败初始化。显式重试与查询分开；原型最多保留 1024 条失败诊断，淘汰后仅回到未加载状态；后续显式 Ensure 或业务请求可再尝试，业务准入仍须判断真实存在性。
5. 新增 `internal/actortrial/benchmark_test.go`：测本地调用、状态查询、容量预留及真实 NATS Call；采集代码只在 `_test.go` 或 `/private/tmp/tx-nats-lazy-capacity-*`。本节点报告对修改前原型的变化，T3 才做完整 ProtoActor 选型对照。

检查项：

- [x] 更新共识、运行时状态和容量边界文档。
- [x] 新语义失败证据与真实 NATS 功能回归。
- [x] 状态、并发激活、容量及清理关键测试。
- [x] 本节点功能、race、基准、跨工程编译/测试及 ponytail-review。

T2c 本轮结果与局限见 [试验记录](../../audits/2026-09-12-nats-actor-runtime-trial.md#t2c新通信语义懒激活与容量准入)。增量基准不替代 T3 的 ProtoActor 共同负载对照；远端 Future 每次调用增加 5 次分配，保留为 T3 资源成本输入。

## T2d：动态归属与正常交接

T2c 的实例准入原语通过后，继续复用 `discovery.SlotStore` 的归属记录与 CAS，验证多个实例和调用方竞争首次激活、容量不足的明确拒绝、同归属卸载后懒重建，以及旧 owner 正常排水后的滚动交接。NATS 根据逻辑主题或已解析实例订阅转发，不能靠 Queue Group 决定同一世界的每条业务归属。分配方不得以超时推断旧 owner 未激活；只在写入归属前收到明确容量/排水拒绝时选择其他候选。

game/slgworld 实例池比例不固定，一个实例可持多个世界；世界数量不是内存预算。正式 txgameslg 现有一进程一槽位约束在 T4 变更前仍有效。世界新加载需预留容量；重型世界可使用容量选择，普通实体可按固定逻辑分区，均保持已有激活的归属。实例增减由部署控制面发起，不在此实现 Kubernetes operator、自动故障接管或实时 CPU 搬迁。

### T2d 本轮实施步骤与验收

本节点验证实例滚动交接：一个旧实例承载的实体全部关闸、排空并完成必要收尾，再逐槽发布新 generation。运行中逐世界热迁移不在本节点实施；普通实体继续使用稳定逻辑分区，不为每个玩家/房间新增 etcd 记录。

- [x] `capacity.go` 提供无副作用容量预检，实际激活仍使用同一预算逻辑原子预留。并发首次分配由 SlotStore.CreateSlot CAS 决定唯一目标；先写 assignment 再加载，CAS 失败或结果未知不能在其他实例启动副本。
- [x] 新增 `slots.go`：组合现有 SlotStore、SlotRouter、Directory 与 Transport，只在未分配槽位挑选 running 候选。明确容量/排水拒绝可在写入 assignment 前跳过；已存在 assignment 始终保持归属。预检与实际预留之间存在竞争，CAS 后容量不足返回错误并保留 assignment，不自动再分配。
- [x] 槽位宿主在工厂前校验归属；增加独立实例控制订阅，允许等待全实例排水而不阻塞 Actor RPC 的接收回调。收尾成功才回复 quiesce，失败保持关闭的闸门和连接，允许明确重试；成功后的重复请求不重复执行业务卸载通知或持久化收尾。
- [x] Handoff 先验证当前完整 assignment 和目标就绪/容量，收到旧实例收尾成功响应后再 PromoteSlot CAS；多个控制调用方竞争只允许一份 next 生效，旧请求结果未知不触发业务重投。每个新 owner 开始加载前再次校验持久归属。
- [x] 新增 `slots_test.go`：真实 NATS + etcd 验证首次并发、容量拒绝、新增成员不改派、普通卸载后同归属重建、滚动交接及 CAS 结果丢失。以专用 Redis key 的收尾夹具验证新方读到旧方排空后的值，明确这不等于正式 GD/Fusion 接线验收。
- [x] 单独验证控制回复丢失时不晋升；测试依赖显式指定，失败保留本次随机命名空间的证据，成功仅删除本次拥有的键。原型功能、race、三个 Go 模块回归、适当基准和后置 ponytail-review 全部完成后交付。

T2d 本批证据见 [试验记录](../../audits/2026-09-12-nats-actor-runtime-trial.md#t2d稳定首次分配与实例滚动交接)。已完成真实 NATS/etcd/Redis 夹具及未知结果边界验证；正式 GD、Watch 和实例租约接线仍由 T4 验收，不提前宣称整个 T2/T4 完成。

## T3：共同负载性能及是否进入示例的决策

已完成同机筛选，**未达到 T4 进入门槛**。新增 `comparison_test.go` 与驱动计数自检；本批材料位于 `/private/tmp/tx-nats-comparison-20260912`，完整结果见 [共同负载报告](../../audits/2026-09-12-nats-protoactor-comparison.md)。以下勾选表示筛选工作完成，不表示候选通过。

- [x] 同进程、同机跨进程及独立机器分别标记；暂时无多机环境就不声称跨主机容量。
- [x] 请求/响应各采用 128B、1KiB、8KiB；并发 1/16/64/256，单实体热点与 1000 实体分散负载分别测试。先预热，再顺序交替运行至少五轮；按饱和结果收窄后续采样，不无目的扩矩阵。
- [x] 同时统计成功完成 RPC/s、P50/P99、超时/拒绝、CPU、RSS、Go 分配及残留 goroutine；累计所有相关进程资源。不能用 NATS 单向发布吞吐对比完整 Actor RPC。
- [x] 用固定到达率补测排队和过载；吞吐按完成而非发送计。性能原型不包含持久化时必须明确，JetStream 单独测试。
- [x] 列出最终可删除代码、必须新增代码、保留依赖与高级 SPI 映射。试验夹具不算生产瘦身；API 简洁度不能仅按行数判断。

门槛：正确性零未解释失败；共同负载中 P99 或完成吞吐稳定退化超过 10% 时先定位并停止默认推广（这是试验筛选线，不是生产 SLA）；较小变化报告波动，不宣称零回退。无明显性能损失仍需确认维护复杂度收益。备选是本地 ProtoActor + NATS，或保留原架构；在评审结论前不删除正式 Runtime。

### T3 本轮采样步骤

本批起点 tx=8760c99，六工程干净。复用正式 `Runtime.Call` 与固定 Kind 路由，对照 NATS `Transport.Call`；公共测试信封、响应校验、载荷及调用截止时间一致。所有采集代码仅在 `_test.go` 和临时材料目录，正式实现不改。

- [x] 增加测试进程入口及共同负载驱动，先验证只统计校验成功的完整 RPC、错误单列、并发工作不丢失。
- [x] 使用已有固定镜像创建本批拥有的临时 NATS/etcd 和负载容器；业务调用端、目标端及 broker 共用一个 Linux 网络命名空间，避免 macOS 到 Docker 额外路径偏置。不修改已有容器，结束只清理本批资源。
- [x] 先扫描 128B、1KiB、8KiB，1/16/64/256 并发及 1/1000 实体；选代表点前后交替复测至少五轮。固定路由和预热后调用为第一道筛选，不能冒充虚拟放置或 GD 全链路对照。
- [x] 记录完成吞吐、P50/P99、错误分类、客户端及目标 Go 分配/CPU/内存/goroutine，同时保存独立 broker/etcd 容器资源；固定到达率补测记录从计划到达起的延迟（含生成器晚点、排队及 RPC）和客户端丢弃。
- [x] 如稳定退化超过门槛，先用测试进程 CPU profile 定位，暂停 T4 推广和无意义矩阵扩展；大型实体、计时器及虚拟归属等尚未验证项明确留待后续。不能为了通过门槛顺手修改生产实现。
- [x] 更新可删代码、保留依赖与 SPI 映射，以及未实测范围；完成适当回归和一次后置 ponytail-review。

### T3 筛选结论与下一项

128B/64 并发/1000 实体的五轮中位完成吞吐为 ProtoActor 5.26 万次/s、当前 NATS 原型 1.80 万次/s，P99 分别约 2.99ms、7.75ms；两侧均无 RPC 返回错误。共享响应表的临时诊断对照有改善，但本轮仍复现发布失败后的等待项残留，因此未采用，不修改仓库原型来迁就基准。

- [x] 评估 NATS 请求等待和失败清理的收敛方案，保留四种语义、有界 pending 和未知结果不重投；修复及共同负载复测见下方 T3a，结果为继续暂停 T4。
- [ ] 当前筛选未通过，暂不扩展大型实体/定时器、Virtual Actor 和跨机器矩阵；这些不是已完成验收。正式 GD、租约和示例接线继续留在 T4。

### T3a：共享响应等待与失败清理

用户已确认继续，起点 tx=cdddf92，六工程干净。本批仅调整隔离原型，四种调用语义、等待上限和不重投边界不变。官方最新发布 v1.53.1 及本轮读取的主线仍存在发送失败后的响应项残留；不引入 SDK fork 或复制依赖源码。

- [x] 先验证多个未完成请求共享一个响应订阅，并覆盖取消后的迟到响应、等待上限、发送失败和连接关闭。
- [x] 用 SDK 原生 Subscribe/PublishMsg 建立单个响应入口，原型内有界表关联既有 Future；用 context.AfterFunc 回收取消等待，成功后停止回调，消除逐请求订阅和常驻等待 goroutine。
- [x] 启用真实依赖完成原型功能与 race 回归；保留调用期间断线、排水及未知结果不重投测试。
- [x] 复用 T3 驱动，在相同环境交替比较 cdddf92、候选和 ProtoActor；检查完成吞吐、P99、资源与错误，不拼接旧批次数据。
- [x] 补同机三节点拓扑对照（同 broker 与跨 broker 的固定调用对），明确其不能证明多机扩容容量；根据结果确定后续边界并更新报告；T4 仍受既定门槛约束。
- [x] 完成后置 ponytail-review：Lean already. Ship.

本轮结果见 [T3a 报告](../../audits/2026-09-12-nats-shared-replies.md)。代表点新版中位吞吐 2.40 万次/s，相对提交版 1.71 万提高约 40%，但仍低于同批 ProtoActor 的 5.09 万；低到达率 P99 并未改善。同机三节点固定调用对未出现反超。聚合吞吐与业务预算的后续证据见 T3b；不得将本轮解释为完整 NATS 集群容量或 HA 验收。

### T3b：多实例聚合负载与业务预算输入

用户已确认继续，起点 tx=4a92907，六工程干净。本批只补测试驱动与证据，T4 保持暂停，不修改既定 10% 筛选线。

- [x] 三组独立调用端/目标端同时运行，保持总并发、实体数及到达率一致；比较 ProtoActor、单 broker、三 broker 就近连接与跨 broker 连接。三节点均承载业务连接，区分分摊连接与跨节点转发。
- [x] 驱动支持共同计划起点，记录实际起止与单调耗时；吞吐按起点偏移加单调耗时计算共同窗口，P99 合并成功 RPC 的原始样本，不能平均各端 P99。样本导出在资源快照之后，采集代码仅进入测试二进制。
- [x] 先自检与小规模试跑，再对 128B/1KiB、闭环饱和及固定到达率各作五轮交替采样；错误、驱动未发送请求、起点偏差和全部进程资源单列，保留可复核材料。
- [x] 说明同宿主竞争、相同总负载与实际新增机器资源的区别；核对实测负载区间并提出游戏流量/延迟预算的估计输入；本批未获得完整负载合格点，不宣称生产 SLA、跨机容量或 HA。
- [x] 完成适当回归、报告和后置 ponytail-review：Lean already. Ship. 架构方向需要用户确认，无明确收益不继续在原型上做微优化。

本批 [T3b 报告](../../audits/2026-09-12-nats-aggregate-throughput.md) 已取得 108 个窗口、592.95 万次完整 RPC 的证据，返回错误为 0。三节点聚合没有反超；固定到达率各点均有驱动未发送请求，候选业务预算未获确认。独立样本核验发现微小墙钟漂移，已用保留的单调耗时重算共同窗口，原始数据和修正结果均保留。全量测试和原型 race 通过；全库 vet 的三项 msgq 测试锁复制诊断在提交版也可复现，未改动该夹具。随后讨论确认应先做 T3c 分层归因；此时直接据完整原型结果否决 NATS Core 尚早，T4 保持暂停。

### T3c：SDK、协议封装与实体调度分层归因

用户已授权继续实施，保留 T3b 未提交变更。仅补测试夹具和性能证据；不预先改连接池、订阅分片或实体调度。正式 API、生产采集及 10% 筛选线不变。

- [x] 在现有独立进程驱动中加入同版本 nats.go 原生 Request/Reply、保留当前协议与等待路径但跳过实体调度的对照；用真实 NATS 检查并发响应与错误，采集代码仅在 `_test.go`。
- [x] 使用已安装 SDK 的官方 Pub/Sub 示例校准环境，明确发布速度、订阅完成速度与完整 RPC 吞吐的区别；示例的无界 pending 不作为业务实现建议。
- [x] 同一单 broker、相同连接数、GOMAXPROCS、载荷及并发交替测各层和 ProtoActor；代表点重复五轮，保留原始完成数、错误、耗时及资源。与旧报告不混算中位数。
- [x] 单独采集调用端、目标端和 broker 的 CPU profile，不把 profile 开销混入正式采样；三端出现调度同步热点后，补 GOMAXPROCS=1/4 的单变量五轮对照，不改变生产默认值，不为微小收益扩展优化。
- [x] 完成必要回归、差异 ponytail-review（Lean already. Ship.）、报告和计划更新；已清理仅本轮拥有的临时资源。T4 继续暂停，下一项由本批证据确定。

本批结果见 [T3c 报告](../../audits/2026-09-12-nats-layered-cost.md)：108 个非 profile 窗口完成 721.83 万次 RPC，0 返回错误；官方 Pub/Sub 校准和 21 份三端 profile 补齐归因。GOMAXPROCS=1 的空回显收益经五轮确认，但同配置仍低于 ProtoActor，T4 不进入。下一项建议评估含 GD/Procedure 的代表业务与 CPU 预算，具体负载尚需评估，不自动改默认配置或继续微优化。

### T3d：链路差异的单变量实验

用户已确认先从完整链路差异归因，不只跟随 pprof 热点；暂不扩大 GD 业务负载。本批只修改测试夹具、报告及计划，不改变正式 API、四种 RPC 语义或默认配置。

- [x] 复用协议层夹具，对比唯一与固定正常回复 Subject；两组保留相同请求关联、结果校验及唯一请求 Reply，确认取消与 broker 无接收者响应仍准确关联。先测路由机制成本，不直接作为正式适配器收益。
- [x] 对比 ProtoActor 默认批量与依赖现有配置的 batch=1；单独采集实际批量分布、NATS SDK 和 gRPC 写入次数/字节。诊断通过临时源码副本与 Go overlay 构建，不修改模块缓存，不将计数开销混入吞吐中位数。
- [x] 对比完整 NATS 基线、仅业务 Receive 使用激活 context、仅远端完成回调省去 Future；生命周期、panic 清理、准入和排水保留。取消超时包装只是归因对照，不视为可直接采用的契约变更。
- [x] 相同版本、单 broker、独立调用端和目标端、GOMAXPROCS=1，128B×16/64 并发分别交替五轮；吞吐按成功完成 RPC 计，P99、错误和资源单列。保存构建、原始样本、路由缓存计数及本轮资源清理证据。
- [x] 完成适当回归、一次后置 ponytail-review、报告及下一项；小于波动的变化不宣称优化，不自动进入 T4。

本批证据见 [T3d 报告](../../audits/2026-09-12-nats-rpc-path-differences.md)。70 个正式窗口完成 711.94 万次 RPC，另有 12 个诊断窗口，均无返回错误。批处理有明确高并发收益，回复主题及接收端分配改动没有稳定吞吐收益。下一项先细化固定发送协程连续 PublishMsg 的隔离对照，让现有 SDK 缓冲实际合并；不直接扩展批量协议或进入 T4。

### T3e：固定发送协程与 SDK 合并写入

用户已授权继续验证，保留 T3d 未提交变更。仅在测试类型和临时覆盖构建中增加有界发送队列，不改消息协议、生产默认值或四种 RPC 语义，不按游戏帧或定时延迟凑批。

- [x] 请求和回复共用每连接一个发送协程，逐条调用现有 PublishMsg；调用方等本地 Publish 完成，保留同步发送错误与现有业务 worker 排水边界。队列复用 maxPending 数量上限，满时明确失败。
- [x] 先验证队列满、发送错误、关闭后拒绝、已入队工作完成及队列中旧连接代次消息拒绝；启用真实依赖检验 Call/Future、TellAccepted、取消清理、连接关闭与排水，单独运行 race。
- [x] 相同单 broker、独立调用端/目标端、128B、1000 身份，GOMAXPROCS=1/4 下分别对照当前路径与固定发送协程，1/16/64 并发交替五轮；记录完整 RPC 吞吐、P99、错误和资源，不复用旧批次性能数字。
- [x] 单独采集两业务进程及 broker 写入计数，确认是否实际合并；计数构建不进入性能中位数。没有稳定收益时不采纳，不继续围绕同一发送队列做边际微优化。
- [x] 完成必要回归、一次后置 ponytail-review、报告、累计未提交行数及下一项，清理仅本轮拥有的容器；T4 继续受既定门槛约束。

本批结果见 [T3e 报告](../../audits/2026-09-12-nats-fixed-sender.md)：正式采样 324.35 万次 RPC，诊断另计 70.62 万次，全部无返回错误。写入合并已确认；单 P 退化，四 P、64 并发有局部收益，尚不足以接入运行时，不继续调发送队列参数。下一项先评估封包级 MessageBatch 是否值得做隔离验证，重点是逐条请求关联、按实例归组、批量尺寸、失败回收及排水；协议变化尚未确认，不自动实施。

### T3f：请求封包级 MessageBatch 评估与实验

用户先确认评估，再批准实施，保留 T3d–T3e 未提交变更。[评估稿](../../audits/2026-09-13-nats-request-batch-assessment.md) 确定请求合批、结果逐条发送，业务侧不新增批量 API；目前候选与验证均已完成，因无稳定收益而保留为隔离实验，结果见本节末尾报告。

- [x] 核对实例订阅、pending 清理、Delivery.RequestId 的现有用途、逐实体准入、worker 回复与 SlotHost.quiesce；确认一次 NATS Publish 的 Reply 和大小限制。
- [x] 比较保持当前、仅请求合批及双向合批；写明有界批次 503 关联、取消清理、混合投递顺序、单条尺寸契约和正常排水，不增加重投、持久完成表或业务级整批屏障。
- [x] 明确实验常量与测量对照，区分理论消息数和实测吞吐；只修改文档并完成文档检查和后置 ponytail-review。
- [x] 用户确认评估稿后，以测试夹具和覆盖构建实现最小候选，保留关键失败与排水自检、真实依赖回归及 race；未修改正式 API、依赖版本或默认配置。
- [x] 对当前路径、新封包 batch=1、新封包 batch≤64 做同批交替复测，使用相同长度的主题和实例名；按实际批数 Q 与回复数 N 校验 broker 的 Q+N 计数，而不是沿用 2N。
- [x] 完成停止条件判断：无稳定收益，本轮不扩展低并发、较大载荷、目标实例分散及同批 ProtoActor 对照，不继续双向合批。T4 的 10% 筛选线和维护成本评审继续有效。

实施结果见 [T3f 报告](../../audits/2026-09-13-nats-request-batch.md)：30 个正式窗口、314.51 万次成功 RPC，独立诊断 76.05 万次，无返回错误。单 P batch64 吞吐中位数 -4.7%、P99 +14.2%；四 P 消息数减少 32.8%，吞吐仅 +3.2% 且不稳定。保留隔离实验，不改默认路径。随后已完成采用取舍讨论，用户确认高流量上行优先直连、Actor RPC 使用 NATS，并批准下方 T3g 共用路由节点。

### T3g：共用实体路由与直连上行

用户确认按网络负载分工，并要求复用第三方选址算法及现有发现。实施见 [本轮计划](2026-09-13-actor-shared-routing.md)，结果见 [验收报告](../../audits/2026-09-13-actor-shared-routing.md)。

- [x] 内部 RoutedCaller 复用 SlotResolver 缓存，直连和 NATS 使用同一 assignment；保持低频首次分配和滚动交接。
- [x] 复用 rpc Host/Sender 增加直接 Call 和 Agent Dispatcher，保留可信会话身份、完成 ACK、取消和错误分类；两个接收入口进入同一个 Runtime。
- [x] 真实 TCP/NATS/etcd 验证共用邮箱、加载前归属、旧实例身份、收尾失败不晋升、交接后 Watch 更新及失效缓存拒绝；必要 race、全量 tx 回归、基准与后置 ponytail-review。
- [x] 完整上行同环境对照、生成 Redis GD 代表负载与连接字节预算已在 [I2](../../audits/2026-09-13-uplink-business-comparison.md) 完成；原 10% 门槛尚未放行。正式宿主、示例接线及 Mongo/Fusion、JetStream 均未完成，不用短窗口推算生产吞吐。

### T3h：归属组件职责与正式接线准备

用户确认继续细化现有原型的正式职责与验收缺口。见 [设计](../specs/2026-09-13-actor-ownership-design.md) 与 [接线计划](2026-09-13-actor-ownership-integration.md)，本轮不修改生产 Go 代码。

- [x] 核对共享加载准入、SlotAllocator、框架 Drain/Stop、Procedure/writeback 和租约启动顺序；列出高级 PID/Props/Context/ActorSystem 能力的迁移要求。
- [x] 明确复用现有目录、CAS、框架收尾进度和控制端点；不整体复制第三方集群模块，不新增第二份归属或通用调度框架。
- [x] I1：试验区补实际事务写回与 Discovery.Runtime 租约组合，验证失败保留资源、显式重试和租约失效不自动改派，见 [报告](../../audits/2026-09-13-actor-ownership-integration.md)；生成 GD 和正式宿主另外验收。
- [x] I2：完成代表业务、完整上行、固定到达率和流量预算评审，见 [报告](../../audits/2026-09-13-uplink-business-comparison.md)。hybrid 的 1 KiB 中位吞吐较 ProtoActor 低 18.0%；原 10% 筛选线保持，是否改用报告中的临时业务预算需单独确认。
- [x] I3/I4：用户接受临时业务预算后，正式宿主与两个独立示例已接入；PrepareHandoff 已实现，StateDrained 仍不能替代最终持久化成功许可。历史性能筛选记录保留，当前边界见迁移收口记录。

## T4：接入两个独立示例并评估正式 API

修改范围：tx/modules/actor、tx/internal/framework/cmod_actor.go 及相关能力装配；txgame/server/services/game/service.go、services/internal/actorcmd；txgameslg/server/services/{slggame,slgworld}/service.go、services/internal/actorcmd、server/internal/placement；两个 server/go.mod 与运行配置、README、集群验收入口按实际依赖更新。

- [x] T3 通过后才整理正式包位置，收敛试验 API；必要高级能力给 game 使用，不把通用运行时细节暴露到示例。
- [x] 先接 txgame 的角色请求和多 Session，再接 txgameslg 的固定世界与跨世界侦查；保留独立示例，不互相 import。
- [x] 在现有 cmd/cluster-e2e 中验证真实网络、实例变化、关闸、槽位代次、重建、GD 写回及多会话下行。沿用工程固定 DB/database/prefix 和 ownership 清理约束，两个套件串行运行。
- [x] 外部协议不变时直接使用原客户端；若必须修改生成输入，先为相关生成器/客户端建同名分支，再经原生成入口生成，禁止手改产物。
- [x] 在 tx 与两个 server 模块运行 `go test ./... -count=1`；两个客户端运行 `npm test`、`npm run build` 并驱动真实集群。集成命令参数从各工程当时 README 核对，不盲用历史环境。

交付：三仓版本矩阵、实际配置入口、功能与性能报告；失败不伪装为既有行为等价。不需要永久双后端开关，原实现由基线快照提供对照。

## T5：JetStream 可靠任务闭环

位置：正式闭环在 `tx/internal/framework/jetstream_tasks_test.go`，生产消费适配位于 `internal/taskqueue`，由 Bee/Ant CModTaskQueue 装配；两个独立示例提供可选配置。下面逐项记录保留历史阶段背景，末尾总表按当前证据结算；工程功能闭环不代表生产部署验收。

- [x] 最小验证：FileStorage、共享 durable pull consumer、正式 Actor 完成后 DoubleAck，以及领取进程退出后的续领；详见 [闭环记录](../../audits/2026-09-14-jetstream-task-loop.md)。这不代表完整 T5 或生产消费运行时完成。

- [x] 结果分类验证：业务明确结束确认、普通业务错误保留并重投、永久加载拒绝转死信；死信流不可用时不 ACK，转存持久确认后才确认源任务。仍为测试内部适配，见闭环记录后续小节。

- [x] SDK 消费生命周期验证：原生 Consume/Drain/Closed 等待正式 Actor 执行及 ACK；等待超时不取消业务，完成消费排水并关闭连接后，新任务由替代消费者首次领取。单条在途配置，尚未接入框架 Stop 或验证批量预取缓冲。

- [x] 慢任务与背压验证：在 2s AckWait 下阻塞超过 3s，以 SDK InProgress 续期；竞争消费者不能领取原任务或下一条任务，MaxAckPending=1 保持一条在途、一条待处理。ACK 后释放名额，后续任务由替代消费者首次领取。仅同一 durable 的测试内控制，非生产自动续期或集群限流。

- [x] 内部适配 `internal/taskqueue`：任务身份协议、正式 Actor 分派、自动 InProgress、永久拒绝先转存后 ACK、消费错误日志与可重复等待的 Drain；真实夹具已改为调用适配层，移除测试内重复分派。尚未开放框架配置或业务公共 API，也未接入默认 server Stop。

- [x] 框架能力接线：Bee/Ant `CModTaskQueue`、`TaskQueue` 配置和窄发布入口；绑定已有文件流与 durable，服务初始化完成后开始消费，Server.Drain/Stop/PrepareHandoff 先排消费再关闭 Actor 准入。真实同实例 Actor 验证 Stop 超时保留资源与重试完成。README 提供使用示例，未默认启用两个游戏示例。

- [x] MaxDeliver 应用恢复：预部署专用文件流捕获通知，有限投递缺少捕获配置拒绝启动；独立 durable 恢复先转存死信、再删源残留、最后确认通知。真实验证处理端离线、转存失败保留、重复通知及旧代次保护。通知流不可写/Broker 故障的捕获缺口与容量仍待部署验收。

- [x] 满额边界验证：源流 DiscardNew 拒绝新任务且保留旧任务；死信满额保留源与通知，释放容量后恢复；通知流满额实测出现捕获缺口。补充 [部署说明](../../deployment/jetstream-tasks.md)，不新增自动扫描/业务重放。真实磁盘、Broker 故障及副本同步验证仍未完成。

- [x] 无通知残留只读核对：复用 NATS INFO / STREAM.MSG.GET，真实验证源残留存在但三个 consumer 计数均为零，查询不改变投递/确认状态。提供受限人工核对步骤，不将只读结果升级为逐条耗尽判定或自动重投。

- [x] 隔离 Broker 重启：复用本地镜像，sync_interval=always / FileStorage / R1，SIGKILL 后重启同一容器；源任务、死信及通知保留，durable 与后续恢复继续通过。不是宿主掉电、容器删除或多副本故障验收，未改变生产默认配置。

- [x] R3 隔离故障：三流及两 durable 配置三副本，确认全部 follower Current 后终止源流 leader；剩余两节点完成新发布及耗尽恢复，再失去多数派时发布不能成功确认，重返后全部副本追平。结果不确定不自动重发；同机三容器不代表物理容灾。

- [x] 强同步框架任务基线：R1/R3 各三轮、每轮 1000 条、32 个热 Actor、256B payload、4 发布/4 消费；分别测 PubAck 与 Processor/DoubleAck 完成，见 [性能记录](../../audits/2026-09-20-jetstream-task-baseline.md)。内存计数业务，不含 GD/数据库，非生产容量与固定到达率验收。

- [x] 固定计划到达率：50/100/200 条/s、5s 窗口、R1/R3 各三轮，共 10500 条执行确认，无重投。报告本地未开始任务、计划到达 P99 与额外排空，200 条/s 的 R3 明显落后；见 [记录](../../audits/2026-09-20-jetstream-task-arrival.md)。短窗口正确性通过不等于长期容量或生产 SLA 达标。

- [x] 同步策略对照：同负载 always→2m→always，18000 条任务全部确认；本机 R3 周期同步明显减轻发布积压，回切后重现。保留一次选主前创建流超时记录；不降低默认可靠性、不声明长期容量，见 [记录](../../audits/2026-09-20-jetstream-task-sync.md)。下一项先确定候选同步策略及目标任务负载。

- [x] 强同步分层对照：五组原生/Actor、4/32 并发与单条/16 条拉取，30000 条正式任务与 12000 条 race 任务通过。原生路径也只有同一量级，扩大窗口未带来数量级改善，停止本机微优化；见 [记录](../../audits/2026-09-20-jetstream-task-layers.md)。下一项评估示例业务接线，生产存储与长期容量另行验收。

- [x] 示例命令任务契约：txgame/txgameslg 约定 task.command 等待 Handler 完成，即使业务仅返回 error；成功时由适配器提供 task.completed 信封，失败传回任务消费者。真实 JetStream 经 txgame 生成命令与懒加载 Actor 验证失败保留、重投成功才 ACK；普通 RPC 形态约束保留。项目队列配置和具体业务启用尚待后续接线，见 [验收记录](../../audits/2026-09-20-generated-task-command.md)。

- [x] 项目可选队列配置：txgame runtime.taskQueue / txgameslg runtime.cmods.taskQueue 默认 null；非空时仅选中业务服务引入 CModTaskQueue，支持纯发布或已有 durable 消费。两工程全量与定向 race 通过，项目配置驱动的真实生成命令重投验收通过。具体 GD 业务入口仍待后续，见 [接续记录](../../audits/2026-09-20-generated-task-command.md#接续项目可选任务配置)。

- [x] txgame 真实 GD 任务：复用 System_StorageProbe 与实际 Game 组合根；Game 启动前持久待消费，启动后 ACK、Redis 生成 GD 回读及 Mongo 最终存盘通过，死信为空。定向 race 与 server 全量通过，见 [记录](../../audits/2026-09-20-generated-task-command.md#接续真实-game-与-fusion-gd)。下一项评估 SLG 世界 Procedure/直接 Mongo 任务，不把最终存盘纳入 ACK 条件。

- [x] SLG 世界 GD 任务：实际 World_MoveUnit 经 Procedure 提交后 ACK，Mongo 保持旧值至正常 Stop 写回；发现并修复 RouteKey JSON 丢值，协议边界与世界验收 race 通过，见 [记录](../../audits/2026-09-20-generated-task-command.md#接续世界-procedure-与任务路由键)。下一项评估任务上下文与失败分类的剩余覆盖。

- [x] 调用上下文复核：既有正式闭环改用生产 Publisher，校验命令号、RouteKey、载荷和操作身份经过持久化、跨实例、重投与本地排水后完整保留；PubAck 后结束发布 Context 不撤销任务。真实闭环 race 通过，见 [记录](../../audits/2026-09-20-generated-task-command.md#接续调用上下文与失败分类收敛)。

- [x] 文件流、共享 durable pull consumer、显式确认；业务 Handler 与 RPC 复用分派，投递和确认归 MQ 适配。
- [x] 测试执行后确认、业务明确结束、永久加载拒绝转死信、临时错误恢复、进程重启接管和排水停止领取。
- [x] 死信先持久转存确认再结束原任务；转存失败保留原消息。捕获 MaxDeliver 通知并持久保存，验证处理端离线与残留源消息恢复。不把 Term 或重试耗尽当成已自动转存。
- [x] 不建成功完成表；结果或 ACK 丢失仅记录可观察 error，接受重复窗口。慢任务续期与背压按真实版本验证。
- [x] 用确定的磁盘同步与副本配置单独测任务链路：R1/R3 always 基线已有记录，单机多容器不宣称高可用。并发激活共享、旧计时器不得唤醒替代实体、战斗到期及卸载后的拒绝已有 actorhost 用例，本轮定向 race 复核通过；任务永久拒绝转死信由真实闭环验证。
- [ ] 生产部署验收：候选同步策略、目标业务负载、长期稳态与物理故障域/掉电。通知流不可写的捕获缺口仍须运维核对，不承诺自动恢复。

交付：符合既定边界的任务演示与失败恢复证据。JetStream 不通过不应迫使采用 Core RPC；通信和 MQ 可分别决定。

## T6：五项整合收益逐项结算，不扩大本轮范围

- [x] 汇总通信配套、请求等待、共享归属、RPC/任务及扩展观察五项实际收益；保留项与未实现项明确列出，见 [T6 收口](../../audits/2026-09-20-nats-integration-closeout.md)。
- [x] MsgQ 按用户既定决定保留，允许与 Agent 同进程组合；本次不立项传输替换，不将 Core pub/sub 当完整邮箱。
- [x] etcd 保留；未来若立项需评估 WorkSlot/SessionID、租约失效、持久槽位 CAS、快照 Watch 和旧代次隔离。本次不实施 KV 迁移。
- [x] 历史代码保留取舍已评估：正式迁移已采用，两个 server 生产闭包均无 ProtoActor；旧 Placement 仍用于历史行为反例，隔离需要扩大宿主 SPI、复制实现或改写证据，当前收益不足，保留并停止该方向。其他历史依赖与冻结基线继续保留，不宣称全部源码清除；见 [取舍](../../audits/2026-09-20-nats-integration-closeout.md#接续旧虚拟-placement-隔离取舍)。

- [x] 本地 demo 配套：隔离 Compose、资源预创建、配置副本及两个正式服务启动通过；txgame 的重复队列准备仅在测试辅助包收敛；独立 task-demo 发布命令与真实 Game 进程使用预创建流完成离线持久等待、消费及存盘。独立示例和关键断言保留，见 [部署入口](../../../deploy/task-demo/README.md)。

最终交付为“采用 / 局部采用 / 不采用”及证据，不以完成代码量作为成功标准。每轮按累计未提交变更给中文 Angular Commit Message，提交由用户操作。

## 2026-09-14：默认迁移收口与 T5 入口

两个正式 server 依赖闭包均无 ProtoActor/actortrial。三工程全量、两客户端测试构建、相关五包 race 和两套真实集群串行通过；本轮只更新文档，不修改冻结基线，不重复性能声明。I3/I4/T4 在默认切换范围收口，保留旧测试与对照所需依赖。下一项先验证 JetStream 持久投递、正式 Actor 执行后 ACK 与消费重启续领；死信、重试和背压仍须完成完整 T5 验收。详见 [收口记录](../../audits/2026-09-14-nats-default-closeout.md)。未提交 Git。
