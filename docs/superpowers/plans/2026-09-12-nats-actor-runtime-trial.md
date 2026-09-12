# NATS 与最小本地 Actor 运行时试验实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. 串行执行，不使用子代理，不自动提交。

**Goal:** 在 feature 分支验证 NATS Core + 最小本地实体运行时能否替代 ProtoActor，并以 JetStream 衔接可靠任务；以正确性、维护复杂度和性能决定是否推广。

**Architecture:** 先隔离原型，再逐步接入两个独立 game 示例。本地实体管理负责身份、激活、邮箱及生命周期；NATS 负责跨进程通信，JetStream 负责任务存储与消费进度；etcd 暂留实例身份、租约和槽位交接。原型可被否决，不预设必须迁移成功。

**Tech Stack:** 当前仓库锁定的 Go 工具链、NATS 官方 Go 客户端、nats-server、现有 etcd/Redis/Mongo；执行前固定版本和镜像摘要，不用浮动 latest 做结果依据。

**Spec:** [生命周期与可靠任务草案](../specs/2026-09-11-actor-lifecycle-reliable-task-design.md)。本计划补充“试验优先且不预设保留 ProtoActor”的最新方向；不代表已有运行时已变更。

## 当前状态与分支

T2d 开始前：六工程工作区干净；tx=96fd69a、txgame=3afc074、txgameslg=2fd67a6 已提交至 `feature/nats-actor-runtime`。本批 T2d 已完成原型验证，只在 tx 保留新未提交变更；下一项 T3，仍不自动提交。以下分支创建信息是准备阶段快照，不代表当前工作区仍干净。

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
- [ ] 记录版本、硬件、GOMAXPROCS、消息大小、并发、目标分布、网络/TLS、采样时长与失败计数。NATS 数据目录使用独立临时目录，结束只清理本次拥有的进程和资源。

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

拟新增：`tx/internal/actortrial/benchmark_test.go`；采集脚本、旧源码和二进制放 `/private/tmp/tx-nats-actor-trial-*`，报告放 T0 证据文档。

- [ ] 同进程、同机跨进程及独立机器分别标记；暂时无多机环境就不声称跨主机容量。
- [ ] 请求/响应各采用 128B、1KiB、8KiB；并发 1/16/64/256，单实体热点与 1000 实体分散负载分别测试。先预热，再顺序交替运行至少五轮；按饱和结果收窄后续采样，不无目的扩矩阵。
- [ ] 同时统计成功完成 RPC/s、P50/P99、超时/拒绝、CPU、RSS、Go 分配及残留 goroutine；累计所有相关进程资源。不能用 NATS 单向发布吞吐对比完整 Actor RPC。
- [ ] 用固定到达率补测排队和过载；吞吐按完成而非发送计。性能原型不包含持久化时必须明确，JetStream 单独测试。
- [ ] 列出最终可删除代码、必须新增代码、保留依赖与高级 SPI 映射。试验夹具不算生产瘦身；API 简洁度不能仅按行数判断。

门槛：正确性零未解释失败；共同负载中 P99 或完成吞吐稳定退化超过 10% 时先定位并停止默认推广（这是试验筛选线，不是生产 SLA）；较小变化报告波动，不宣称零回退。无明显性能损失仍需确认维护复杂度收益。备选是本地 ProtoActor + NATS，或保留原架构；在评审结论前不删除正式 Runtime。

## T4：接入两个独立示例并评估正式 API

修改范围：tx/modules/actor、tx/internal/framework/cmod_actor.go 及相关能力装配；txgame/server/services/game/service.go、services/internal/actorcmd；txgameslg/server/services/{slggame,slgworld}/service.go、services/internal/actorcmd、server/internal/placement；两个 server/go.mod 与运行配置、README、集群验收入口按实际依赖更新。

- [ ] T3 通过后才整理正式包位置，收敛试验 API；必要高级能力给 game 使用，不把通用运行时细节暴露到示例。
- [ ] 先接 txgame 的角色请求和多 Session，再接 txgameslg 的固定世界与跨世界侦查；保留独立示例，不互相 import。
- [ ] 在现有 cmd/cluster-e2e 中验证真实网络、实例变化、关闸、槽位代次、重建、GD 写回及多会话下行。沿用工程固定 DB/database/prefix 和 ownership 清理约束，两个套件串行运行。
- [ ] 外部协议不变时直接使用原客户端；若必须修改生成输入，先为相关生成器/客户端建同名分支，再经原生成入口生成，禁止手改产物。
- [ ] 在 tx 与两个 server 模块运行 `go test ./... -count=1`；两个客户端运行 `npm test`、`npm run build` 并驱动真实集群。集成命令参数从各工程当时 README 核对，不盲用历史环境。

交付：三仓版本矩阵、实际配置入口、功能与性能报告；失败不伪装为既有行为等价。不需要永久双后端开关，原实现由基线快照提供对照。

## T5：JetStream 可靠任务闭环

位置：先在 `tx/internal/actortrial/tasks.go`、`tasks_test.go` 验证，采用后再收敛正式适配目录；示例以现有命令或试验内部消息构造任务，不为演示改变客户端协议。

- [ ] 文件流、共享 durable pull consumer、显式确认；业务 Handler 与 RPC 复用分派，投递和确认归 MQ 适配。
- [ ] 测试执行后确认、业务明确结束、永久加载拒绝转死信、临时错误恢复、进程重启接管和排水停止领取。
- [ ] 死信先持久转存确认再结束原任务；转存失败保留原消息。捕获 MaxDeliver 通知并持久保存，验证处理端离线与残留源消息恢复。不把 Term 或重试耗尽当成已自动转存。
- [ ] 不建成功完成表；结果或 ACK 丢失仅记录可观察 error，接受重复窗口。慢任务续期与背压按真实版本验证。
- [ ] 用确定的磁盘同步与副本配置单独测任务链路；单机多容器不宣称高可用。对同一次激活的重复、旧计时器、过期战斗作明确分类断言。

交付：符合既定边界的任务演示与失败恢复证据。JetStream 不通过不应迫使采用 Core RPC；通信和 MQ 可分别决定。

## T6：五项整合收益逐项结算，不扩大本轮范围

- [ ] 汇总通信配套、请求等待逻辑、发现拓扑和非 Actor 接入的实际收益；没有删除或验证的收益标为未实现。
- [ ] msgq 仅在前述阶段有收益后单独立项：对等比较现有 PubSession/PubRole→邮箱→Fetch/Ack，不将 Core pub/sub 当完整邮箱。Seq/Ack、连接隔离、Topic owner 范围必须保持；无收益就不替换传输。
- [ ] etcd 替代只给独立评估输入：WorkSlot/SessionID、租约失效、持久槽位 CAS、快照 Watch、旧代次隔离。不在本试验中自动实施 KV 迁移。
- [ ] 确认采用后才删除被替代 ProtoActor 代码和依赖；重新核验高级 SPI、两示例和客户端。未采用则保留报告，不合并原型，旧分支和未提交工作不得自动删除。

最终交付为“采用 / 局部采用 / 不采用”及证据，不以完成代码量作为成功标准。每轮按累计未提交变更给中文 Angular Commit Message，提交由用户操作。
