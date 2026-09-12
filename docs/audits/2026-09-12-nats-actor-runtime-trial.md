# NATS Actor 试验：本地运行时与真实 RPC

日期：2026-09-12。最新状态：T2d 稳定首次分配与全实例排空后逐槽交接已通过真实 NATS/etcd/Redis 试验、race、三个 Go 模块回归及增量基准复核。下一项为 T3 的 ProtoActor 共同负载选型对照；正式 GD/实例租约接线及两示例接入留在 T4。本批只修改 tx，尚未提交，不是生产替换结论。

前面 T1/T2a/T2b/T2c 各节保留当批证据；旧取出确认语义由 T2c 覆盖，旧成员重算建议由 T2d 稳定归属覆盖，不作为当前能力说明。

## 分支与输入

以下为 T1 开始时的历史快照；T2d 的本轮起点见文末。三个仓库当时均已位于 feature/nats-actor-runtime，且工作区干净：

| 仓库 | HEAD |
|---|---|
| tx | e98c9c673a9f6aba51bdf48cf4970ceb895e44a7 |
| txgame | b611ebb80af3908d62660d9dd551f434265a5c12 |
| txgameslg | 0b465210ef878cbc39039a36a228bd7b1dfff085 |

其余仓库仍在 main 且干净：txgen=8bc6c97334da8e34b207ce8dcdf05cd8132f491f，txclient=5514f2ac0b13c59dbb9f361aa40816212a886496，txclientslg=9f2d571a9bafa84865674a46bedaf59d6e85c4b8。两 server 的 go list -m 实测均通过 ../../tx 解析至 /Users/a47/workspace/tx，无 go.work。

材料目录：/private/tmp/tx-nats-actor-trial-20260912。baseline/manifest.json 保存六仓状态及模块解析；baseline/tx.tar 是上述 tx HEAD 的 git archive，baseline/actor.test 是改动前编译的 Actor 测试二进制。Go 1.27.0 darwin/amd64，模块声明 Go 1.26，系统报告 16 个逻辑 CPU；GOMAXPROCS 未显式设置，CPU 型号读取被沙箱拒绝，性能阶段须补齐环境信息。

默认 Go 构建缓存被沙箱拒绝，本轮使用 GOCACHE=/private/tmp/tx-go-cache。没有修改模块依赖、机器环境配置或第三方源代码。

## 现行行为与试验边界

| 行为 | 当前依据 | 本批处理 |
|---|---|---|
| 虚拟 Kind+Identity / 固定 RouteKey 寻址 | modules/actor/runtime.go、fixed_host.go | 原实现保留；原型仅本地 Identity，不提供跨实例 owner |
| 接收与完成分离、取消等待不撤销处理 | modules/actor/delivery.go、future.go | 原型分别 Accepted/Await；尚未映射正式 Delivery/Result |
| 有界邮箱、准入与排水 | modules/actor/mailbox.go、lifecycle.go | 原型以同一锁保护非阻塞入队与关队，关队后排空已接收工作；尚未接入进程全局生命周期 |
| 身份、租约、世界槽位交接 | modules/discovery/etcd.go、slot_etcd.go | 完全保留，不增加第二套协调机制 |
| 五个实体事件及业务截止时间 | 生命周期草案；原实现尚无统一 SPI | 仅 internal/actortrial 中实现可选回调与激活绑定句柄 |
| 异常停止 | 新原型限定语义 | panic 失败通知当前及排队请求，只做后清理，不调用正常退出前业务通知 |

原型只有 runtime.go 与 runtime_test.go。工厂完成加载准入后返回本次独立 Behavior；永久拒绝用 ErrActivationDenied，其余错误保留。每个活跃实体一个 worker，延时投递暂用可被激活结束取消的 timer goroutine；并发调用共享一次激活，不预建线程池/计时轮。当前限制需在 T3 的大量实体与定时器场景验证，不能据此承诺大规模内存成本。

Context 绑定本次激活；内嵌 context 是当前回调的有界上下文，返回后取消，长期投递句柄通过 Post/After 使用独立激活生命周期。业务自定义的逐任务错误仅影响当前请求；panic 则结束本次激活。单次错误事件记录 error，不阻止正常卸载。超时不能安全强杀不合作的 Go 回调。

工厂失败、初始化失败及卸载后清理都在释放激活占位前处理，避免相同身份的新实例与旧清理并发。行为对象可选事件尚未加入正式原因枚举、Messenger 或稳定 API；正式能力将在 NATS 与框架装配阶段按实际需求收敛，不声称已经实现完整设计。

## T1 首批验证

先写测试，缺失 New/Context/Behavior 时编译失败（red.log）；实现后通过。新增 panic 收尾测试进一步暴露正常退出通知误执行，panic-red.log 保存失败；修正后重新完成下列验证。

| 命令（均使用上述 GOCACHE） | 结果 |
|---|---|
| go test ./internal/actortrial -count=1 -json | 8 个顶层测试通过，含子测试 12 项通过，无 skip/fail |
| go test -race ./internal/actortrial -count=1 -json | 同上，无 race 报告 |
| go test ./modules/actor -count=1 -json | 27 个顶层测试通过、2 skip，无 fail |

日志分别为 tests.jsonl、race.jsonl、actor-regression.jsonl。两项跳过为 TestActorDrainAcrossProcesses、TestActorRemoteCallLifecycle，缺少显式真实 etcd 测试环境；不把包通过称为真实跨进程回归通过，也不覆盖历史 Provider race。

测试覆盖：同身份并发加载、不同实体独立推进、串行计数、有界队列溢出、卸载顺序与通知报错、准入/激活失败、清理期间占位、等待取消后业务继续、Drain 等待后清理、panic 释放排队等待者、旧句柄/定时器隔离。60s/65s 战斗使用 testing/synctest 虚拟时间，不实际等待一分钟；bubble 结束检查后台工作退出。

尚未运行 NATS、JetStream、六工程全量测试或 benchmark；不提供吞吐、P99 或性能变化结论。新增试验运行时 283 行、测试 333 行；正式运行路径未改，不计为生产瘦身收益。

后置 ponytail-review：Lean already. Ship. 原型限于内部包，复用标准库 channel/context/timer，没有新增公共后端接口或生成模板。

下一项：T2 固定 NATS 版本，验证真实 RPC 及归属适配；运行时参数、载荷和网络拓扑在 T3 对照前冻结。示例接入仍受性能及复杂度评审门槛约束。


## T2a：真实 NATS RPC、故障与收尾

本批只推进隔离原型及必要依赖同步。Transport 提供 Call/TellAccepted，接收稳定实体 Identity 与路由层已确定的实例 ID。每个实例使用一个普通订阅，不使用 Queue Group；实例 ID 必须来自现有启动唯一身份，重复 ID 的两个订阅不能保证唯一执行。NATS 不负责选 owner。

### 实测环境与依赖

- 服务端：本机 Docker `tx-nats-trial`，`nats:2.14.6-alpine`；Image ID 与 RepoDigest 均为 `sha256:ad7a43eb7e3337c3c38ce5d784d1461791f95f730f252d2b25eee699752a0ca3`。
- TCP `nats://127.0.0.1:4222`，无 TLS；这是同机跨进程验证，不是跨主机网络或容量结果。用户数据卷 `tx-nats-trial-data:/data` 保留，未重启/停止容器、未创建 JetStream stream/consumer。
- 客户端固定 `github.com/nats-io/nats.go v1.53.1`，新增 nkeys v0.4.15、nuid v1.0.1。客户端要求 compress v1.18.5；tx 与两个 server 只同步这个补丁升级，其他版本不变。两个 server 仍通过 ../../tx 使用当前工作区。
- nats.go v1.53.1 默认共享响应表在发送失败分支保留 token。独立探针连续 16 次超大消息发送失败，默认模式残留 16 条记录，SDK `UseOldRequestStyle` 模式残留 0 条。证据为材料目录的 sdk-response-map-probe.go/.log；探针只在测试目录使用反射观察，没有改动 SDK。
- 当前复用 SDK 独立 inbox 模式，配合 `ReconnectBufSize(-1)` 禁止断线缓存 RPC。未实现自定义请求表、自动重发或 SDK fork。独立订阅的吞吐/分配成本必须纳入 T3，不因修复清理行为就宣称更快。

### 行为与实现

1. 本地调用直接投递实体队列；远程使用 NATS headers 携带实体身份，protobuf Delivery/Result 保持 BON 字节、CommandID、SessionIdentity、UpSeq/ProcessedUpSeq 等字段。输入先复制，模式由传输确定；这是内部试验信封，不是新增公共协议或稳定高级 SPI。
2. 取出确认和业务结果分别返回。响应动作在已有实体 worker 内完成，纳入 Runtime.Drain；NATS 接收回调只解码并入队，不为每条 RPC 增加等待 goroutine 或第二套业务排水屏障。
3. 发送端限制在途等待数量，邮箱满、永久加载拒绝、draining 与业务错误明确返回；包装后的框架错误仍可 errors.Is 分类。上行 ACK 与业务结果互斥，非法结果明确失败。结果已无法送达时，仅对可观察的发送错误记 error；Core NATS 无法证明超时调用方是否收到迟到结果。
4. 关闸后已接收业务继续完成，然后卸载前 RPC、收尾回调、Flush 和关闭连接。收尾失败保持连接，允许在关闭准入状态重试；回调成功只证明调用顺序，不等于真实 GD 持久化已验收。
5. 固定槽位夹具复用现有 SlotResolver、ServiceDirectory 和 ValidateSlotPromotion：错误 owner 加载被拒绝、draining 不再可解析，旧实例排水完成后由 generation 1 交接到另一唯一实例的 generation 2。测试用内存 SlotReader 不替代生产 etcd CAS，也不宣称覆盖 Watch/租约故障。

示例：给房间发 Buff，TellAccepted 返回只说明已经取出；Call 等待业务结果。若等待超时，房间仍可能完成 Buff。本批实测超时后释放业务阻塞，后续请求正常返回且旧订阅被清理；不会自动再投递一次 Buff。

### 本批验证与边界

所有命令使用 `GOCACHE=/private/tmp/tx-go-cache`，NATS 用例显式设置 `TX_ACTORTRIAL_NATS_URL=nats://127.0.0.1:4222`。设定该变量后连接失败是测试失败；未设定时普通无依赖运行会跳过 NATS 用例。

| 验证 | 结果 |
|---|---|
| 试验包真实 NATS 功能测试 | 16 个顶层测试、含子测试 26 项通过，无 skip/fail |
| 试验包真实 NATS `-race` | 同上，无 race 报告；包含真正子进程与测试 TCP 断连代理 |
| tx `go test -mod=readonly ./... -count=1` | 含子测试 1136 项通过，39 项真实 etcd/Redis/Mongo 环境用例跳过，无失败 |
| txgame/server 同上 | 含子测试 236 项通过，3 项跳过，无失败 |
| txgameslg/server 同上 | 含子测试 331 项通过，5 项跳过，无失败 |

证据：nats-red.log（缺少传输实现）、nats-error-red.log（包装拒绝/本地业务错误不一致）、nats-contract-red.log（混合 ACK 结果未拒绝）、nats-green.jsonl、nats-race.jsonl、tx-full.jsonl、txgame-full.jsonl、txgameslg-full.jsonl。完整 skip 清单可用材料目录 summarize.py 重现；包通过不等于这些真实依赖验收通过。TCP 断连只切断测试拥有的代理连接，没有中断共享 NATS。

未验证：真实 GD 收尾故障、虚拟实体拓扑变化、TLS/跨机器、JetStream、故障重连后新请求恢复、六工程完整验收及 benchmark。NATS 订阅自身也有 SDK pending 上限，超过该层上限时可能发生 slow-consumer 丢弃并导致调用超时；显式 mailbox-full 只覆盖已到达适配器的请求。不能把目前的拒绝测试称为任意负载下都可明确拒绝。

累计试验代码：runtime.go 302 行、nats.go 283 行；测试 runtime_test.go 333 行、nats_test.go 622 行。没有删除正式运行时代码；这 585 行实现与 955 行测试不是生产瘦身收益。尚无性能数据，不能声明性能改善或无回退。

下一项为 T2b：沿当前 disthash 的归属变化行为核对虚拟角色重建、旧 owner 关闸与卸载顺序，再接实例身份生命周期。之后才冻结共同负载进入 T3；T3 未评审前不进入 T4 正式替换或 T5 可靠任务。


本批后置 ponytail-review：Lean already. Ship. 未发现值得在当前节点删除的抽象或依赖；保留准入、真实故障、排水与错误契约测试。请求清理复用 SDK，槽位解析复用 discovery；不为小幅行数变化继续拆合辅助方法。两个示例的差异仅为 compress 版本与校验和同步。三工程 git diff --check 通过，未执行 Git commit。


## T2b：本地归属关闸与跨实例交接决策

本批新增 placement.go。按 Kind 保存不可变成员快照，使用仓库已有的 go-rendezvous 与 xxhash 选择实例；只提升为直接依赖，没有改变版本。先复制并排序输入成员，使各实例在相同集合下得到同一结果。具体哈希映射不是旧 ProtoActor PID 分配的兼容承诺；原型仍隔离。

Runtime.SetPlacement 与 Submit 共用锁，避免“已发布新视图、旧实例仍接受新命令”的本地窗口。旧队列继续排空，生命周期清理完成后删除占位；即使成员再次变化、归属重新回到原实例，也不会在旧清理期间重建。过期路由收到 ErrNotOwner，允许调用方以后重新解析，不能分类成业务永久拒绝或自动重发当前 RPC。进程全局 Drain 后刷新视图不会重新开闸。

该接口只用于虚拟 Kind 的独立试验运行时，不能拿一个仅列虚拟 Kind 的快照去更新混有固定 Actor 的运行时。未实现 etcd Watch 接线、快照 revision 校验、全局交接屏障或真实持久化迁移；调用方须串行发布快照。按实体遍历与按候选成员选 owner 的成本留待 T3 实测，不提前增加缓存或二级索引。

### 当前代码证据

- 锁定的 ProtoActor disthash/manager.go 更新 rendezvous 后向 placement actor 发送拓扑消息；placement_actor.go:onClusterTopology 对归属变化实体投递 Poison，onActivationRequest 在新 owner 直接 Spawn。该路径没有旧 owner 完成卸载再允许新 owner 激活的握手。
- txgame/server/services/internal/actorcmd/command_actor.go 的角色是普通 NewKind，命令 Actor 自身只保存 Handler/Publisher；不能把其停止等同于任意业务 GD 已完成迁移。
- txgameslg/server/services/slggame/service.go 的角色使用 NewFixedCommandKind，并使用 Procedure；固定角色/世界不得接入这一虚拟哈希迁移机制。
- 新增真实 NATS 用例在旧请求被阻塞时切换 owner，新 owner 可以返回结果，而旧请求尚未结束。测试通过证明这个限制存在，不是跨实例串行性的成功证据。

### 需要决定的边界

建议：虚拟寻址用于无跨请求内存状态的处理器；依赖内存 GD、战斗进度等状态的 Actor 保持固定归属，并沿用“旧方关闸、排空、必要写回成功、再发布新 assignment”的显式交接。现有 SLG 固定归属继续保留。

例如旧 owner A 正在处理角色命令，扩容后映射到 B。A 拒绝新请求不代表 A 的当前命令已经结束；B 若立即处理另一条命令，两个实例的执行会重叠。即使 Handler 不缓存状态，业务若要求跨迁移严格串行，也不能直接选择这一虚拟模式。

若希望持有状态的虚拟 Actor 也自动扩缩容迁移，需要额外决定新 owner 激活要等待什么确认、确认失败是否停止交接及如何排查；这是新增的跨进程协议，不能以最小本地运行时或 NATS Request/Reply 自动推导出来。本批到此暂停归属推广，不默认引入分布式锁、持久完成表或迁移协调器。

### 本批验证

placement-red.log 保存缺少实现的编译失败；placement-green.jsonl 与 placement-race.jsonl 分别记录真实 NATS 功能及 race 检查。新增两项本地状态测试与一项真实 NATS 用例，覆盖成员顺序、Kind 隔离、失去/重获归属、清理占位、全局 Drain 不重开闸、旧路由错误分类，以及上述跨实例重叠边界。生产实现及两个 server 业务代码未改，本批不重复引用上一批 server 测试为新结果。

本批新增试验实现 65 行、测试 167 行；累计试验实现 650 行、测试 1122 行，正式代码删除 0 行。尚无 benchmark，不提供性能变化结论。仍未执行 Git commit。

本批最终结果：试验包功能及独立 race 检查各 19 个顶层测试、含子测试 29 项通过，无跳过或失败；tx 全量含子测试 1139 项通过、39 项真实依赖用例跳过，无失败，见 placement-tx-full.jsonl。后置 ponytail-review：Lean already. Ship. 快照不额外包装管理器，不手写哈希算法，保留重获归属与跨实例边界测试。git diff --check 通过；两个 server 本批没有新增差异，累计差异仍仅为上一批 compress 补丁同步。


## T2c：新通信语义、懒激活与容量准入

本轮从三仓已有未提交试验继续实施，未执行 Git commit。`txgen`、`txclient`、`txclientslg` 工作区干净且没有本轮修改；两个 server 累计差异仍仅为 compress v1.18.5 的版本及校验和同步。正式 Actor 包、生成协议与业务服务没有替换。

### 本轮实现与边界

- 原型补齐 Tell、TellAccepted、CallFuture、Call。Tell 交给本地 NATS 客户端；TellAccepted 用 Publish + Flush，包含同实例目标也通过 NATS，仅确认当前连接服务端收到此前输出。没有订阅者也能返回成功，这不持久保存消息，不能据此认为离线可靠任务已实现。异步权限/连接错误由 NATS ErrorHandler 记录；Flush 不是逐条发布许可或持久化 ACK。确认跨过重连时返回不确定错误。
- 原型删除旧 `Future.Accepted`、出队回调及响应标记。Call 复用 CallFuture 的结果校验；本地直接入邮箱，远端用受 pending 上限约束的 goroutine 等待 SDK 独立 inbox。调用方超时或取消可以先返回，SDK 随后回收等待资源；不自动重发。Future 同时遵守创建与等待上下文的取消，本地与远端不因换一个 Await 上下文而延长调用。
- `Probe` 只读运行时元数据；`EnsureActivation` 发起或共享首次加载后立即返回；`RetryActivation` 明确重新尝试已结束的失败加载。NATS 的 Actor-Control 信封直接访问元数据，不进入业务邮箱，所以工厂阻塞或业务忙碌时仍可查询。
- 状态区分 Unloaded、Initializing、Ready、Closing、Failed。Unloaded 不代表业务不存在。容量不足返回可分类的 ErrCapacity；工厂返回的业务错误码原样保留，例如夹具的 404 表示业务明确不存在，这个数字不是 tx 新增的统一业务协议。失败缓存最多 1024 条，任意淘汰；Probe 永远不启动加载，淘汰后的新 Ensure/业务投递可能重新尝试，由业务准入再次判定，不是永久拒绝登记表。
- `ActivationLimits` 在装配阶段设置字节预算、同时初始化数量及轻量估算/压力准入函数。占位、预算检查、预留和关闸共用锁，工厂在锁外运行；额度在清理完毕后释放。压力准入只影响新激活，不中断已经接受的业务。估算必须包含加载峰值，函数不能做 I/O 或回调 Runtime；本轮只提供压力查询接点，没有在生产路径增加采样器、GC 或堆扫描。预算不能保证没有 OOM。
- 固定槽位交接夹具在旧 owner 排水成功、assignment 更新后显式 RetryActivation，再发送业务请求。此前错误 owner 的加载失败不会因下一条业务消息而偷偷重试。仍使用内存 SlotReader，未据此宣称完成真实 etcd CAS、租约或持久化交接。

例子：game 请求 `world/51` 激活，在 slgworld 仍加载数据时立即得到 Initializing，可返回“场景初始化中”；容量不够返回 ErrCapacity；只有业务加载明确拒绝才能返回“世界不存在/已结束”。game/slgworld 实例池比例独立，但本轮还不负责从多个候选实例中自动选择承载方。

### 本轮验证

新证据统一保存在 `/private/tmp/tx-nats-lazy-capacity-20260912`。所有 Go 验证使用 `GOCACHE=/private/tmp/tx-go-cache`、`-mod=readonly`；真实 NATS 使用显式 `TX_ACTORTRIAL_NATS_URL=nats://127.0.0.1:4222`，没有依赖默认跳过来声称通过。

| 本轮命令范围 | 新结果 |
|---|---|
| 原型 `go test ./internal/actortrial -count=5 -json` | 每轮 37 项，五轮共 185 项通过，无 skip/fail |
| 原型 `go test -race ./internal/actortrial -count=1 -json` | 37 项通过，无 skip/fail，无 race 报告 |
| tx `go test ./... -count=1 -json` | 1147 项通过、39 项跳过，无失败 |
| txgame/server 同上 | 236 项通过、3 项跳过，无失败 |
| txgameslg/server 同上 | 331 项通过、5 项跳过，无失败 |

以上项数包含子测试。`green.jsonl`、`race.jsonl`、三个 `*-full.jsonl` 保存输出，`full-summary.json` 保存完整跳过清单。跳过项属于需显式配置的真实 etcd/Redis/Mongo 等集成环境；没有覆盖正式集群、GD 写回故障及历史第三方 Provider race。其余三个未改动工程没有重跑全量测试。

失败到通过的证据包括：`rpc-red.txt` 的无订阅目标 no responders；`activation-red.txt`、`probe-red.txt` 的缺失入口；`future-red.txt` 的本地 Future 创建取消失效；`error-red.txt` 的错误文本重复包装。功能复核另修正测试时序：新的服务端确认不再等于子进程业务已进入，Future 结果在测试上下文取消前断言，断线检查等待 SDK 有界回收后再验证后续请求。五轮功能回归覆盖这些时序变更，未删除关键可靠性断言。

### 本轮增量 benchmark

环境：Intel Core i9-9880H 2.30GHz，32 GiB，Go 1.27.0 darwin/amd64，GOMAXPROCS=4；SDK v1.53.1；已启动的本机 Docker `tx-nats-trial`、`nats:2.14.6-alpine`。请求/响应有效载荷各 128 B，顺序单请求，不包含 GD/数据库。NATS 两个端点在同一 Go 基准进程，服务端在本机 Docker，不能称为业务跨进程或跨机器性能。

修改前源码保存于 `before/`，通过 `baseline-overlay.json` 编译 `before.test`；修改后为 `after/` 与 `after.test`。同一份共享 benchmark 分别编译，两侧逐轮交换先后顺序，6 轮、每项 1s，功能测试与采集串行。`benchmark-manifest.json` 记录二进制 SHA256 和参数，`bench-before-*.txt`、`bench-after-*.txt` 为原始数据，`benchmark-summary.json` 为中位数和范围。

| 路径 | 修改前 ns/op | 修改后 ns/op | 中位耗时变化 | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|---:|---:|
| 运行时本地 Call | 1993.5 | 1876.0 | -5.9% | 560→432 | 7→6 |
| NATS 适配器本地直达 Call | 3088.5 | 3041.5 | -1.5% | 960→856 | 10→10 |
| 经 NATS 服务端的 Call | 1775915.5 | 1735704.5 | -2.3% | 4436→5017.5 | 58→63 |

数字是六次每操作均值的中位数，不是逐请求 P50/P99。远端单轮均值范围为前 1.650–2.426 ms、后 1.679–2.313 ms，波动明显，不认定为提速，也不据此推断高并发性能。新增异步 Future 带来远端每次约 582 B、5 次分配的成本；本地移除出队确认通道减少分配。这是 T3 的资源评估输入，没有用“耗时无明显回退”掩盖分配增长。

新能力单独测六次：Initializing 状态查询中位 62.805 ns/op、0 B/op、0 allocs/op。完整激活/处理/卸载/清理循环：未限额 9307.5 ns/op，开启预算 9445.5 ns/op（+1.5%）；两者均 1536 B/op、20 allocs/op。仅表示当前单线程夹具中预算检查的增量，未模拟真实世界大小或内存压力。原始数据为 `bench-capacity.txt`，采集只存在 `_test.go` 与临时目录。

### 代码量、后置审查与下一项

相对本轮开始快照，原型实现由 650 行增至 900 行（净增 250）；测试及基准由 1122 行增至 1583 行（净增 461），合计净增 711 行。正式生产代码删除 0 行。本轮是在补功能与验证边界，不计作生产瘦身收益。

验证后执行 ponytail-review，检查新容量/状态入口、前后通信及生命周期差异、测试与累计依赖差异。结果：Lean already. Ship. 保留类型化 Future、业务容量估算 SPI、独立控制入口及关键时序测试；没有增加完成表、相关 ID 管理器、线程池、计时轮或自动故障接管。未新增相对上一批的依赖。

下一项 T2d：复用 discovery.SlotStore 的 CAS 验证多个候选实例的首次分配、容量拒绝、稳定已有归属，以及旧 owner 关闸、排空、必要写回完成后交接。之后才做 T3 的 ProtoActor 共同负载性能对照，T4 再进入正式两个示例的多实体改造。当前 txgameslg 一进程一世界槽位约束仍在，未删除 ProtoActor 或 etcd。

## T2d：稳定首次分配与实例滚动交接

本批起点六工程干净。tx=96fd69a85c9db940184f52ce7e5ddb22d128aa43、txgame=3afc0747e6a63052a2df55fc341f265f8880b5cd、txgameslg=2fd67a616e436e43a68654353eff4b43d6187761 已提交在同名 feature 分支；其余三工程 HEAD 与 T1 快照相同。本批只修改 tx，不自动提交，不再把已提交的 T1–T2c 和两个 server 依赖变化计入本批提交建议。

### 实现与边界

- `SlotAllocator` 复用现有 `SlotStore`、`SlotRouter` 和 `Directory`。只对没有 assignment 的槽位选择运行中的候选；哈希决定首次尝试的起点，新增成员不改变已有归属。并发分配由 etcd Create CAS 决定唯一目标，业务工厂必须在归属写入后才执行。
- 容量预检只返回快照，实际激活在 Runtime 锁内原子预留。写归属前的明确容量/排水拒绝允许尝试其他候选；一旦尝试 CAS，冲突读取胜方，其他失败或结果未知直接返回。CAS 后容量不足或准入关闭保留 assignment，不自动改派。预检不构成跨实例容量预留事务。
- `SlotHost` 在冷加载业务工厂前读取权威归属，拒绝错误宿主；已激活消息不增加 etcd 读取。每实例增加一个独立控制订阅，使滚动交接等待排水时，原业务接收订阅仍能返回关闭拒绝和处理必要 RPC。没有为每个 Actor 建立 NATS 订阅。
- `Handoff` 校验当前完整 assignment、目标就绪和容量，然后请求源实例关闭全体实体准入，等待已接受工作、退出回调与最终持久化收尾。只有收到成功回执后，才以现有 PromoteSlot CAS 晋升 generation 并允许新方懒加载。
- 源方收尾失败保留关闭的闸门和控制连接，允许显式重试。成功收尾在该宿主中记忆，剩余槽位可逐个交接，不重复刷盘或发送退出通知。源回执丢失不晋升；CAS 回执丢失返回未知结果，不猜测新归属、不重投业务，下次显式操作重新读取权威记录。

例子：A 承载 world/51 和 world/52，B 加入后两世界仍归 A。A 的 world/51 先有值 2，排水时尚有 +5 执行中和 +3 排队。交接先等待得到 10，再完成 Redis 收尾，最后把 world/51 晋升至 B/gen2。B 加载读到 10，退出前 RPC 成功发出；收尾首次失败时，assignment 保持 A/gen1。world/52 随后交接，复用 A 已完成的收尾。

本节点是**全实例排空后逐槽交接**，未实现运行中单世界热迁移。普通玩家/房间应映射到稳定逻辑分区，重型世界可以各占槽位；这不是为所有实体逐个写 etcd 的授权。容量是业务估算，既不按固定世界数量限额，也不保证不会 OOM。租约与实例状态更新、Watch 缓存接线、正式 GD/Procedure/Redis/Mongo/Fusion 收尾及两个示例装配留在 T4；本批 Redis 夹具只验证真实存储与协议顺序，不替代这些验收。正式 txgameslg 一进程一世界槽位约束仍在。

### 本批验证

材料目录：`/private/tmp/tx-nats-slots-20260912`。`baseline.json`、`tx-before.tar` 固定六仓起点和 tx 已提交源码；`test-summary.json` 保留逐测试 skip 清单，`summarize.py` 可从原始 JSONL 重算。所有 Go 测试使用 `GOCACHE=/private/tmp/tx-go-cache`、`-mod=readonly`。

真实试验显式设置 `TX_ACTORTRIAL_NATS_URL=nats://127.0.0.1:4222`、`TX_ACTORTRIAL_ETCD_ENDPOINT=127.0.0.1:2379` 和 `TX_ACTORTRIAL_REDIS_ADDR=127.0.0.1:6379`。使用现有本机容器，未修改容器配置或停止共享服务；etcd 与 Redis 只写本次随机命名空间，成功时校验 ownership 后删除本次键，失败时保留证据。未启用变量时跳过相关集成测试；显式配置后连接失败必须报失败。

| 本批范围 | 实测结果（含子测试） |
|---|---|
| 试验包 `go test ./internal/actortrial -count=3 -json` | 33 个顶层测试，每轮 45 项，三轮共 135 pass，零 skip/fail |
| 试验包独立 `-race -count=1` | 45 pass，零 skip/fail，无 race 报告 |
| tx `go test ./... -count=1 -json` | 1155 pass、39 skip，零 fail |
| txgame/server 同上 | 236 pass、3 skip，零 fail |
| txgameslg/server 同上 | 331 pass、5 skip，零 fail |

两个 server 仍通过 `replace github.com/ngnl47/tx => ../../tx` 引用本次工作区。全量回归中的 47 个 skip 为另需显式环境的既有 etcd、ProtoActor、Redis/Mongo 等集成入口，完整名称见材料；包通过不代表这些入口实测通过。未改动的 txgen 与两个客户端没有重复运行测试。

新增八个顶层测试覆盖：无副作用容量预检；24 个调用方共享激活；两个首次候选同时进入真实 CAS 前都不加载，只有胜方加载；满容量不建归属；新增成员与普通卸载不改归属；全实例关闸、排队工作及退出 RPC、真实 Redis 收尾失败重试；源回执和 CAS 回执丢失；CAS 后容量变化；两个交接者只产生一份新 generation。丢失 CAS 回执使用测试包装器在真实 etcd 写成功后返回超时；源回执丢失使用仅丢响应的测试控制订阅，不声称真实断网后强一致恢复已实现。

### 本批增量 benchmark

环境：Go 1.27.0 darwin/amd64，Intel Core i9-9880H 2.30GHz、32 GiB、16 逻辑 CPU，GOMAXPROCS=4。nats.go v1.53.1，Docker NATS 2.14.6-alpine；两个调用端在同一 Go 基准进程，服务端在本机 Docker，无 TLS。请求/响应各 128 B、单请求顺序完成；不含 GD 或数据库。`benchmark-manifest.json` 保存源文件/二进制 SHA256、容器镜像 ID 和参数；`benchmark.py` 保存编译及采样命令。

基线从本批 HEAD=96fd69a 的 archive 独立编译，当前版单独编译；共同 benchmark 源码逐字一致。前后版本交替执行六轮，每轮每例 1s；Call 路径计时前预热，冷激活计入完整生命周期，所有调用成功。下表为六个每操作均值的中位数，**不是逐请求 P50/P99**。

| 路径 | 前 ns/op | 后 ns/op | 变化 | B/op 前→后 | allocs/op 前→后 |
|---|---:|---:|---:|---:|---:|
| 本地 Runtime.Call | 2025 | 1998 | -1.3% | 432→432 | 6→6 |
| NATS 适配器本地直达 Call | 3292.5 | 3419 | +3.8% | 856→856 | 10→10 |
| 经 NATS 服务端 Call | 1913453.5 | 1921705 | +0.4% | 5030→5022 | 63→63 |
| 无预算完整激活/卸载 | 7265 | 7956.5 | +9.5% | 1536→1536 | 20→20 |
| 有预算完整激活/卸载 | 7234.5 | 7858 | +8.6% | 1536→1536 | 20→20 |

远端单轮均值范围：前 1.560–2.327 ms、后 1.738–2.166 ms；本地直达前 2.939–4.795 μs、后 3.026–4.496 μs，轮次波动明显，不宣称提速。新增 SlotHost 的已激活远端基准中位数 1.921 ms、5046.5 B/op、63 allocs/op，六轮断言归属读取仅首次一次；该检查使用计数 SlotReader，不把 etcd 冷读成本计入热路径数字。

首批冷激活中位数上浮接近 9%，因此只针对该路径补做六轮前后交替采样，每例延长至 3s，期间不运行远端基准。仍使用相同二进制，命令及原始结果见 `benchmark-cold.py`、`cold-*.txt`、`cold-summary.json`。

| 冷激活复测 | 前 ns/op | 后 ns/op | 变化 | B/op、allocs/op |
|---|---:|---:|---:|---|
| 无预算 | 7274 | 6863 | -5.7% | 均为 1536 B、20 次 |
| 有预算 | 7230.5 | 6930.5 | -4.1% | 均为 1536 B、20 次 |

复测范围仍重叠：无预算前 6.567–8.860 μs、后 6.752–8.335 μs；有预算前 6.539–8.281 μs、后 6.750–8.160 μs。两批中位数变化方向不同，尚无稳定退化或提速证据；保留首批上浮数据，不挑选复测数字宣称优化。全部分配次数不变。T3 仍须测饱和、并发、大量实体和跨进程总资源，本批不能替代这项决策。

### 差异、后置检查与下一项

相对本批已提交起点，试验实现由 900 行增至 1162 行，净增 262；测试及基准由 1583 行增至 2302 行，净增 719，Go 合计净增 981 行。正式生产代码删除 0 行，未新增依赖或生产采集；这是功能和关键边界验收，不计作生产瘦身收益。

后置 ponytail-review：Lean already. Ship. 检查覆盖本批代码与新增测试差异；现有 SlotStore/SlotReader SPI、故障包装器和 ownership 清理属于实际边界验证，未为删行而去掉。正确性检查与 race 单独完成，无新增公共后端层或生产采集。

下一项是 T3：固定共同负载，对 ProtoActor 与 NATS 原型测完成吞吐、P50/P99、CPU、内存和拒绝/超时，再决定是否进入 T4 两示例正式接入。本批仅对上一版原型做增量测量，不支持“已替换 ProtoActor/etcd”或“整体性能更好”的结论。
