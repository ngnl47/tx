# Actor 生命周期与可靠任务衔接设计草案

状态：设计与分支试验规划；未实施，未确定生产 MQ。初稿：2026-09-11；试验方向更新：2026-09-12。

最新方向：优先验证 NATS Core + 最小本地 Actor 运行时，保留本地 ProtoActor 为备选，不预设生产替换成功；etcd 仍承担身份、租约和槽位交接。分支范围、步骤和退出门槛见 [试验实施计划](../plans/2026-09-12-nats-actor-runtime-trial.md)。本草案中的现行 ProtoActor 扩展限制是对照依据，不要求新运行时复制物理激活壳。

## 1. 已确认范围

- Actor 卸载策略可由业务定义，空闲钝化只是可选策略。支持绝对截止时间、业务消息触发、主动请求等，不预建策略枚举或通用规则引擎。
- 实体生命周期回调命名为 OnInitAfter、OnActivateBefore、OnActivateAfter、OnDeactivateBefore、OnDeactivateAfter。
- 创建前允许业务按实体身份拒绝激活。实体激活与节点加入集群分层；节点入群切面退出本次决策，当前没有必须增加公共回调的业务需求。
- 战斗结束、Buff、主动卸载通过邮箱串行处理。定时器只投递消息，不在后台协程修改业务状态。
- 可靠消息完成条件：game 业务执行成功，再确认 MQ 消费；不以 GD 刷盘成功为消费确认前提。不为消息幂等要求业务 GD 新增字段。
- 已失效任务的处置与临时失败必须区分；业务终态的具体处置规则仍由项目确认。接受执行成功后结果或 ACK 丢失带来的重复窗口，不建设完成表；MQ 产品尚未确定。
- 不修改既有 RPC 正常运行语义、进程排水持久化边界或异常排水运维策略。不承诺强杀时生命周期回调执行。

## 2. 当前代码基线

六工程本轮开始均为干净工作区。tx 为 638d04a，txgame 为 b611ebb，txgameslg 为 0b46521。

| 当前能力 | 代码依据 | 本次设计要补齐的部分 |
|---|---|---|
| NewKind/NewFixedKind 接收无参 producer；AdaptKind 保留原生高级入口 | modules/actor/kind.go | 实体身份、创建准入和标准生命周期契约 |
| WithIdlePassivation 最终发送 Poison，后续允许重建 | modules/actor/passivation.go | 可由业务请求的有序卸载，不把停止等同业务终结 |
| fixedHost 收到路由消息时 Spawn 子实体，Terminated 后删除引用 | modules/actor/fixed_host.go | 关闭中的实体不能被旁路重建；创建拒绝需返回调用结果 |
| boundedMailbox 在入队前登记 Runtime 级工作，执行后注销 | modules/actor/mailbox.go、lifecycle.go | 实体级准入与收尾；不能假定已有全局计数就是实体屏障 |
| CommandContext 不暴露原生 Actor Context；高级业务通过自定义 Kind 控制生命周期 | 两示例 server/services/internal/actorcmd/command_actor.go | 普通业务能通过受限上下文使用生命周期，不直接管理 ActorSystem |
| Actor Runtime 先 StartMember，再创建固定宿主和响应接收器，最后置 started | modules/actor/runtime.go | 入群回调的能力可用时点必须明确 |
| Actor CMod 先启动，Service.Start 后才 MarkRunning | internal/framework/cmod_actor.go、server.go、service_host.go | Service.Start 不能代替入群前回调；本地入群与业务 Ready 不同 |
| Scheduler 领取、执行、成功 Complete、失败 Retry | storage/fusion/scheduler.go | 仅借鉴消费骨架；Fusion 合并存盘任务，不复用为业务任务语义 |

以上是代码与文档核对，不是本轮运行时验证结论。

## 3. 两种身份、两层生命周期

实体身份：Kind + 稳定 RouteKey，例如 room + battle-123。每次激活另外有内部激活代次，用于隔离旧定时消息、关闭请求与迟到回调。业务 RoomID 若被复用，应携带独立 BattleID/业务代次，不能用物理 PID 或本次激活代次替代它。

节点身份：Runtime 所在进程及其集群成员身份。节点入群一次，可以激活或卸载许多实体；不对每个实体重复调用入群回调。

卸载是释放本次执行实例，不是永久删除实体。是否允许重新激活，由业务准入依据原始截止时间、现存状态或已结算事实决定。不能仅凭 PID 不存在认定业务结束，也不默认维护永久禁止创建的全局表。

## 4. 实体生命周期顺序（建议方案，待评审）

```text
收到针对稳定身份的请求
  → 创建准入
  → 构造及初始化本次业务实例
  → OnInitAfter
  → OnActivateBefore
  → 标记激活
  → OnActivateAfter
  → 开始分派普通业务消息
  → 收到卸载请求，关闭本实体新业务准入
  → 完成此前已准入请求的处理或明确业务拒绝
  → OnDeactivateBefore
  → 停止并从实体路由表移除
  → OnDeactivateAfter
  → 本次激活结束，后续请求重新走创建准入
```

激活阶段允许有界排队，但不得在 OnActivateAfter 成功返回前执行业务，也不得提前确认 MQ 消费。并发首次请求在同一 owner 内共享一次激活尝试。该规则不扩展为网络分区下的全局单实例保证。

### 4.1 创建准入

建议语义：CheckActivation(context.Context, EntityIdentity) error。名称不是新增生命周期通知；它是有返回值的业务决策。

- nil：允许本次激活；终态拒绝：明确不存在、已结算或业务明确过期；其他错误：暂时无法判断，例如状态读取失败。
- 输入为目标实例收到并校验的稳定身份，不能只在发送方预检，也不能把客户端自报标记当权威结论。
- 准入检查不得扣道具、结算或产生一次性副作用；失败后可能再次检查。
- 准入与初始化衔接时仍需使用一致的业务状态，避免检查通过后战斗结束却初始化成新战斗。跨实例一致性沿用现有归属机制；需要更强业务原子性时由具体加载逻辑解决。
- 对固定 Kind，可以在身份已知的宿主路径执行准入，但慢 I/O 不应阻塞该宿主所有实体的激活。
- 对虚拟 Kind，现有无参 producer 无法直接完成身份相关准入。建议允许框架激活壳先存在，在构造业务实例和处理首条业务前检查；不承诺底层物理 PID 完全不创建。
- 拒绝应到达 Call/MQ 消费适配层；只停止壳对象让请求超时不满足契约。纯 Tell 原本无执行结果，不新增隐式回执。
- 若要求拒绝前连物理壳也不得生成，需要另行验证锁定 ProtoActor 的激活扩展能力，不先改第三方依赖或伪造支持。

### 4.2 回调参数与能力

不在本草案冻结 Go 结构字段布局。建议五个回调均接收受限的实体生命周期上下文；卸载两个回调另接收卸载原因（业务请求、策略触发、服务关闭、激活失败）。回调由每次激活创建的业务实例承担，不能把有状态回调挂在多个实体共享的 Handler 单例上。

| 回调 | 返回及失败语义 | 可用能力 |
|---|---|---|
| OnInitAfter | error；失败中止激活 | 身份、已初始化业务状态、受限 Messenger、context |
| OnActivateBefore | error；失败中止激活 | 同上；可准备订阅和定时消息 |
| OnActivateAfter | error；失败撤销激活，不能分派普通业务 | 同上；标记已激活但首次普通业务尚未执行 |
| OnDeactivateBefore | error 仅用于记录；返回错误后继续卸载，不具备否决权 | 身份、业务状态、仍可用的受限 Messenger、有界 context |
| OnDeactivateAfter | error 仅上报；不撤销已经完成的移除 | 身份、原因和清理 context；不承诺邮箱、RPC、业务状态仍可使用 |

受限上下文建议提供：实体身份、context、投递到本次实例邮箱、安排可取消的本次实例定时消息、请求本次实例卸载、现有 Messenger。概念签名采用 `OnInitAfter(ctx LifecycleContext) error`、`OnActivateBefore(ctx LifecycleContext) error`、`OnActivateAfter(ctx LifecycleContext) error`、`OnDeactivateBefore(ctx LifecycleContext, reason DeactivationReason) error`、`OnDeactivateAfter(ctx LifecycleContext, reason DeactivationReason) error`；这些类型名是草案，不是当前 API。具体方法集合须按房间示例实际调用收敛，不透出 Runtime.Shutdown、ActorSystem 所有权、数据库 Close 等能力。

原生 ProtoActor Context 不得逃逸到 goroutine；定时投递句柄绑定本次激活，卸载后取消。旧激活的计时器不能通过稳定身份误唤醒新的激活实例。

OnInitAfter 不是初始化函数本身；由工厂/加载步骤完成初始化后通知。工厂失败应自行清理未交付的资源；已交付实例的初始化或激活回调失败，由框架进入清理阶段，调用一次 OnDeactivateAfter，原因是激活失败，不调用尚未完成激活实体的业务卸载前收尾。

回调可选，未实现不调用；建议采用一个可选回调集合，不要求业务为五个方法都写空实现，也不预建多层观察者注册系统。既有原生 Kind 高级接入保留。

### 4.3 错误、超时与重入

- 生命周期回调在本实体串行边界调用。预计有网络 I/O 的回调必须遵守 context；仅设置 deadline 不能强制终止不返回的 Go 函数。
- 不采用后台运行回调、超时后并发释放同一实例的方式假装安全中断。失控回调最终由既有进程失败/运维策略处理。
- OnDeactivateBefore 允许等待其他 Actor 的 RPC，但不能同步等待自身邮箱后续消息，也不能构造 A 等 B、B 又等 A 的调用环。
- 已确认：OnDeactivateBefore 是尽力通知，RPC 失败或回调返回 error 时记录 error 后继续卸载；不自动重放，不保留等待业务重试的关闭态。是否允许卸载应在请求卸载之前，由业务在串行边界判断。回调必须配合有界 context，不能以“不阻止卸载”声称可以安全强杀任意不返回的 Go 函数。
- 进程正常关闭仍沿用既有有限重试与失败退出规则，不新增无限重试。强杀、致命故障不保证回调执行。
- 回调 panic 视为该阶段失败，须保留错误来源；不能由 ProtoActor 默认重启把永久拒绝变成无限激活循环。具体监督适配是实现验收项。监督导致业务实例重建时必须重新走准入和初始化，不能只重建 producer 绕过生命周期；旧实例异常清理不承诺正常卸载前 RPC 执行。

## 5. 卸载与邮箱协议

采用“业务结束消息”和“卸载请求”两个动作。BattleEnded 负责结算并改变业务状态，卸载请求只负责执行实例退出。

1. 所有卸载策略调用同一请求入口，不直接 Stop/Poison 绕过流程。空闲策略保留并改为使用此入口。
2. 卸载请求在邮箱中处理时，原子关闭本实体新业务准入；重复请求合并到同一次卸载。
3. 此前已经取得入队资格、正在入队或已排队的请求仍须有结果。业务已结束的请求可由 Handler 明确拒绝，不能仅因卸载而静默丢弃。
4. 使用与真实入队登记对应的实体级收尾机制，覆盖关闭前尚未完成入队的竞态；不能只追加一个标记就假定所有已准入请求都在它之前。
5. 实体级跟踪应与现有 Runtime 计数配合，避免重复注销；不额外发明第二套进程全局排水屏障。生命周期控制工作也必须被正常关闭等待，不能因其不是 Delivery 而提前结束进程。
6. 在已准入业务收尾后运行 OnDeactivateBefore；其执行及关闭后的清理期间，同一 owner 不得并发创建相同身份的新业务实例。OnDeactivateAfter 完成或报告失败后才释放激活占位。
7. Runtime 因进程排水停止新激活；现存实体的卸载回调在 Messenger 和必要依赖销毁前完成。

当前框架先排水/持久化、后关闭 Actor Runtime。不能简单把新回调挂到最终 ProtoActor Stopping：此时 Runtime 已开始清理 pending 调用，RPC 收尾可能不可用。建议在业务排水结束后、最终 CMod 持久化收尾前加入实体停用阶段；该阶段必须早于其所依赖的业务任务上下文取消和资源清理；不能复用已取消的 handler 请求 context。业务卸载前回调产生的受控变更仍归既有持久化收尾处理。MQ 的单条 ACK 依然不等待这个进程关闭阶段。

这是实施前必须验证的顺序调整，不代表当前框架已具备实体停用协调能力。未经受控入口的原生 Stop、高级私有消息、第三方监督强制停止不自动获得同等保证。

## 6. 一分钟战斗示例

业务保存或能计算 battle-123 的原始 EndAt；卸载时间为 EndAt + 5s。后续重建使用同一 EndAt，不从重建时重新计时。

- 激活时若已过卸载期限，创建准入返回业务终态；读取状态失败则返回暂时错误。
- EndAt 到达时仅投递 BattleEnded。邮箱内结算并置结束状态。
- Buff 与 BattleEnded 串行处理。严格时间限制由 Buff Handler 在邮箱内再次检查 EndAt，避免定时器调度延迟造成超时加 Buff。
- EndAt 后的五秒内，仍存活的实体可以直接返回战斗已结束；期间不把 Buff 视为可重试故障。
- EndAt + 5s 投递卸载请求，OnDeactivateBefore 可执行有界 RPC 收尾；RPC 成败均继续清理定时器和订阅。
- 卸载后的迟到 MQ 任务由激活准入识别为永久拒绝并进入死信，不重新开战。不能将“找不到 PID”或网络错误映射为战斗已结束。
- 道具返还属于项目处置规则。若要求返还，终态识别本身不代表返还完成；不能因 ACK 而默默免除该业务动作。

## 7. 可靠任务衔接（待 MQ 选型，不冻结公共 SPI）

任务信封建议只保留实际所需信息：稳定 TaskID、目标 Kind/业务身份、命令及载荷；业务需要时带截止时间和战斗代次。投递重试保持 TaskID 不变，不把物理实例地址当永久目标。

| game 处理结果 | MQ 适配动作 | 边界 |
|---|---|---|
| 执行成功 | ACK/提交消费进度 | 不等待 GD 刷盘 |
| 业务明确返回无法处理并结束任务 | ACK，并记录无法处理原因 | 属于任务完成，不伪装成 Buff 添加成功 |
| Actor 加载准入明确永久拒绝，业务处理器未运行 | 转死信后结束原消息 | 不把网络错误、排水或状态读取失败当永久拒绝 |
| 暂时错误或结果不确定 | 保留/重新投递 | 超时不能证明业务未执行 |
| 不可处理的协议/配置错误，或重试预算耗尽 | 转可追查的失败队列后结束原消息 | 转移失败不 ACK；自动原子能力按产品确认 |

消费者属于稳定逻辑消费组/订阅，实例只是执行者。没有该 Actor 并不意味着没有消费者：共享消费者可解析目标、触发准入或返回终态。不能为临时房间建立一个随 Actor 销毁就无人接管的永久独占队列。

排水停止领取新任务；在途任务等待业务结果并确认，未完成任务按 MQ 的超时/连接关闭/接管规则恢复。任务 TTL、失败队列保留期与重试期限必须一致；不能默认过期删除就满足业务处置要求。

### 7.1 已确认：不处理执行成功后的重复窗口

执行成功后结果或 ACK 丢失，不增加 Redis 完成表、其他完成表或通用去重层；不要求业务 GD 记录 TaskID，也不以另行建设幂等方案作为接入前提。MQ 自身仍可能重新投递，框架不保证任意业务只执行一次。

能观察到结果返回失败、ACK 失败等异常时记录 error，附 TaskID、目标身份、失败阶段及错误；无法观察到的丢失不承诺检测。消费端超时也不能断言业务执行成功，应记录结果不确定，不伪造成功日志。TaskID 只用于关联，不据此存储执行完成状态。

此决定不改变正常失败重投，也不要求为了避免重复而提前 ACK。选用产品自带的发送去重或事务能力，不代表其能覆盖 game 中的任意内存副作用；本期不为此追加设计。

### 7.2 已确认：保留有序能力，默认不承诺业务执行顺序

共享消费者接收任务，再按稳定身份路由到 Actor。保留产品已有的顺序能力和必要高级入口，但默认并发模式不宣称业务有序；Actor 邮箱只保证实际到达的业务串行执行，不还原发送先后。

单实例也可能并发处理，不能仅按进程数判断。要声明某范围有序，需限定同一队列/分区、单个串行执行通道、等待前条业务结果、不越过未决失败；同时明确重投、重连和所有权切换后的边界。本期不为尚未提出的强顺序场景预建分片调度器。

先发送 A 再发送 B，在并发消费下仍可能 B 先执行。若 B 必须依赖 A 的结果，应由 A 成功后的业务逻辑再投递 B，或者在同一任务/同一 Actor 的业务流程中组织步骤。单纯拆成多个任务并先后发送不能建立这个保证。

A 执行与 B 投递也不是原子动作，故障可能发生在两者之间。本期不隐式增加工作流引擎、Outbox 或跨系统事务；严格要求跨故障连续完成的链式业务，需单独定义其恢复要求。这不影响当前无序、独立任务的设计。

## 8. MQ 选型参考与初始输入

核验日期为 2026-09-11。以下是官方文档、客户端及公开项目证据，不是部署测试、市场占有率或吞吐排名。未发现可据以对整个 Go 行业做占比排名的可靠统一统计，不用仓库星数替代实际使用率。

### 8.1 Go 生态与游戏适配

| 候选 | Go 生态依据 | 本项目适配及额外用途 |
|---|---|---|
| RocketMQ | Apache 提供 Go SDK；5.x gRPC SDK 要求相应服务端和 Proxy，需匹配所选云产品版本 | SimpleConsumer 的领取、执行后 ACK、不可见期调整贴近任务模型；延迟消息可用于离线提醒、延后检查等，不能作为精确战斗计时器 |
| RabbitMQ | RabbitMQ 团队维护 amqp091-go | 手动 ACK 和逐条任务分派直观；Exchange 路由可服务业务通知、异步任务和削峰。客户端重连及拓扑恢复责任需实测确认 |
| Kafka | franz-go、Segment kafka-go 等 Go 客户端可选；Grafana Mimir 用 Kafka 作为摄取管道 | 适合日志打点、统计、独立消费者读取同一事件与历史重读。也能做任务，但按 partition 提交 offset，异步分派给多个 Actor 后要协调连续完成进度 |
| NATS JetStream | 官方 nats.go；Argo Events 实际提供 JetStream EventBus | 轻量部署、持久消费与事件分发有吸引力；评估 JetStream 而不是无持久化的 Core NATS。本次查阅的腾讯云 MQ 产品目录未列出其托管版 |
| Pulsar | Apache 提供 Go 客户端，支持不同订阅模型 | 可同时覆盖任务与保留事件流；当前没有足够需求让其优先于前两者，部署和托管功能差异留待候选验证 |
| Redis Streams | Redis 官方提供消费组及 Pending 接管机制 | 可利用已有 Redis，但接管、裁剪、失败队列等仍需组合；不能因为 Fusion 已用 Redis 就复用其合并存盘语义处理业务任务 |

生态与特性来源：

- [RocketMQ Go SDK](https://rocketmq.apache.org/docs/sdk/05go/)、[消费类型](https://rocketmq.apache.org/docs/featureBehavior/06consumertype/)、[延迟消息](https://rocketmq.apache.org/docs/featureBehavior/02delaymessage/)。
- [RabbitMQ Go 客户端](https://github.com/rabbitmq/amqp091-go)、[消费与发布确认](https://www.rabbitmq.com/docs/confirms)。
- [franz-go](https://github.com/twmb/franz-go)、[Segment kafka-go](https://github.com/segmentio/kafka-go)、[Mimir Kafka 摄取存储](https://grafana.com/docs/mimir/latest/get-started/about-grafana-mimir-architecture/about-ingest-storage-architecture/)。这些证明有实际生态，不推导某种 MQ 占据多少 Go 市场。
- [nats.go](https://github.com/nats-io/nats.go)、[Argo Events EventBus](https://argoproj.github.io/argo-events/concepts/eventbus/)、[JetStream 消费者](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/consumers.md)。
- [Pulsar Go 客户端](https://pulsar.apache.org/docs/client-libraries/go/)、[Redis Streams](https://redis.io/docs/latest/develop/use-cases/streaming/)。

### 8.2 日志、事件重读与业务任务的关系

Kafka 消息消费后可按保留策略继续保存，多个消费者分别读取；游戏可据此构建埋点分析、经济系统统计、反作弊分析输入或重建排行榜投影。这里是用途建议，不代表现有 tx 已实现这些系统。[Kafka 概述](https://kafka.apache.org/intro/)

“重读战斗结束事件重新统计胜率”与“把发奖命令再执行一次”不同。事件重读不自动重建 GD，也不等于已经具备确定性战斗回放；后者还涉及初始状态、输入顺序、随机种子和版本。RocketMQ 延迟任务、RabbitMQ 业务路由也有价值，但不为可能用到的能力预先引入多个 MQ。

若后续确需大量日志，先隔离任务与日志的 topic/队列、消费组、并发和配额；是否分集群再看压测，避免日志洪峰占满业务资源。当前不增加采集生产路径。

### 8.3 部署输入与候选优先级

已知输入：demo 单机 Docker 或 Kubernetes；生产倾向腾讯云等云厂商托管。腾讯云目录列出 CKafka、RocketMQ、RabbitMQ、Pulsar 等产品，说明这些候选有托管路径，不代表地域、版本、插件和 SDK 能力完全等同开源版。[腾讯云消息队列产品目录](https://cloud.tencent.com/product/message-queue-catalog)

根据最新评估，JetStream 提升为首轮验证候选，详见第 8.5 节；RocketMQ 与 RabbitMQ 保持对照。此前二者的比较重点仍是：前者重点验证 Go 消费接口及云端重试/延迟能力，后者作为逐条任务确认的简洁基准。Kafka 保留为日志与事件流优先候选；有日志需求不自动意味着业务任务必须选 Kafka。Pulsar、Redis Streams 本轮不先做同等深度试验。以上是建议，不是最终选型。

demo 建议先 Docker Compose 验证链路，只有需要验证 Kubernetes 调度和排水时再迁入单机 Kubernetes；单机多容器不作为高可用证据。产品选定后再核对云版 Go SDK、认证、重投、失败队列和重启接管，不能用云资源管理 SDK 替代消息收发 SDK。

### 8.4 初始时长、积压与保留假设

下列数值是供 demo 与首轮压测使用的工程假设，不是行业统计、生产 SLA 或框架固定配置。按业务场景调整，不按 MMO/SLG 名称直接推算容量。

| 场景 | 首轮时间预算建议 | 积压与失效 |
|---|---|---|
| 60s 房间内加 Buff 等即时任务 | 端到端目标 P99 ≤ 1s；首次以 3s 调用预算发现慢任务 | 业务截止时间为原始 EndAt；过期转业务终态，不等 MQ 长期重试。战斗生命周期不等于每个 Handler 要执行 60s |
| MMO 离线通知、SLG 跨世界异步操作等普通任务 | 领取至获得业务结果的预算先设 10s；通常 Handler 应远小于此值 | 按场景设截止时间；无需即时生效的任务，以故障积压 24h 为恢复演练输入 |
| 批量结算、跨大量玩家任务 | 单批先控制在 30s 内，批量数量由测试确定 | 优先分批，避免一个长 Handler 阻塞 Actor；不为分钟级工作预先占满所有消费槽 |

上述端到端预算包含本地等待、路由、Actor 邮箱等待和执行，不是单纯 Handler 时间。MQ 的领取不可见期/确认超时与业务调用超时分开：对 10s 任务可先试 30s 窗口；超长任务按产品续期或减少领取量，不把已领取任务无限排在本地。RabbitMQ、Kafka 与租约型产品的配置含义不同，选型后再映射，暂不新增统一参数层。

普通未完成任务先按至少 3 天存储容量规划，以覆盖 24h 故障和恢复余量；可追查失败任务先按 7 天，日志流先按 3～7 天评估。保留期不是消费期限，更不是到期即可静默丢弃的授权。短命 Buff 到期后仍需完成终态处置；保留到期/容量超限必须能发现并处理，云产品可配置范围另核对。

首轮合成负载可用平均 1KB/任务、持续 100 条/s、短时峰值 1000 条/s 开始，逐步加压；这是测量起点，不推测真实游戏的玩家数。单份原始载荷容量 = 每秒消息数 × 平均消息大小 × 保留秒数：100 × 1000 × 86400 = 8.64GB/天；3 天 25.92GB，若三副本则原始副本量约 77.76GB，另计元数据、索引及空间余量，压缩效果不预支。

积压恢复时间约为积压条数 ÷（可持续完成速率 − 新增速率），后者必须为正。例如积压一天的 864 万条，若实测可持续完成 500 条/s、新增 100 条/s，约需 6h 清空；500 条/s 这里只是算例，不是测得能力。任务流通常完成后可释放空间，事件流按保留期持续占用，分别计算，不把两者相加后误称实际占用。

### 8.5 NATS JetStream 专项评估

结论：适合进入本项目首轮验证，尚不直接定为生产产品。优势是官方 Go 接入、逐条确认、有界拉取和较小的 demo 部署组合；主要代价是死信转移需要适配、文件同步配置需要明确、生产托管路径需落实。不能以 Core NATS 的性能宣传替代带持久化和复制的任务链路结果。

**FunPlus 证据边界。** 用户提供其采用 NATS 的线索；本轮公开检索未找到可核验的 FunPlus 官方技术说明。不能据此确认使用的是 Core NATS、JetStream 或历史 Streaming，也不能确认承载 RPC、实时广播还是可靠任务。无需等该线索补齐才评估 NATS，但不把它写成已证实的同类案例。

**建议的最小任务拓扑（待选型）。**

```text
生产者 → JetStream 发布并取得存储确认
       → TASKS 文件流 → 稳定 durable pull consumer
       → 多 game 消费实例按本地空闲槽领取
       → 现有 Actor 路由 → 加载准入 → 邮箱业务处理
       → 业务完成 ACK / 暂时失败延后重投 / 永久加载拒绝转 TASKS_DLQ
```

一个逻辑任务池共享 durable consumer，消费进程重启后重新绑定；不按临时 Actor 创建 consumer。消息携带稳定身份，复用 ProtoActor 路由，不用 NATS 再建一套 Actor 定位或 RPC。Go 接入优先研究官方 `github.com/nats-io/nats.go/jetstream` 的明确确认与拉取消费接口，逐步领取而非无界 goroutine 分派。[官方 Go 接口](https://github.com/nats-io/nats.go/blob/main/jetstream/README.md)

TASKS 可先验证 WorkQueuePolicy：同一 subject 范围由一个逻辑 consumer 承接，多进程共享该 consumer。业务完成后可移除消息；独立审计/统计重读则另用按保留期保存的事件流，不在工作流上硬加重叠消费者。容量满时优先拒绝新发布并让发送方看到错误，不用默认淘汰旧消息造成未完成任务消失；MaxAge 到期也不能等同“已自动转死信”。[保留策略](https://docs.nats.io/learn/jetstream/retention-policies)

**正常重投与慢任务。** 逐条 ACK，不采用会覆盖其他未完成任务的批量确认。AckWait 覆盖排队与业务处理；慢任务用 InProgress 延展等待。临时失败可 NakWithDelay；普通 Nak 不自动遵守 BackOff，后者用于等待 ACK 超时的重投。实际并发同时限制本地槽位及 consumer 在途量，不把配置中的 MaxAckPending 当吞吐承诺。[确认与重投](https://docs.nats.io/learn/jetstream/acknowledgment)

**死信不是内置自动搬运。** JetStream 的 MaxDeliver 到顶会停止该消费者重投、发 advisory，原消息仍留在流中；Term 也不代表消息已存入另一个 DLQ。需要分开设计主动永久拒绝与无人成功处理的耗尽路径。[确认与重投](https://docs.nats.io/learn/jetstream/acknowledgment)

- 永久加载拒绝：适配层把原消息和原因发布到独立文件 DLQ，等 PubAck 后再 ACK 原任务。ACK 在此表示已完成死信转交，不表示业务执行成功。转存失败保留原任务；两步间宕机允许产生重复死信，沿用“不建设完成表”的决定。
- MaxDeliver 耗尽：监测并持久捕获对应 advisory，按源 stream/sequence 定位原消息，转存后再处理源记录。只有临时 Core 订阅会在处理进程离线时错过通知，不能作为唯一恢复依据；需要核对异常残留源消息的运维检查入口。捕获/转存故障及保留到期列入验收，不先建通用恢复平台。
- 正常永久拒绝优先在持有原消息时完成转存，不先 Term 再指望 advisory 拯救原消息。工作流策略和消息清理的具体 SDK 行为以选定版本验证。

上述转移逻辑由 tx 的 MQ 适配负责；game 只返回业务完成或准入拒绝原因。它属于尚未完成消息的失败处置，不是处理业务成功后 ACK 丢失，也不记录所有成功任务。[Advisory 机制](https://docs.nats.io/learn/monitoring/advisories-and-events)

**顺序边界。** JetStream 专门的 OrderedConsumer 用于顺序读取，采用临时、无普通逐条业务确认的消费模型，不能因为名称有 Ordered 就用于本期可靠任务。可靠任务仍采用 durable + explicit ACK。单通道在正常运行中逐条等待业务结果可保留到达顺序；扩成多个 worker、重投或向多个 Actor 异步分派后不承诺顺序，严格跨失败顺序另行验证。[Ordered consumers](https://docs.nats.io/learn/jetstream/ordered-consumer)

**持久化与部署。** demo 一个启用 JetStream 的 nats-server 加持久卷即可开始，单机多个副本不能抵御宿主机故障。生产初步建议文件存储、三副本分散到独立故障域，验证多数派丢失时的不可用及恢复。PubAck 与每条 fsync 到物理盘并非同一保证；官方说明文件同步存在间隔，必须用最终同步策略做测试，不用默认异步落盘结果宣称任意掉电不丢。[复制及磁盘同步](https://docs.nats.io/learn/clustering/replication-and-r3)

Kubernetes 有官方部署工具路径，但选择它不免除持久卷、故障域和恢复验证。[Kubernetes 部署](https://docs.nats.io/learn/deployment/kubernetes)

生产可考虑云上自建或商业托管。Synadia 提供托管 NATS；这不等于已确认腾讯云指定地域/VPC 可直接使用。需要落实网络位置、延迟、数据存放和服务支持；否则 JetStream 的 Go 集成便利，要与 RocketMQ/RabbitMQ 的目标云托管便利一起权衡。[Synadia Cloud](https://docs.synadia.com/cloud)

**额外游戏用途。** subject 路由可承载内部通知，JetStream 的独立事件流可供统计重读。KV/Object Store 等能力只作候选认知，不替换已有 Redis/Mongo/发现组件；Core request/reply 不替换现有 ProtoActor RPC。跨世界侦查沿用现有路由，MQ 只承载需要脱离当次连接继续处理的任务。

**选型验证范围（未执行）。** Go 发布确认→跨实例 Actor 分派→业务后确认；无 Actor 永久拒绝转存；临时加载失败恢复；consumer 重启接管；DLQ 不可写及转存后崩溃；MaxDeliver advisory 处理离线及恢复；单通道与多 worker 顺序边界；真实文件同步策略下的 P99、CPU、分配、磁盘、积压恢复和副本故障。不预报比其他 MQ 快多少，采集与故障注入留在独立测试环境。

## 9. 节点入群切面退出本次决策

可能的需求及当前判断：

| 需求例子 | 是否必须增加节点入群回调 |
|---|---|
| 入群前校验配置、密钥，或预热基础依赖 | 通常属于启动前置条件，可先评估现有 CMod 依赖与启动阶段；不必增加围绕 StartMember 的公共钩子 |
| 启动后订阅业务通知、向控制面报告就绪 | 属于 Service 启动/Ready 契约；本地入群不代表全部 Service 已可处理业务 |
| 按集群成员变化重算跨节点世界分配 | 需要持续成员事件和业务归属协调，只有本节点 JoinBefore/After 无法解决，当前也未提出该功能 |
| 在真正发布成员身份前执行专属登记协议 | 理论上需要精确切面，但当前没有具体协议或调用方，不为这个假设预留接口 |

因此本期只设计实体生命周期，不新增或冻结 OnClusterJoinBefore/After。现有启动流程是否满足具体业务，要在出现真实调用方时核对；不声称 Service.Start 能代替入群前时点，也不承诺本期修改集群发现和 Provider 关闭行为。

## 10. 待评审建议的具体解释

已确认：五个实体回调名称、自定义卸载、创建准入、邮箱串行、绝对截止时间及重建校验、ACK 不等待 GD 刷盘、不建完成表、默认无业务顺序承诺但保留产品有序能力。部署输入已补齐；时间和容量是第 8.4 节的待验证假设。

### 10.1 先创建框架激活壳，再决定是否创建业务实例

这里区分底层 ProtoActor 实例与承载战斗状态的业务对象。现有虚拟 Kind 的 producer 没有身份参数；框架可能要先获得一个底层实例，才能在收到身份后判断 battle-123 是否已结束。

建议允许该框架对象短暂存在，但在准入通过前，不加载成一场可运行的战斗、不执行 Buff、不调用激活成功回调。若战斗已结束，返回明确业务终态并清理框架对象；若初始化中途失败，清理已交付资源，不能留下半激活实例。

要决策的是：是否允许这种内部实现。业务仍获得“禁止已结束战斗重新运行”的保证；如果要求连底层 PID 都不能出现，需要额外研究或改动 ProtoActor 激活机制。建议接受前者，避免为不可见内部细节扩大范围。

### 10.2 已确认：卸载前回调不否决卸载

房间退出前有机会 RPC 通知其他 Actor；失败记 error 后继续卸载，不自动补发、不保留业务失败等待态。业务如果需要判断是否允许卸载，应在发起卸载之前处理，而不是在 OnDeactivateBefore 中反向中断。该决定不放宽已有持久化安全收尾规则。

### 10.3 业务结束、准入拒绝与死信

业务明确返回“无法处理，本任务结束”，记录原因后 ACK；若项目承诺返还或其他处置，应由业务完成相应动作再结束。不能把任何 error 都解释成这个明确结果。

本期特别处理：Actor 因永久加载条件不满足而拒绝激活，Handler 根本没有执行，消息进入死信。临时状态读取失败、暂时无 owner、排水拒绝则重试。准入结果必须区分这两类；不能看到 PID 不存在就推导永久拒绝。

常规处置流程为：分类 → 隔离不可处理消息 → 告警/可追查 → 修复后有选择地重投，或确认失效后清理。终态已知无需耗尽全部重试；暂时错误超过有限预算也可进入死信。按失败原因、消息年龄、数量观察，预期过期任务可以汇总，避免每个过期 Buff 都触发人工告警。RabbitMQ 支持拒绝不重新入队并经 DLX 转发；要求转移期间保留原消息时需核对 quorum 的 at-least-once dead-lettering 配置，不能假定默认安全转存。[RabbitMQ 可靠性](https://www.rabbitmq.com/docs/reliability)、[Quorum 死信机制](https://www.rabbitmq.com/docs/quorum-queues)

死信保存原任务、稳定目标、拒绝原因、源消息定位和投递信息，不是成功完成表。修复配置后可有界重投；已结束战斗不应机械重投 Buff。归档、清理或业务补偿由项目/运维决定，不默认重开房间，也不自动引入返还系统。死信仍需有限保留和到期处理规则，不能当无限仓库。

后续验证清单（本轮未运行）：

- 虚拟/固定 Actor：准入拒绝可观察、并发首次激活、各回调顺序与失败清理、重建身份一致。
- 卸载：入队竞态、关闸后明确拒绝、已准入请求均有结果、重复卸载合并、旧计时器不影响新实例。
- 卸载前 RPC：普通卸载可调用；全进程排水时本地资源仍可用、远端拒绝正确返回；失败不伪装成功。
- 60s 结束/65s 卸载场景采用可控时间源；计时延迟、迟到 Buff 与同房间新战斗隔离。
- 候选 MQ：发送确认丢失、业务成功后确认丢失、消费者崩溃、Actor 重建/迁移、终态无积压、慢任务及失败队列转移故障。
- 实施涉及热路径时做前后 benchmark；采集与故障注入仅在测试/临时目录，不能进入生产路径。

已补充分支试验计划以支持选型，尚未冻结通用 MQ SPI 或承诺上线；生产替换在阶段验证与评审后决定。
