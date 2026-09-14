# Kind 与正式实体宿主切换前的分区决策

状态：默认 CModActor、统一 Kind 与两个示例已切换，真实集群及单轮完整上下行对照通过；冻结 I2 正式入口验收已完成，正式数量配额/状态入口本轮接线，两个示例开放数量配置；以末尾数量决策为准。下文保留历次阶段记录，以末尾最新进展为准。

核对起点：tx `021bb2f`，六工程工作区干净。上一轮行为契约与示例命令执行变更已提交。

## 决策时核对的接线条件

- `modules/actor.NewKind` 仍接收 ProtoActor producer；公开 BehaviorFactory 还没有正式 Kind 消费入口。
- `internal/framework/cmod_discovery.go` 当前在 CModActor 之后启动，以便登记旧 ActorEndpoint。新宿主需要先取得 Discovery 身份、Watch 和 Ownership，不能只换工厂签名而沿用原启动顺序。
- `internal/actorhost.BindSlots` 接收身份到槽位的函数；`internal/actortrial.RoutedCaller`、SlotAllocator 同样由组合根注入函数。还没有正式普通实体分区布局。
- 原实施计划 I4 明确保留“普通实体稳定分区先确定编码与数量”，此前负载测试中的 512 个实体不是分区数。
- SLG 示例已使用 `slgworld-<正十进制 worldId>`，继续保持这个业务映射。

因此下一批应同时完成 Kind 工厂、身份路由与正式宿主接线，不能再增加只能声明而不能执行的过渡 Kind。分区布局决定持久归属记录的键，现已确认，按下述契约实施。

## 已确认布局：普通 Kind 固定 1024 个逻辑分区

这是本项目已确认的工程取舍，不是压测结果或行业标准。

1. 未声明业务稳定映射的普通实体使用 `xxhash.Sum64String(identity.Key) % 1024` 计算分区，复用已有 xxhash 依赖。输入使用原始 Key 字节，不 trim、不转换整数、不根据实例数量重新取模。
2. 普通槽位使用 `actor-v1.<RawURLEncoding(kind)>.p<十进制分区号>`。Kind 是独立命名空间；同一 Key 的 role 与 room 不共享 assignment。`actor-v1.` 前缀保留给此布局，业务自定义世界槽位沿用现有前缀。
3. 1024 是 v1 布局常量，不提供可热改的 YAML 开关。版本、哈希、Key 编码或分区数改变都需要另行制定迁移方案，不能在滚动发布中混用后自行重建归属。
4. assignment 按需创建，每个普通 Kind 最多 1024 条；一个槽位包含任意数量的实体，各实体仍有独立加载、邮箱、生命周期和容量预留。
5. assignment 指向的实例仍由 L2 SlotOwnership 首次分配。成员新增不重算已有 assignment；交接继续先排空源实例，再逐槽 CAS。哈希只计算逻辑分区，不直接计算物理实例。
6. SLGWorld 不进入这套普通分区：每个世界保留独立逻辑槽位，多个世界可归属同一实例，也可在正常交接后改变实例。房间默认使用普通分区，65 秒卸载不会删除其所在分区的归属记录。

例如 role 的 Key 得到分区 37 后，Agent 直连和 Actor NATS 调用都解析同一槽位；两条入口最终进入同一宿主、同一实体邮箱。新建 role Key 不创建逐角色归属表，只在落入尚未分配的逻辑分区时创建 assignment。

## 取舍与限制

- 1024 与实体数量无关，不是最多 1024 个角色或房间，也不是预先创建 1024 个 Actor、线程或 NATS 订阅。订阅仍按实例组织。
- 以 100 个普通服务实例为例，1024 个分区在理想均匀情况下约为每实例 10 个；256 个则约为 2～3 个，交接粒度更粗。实际归属和负载不保证均匀，已有的首次分配策略不按实际热度平衡。
- 单个普通 Kind 的分区归属最多能分布在 1024 个实例上。超过此规模或需要更细的热点迁移时，必须评估新布局，不能静默增大常量。
- 单个槽位可能积累很多实体；某个 owner 容量不足时，不把这个槽位内的某个新实体偷偷分配到别处。仍按既有约定返回容量失败，通过部署与正常交接调整容量；首版不新增自动热点拆分或单实体热迁移。
- 归属表不是完整实体目录，不从分区存在推断某个房间仍存在。房间结束后是否允许再次加载仍由业务工厂判断。

## 连续实施顺序

1. 固定布局函数与关键映射测试，绑定同一份 Kind/服务规格供普通路由、首次分配和宿主权威加载校验消费，普通 handler 只提供 Kind/Key。
2. 将 NATS 传输与正式装配所需逻辑移出试验包；保留隔离的比较/overlay 驱动和原始基线，不把压测代码迁入生产路径。
3. NewKind/MustKind 工厂承接 BehaviorFactory，删除 NewFixedKind/MustFixedKind/IsFixed；通过 WithSlotMapping 保留自定义归属。两个示例的命令适配返回业务结果，保留必要本地控制，不模拟整套 ProtoActor Context。
4. 同步接入唯一宿主和共享 gRPC 接收入口，启动顺序改为 RPC 绑定、Discovery 就绪、Actor 入口、服务就绪、running。上下行保持 gRPC 直连，Actor RPC 使用 NATS。
5. 验证身份、准入、排水、持久化收尾、NATS 连接关闭和 Discovery 注销顺序；失败不得自动接管或提前移除旧归属。
6. 两示例与必要高级能力承接后清理旧 ProtoActor 生产路径，运行真实集群回归与既定 I2 proto/hybrid 基线。未通过之前不宣称替换完成。

## 初始草案检查

本轮仅补充设计与实施计划，没有修改代码、配置或归属数据，不运行功能测试或 benchmark。

按纯文档风险简化后置 ponytail-review：未引入运行时代码或新的协调组件；保留分区契约所需的版本边界与验收条件。差异格式检查通过。

## 布局落地

唯一布局函数已实现于 `internal/actorhost.PartitionSlot`，普通路由、首次分配与宿主权威加载校验在未提供自定义映射时复用它。世界的显式映射保持不变；正式 framework 的新宿主装配仍待后续接线，多世界示例进展见下文。

## 稳定分服与分战区 SPI

用户确认：1024 是默认规则，业务需要保留稳定分服/分战区映射；运行中单实体热迁移当前没有需求。

公开 `actor.SlotMapping` 为函数型 SPI，输入 Identity、输出稳定槽位字符串。零值使用 1024 分区；显式结果不再哈希到 1024 分区，亦不在分组缺失时回退。自定义槽位应有业务命名空间，避免不同 Kind 意外共享归属；同组实体共享 owner，但仍使用独立邮箱与状态。

例如稳定业务目录把角色 1001、1002 都归到战区 10，映射返回 `role-zone-10`。分组信息必须能由所有参与路由和加载的实例一致获得；不依赖当前登录 Session，也不在消息处理中按临时状态改变。返回空值时现有归属入口返回错误，不自动分配默认分区。

当前三处迁移接线已消费同一个函数型契约；正式 Kind 声明与自动装配尚未迁入，不能把这个阶段描述为业务已经能通过现行 ProtoActor NewKind 设置该规则。正式接线时业务在 Kind 规格声明一次，由 tx 绑定所有消费者，不能要求业务重复配置三处。

## 应用级 Kind 与服务归属

已确认：一个 Kind 对应一个服务类型，一个服务类型可承载多个 Kind；例如 world、seasonWorld 均对应 slgworld。名称在同一应用的全部服务注册中唯一，而非仅本次选中服务唯一。共用服务池不意味着实体落在同一实例，各 Kind 保留自身映射与工厂。

framework 已从全部注册规格建立私有 actorCatalog，并在构造时拒绝重名；本地 actorKinds 与 CMod 依赖仍只收集选中服务。只启动 Agent 时可保留世界规格，但不调用世界服务或 Actor 工厂。目录尚未绑定到新的公开 Kind/EntityRouter 自动装配，不能把规格可用解释为默认后端已切换。

## 示例单实例承载多个世界

txgameslg 的 slgworld 服务不再要求启动时声明一个世界 logical-slot。命令入口和桶加载前通过共享世界映射查询目标槽位，检查归属实例及非零 Generation；缓存命中仍执行入口校验。桶缓存继续按 worldID、bucketID 隔离，Procedure 默认写回和 Mongo 版本条件保护不变。

例如 slgworld-490001、slgworld-490002 的 assignment 均指向同一次启动的实例 ID，该实例可处理两个世界；未知槽位或归属其他实例的世界拒绝处理。真实 Mongo 集成测试已在不传 logical-slot 的单实例上验证两个世界写入及旧版本覆盖拒绝；slgworld 单测、race 和 server 全量测试通过。

当前检查使用现行 CModSlots 读取接口；这不替代新宿主激活时的权威校验。逐世界预估内存准入、自动分配接线和正式 Kind 后端切换仍未完成；现有缓存容量限制仍按实例生效。本轮未做 benchmark，入口新增槽位查询，不宣称性能无变化。

## 清理闲置进程槽位声明

解除单世界绑定后，六工程已无业务读取 `LogicalSlot()`；删除 `--logical-slot`、`TXGAMESLG_LOGICAL_SLOT`、Ant/框架 RunOptions 字段、Context 访问器及专属装配测试，同步清理压测进程参数。保留实际承载多槽位归属的 SlotAssignment、查询与 CAS 交接测试。旧设计中的进程声明方式仅为历史记录，不再是当前 API。

## 按 Kind 解析服务池

EntityRouter 的单一 Service 字符串改为 Kind 到服务的解析函数，由装配方提供查询；首次分配和缓存寻址都会先拒绝未知 Kind。真实 NATS/etcd 回归验证同一入口处理 role→game、world/seasonWorld→slgworld，三类实体状态与默认分区归属独立；未知 Kind 不创建 assignment。原默认分区和自定义战区测试继续覆盖四种 NATS 调用及 gRPC 共用实体。

当前完成的是路由器契约，验证夹具仍显式装配；正式框架应从已有 actorCatalog 绑定查询，默认 CModActor 尚未接入。后置审查删除了仅供测试调用的预留框架查询方法，待正式装配时一并实现。正式 Kind 的工厂、业务 SlotMapping 和新宿主必须继续一起切换，不能把本轮描述为生产后端已替换。

## 缓存路由服务池一致性

接线检查复现了冷热路径不一致：Ensure 检查 owner 是否属于 Kind 对应服务，而 Resolve 的缓存路径只检查在线状态。现在 EntityRouter 使用 L2 SlotResolver.ResolveServiceSlot，未知或不承载目标服务的 owner 返回 ErrSlotRuntimeUnavailable，不发送业务、不触发重新分配。内置 ServiceDirectory 在同一读锁内检查服务归属并返回只读视图；自定义 Directory 回退到基础 LookupInstance 契约。

真实 NATS/etcd 三轮 race 回归通过，覆盖已有归属不匹配、默认分区和业务战区映射。本地只读寻址微基准五次中位数：不检查服务 109.6 ns/op，检查服务 115.1 ns/op，均 0 B/op、0 allocs/op；这是同轮两个入口的开销比较，不是端到端吞吐结论。采集仅在测试中。正式宿主装配仍待完成，本轮只修复其前置路由契约。

## 框架双入口装配与失败收尾

框架私有 startEntityTransport 已统一共享 gRPC 接收注册、NATS 连接和实体宿主所有权；使用 Discovery 给出的本次启动身份，不给业务开放连接关闭权。现有框架交接夹具改为调用该装配入口，真实 etcd 用例中两个 Server 通过 NATS 与 gRPC 操作同一实体，源实例卸载后逐槽 CAS 晋升，新实例按权威归属懒加载，源租约直到 Stop 才注销。

本轮同时补齐启动失败的收尾：已经接受消息的新宿主先关闭准入并等待卸载；超时保留资源，不允许重复 Start；Stop 可继续收尾，Run 复用原有有限重试并仍报告启动失败。最终 NATS Flush 使用框架排水期限，业务无需为 Stop(context.Background()) 额外了解 SDK 的 deadline 要求。正常写回与 Flush 失败保持可重试、不会重开准入。

框架已生产导入 actorrpc，两个示例补齐与 tx 一致的 NATS 间接依赖及校验和，未升级版本。默认 CModActor 与公开 Kind 工厂仍是 ProtoActor；当前装配入口由框架集成夹具显式调用，不能描述为默认切换完成。下一阶段继续把公开 Kind 的工厂、映射和应用目录绑定到该入口，不新增后端选择开关。

## 已确认：统一 Kind 构造入口

核对起点：tx `29c5f63`，六工程工作区干净。用户已确认以下对原计划第 3 步的调整；公开 API 与默认后端尚未修改。

原 NewKind 表示 ProtoActor 虚拟放置，NewFixedKind 表示固定宿主，两者的区别会随旧放置实现退出而消失。新架构所有 Kind 都使用稳定 assignment：默认采用 1024 分区，自定义映射直接返回稳定业务槽位。因此统一为 NewKind/MustKind，删除 NewFixedKind、MustFixedKind、IsFixed；不保留同义兼容包装。

已确认调用形态（WithSlotMapping 尚待实现）：

```go
actor.NewKind("role", roleFactory)
actor.NewKind("world", worldFactory, actor.WithSlotMapping(worldSlot))
actor.NewKind("seasonWorld", seasonFactory, actor.WithSlotMapping(seasonSlot))
```

- 工厂统一使用现有 BehaviorFactory 契约，生命周期事件仍由宿主执行。
- WithSlotMapping 是必要业务 SPI：可表达独立世界、稳定分服和分战区；未设置时使用默认布局，映射缺失不回退。
- Kind 的服务池仍来自 ServiceSpec 所属服务，不另加 ServiceName 配置，不要求调用方重复声明。
- 已有 WithIdlePassivation 保留；定时卸载和显式生命周期策略仍使用 Context.After/Post/Deactivate，不把卸载定死为空闲策略。
- txgameslg 的 NewFixedCommandKind 与内部 fixed 布尔分支同步合并到 NewCommandKind，通过 KindOption 传递映射。txgame 的普通命令 Kind 使用同一构造入口。
- 框架不再根据 IsFixed 决定 Discovery 依赖；新宿主统一依赖 Discovery 与共享 RPC，现行 ProtoActor 分支在整体切换前保持不动。
- 固定归属、槽位 CAS、世界路由和关键生命周期测试保留；只删除旧构造分支的专属断言。

构造入口、两个示例适配和默认 CMod 接线作为连续实施批次，不能先发布只有声明却不能执行的新 Kind。NewKind 返回声明错误，MustKind 对同一声明错误 panic；懒加载工厂的业务错误仍按运行时错误处理。

## 已确认并落实：初始化不附加统一执行期限

用户确认 Actor 初始化不能沿用试验夹具的一秒期限，大世界加载可能需要更长时间。本次调整限于工厂、OnInitAfter、OnActivateBefore、OnActivateAfter；Receive 与卸载回调保留新宿主原有期限，不能将本次确认扩大为移除全部回调期限。

新宿主以上四个初始化阶段使用激活生命周期派生的可取消 Context，不附加统一 deadline，回调返回时仍取消当前 Context。加载前的权威归属查询单独保留原查询期限，不向业务工厂传播该 deadline。具体数据库查询或下游 RPC 保留各自超时，调用方等待超时不撤销已开始的加载。正常排水仍限时等待实际加载、已接受消息和卸载回调结束；排水超时不提前释放激活或容量预留，也不自动交接。

例如框架试验宿主使用一秒回调期限，而世界工厂和三个激活事件分别耗时两秒，加载仍可完成。新增 synctest 回归用虚拟时间覆盖该场景，同时验证调用方等待超时、排水超时后保留容量、已接受消息继续执行、回调结束取消 Context，以及最终排水释放容量。WithTimeout 本身不能强制停止不检查 Context 的业务；工作完成仍以实际回调返回为边界。

这项调整尚未切换默认 CModActor。下一步继续已确认的统一 Kind 工厂、业务映射、两个示例及默认宿主接线，生产配置不得把调用方 CallTimeout 或 DrainTimeout 直接当作初始化期限。原始性能基线不变，正式切换后仍需同口径上下行验收。

本轮验证：新增回归先复现初始化超时，分离权威查询期限时再次验证缺少查询 deadline 的失败，最终均通过。最终 tx 全量功能回归通过（部分既有测试复用 Go 缓存）；启用本机 NATS/etcd 的 actorhost、actorrpc、framework 三包 race 回归通过。扩大检查旧 modules/actor 时，ProtoActor 成员更新、远程通信和关闭路径报告数据竞争，因此全范围 race 未通过，不将功能通过等同于 race 通过。日志分别为 `/private/tmp/tx-init-deadline-final-full.log`、`/private/tmp/tx-init-deadline-final-race.log` 与 `/private/tmp/tx-init-deadline-race.log`。

后置 ponytail-review：未发现需要删除的新增抽象；复用同一回调执行器，仅按阶段选择上下文期限，一条虚拟时间回归覆盖加载与排水边界。未运行 benchmark，不声明性能收益或无回退。未修改两个示例或客户端，未提交 Git。

## 默认后端与两个示例已接线

本轮完成 NewKind/MustKind(BehaviorFactory)、WithSlotMapping、应用目录自动装配、唯一宿主和共享 gRPC/NATS 双入口。删除生产 PID、FixedKind 与 ProtoActor 配置；旧运行时隔离为历史测试基线。Agent 通过 CModActorDirect 直连，业务只解析 Identity；初始化期限调整一并保留。

最终全量、相关 race、两客户端测试构建、两个真实 cluster-e2e 均通过；同机 Ping/Pong 对照无退化。未重写冻结 I2 基线，也不把单轮闭环测试称为固定到达率验收。完整原因、版本矩阵、性能与下一项见 [默认切换记录](../../audits/2026-09-13-default-actor-cutover.md)。本轮未提交。

## 2026-09-14：正式入口 I2 验收完成

默认后端迁移已提交（tx eef61a5、txgame c305eab、txgameslg 2b99d1f）。正式入口五轮 1000/s 共完成 20,000 次，零错误/未发送，计划到达 P99 最差 38.59ms，生成 GD 回读通过。闭环吞吐较冻结 hybrid 下降 10.53%；同二进制三组补充诊断未稳定复现额外装配损失，不宣称无退化。冻结基线保持不变，详见 [正式入口验收](../../audits/2026-09-14-formal-actor-uplink-acceptance.md)。

I3/I4 不整体勾完；下一项为正式 Kind 容量声明、逐世界估算与状态探测 SPI，继续明确业务子协程实际完成边界。暂不进入 JetStream。本次仅修改隔离夹具和证据文档，未提交。

## 2026-09-14：正式容量与激活状态入口

- 实例 `ActivationBudget` 与 Kind `WithActivationEstimate` 接入唯一宿主的已有原子预算；启动前检查本地声明，未选中服务不占用预算。
- Bee/Ant 的 CModActor 返回 `actor.Runtime`，组合现有 Messenger 和三个激活控制方法；保留小接口供普通业务注入。
- Probe 只读权威归属，不创建槽位，也不因 Watch 延迟误报未加载；Ensure/Retry 复用 L2 分配与现有 NATS 控制。普通消息热路由不变。
- 正式双实例覆盖加载阻塞期间探测、预算拒绝无归属副作用、卸载释放、业务拒绝与显式重试；原宿主并发初始化/清理可靠性测试保留。

下一项：txgameslg 的世界 GD 存在于独立实体缓存，生命周期长于 Actor；先明确逐世界缓存的持有与安全释放，再接入业务估算。当前容量只约束激活声明覆盖的资源，不能把 Actor 卸载描述为缓存内存全部归还。world Handler 当前同步等待 Procedure 完成，没有子协程池；以后引入并行工作时必须纳入实际业务完成和排水边界。本节点不新增自动迁移或 JetStream。

本轮验证及性能原始记录见 [容量与状态入口验收](../../audits/2026-09-14-actor-activation-api.md)。

## 2026-09-14 决策修订：数量配额替代字节估算

以本节覆盖此前容量估算方案：删除本轮未提交的 ActivationBudget、WithActivationEstimate 及宿主字节预留。保留三个激活控制 API。容量配置统一为 Maximum / Scope / Initializations：activation 用于临时房间，assignment 用于一世界一稳定槽位的重型世界，多个本地 Kind 共用实例上限。

不预绑定 worldId、不恢复 logical-slot。默认 etcd 复用现有 assignment 与实例规格原子检查数量，不另建配额归属表；卸载不释放世界名额，正常晋升在排水前预检、写入时再次检查，迁出成功后才释放源名额。新增空闲实例可接纳新世界，全部满载报容量不足。普通分区不按槽位数量限制 Actor。

两个示例开放 YAML 数量配置，合并调试默认无限制；独立 slgworld 可配 assignment=1，FightRoom 可配 activation=1000。世界 GD 的独立缓存和直接 Mongo 写回保持原职责，不再以估算 GD 大小作为下一项。自动扩 Pod、世界注销控制、CPU/内存阈值及故障接管不随本次实现。完成项与验证见 [本轮记录](../../audits/2026-09-14-actor-activation-api.md)。

## 2026-09-14：数量配额真实集群与正常世界交接

基于已提交的 tx bbdd202、txgame 6ab2f20、txgameslg c3c76db 继续。SLG 套件通过独立 YAML 发布 assignment=1，首次客户端请求自动分配世界，满载返回 ResourceExhausted 且无归属副作用；保留故障恢复到第二代，再验证 PrepareHandoff 收尾确认、源租约保留、CAS 晋升第三代与客户端重载/Mongo 写回。两个真实集群、SLG server 全量与控制面 race 均通过；未改冻结压测负载。详见 [本轮验收](../../audits/2026-09-14-world-capacity-cluster.md)。

下一项集中核对 I3/I4 剩余交接可靠性证据（并发 Stop、确认回复丢失、多槽部分晋升），先复用现有回归，缺口再补。单世界正常交接不等同于多世界规模验收；不整体勾完 I3/I4，不开始自动扩 Pod 或 JetStream。本轮未提交。

## 2026-09-14：交接可靠性证据收敛

复用已有并发 Stop、写回重试与身份失效回归，增强真实 NATS/etcd 双实例测试中的成功确认丢失和多槽部分晋升，并补齐 L2 晋升已提交但结果未知的存储故障注入。相关四包完整功能与 race 通过，生产实现不变，未提交。详见 [证据记录](../../audits/2026-09-14-handoff-reliability.md)。下一项为 I4 多世界实例池及跨世界访问的正式示例验收，不整体宣称迁移完成。
