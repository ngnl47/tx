# Tx storage metadata implementation plan

本文是 `github.com/ngnl47/tx` 存储元数据与运行时能力的实施计划。

公共 API、配置和使用示例以根目录 [`README.md`](../README.md) 为准；内部模块职责、状态机和技术选型以 [`design.md`](design.md) 为准。本文只记录实施状态和后续计划。架构质量以 [`architecture_audit_standard.md`](architecture_audit_standard.md) 为重复审计基线。核心模型、公共 API、存储/RPC 基础设施发生重大变化或准备发布重大版本时，必须重新执行审计；审计结论确认后再进入重构实施。

目标是提供一套独立、稳定、可由 `txgen` 生成代码消费的 Go runtime 能力：

- 使用 Go 注释作为元数据主入口，保持类型定义和存储定义靠近。
- 使用 Go AST 读取 Go 类型结构和注释，建立注释元数据与 Go 类型之间的绑定关系。
- 语法形态使用紧凑注解写法，输入侧统一使用 Tx 语义。
- 不支持独立 schema 文件；存储元数据只来自 Go 注释、struct tag 和 AST 扫描结果。
- 运行时元数据只允许初始化期注册；不支持隐式运行时追加 tag。
- `fusion` 作为 Redis 热数据与 Mongo 冷数据组合模式名。
- Redis key 默认使用 `shardBy` 作为 Redis Cluster hash tag。
- Fusion 延迟任务抽象成可替换任务后端。
- 跨包行为和公共 API 回归放在 `tests/`；未导出算法、脚本与 driver glue 的白盒测试允许和实现包同目录，不能为了测试扩大生产 API。

## Current Stable Decisions

后续实现、代码审查和模型切换后接力均以本节为准；如果代码、README、设计文档和本节冲突，先停下来核对，不要按记忆继续改。

- `tx` 是 runtime 基础库，不依赖 `txgen`；`txgen` 生成的代码只依赖 `github.com/ngnl47/tx`。
- 代码审计与瘦身遇到通用基础能力时，先评估维护活跃、语义契合、性能可验证且依赖边界清晰的成熟库；只有现成库无法满足业务语义、会扩大核心依赖面或基准证明不适合时才保留自研。引入前必须明确版本维护、许可证、可观测性、替换成本和最小封装边界，不能把业务模型或生成代码契约交给第三方。
- `util` 是面向业务的可选工具包，不能因当前库工程未引用而删除。`EventBus` 保留反射订阅、同步/异步、一次订阅和 transactional async 语义；普通异步事件允许多协程并发执行，只有显式 transactional async 才按订阅者串行。它不参与 Fusion、事务或有状态运行时的顺序控制。`DelayQueue` 是单个业务场景的严格有序命令执行器：同到期任务按添加顺序、单 worker 执行并支持取消；它不承担 Redis 延迟任务、持久化回写或全局调度职责。后续分别评审成熟库，只有完整语义覆盖并通过基准与并发回归后才替换。
- 本地 TTL 缓存不是当前 tx runtime 的需求，不能为了迁移工具而提前引入依赖；将来业务项目明确选择本地 entity/cache 层时，再单独评估成熟库与项目级装配方式。
- `tx/log` 是基于 `log/slog` 的最小日志门面。项目启动时通过 `SetDefault` 注入 handler、级别和格式；业务项目按需定义 `var RoleLog = log.NewLogger("role")` 这类包级命名 logger。`NewLogger` 是延迟代理，后续替换 default logger 后仍使用新配置；tx 不维护项目级 logger registry、名称枚举或自动包名推导。
- `txgen` 负责生成 `GD*.go`、`GD*Store.go`、cmd、字段名函数、字段编号和类型元数据；业务模板和集成样例放在 `txgame`。
- `tx/server` 统一持有进程级 Storage、共享 gRPC Host、Discovery、Actor、Transaction、健康探针、后台 worker、服务注册和反序关闭；`txgame/internal/app` 只作为项目 composition root。`agent`、`msgq`、`game` 始终注册为可选服务，仅 `--services` 决定当前进程启动集合。YAML 只保存服务静态运行参数，agent/msgq 配置转换延迟到对应服务被选中时执行；未选服务不校验其专属参数，选中服务的配置错误必须启动失败。
- `server.Server` 是上述进程资源的唯一 owner；完整 runtime 为未导出实现。`server.Service.Start` 和
  `server.WithSetup` 只接收受控 `server.Context`，其能力分别收敛为 `lifecycle.Tasks`、`rpc.Registrar`、
  `discovery.Directory`、`actor.Messenger`、`txn.Executor` 与 `server.Storage`。服务不能取消根生命周期、关闭共享
  Host、停止 Actor/Transaction、替换 discovery 快照或接管存储 client。门面返回专用 wrapper，不能通过类型断言
  取回 owner API。
- `Context.Advanced()` 是第一版受控高级 SPI，只允许复用 `redis.ScriptRoutes` 和
  `discovery.SlotStore`；前者不暴露 Redis client 关闭/连接池所有权，后者只提供 assignment 与 generation CAS，
  不暴露 etcd client 或成员 lease。普通 handler 不持有 Context，项目组合根应继续包装为 `db.Save(role)` 等 facade。
- 可选标准服务不能直接导入同级服务实现。共同能力必须依赖更低层模块契约或下沉到中立 adapter；当前
  `server/adapters/msgqredis` 负责将已经装配的 Redis 路由适配为 `msgq.SessionStore`，`agent` 与 `msgq` 分别消费它，
  不再形成 `agent -> services/msgq` 实现依赖。该规则由架构回归测试持续约束。
- `GD*` 是纯数据对象；`GD*Store` 是独立、完整、可组合的变更追踪结构，绑定 `GD*` 后才具备读值、修改和投影意义。Store 只持有业务数据引用，不保存第二份业务数据。
- `GD*` 可以懒持有默认主 Store，作为 `role.SetName(...)` 这类代理入口；生成 Store 也可由框架显式创建并绑定 GD。业务不复制主 Store；下行捕获由生成的独立 `GD*StoreView` 承担。
- Store 树按持久化路径组织：父 Store 持有复合字段、map key 或 slice index 对应的子 Store；子 Store 不向 `GD*` 写入父回调或父路径。`VisitChanges(visitor)` 可从整棵 Store 树同步遍历规范化变更；Redis、Mongo、Fusion、cmd Provider 与 Runtime 直接消费 `change.Source`，不创建中间变更批次。
- 同一个 `*GD...` 可以出现在多个根对象、集合字段或集合 key 中。每条路径拥有独立 Store 节点和 dirty 状态，业务数据引用可以共享；框架不维护全局反向引用表，也不自动把一次路径内修改扩散到其他引用位置。
- `store.View(configure)` 是首批同步捕获边界：允许回调通过 `Touch/UnTouch` 调整选择，并立即把所选路径在当前时刻的值复制到生成的稀疏 `GD*StoreView`。selector 只用于选择路径，调用 Set/Clear/Replace 或集合增删改会立即 panic；返回的 StoreView 不持有根 GD 或主 Store，也不实现持久化 Store 接口。
- `GD*StoreView` 由稀疏 `GD*` 值、txgen 为该类型生成的强类型视图状态树和同源身份组成。普通字段使用定长 `ops[fieldID-1]`，集合使用强类型 key/index map，生成对象子节点按需创建；不保存通用完整 `change.Path` 集合。标量直接复制；被选中的指针、map、slice 与嵌套对象只复制必要分支；整字段 Replace/Clear 才复制整个字段。业务用 `view.Role` 查询捕获值，用生成的 `Has<Field>()` 或 `Contains(path)` 区分“选中了零值”和“没有选择”。StoreView 生成后不再受根 GD 后续变化影响。
- 同一帧或同一锁定周期内需要累计多批广播变更时，若根 GD 仍处于当前所有权边界，优先使用 `view.Capture(store, configure)` 直接把同源 Store 的当前路径和值追加到既有 StoreView；它不清理源 Store。后续批次已经创建为独立 StoreView 或进入队列时，再使用 `view.Merge(next)` 合并两个独立视图；`Merge` 不修改 `next`，合并后它仍可读取。两条路径都遵循后批次覆盖和祖先吸收规则，不同主 Store 来源返回 `change.ErrValueMismatch`；只有 `Merge` 完全不再读取可变根 GD。
- 业务输入模型和协议字段不能直接声明生成的 `*Store` 类型；Store 只用于框架持久化和 Provider 投影输入。
- 业务 facade 和框架 API 分层。无状态 handler 的 Store 拦截路径使用 `db.Save(role)`，snapshot diff 路径使用 `db.SaveChange(role)`；有状态服务通过项目装配的 `txn.Runtime.Call(ctx, procedure)` 进入 Procedure 运行时，由 Runtime 在后续 Flush 内部取得 Store 并调用 `storage.Save`。
- `txgame/server/internal/db` 已提供泛型 `db.Save(role)` facade：它使用服务启动时装配的根 context 与 writer，并只在 facade 内部获取生成数据对象的 Store。
- 通用 `db.Create` facade 不作为当前目标。新建对象、默认字段初始化、初始资源发放属于业务流程；框架层保留 `storage.Create` 表达 upsert/首次缓存语义。
- 后续单机事务能力放在可选的 `tx/modules/txn`，`tx` 核心和普通 Store 路径不依赖该模块。没有进入 `txn.Run(...)` 时，不创建 undo log、不记录旧值，也不增加事务运行时开销。
- `@transaction` 是生成期类型能力声明，不是常驻事务开关。它独立于 `storage` 注解，为持久化 GD 或纯内存 GD 生成统一的本机 `LockKey()` 和 `LockTarget()`；`scope` 是一致性域分类，`keyField` 是域内对象键，`order` 是可选的非负锁层级，默认 `0` 且较小值先获取。
- 固定锁集使用 `txn.Locks(guild, role)`；前置查询后需要循环收集锁时使用 `txn.NewLockSet().Add(...).Build()`。两者只负责收集，重复锁、顺序、完整性和超时仍由 `txn.Run` 统一处理。
- `txn.Run(...)` 只处理单机内存事务：对输入锁对象的锁键去重、按逻辑 Key 全序获取本机锁，并在回调期间临时绑定 undo recorder。锁获取受传入 `context.Context` 控制；超时或取消时释放本次已获取锁，回调不会开始执行。它不解决分布式锁、跨实例一致性或 Redis/Mongo 分布式事务。
- `txn.Config.Debug.CheckCleanLockTargets` 是可选调试期检查。默认关闭；开启后，事务在获取全部锁后检查每个可检查根 Store 是否已有变更，若有则释放锁并拒绝进入回调，防止事务外 Store 修改混入本次提交边界。项目负责从自己的 yml 配置转换并调用 `txn.Configure`。
- 可复用事务逻辑使用 `Procedure`：根 `txn.Call(ctx, procedure)` 在锁定完整目标集后执行；事务内只能通过 `tx.Call(child)` 调用子 Procedure，并建立 savepoint。子 Procedure 的目标必须是根事务已持有锁键的子集；发现新目标时返回包含 `A -> B` 调用路径的明确错误，不等待形成死锁。直接嵌套 `txn.Run` 不是业务 API，应直接拒绝。
- 根 Procedure 需要在进入事务前声明自身及子 Procedure 的完整锁键并集。若锁键依赖查询结果，先做只读预取，再按完整键集加锁并重新校验；不支持事务运行中动态扩张锁集。
- 需要在根事务提交后开启独立事务时，通过 `AfterCommit` 调度新的 `Procedure`，不与父事务共享原子性或锁。
- 单机事务只保证经 Store 受控 API 发生的修改可回滚；裸字段赋值和 snapshot diff 不参与事务回滚。undo log 属于一次事务的临时状态，不写入 GD，也不作为 Store 的长期业务数据。
- 事务锁对象由生成代码在 GD 与 Store 侧提供：业务通过 `txn.Locks(guild, role)` 收集，GD 的 `LockTarget()` 只代理默认 Store，不复制业务数据。字段 setter、map/slice 的 `Set/Delete/Replace` 和生成对象子 Store setter 在事务内登记 undo；事务成功后的存储写入、消息发送或后续业务动作通过 `AfterCommit` 等机制调度；这表示内存事务已提交，不等价于 Mongo 已可靠落盘。
- `txn.Runtime` 是有状态运行时的标准搭档。它按 `LockKey` 聚合脏 Store；`Runtime.Call` 在事务成功后登记 `DirtyTargets`，不执行 I/O；`Runtime.Flush` 逐目标取得同一把本机锁，通过 Runtime 私有 visitor 装饰器把容量计数并入 adapter 的正常遍历，成功才清理变更。未超 `MaxRootChanges` 时只遍历一次；超预算时第一次遍历在外部 I/O 前中止，Runtime 明确重写 Store dirty 树为顶层 Replace 后再保存。Runtime 不保存业务副本，不把 Store 作为异步队列项。
- 有状态运行时的 Store dirty 状态是按路径覆盖的集合，不会因同一路径重复修改无限追加；但未成功 Flush 前的不同集合 key/index 会持续累积。`map[K]*GD...` 和 `[]*GD...` 会缓存已访问元素的子 Store，但另行维护 dirty 子路径索引，变更遍历与清理不再扫描全部缓存子 Store。`RuntimeConfig.MaxRootChanges` 限制单个 pending root 的变更路径数量；Runtime 只在 Flush 的持锁遍历点检查该预算，超过时清理该 Store 树的细粒度 dirty 状态，并将当前值收敛为全部可更新顶层字段的 `Replace` 变更，跳过 BSON `_id`，记录一条告警后继续服务，避免每次事务提交递归遍历整棵树。txgen 生成 Store 实现 `storage.CollapsibleChangeStore`；配置非零预算时，自定义持久化 Store 若不具备相同能力，`Runtime.Call/MarkDirty` 必须在修改或登记前返回 `ErrChangeCollapseUnsupported`，不能让预算静默失效。`RuntimeConfig.HeapSoftLimitBytes` 按 `HeapSampleInterval` 低频读取进程 `HeapAlloc`，仅在超过阈值时记录告警；它不能作为拒绝业务请求的根因。`RuntimeConfig.MaxPendingRoots` 仅是可选硬保护；满额时才拒绝新增 root。部署项目负责设置 Go 的 `GOMEMLIMIT`，tx 不修改全局 runtime 内存限制。预算限制的是 dirty 路径，不负责限制业务对象本身或干净子 Store 缓存的大小；setter 内不得强制 Flush 或 I/O。
- 无状态服务不使用 `txn.Runtime`：请求内临时加载 GD，直接由项目 facade 调用 `db.Save` 或 snapshot `SaveChange` 写 Fusion。启用有状态运行时后，所有写操作都必须通过 `Procedure + txn.Runtime`；读取不要求进入事务。
- 持久化 adapter 的跨模块协议是 `change.Source`：它组合 `VisitChanges(visitor)` 与 O(1) 的 `HasChanges()`，避免 adapter 为判空重复遍历。txgen 为普通字段生成定长 `ops[fieldID-1]` 槽位，为集合元素生成 `ops[key/index]`，并用 `dirtyChildren` 索引真正变脏的子 Store；同一位置重复修改原地覆盖。`Document(kind)`、Redis、Mongo、Fusion 与 Runtime 都直接消费规范化状态；Go/JSON/BSON/Msgpack 四套字段名由各自输出边界显式选择。
- 每个生成 `GD*` 只内联一个私有 `state` 指针。Store 引用、Store/snapshot 模式和 snapshot 基线放在按需分配的 sidecar 中；只构造、编解码或传递纯数据对象时不创建 sidecar。`Copy()`、`ResetStore()` 和完整加载替换会显式解绑旧 sidecar，避免复制 Store/快照运行时状态。
- 访问事件只传递 `Path + Op`，不持有字段值；生成 Store 会在输出边界按 Path 读取当前值。后续如需性能优化，只做局部 fast path，不提前引入自研 value 编码。
- Redis/Mongo 写入边界要求 `ValueResolver` 能解析每条 Set/Replace 变更的当前值；`(nil, true)` 是合法业务空值，`(_, false)` 必须显式返回错误并保留变更，不能静默写成 `null`。Clear 必须按完整路径解析实际 map/slice/array 类型并构造同类型空值，不能按根字段猜测嵌套集合类型。
- 所有外部协议返回值都必须在 adapter 中显式枚举：仅已定义的成功、冲突或未命中状态可继续处理；空值、未知值和不完整结构一律返回错误，不能以 `nil` 兜底或跳过异常片段。
- `Op` 是 adapter 输入协议的一部分；生成式 visitor 只接受 `Set/Replace/Delete/Clear`，未知操作必须返回错误，不能隐式按 Set 处理。
- 核心命名按职责收敛：`Source`/`Visitor`/`VisitChanges` 只表达同步变更遍历，`BuildDocument` 才表达文档构建；`Path` 是结构化字段路径，`DocumentPath` 是输出边界字符串路径；`Op` 表达变更操作，生成 Store 的操作槽叫 `ops`，`dirty` 只保留给布尔脏状态与脏子节点。字段命名体系使用 `FieldNameKind`，Redis 直接选择 JSON 名；删除文档路径使用 `Document.Delete`，容器清空仍使用 `OpClear`。带口径的配置显式使用 `TTLSeconds`、`MaxBacklogMessages`、`HeapSoftLimitBytes` 等名称，不为建设期旧名称保留兼容入口。
- `Path` 只接受 `1..MaxPathDepth` 的深度、非空字段编号和已定义 key 类型；外部 Source 或高级 API 输入非法路径时，通用文档、Redis、Mongo 与生成 resolver 都必须稳定返回 `change.ErrInvalidPath`/解析失败，不能数组越界 panic，也不能忽略 key 并退化为父字段写入。
- `storage.DocumentLoader` 的结果必须一致：`exists=true` 对应非 `nil` 文档，`exists=false` 对应 `nil` 文档；批量结果逐项遵守同一契约。adapter 或外部实现返回矛盾组合时立即失败，不能被误当作未命中继续回源。
- 项目 yml 读取必须使用严格字段校验：`app/runtime/storage` 的未知节点或属性在解析阶段报错；Redis/Mongo 路由表的 routeType 名保持项目自定义，但必须是非空、无首尾空白的唯一标量，规则对象只允许 `min/max/shards`，同一 routeType 的规范化区间不得重叠；每个 endpoint 必须非空且不含首尾空白，具体 URI 协议与连接参数由项目适配层解析。YAML anchor 统一放入项目显式声明的 `aliases` 节点，不能依赖被静默忽略的根字段。
- `storage.DocumentReplacer` 的输入文档必须非 `nil`；空 map 代表合法空对象，`nil` 返回 `storage.ErrNilDocument`，不允许以 no-op 伪造写入成功。延迟删除需要重排而队列未装配时同样返回错误，保留任务重试机会。
- 所有实际存储 I/O、路由和锁操作必须先校验 `storage.RecordKey`：`ID` 不得为 nil、typed-nil 或空白字符串；当存储元数据声明 `routeBy != idField` 时，`ShardKey` 必填。缺失 `ShardKey` 返回 `storage.ErrInvalidRecordKey`，不得回退用 ID 选择 Redis/Mongo endpoint 或生成 Redis hash tag。
- 非空 Source 经 Redis/Mongo 边界转换后必须至少产生一个有效 patch/update；否则返回 `storage.ErrNoEffectiveChanges` 并保留 Store 变更。Mongo upsert 仅剩不可变记录 ID 时同样失败，不能清理 dirty 后伪造已创建文档。
- 涉及生成式投影、帧内合并、集合递归、批量读取、Redis/Mongo 写入或状态刷盘等热点路径的结构与算法调整，必须先补可复现 benchmark，并记录对象规模、嵌套深度、变更密度、耗时与分配结果；只有数据证明收益覆盖复杂度时才引入优化，避免凭直觉堆砌机制。
- 字符串路径只在输出边界生成，例如 `Document(kind)`、Redis JSON patch、Mongo update document 和 cmd Provider body。`VisitChanges` 在输出边界之前只传递定长 `Path` 与 `Op`。
- 确定性帧内累计由生成 StoreView 的 `Capture` 和 `Merge` 完成：同位置后写覆盖先写，后发生父字段 Replace/Clear/Delete 清理更早子路径，Clear/Delete 后出现子路径时提升为父级 Replace。`Capture` 面向同一根对象所有权区间，`Merge` 面向已经创建的独立视图批次。Store 状态树自身按位置覆盖，不需要额外批次压缩；超深路径在生成 Store 捕获前提升为最近安全祖先 Replace。框架不猜测需要数据库基线的“新建后删除抵消”，跨对象结果应先分别创建独立视图或编码载荷，再由调用方组合。
- Fusion 是完整装配的存储策略，不保留半成品 no-op 兼容路径：运行时必须同时具备 Redis change/document/load-lock/delete 能力、Mongo load/delete 能力和延迟队列；`Strategy.Validate` 负责汇总校验，业务工程启动时必须在 Redis/Mongo endpoint 探活后调用它，任一依赖或连接不可用即失败退出。非空变更 的热写还会在任何 Redis I/O 前检查 Mongo document loader 已装配，不能把只有 Redis 与队列的半装配 Strategy 当成可写实例。运行期不在每次 Fusion 调用前额外 Ping 两端；项目运行时通过后台探针和真实 I/O 错误维护 Redis、Mongo 两个独立健康状态，Fusion 业务读写在进入底层 Strategy 前要求两端都健康，即使 Redis 能命中热数据，Mongo 已知不可用时也直接返回 `error`。热写成功写入 Redis 并入队后不等待 Mongo；Mongo 短暂不可达时既有任务保留，由 worker 的 at-least-once 重试在恢复后回写。Redis/Mongo 短时服务波动只能作为业务调用的 `error` 返回，runtime 不得把通信失败转换为 panic、静默成功或后台 goroutine 退出。Redis 写入与任务入队是跨组件的两步操作，不承诺跨路由原子性；若 Redis 已写而任务入队失败，写入 API 必须返回错误且保留 Store 变更，`txn.Runtime` 下次 Flush 会重试；无状态 facade 必须把错误交给上层重试或失败处理，不能把本次调用视为已完成持久化。
- `tx/server` 运行时不在每个业务操作前 Ping。启动时先同步探活；后台探针及 guarded reader/writer、延迟回写 handler 观察到的真实 Redis/Mongo I/O 错误会关闭对应 backend 的健康状态。mode-aware writer 按 `TypeSpec.Storage.Mode` 选择 Redis、Mongo 或 Fusion，不把单后端类型误送入 Fusion；Redis 类型只要求 Redis 健康，Mongo 类型只要求 Mongo 健康，Fusion 类型要求两端都健康。新的 Fusion 读取、`txn.Runtime.Call` 和 `db.Save` 返回 `server.ErrStorageUnavailable`，不执行后续业务或存储 I/O。错误分类发生在具体 go-redis/Mongo driver 边界；通用 adapter 只传播标记。RecordKey、变更、字段映射、解码、未知脚本结果等请求或协议语义错误，以及调用方 context 取消，不得污染全局健康状态。已有 pending root 不丢弃；延迟任务 handler 仍执行并把失败返回 scheduler，由 lease/retry 机制保留任务，恢复后继续回写。Probe 以 backend generation 防止较早启动的成功探针覆盖期间发生的新故障；只有没有观察到更新故障的探针才能重新开放对应 backend。外部服务异常一律通过 error 传播，不得 panic。
- Fusion 显式删除要求 Redis 与 Mongo 删除器及 Redis 记录锁均已装配，并在任何删除 I/O 前完成检查；缺少任一组件时返回 `ErrStrategyDependenciesNotConfigured`，不删除 Redis 热数据或 Mongo 文档。显式删除、延迟保存与延迟删除使用同一记录锁：保存任务在锁内读取 Redis 再回写 Mongo，删除在锁内删除 Redis、Mongo，避免已读取的旧热文档在删除后重新写回 Mongo。完整装配后仍按 Redis、Mongo 的顺序执行，运行时连接错误原样返回给调用方。
- `OpSet` 用于标记当前字段值；`OpReplace` 用于集合、结构体指针和集合元素的覆盖赋值。`SetItems(value)`、`ItemsStore().Set(key, value)`、`SetAt(index, value)` 都遵循 Go 赋值语义：持久化时不保留旧值中未出现的字段；对子对象的 setter 才保留局部 `OpSet` 路径。
- `MaxPathDepth` 是 Store 树投影和 snapshot diff 共用的真实递归预算，当前为 4 层 `PathNode`。即将超过预算时必须立刻停止向下比较或投影，将最近可解析祖先标为 `OpReplace`；不能 panic，也不能继续做无效深层 diff。每个生成数据类型通过 `sync.Once` 输出一次告警，提示该类型发生了深度降级。
- `map[string]` 的 key 如果为空字符串、包含 `.` 或 `$`，Mongo 边界不转义 key，而是降级整字段写，保证数据库中保留业务真实 key。
- Store 拦截路径和 snapshot diff 路径同时存在，但同一变更周期互斥。`ModeStore` 用于 setter/Store wrapper 标脏，`ModeSnapshot` 用于 `BeginChange/SaveChange`。
- `BeginChange()` 必须在业务直接修改字段之前调用；`SaveChange()` 成功后清理 snapshot 和本轮 dirty，失败时保留可重试状态。
- Store 路径适合高频对象和大集合字段；保存前可用 `store.View(nil)` 捕获当前全部 dirty 路径，避免保存成功清理主 Store 后丢失广播值。只广播指定字段时，在 configure 回调中先 `AllUnTouch()`，再调用目标 `Touch*`。
- 当前集合策略：map wrapper 支持 key 级状态和生成对象指针子 Store；slice wrapper 支持稳定 index 级状态和 `[]*GD...` 元素子 Store。`InsertAt`、`Append`、`RemoveAt`、`Replace`、`Clear` 这类长度或整体语义变化按整字段处理；StoreView 在同步捕获时复制被选中的 key/index 或整字段，不依赖后续结构漂移防御。
- 字段名体系本体是 `go/json/bson/msgpack`。Redis 当前选择 JSON 名，Mongo 当前选择 BSON 名，cmd Provider 当前选择 Msgpack 名；`alias` 只是输入侧语法糖。
- 客户端上下行协议采用独立的 BON 二进制文档，不使用 Protobuf；WebSocket 传输层使用可协商的 permessage-deflate 压缩。会话字典和动态 key 压缩属于后续可选优化，必须先以真实消息体基准证明收益，再处理字典同步、重连重置和可观测性。
- 内部跨进程通信的目标运行时是可选的 `modules/actor`，直接使用 `protoactor-go` 及其官方 etcd Cluster Provider；tx 只负责项目 yml 装配、`WaitGroupContext` 生命周期和 BON 业务载荷适配。邮箱沿用 ProtoActor 的有界 RingBuffer 与调度语义，仅以薄包装补充满队列限频日志；不自研调度器、远程 PID、成员发现或 Actor 重平衡。
- `msgq` 是客户端会话邮箱模块名，负责 Session、下行 `sndQueue`、Seq/Ack、重连、Topic 和广播；它不承担所有业务 Actor 的中心中继。`modules/msgq` 已提供独立 protobuf/gRPC `MailboxService`，覆盖 Init、Rec、Pub、Fetch、FetchSome、Confirm、Touch、Topic 与 Close；它不复用 `modules/rpc`，payload 始终是 BON bytes。上行业务消息由 Agent 经过会话校验后直接投递目标 Player/Game Actor；任意业务 Actor 的下行消息投递到 `msgq`，再由 Agent 发给客户端。
- `modules/agent` 已提供 TCP 长度帧和 WebSocket binary message 两种默认接入，并以项目注入的 `IdentityResolver` 和 `Dispatcher` 完成身份解析与 BON 业务分发；tx 不实现账号认证。Agent 在握手后按已有 session owner 优先、running msgq 兜底的规则选择本地或 gRPC `SessionClient`。同进程 `agent,msgq` 复用本地邮箱，不额外建 gRPC 连接；仅启动 agent 时必须经服务目录选择远端 msgq。上行 `Rec` 在业务 Dispatch 前推进 Seq，重复帧不重复分发。`FrameBusiness` 使用版本 2 的 32 字节头，携带生成的非零静态命令号和可选强类型路由键，但 Agent 不解析 BON body；业务幂等必须使用独立业务幂等键，不能复用静态命令号。业务帧携带下行 Ack，空闲时使用无 payload 的 `FrameAck`；Ack 只能确认当前连接已成功写出的序号，越界或倒退会中止本次连接并保留未确认队列。断线不主动 Close Session，由 OfflineTimeout/重连 Init 管理。
- Agent 的会话来源采用显式失败契约：配置 `LocalMsgQ` 后只使用本地 `SessionClient`，provider 失败会保留错误链并终止启动，不得静默回退远端 discovery；只有未配置 `LocalMsgQ` 时才解析远端 msgq。单元测试只依赖 `modules/msgq` 契约，真实同进程服务装配由 txgame 集成测试覆盖，避免同级标准服务实现互相依赖。
- `txgen` 已从同一份 data/cmd 输入导出 TypeScript `data.ts`、`cmd.ts` 和 BON schema 到 `txclient/src/gen`，并在 Go cmd 包生成 `DecodeBONCommand`/`EncodeBONCommand`、稳定 `CommandID` registry 与无反射 `ExtractRouteKey`。项目在 `gen/protocol/routes.yml` 定义服务名及命令号段，号段只负责新命令首次分配，不是永久路由语义；已经分配的 `CommandID` 是客户端协议身份，即使 `toService/actor/actorKey` 变化也不能重编号。生成器维护并版本化 `gen/protocol/command_ids.yml`：当前命令位于 `commands`，显式 tidy 后已删除命令移入 `retired`；两部分的号码都永久占用，同名命令重新出现时恢复原 ID。清单只接受单个严格 YAML 文档，读改写由跨进程文件锁串行化并以原子替换发布；非法 cmd 在完整校验前不能占号。`cmd tidy` 只同步、校验和规范化清单，不自动回收任何可能进入生产的 ID。`@cmdRoute` 可为请求声明 `toService`、`actor` 与顶层 `msgpack` `actorKey`；未声明时默认 `game`。BON、客户端字段、cmd Provider 和 Store 下行变更投影统一使用 Go `msgpack` tag 名；`number` 只承载 JavaScript 安全整数范围内的 Go 整数。`tx/protocol/bon` 直接实现 BON 文档类型、7-bit 长度、小端数值和字符串引用；`txgame` 的 `NewBONDispatcher` 在 Agent 之后把 bytes 适配为生成 `*cmd.X`，并复核 Frame 命令号、路由键类型和解码字段。TypeScript `decodeEnvelope` 从 `{ c, b }` 自动选择生成 schema，并拒绝未知命令、无效信封和尾随字节；已知命令才使用 `decodeCommand` 做额外命令名断言。`resolveCommandRoute` 从生成请求对象取得 Frame 头，客户端不手写命令号、路由键或 Frame 头。
- Actor 分为两条互不混用的运行路径：可恢复实体使用 Cluster Virtual Actor，成员扩缩容后允许重新路由并从 Fusion 恢复；Battle、SLG 城市、MMO 场景等内存权威对象使用固定逻辑槽位的本地 Actor 与 Remote PID 直连，业务维护 `entityID -> logicalSlot -> runtimeInstance` 归属，不参与自动重平衡。SLG/MMO 的同服、同战区玩家与场景必须绑定同一逻辑槽位，例如 `realm:1001 -> game-slot-1`；`game-slot-1` 只在旧新 generation 间交接，不能因 `game-1@v42` 重启而改派到 `game-2`。固定归属 Actor 的迁移必须显式执行保存快照、停止旧实例、更新归属和目标恢复，节点短暂不可用时返回业务错误或由客户端重连，不得静默改派。
- 滚动重启必须先 Drain：服务目录切为 `draining`，网络入口封口并等待已经受理的 handler；之后才关闭服务资源、取消全局 `WaitGroupContext`、完成要求的最终 Flush、从成员表注销并等待成员视图收敛，最后停止进程。排水不读取或拒绝 `Procedure`、`db.Save`、`db.SaveChange` 或 Store；外部存储可用性仍由实际 I/O 错误和项目健康闸门决定。ProtoActor 成员变化会停止不再属于本节点的 Virtual Actor；它不迁移业务内存状态，也不提供分布式事务、分布式锁、跨 Actor 回滚或 exactly-once 投递。无状态服务可按先扩后缩发布；可恢复 Virtual Actor 必须限制恢复并发，避免成员变化后集中回源；固定 Scene/Battle 等内存权威 Actor 不自动迁移，应由业务选择自然结束、显式快照交接或客户端短暂重连。
- 排水入口采用严格封口，不提供按 `CommandID` 放行的白名单。`lifecycle.Admission` 是 Agent 与通用 RPC Receiver 共用的入口账本；`BeginDrain` 后所有新入站请求均返回错误，只有此前成功 `Enter` 的请求会继续执行并由 `WaitDrained` 等待。充值回调、发奖等必须完成的跨服务动作必须通过项目选择的可靠消息队列持有和重试，不能依赖排水例外、静态命令号或进程内 WaitGroup 提供可靠性。
- `storage.DocumentLoader` 和 `DocumentReplacer` 的公共文档契约统一使用 JSON 字段名。Redis 原样读写该文档；Mongo adapter 在完整文档读取与替换边界通过 `TypeSpec.DocumentFactory` 构造生成类型，再按 BSON tag 转换。Fusion 只传递公共 JSON 文档，不直接搬运 Mongo 的 BSON map。
- Fusion 是 Redis 热数据加 Mongo 冷数据。Redis main/subKey 都使用 string JSON 文档；RedisJSON/ReJSON 仅作为后续可选 backend，不是当前默认路径。
- Redis 整文档替换在文档只包含 subKey 或为空时仍写入 main `{}`，保持 main key 作为热记录存在标记；不能因字段拆分形成只有 subKey、没有根记录的状态。
- Redis 局部 patch 只会创建缺失的 JSON object 父节点；已有 `null` 或标量父节点返回 `storage.ErrPathTypeMismatch`，不做静默类型修复。Lua 使用 cjson array metatable 保留空数组的 `[]` 语义。对已有数组，仅允许生成 Store 用稳定整数 index 进入并更新既有元素的子字段；不允许 Lua 扩容、插入、删除或修复数组。`InsertAt`、`Append`、`RemoveAt` 等长度变化仍由 Store 降级为字段级 `OpReplace` 覆盖。
- `delaySaveTime` 表示 Redis 热数据变更后多久回写一次 Mongo；`delayDelTime` 表示 Redis 热数据 TTL 和自动续期时间，不表示 Mongo 删除延迟。
- Redis `PX` 和延迟队列 score 的时间边界统一使用毫秒精度：记录锁 TTL、msgq 会话 TTL 与延迟任务显式 lease 必须至少为 `1ms`。亚毫秒值必须在外部 I/O 前返回错误；标准 server 配置还要在启动校验期 fast-fail，不能把截断后的 `0` 交给 Redis，也不能创建立即过期的任务 lease。
- Fusion 延迟任务队列只保存记录定位信息；到期后读取 Redis 当前最新热数据，整文档 replace 到 Mongo。队列不保存中间态业务对象或本轮字段 diff。Mongo 回写失败且 Redis 可用时，Scheduler 使用基于 Redis 持久化 `Attempts` 的指数退避：初始延迟、乘数、最大延迟和有界 jitter 计算出的 `nextDue` 写回 Redis ZSet；达到最大延迟后持续以带 jitter 的最大周期重试，不限制次数或丢弃任务。若 Redis 不可用而无法写回重试时间，任务保持 busy lease，Redis 恢复并在 lease 到期后重新领取；运行时只返回 error，不 panic 或静默成功。
- 从 Mongo 回填 Redis 必须具备 `delayDelTime > 0` 的热数据 TTL；并发 miss 回填要通过 Redis 锁和二次读取避免旧 Mongo 快照覆盖新 Redis 热数据。
- Redis `<mainKey>@v` 是热数据生命周期版本键，只用于 Redis 删除/过期窗口并发保护，不是业务字段，也不是 Mongo 字段；版本键和 main/subKey 必须使用同一 hash tag，并在每次热写时使用同一有效 TTL 续期或同步转为持久化。版本值由 Redis `INCR` 生成，读取、条件删除和 Lua 返回值只接受正整数；缺失或非数字版本必须返回错误，不能退化为无条件删除。
- Redis 批量读取默认按生产云代理能力走 endpoint 内原生 `MGET`；本地原生 Redis Cluster debug 才显式打开 `LoadOptions.ClusterSafe` 按 hash tag 分组多次读取。
- Mongo 不内置业务删除策略。物理删除接口存在，但是否调用由业务决定；TTL 索引是 Mongo 自身按字段值删除整条文档的能力，生成阶段限制同一类型最多一个 TTL 字段。
- 持久化 GD 的字段演进是项目使用约定：只允许新增，不删除、不改名、不改变类型或 JSON/BSON/Msgpack 字段名；迁移时保留旧字段并由业务处理。runtime 与 generator 不维护自动迁移或历史字段缺失校验。
- 后续如发现中间过渡结构、废弃测试或过时文档描述，应持续瘦身；保留稳定决策，不保留已经放弃的实现路线。

## Next Work Items

当前后续实施优先级：

已完成基线：

- 进程装配已完成职责下沉审计：完整 runtime 与 Storage 实现均为 `server` 包内私有，标准服务和项目 setup
  统一接收受控 `server.Context`；`lifecycle.Tasks`、`rpc.Registrar`、`discovery.Directory`、
  `actor.Messenger`、`txn.Executor` 分别由能力所有者维护。Context wrapper 阻断服务通过类型断言取得
  Cancel/Stop/Shutdown/Replace/Flush。高级 SPI 第一版只开放 Redis 路由复用与逻辑槽位 Store。
  `server/services` 的架构回归禁止同级服务实现依赖；Redis 会话存储桥接已下沉到中立
  `server/adapters/msgqredis`。
- 生成 Store 已使用父 Store 路径树组织直接指针、map key 和 slice index 子 Store；`VisitChanges()` 在边界递归同步遍历。`View(configure)` 选择路径并同步创建首批强类型稀疏 StoreView，返回值不再引用根 GD；`Capture()` 在同一所有权区间直接累计当前 Store，`Merge()` 合并已经创建的独立视图批次，`Document(kind)` 不创建旧式 Delta/字符串路径批次；cmd Provider 固定消费 StoreView 并使用 `FieldNameMsgpack`。
- `map[K]*GD...` 遇到不能表达为 `PathKey` 的 key 类型时，仍可通过子 Store 修改数据，但导出时保守替换整 map；`txgame` 已覆盖枚举 key 的真实输入回归。
- `txgame` 的 `GDRole`、`GDLegion` 已使用 `@transaction` 生成锁键与锁对象，并以 `PGuildContribution` 覆盖多对象锁序、生成 Store undo 和回滚后变更文档清理。
- `txn.Tx.DirtyTargets()` 已按锁顺序暴露本次经受控 Store API 修改且仍有变更的根目标；savepoint 回滚或清空变更的目标不会出现。`txn.Runtime` 使用它在事务成功后登记落盘对象；项目 Host 直接持有 `txn.Runtime`，业务使用 `runtime.Call(ctx, procedure)` 作为有状态入口。`runtime.txn.flushInterval` 控制生命周期内的定时 Flush，`runtime.txn.shutdownFlushTimeout` 控制服务退出时的最终 Redis 刷盘窗口。生命周期先用服务全局 `WaitGroupContext` 等待已经进入的业务任务结束，再使用不受运行 context 取消影响的限时 context 进行最终刷盘；它不等待 Fusion 延迟队列回写 Mongo。`runtime.txn.debug.checkCleanLockTargets` 仅用于临时、无历史变更的直接 `txn.Call`；Runtime 会显式允许此前未 Flush 的变更。
- `txgame` 已以真实 Redis、Redis 延迟任务和 Mongo 覆盖 `txn.Runtime`：`Procedure` 成功后 Redis 尚无记录，`Flush` 后热数据出现并清理 Store 变更，延迟任务消费后 Mongo 获得相同最终文档。集成回归会以 `MaxRootChanges: 1` 强制收敛 `Items[1001].Quantity`，确认 Redis 与 Mongo 都按整个 `Items[1001]` 当前值替换、不会残留旧 `quality` 字段。生命周期会把最终 Flush 错误返回给项目宿主，不静默吞掉超时或失败。默认本地配置以 `maxRootChanges: 1024` 收敛单根细粒度路径，并以 `heapSoftLimitBytes: 536870912`（512 MiB）和 `heapSampleInterval: 1s` 记录 Heap 告警；`runtime.health.interval: 1s`、`runtime.health.timeout: 500ms` 维护 Fusion 新写入闸门。实际部署值必须按实例规格和 `GOMEMLIMIT` 调整。
- Fusion 批量混合命中先批量读取 Redis；Mongo reader 支持批量读取时，所有 miss 只做一次 Mongo 全文批量读取，再逐条进入记录锁、二次 Redis 读取与带 TTL 回填。锁内命中 Redis 时直接返回新热数据，不能以旧 Mongo 候选覆盖它。

1. 保持 `txgame` 集成验证与生成 Store 行为同步：新增集合、指针、存储边界行为时，补业务侧代码示例及真实 Redis/Mongo 回归。
2. 持续收紧 Redis/Mongo/Fusion adapter 的失败契约：未知脚本协议结果、路径类型冲突和路由错误必须显式返回错误，不能静默成功或做运行时数据修复。
3. 继续以 `VisitChanges` 与存储边界 benchmark 为基线审计热点路径。Redis/Mongo/Fusion 已直接同步遍历 Store 变更，并保持 Store 成功清理、失败重试、Mongo 父子路径降级和 Runtime 容量预算语义；后续优化不得重新引入中间变更批次。
4. 持续清理不再作为主线的字符串路径变更代码、过渡接口和旧测试，只保留必要历史 benchmark 结论。
5. RedisJSON/ReJSON 仅作为未来可选 backend 单独评估；SQL、消息队列等其他 backend 也不进入当前主线。
6. 完善可选的 `tx/modules/txn`：`@transaction` 已生成 `LockKey()` 和 `LockTarget()`；受控字段、map、slice 与生成对象子 Store 已登记 undo，并覆盖事务失败后的回滚。事务锁使用分片的精确 Key 锁注册表：哈希分片只保护 `LockKey -> lockEntry` 元数据，完整 Key 仍拥有独立 channel 信号量；锁按稳定顺序获取，并由 `context.Context` 提供等待超时，超时必须释放已获取锁且不得进入回调。entry 在引用归零后从注册表移除并通过 `sync.Pool` 复用，四把以内的常见锁集内联持有，单锁入口不再分配预处理副本。2026-08-21 在本机 i9-9880H 的正式基准中，锁内核单 key 串行为 `247.6-255.9ns/op`、四 key 串行为 `965.6-995.2ns/op`、16 线程离散 key 为 `43.99-62.26ns/op`，三者均为零分配；共享同 key 仍严格串行。`Procedure.Locks`/`tx.Call` 已约束锁集和输出嵌套路径；`txn.Runtime` 已覆盖事务后登记、持锁 Flush、失败重试，以及先等待全局 `WaitGroupContext` 再执行的最终限时刷盘。2026-08-09 在本机 i9-9880H 的基准中，100 元素 map 的 snapshot diff 约为 `83.8-87.0µs/op`、`45.4KiB/op`，生成 Store 路径约为 `9.0-9.3µs/op`、`15.6KiB/op`；当前 Store 树、同步 Flush 与失败重试尚不需要不可变写入批次。只有真实 I/O 或锁边界要求把投影结果交给其他 goroutine 时，才以独立设计重新评估不可变批次。第一版不处理分布式锁、跨实例事务或持久化层 ACID。
7. 有状态脏数据容量保护：已补“固定路径重复修改”“大量不同 map/slice 路径”“成功 Flush”“持续失败 Flush”与 clean cached 子 Store 的基准/回归，并实现 `txn.RuntimeConfig.MaxRootChanges`、`HeapSoftLimitBytes`、`HeapSampleInterval` 与可选 `MaxPendingRoots`。固定路径不会累积变更项；1024 个干净缓存 map 子节点投影已从全量扫描收敛为只检查 dirty 索引。2026-08-09 在本机 i9-9880H 的 `BenchmarkGeneratedStoreDirtyRuntime` 中，1024 个不同 map key 的整次构造与投影约为 `40.2-40.9ms/op`、`1.57MiB/op`；成功 Flush 约为 `37.7-45.7ms/op`、`1.51MiB/op`；失败 Flush 后保留同一批 1024 路径并重新投影约为 `0.71-0.73ms/op`、`1.48MiB/op`；1024 个干净缓存 map 子节点投影约为 `16.0-17.6ns/op` 且零分配。因此预算检查只放在 Flush 的既有投影点，不额外加入每次事务提交热路径。单根变更 超预算会收敛为顶层字段 `Replace`；进程 Heap 超过阈值只记录告警，业务写入可用性由项目级 Redis/Mongo 健康闸门决定。部署项目应通过 `GOMEMLIMIT` 约束进程整体 Heap，tx 只读取采样结果、不修改全局 runtime 内存限制。后续应以真实业务对象压测校准这三个阈值，并评估是否需要增加 byte 预算；不得在生成 setter 中隐式 I/O 或强制 Flush，避免破坏 Procedure 的锁边界、undo 与批量合并。
8. 服务发现第一阶段已完成 `modules/discovery` 与 etcd 后端：`EtcdRegistry` 用 lease 注册、续约和自动过期摘除实例；`ServiceDirectory` 以 watch 快照维护实例 ID、endpoint、版本、状态和服务列表。项目 yml 只提供 etcd endpoints、prefix、TTL 等静态集群参数并转换为 `EtcdConfig`；实例 ID、endpoint、版本和服务列表必须在启动期动态构造，不能写死在 yml。`txgame` 模板从环境读取实例 ID、endpoint 和版本，并从实际选中的模块导出服务列表；tx 不读取项目配置或环境变量。实例启动时注册为 `starting`，所有选中模块启动成功后切换 `running`；排水时先切 `draining`，排水完成后才切 `stopping` 并撤销租约。服务路由只选择 `running` 实例；精确实例路由允许解析 `running/draining/stopping` 实例，供会话类服务排空。`tx/server` 的 discovery runtime 持有共享 `ServiceDirectory`，并只通过 `server.Context.Directory()` 暴露只读查询；生命周期托管 watch，异常时立即清空目录，后续跨实例路由必须显式失败而不能沿用旧 endpoint。服务关闭时先取消并等待 watch，再撤销 lease。`EtcdRegistry.Slots()` 已提供不挂 lease 的持久 `SlotStore`：首次 assignment 以 `CreateRevision == 0` 创建，generation 交接以完整旧 assignment 和 etcd `ModRevision` CAS Promote；`SlotRouter` 只向 `running` 目标解析固定槽位，目标缺失或排水时明确失败，绝不随机改派。该 Store 只通过 `server.Context.Advanced().Slots()` 向高级项目装配开放，业务负责 SlotID、旧槽位准入关闭、最终 Flush 与 FixedPID 创建。`modules/actor` 已接入 `protoactor-go` 官方 etcd Provider，提供 `actor.Config`、启动期 etcd 预检、`Runtime.Start/Shutdown`、Virtual PID 解析、固定 Remote PID 构造和携带 BON 字节的 `actor.Delivery` protobuf 信封；启动异常返回错误而不是让底层 panic 扩散，真实 etcd 集成回归覆盖动态端口、Virtual Actor 请求和成员注销。`txgame` 已能从严格 yml 和部署环境装配该运行时，并在最终事务 Flush 后注销 Actor 成员。`modules/msgq` 已接管 `roleID -> (sessionID, connectionID, msgq instanceID)` 的 Redis 条件绑定、TTL 续期与精确 owner 解析，不再依赖 `modules/rpc.Route`；`msgq.Server` 已实现有界进程内会话邮箱，`Init` 原子建立 owner 并恢复同 SessionID 的未确认队列，`Rec` 校验上行顺序并确认下行 Ack，`Pub` 以只读所有权移交 BON payload 写入 `sndQueue`，`Fetch/FetchSome` 按调用方 context 长轮询，`Confirm` 释放已确认消息，`Touch/Close` 条件续期或解绑。它已支持当前实例的 Topic 加入、离开、查询与广播；本地广播会统计成功、跳过和积压，任一目标积压时返回错误但不回滚已投递的会话。`Server.Start(WaitGroupContext)` 会按 `OfflineTimeout/SweepInterval` 清理无客户端活动的会话；活动仅来自 `Init/Rec/Fetch/Confirm/Touch`，业务 `Pub` 不延长离线保留。Reap 时 Redis 条件解绑失败会返回错误并记录日志，但仍移除本机失活会话，由 Redis TTL 兜底清理 owner，不能让断线对象无限驻留内存。下行 payload 仍不进 Redis，旧 connection 与旧 owner 只能返回明确错误。`msgq.Sender` 已通过 `SessionRouter` 按 roleID 解析 owner、携带当前 SessionID 校验远端 `Pub`，并复用每个 msgq endpoint 的 gRPC 连接；它不自动重试不确定结果的投递。`modules/rpc` 保持独立的 Service/Instance gRPC 投递，不承载 Actor 或会话路由。客户端协议 body 保持 BON 字节，内部传输只使用 ProtoActor 的 protobuf 信封；不生成 protobuf 版 GD 或客户端协议。`msgq` 保持独立的会话邮箱语义，实例崩溃允许丢失尚未确认的下行，由客户端重连和业务幂等恢复，不引入持久化消息队列。内部命令的超时重试与幂等去重必须按消息类型定义 `CommandID` 契约，框架不得盲目重试。Actor、事务和 Runtime 只在业务项目编排层组合，不反向依赖：Actor 不代表 GD，不依赖 Store、Fusion 或 `txn.Runtime`；事务只负责当前进程内的多对象锁定、回滚和提交；Runtime 只负责当前进程的脏实体落盘。Actor mailbox 必须有界，满时不得无限阻塞或静默丢弃；默认 `Tell` 可以丢弃新消息，但必须由目标实例记录限频溢出错误。
`server.Context.Instance()` 已改为并发安全的 ownership-safe 本地实例快照，`Services` 深复制且状态与进程生命周期同步。启动顺序固定为共享 RPC Host、控制面和 discovery 全部就绪后才发布并记录 `running`；Drain 发布 `draining` 后同步本地快照并封口入口；最终关闭阶段记录 `stopping`。集成测试覆盖真实 etcd 下的 `Start -> running -> Drain -> draining`，避免只更新成员目录而让进程内服务长期观察到 `starting`。
9. 增加可选运维控制面，面向腾讯云 TKE/Kubernetes 或自建发布脚本的滚动更新：发布平台负责版本兼容、实例替换节奏和启动参数注入；tx 只提供实例生命周期接口与状态观测。第一阶段只覆盖正常优雅发布，不引入 Lease、Redis fencing 或异常宕机后的自动接管。`StatusDraining` 已完成最小状态地基：普通服务路由跳过 draining，精确实例路由保留它，真实 etcd lease/watch 已覆盖该传播。实例必须区分逻辑槽位、运行实例 ID 和 generation，例如 `game-slot-1`、`game-1@v43@uuid`、`43`。`LogicalSlotID` 由部署声明，不能由在线实例数推导：原生进程以 `--logical-slot` 或 `TXGAME_LOGICAL_SLOT` 传入；Kubernetes 有状态服务优先使用 StatefulSet ordinal 注入，镜像不得写死槽位。`tx/server` 校验该身份并只通过 `server.Context.LogicalSlot()` 暴露只读值，但不替业务计算实体归属、不自动 Create assignment。正常交接固定为：旧实例先 `draining` 并停止该槽位的新命令准入，再由全局 `WaitGroupContext` 等待已进入 Procedure 与后台任务，完成最终 Flush，之后才标记 `stopping`、撤销旧成员 lease；新运行实例以连续 generation 通过 `Promote` 接管相同 SlotID。固定槽位在此窗口内只能等待或返回业务重试错误，不得静默改派到其他 game 实例；新旧进程不共享内存，恢复仍由业务按需加载。先扩容不是正确性前提，只是可选的发布加速手段。运维脚本不得直接修改实例自己的 etcd lease、心跳记录或槽位 key，而应调用 tx 暴露的受控接口请求 `Drain`、查询排水状态、等待 `Drained`，再由发布平台终止旧进程。`modules/control.SlotService` 与 `txctl promote-slot` 已使用“完整旧 assignment + 下一运行实例 ID”执行 generation CAS；下一 generation 由框架唯一推导，目标实例必须已经为 `running`，陈旧脚本不能覆盖更新后的 assignment。异常宕机、网络分区和自动安全接管需要单独设计，不得假定 `WaitGroupContext` 可以覆盖。etcd 只保存成员事实和槽位映射，不承担发布编排。

`ServiceDirectory.RunningInstances(service)` 返回按实例 ID 稳定排序的 running 快照，并深复制 `Services` 切片；业务可据此实现随机、哈希或负载策略。目录不保存地图、GD 或 Actor 的归属关系。

`modules/control` 已提供最小运维 gRPC：生命周期面包含 `Status`、`Drain`、`WaitDrained`，槽位面包含 `PromoteSlot`。生命周期服务只面向 `LifecycleController` 抽象；`tx/server` 的内部 drain coordinator 将其适配为 discovery 状态更新和 service host 排水。槽位服务只面向 `SlotController`；`tx/server` 的内部 slot promoter 组合 `SlotRouter`、当前成员目录和 etcd SlotStore，在目标进入 `running` 后执行 CAS。两者可由 `--control-listen` 或 `TXGAME_CONTROL_LISTEN` 启用。`Drain` 不取消根 context、不拒绝 Procedure/DB/Store；它严格封口 Agent 和通用 RPC 等服务入口并等待已受理 handler，不支持按命令号继续准入。`cmd/txctl` 以 `status`、`drain`、`wait-drained` 和 `promote-slot` 供发布脚本串联；它不修改实例 lease 或成员状态，`promote-slot` 也只能通过受控服务修改槽位 assignment。发布平台在 `WaitDrained` 后发送正常终止信号，进程再关闭资源、取消 `WaitGroupContext`、最终 Flush 并撤销成员记录。后续只评估认证和 TLS/mTLS，不改变这一职责边界。

`modules/rpc.Host` 已提供每个物理进程唯一的普通 gRPC Host，并直接接入全局 `WaitGroupContext`。`tx/server.Server` 负责创建 Host、让所选服务注册 protobuf service、统一启动并把实际 endpoint 发布到 discovery；`txgame` 只通过 `RunOptions` 注入监听地址和实例身份。本地未启用 discovery 时可用 `127.0.0.1:0` 调试。`server/services/msgq` 只注册 `MailboxService`，不自行创建 listener/server。共享 listener 在 Drain 阶段保持可用，让已受理业务完成下行；Host Stop 时才统一关闭。通用 `modules/rpc.DeliveryService` 每进程只允许注册一次，聚合服务由项目进程级 Receiver 分派，不能由 `game/room/battlefield` 重复注册；Actor Remote 和 control 继续使用专用入口。

`txgame/internal/agent.NewFixedSlotTargetResolver` 已把项目提供的 `Identity -> SlotID/ActorID` 规则与 `SlotRouter` 组合；它只为 running 宿主构造 Remote PID，draining、stopping 或缺失宿主保留 `ErrSlotRuntimeUnavailable` 错误链，不替业务改派。

`txgame/internal/agent.NewActorDispatcher` 已提供 Agent 到业务 Actor 的项目级桥接：生成 registry 先以 Frame
`CommandID` 找到 `CmdDescriptor` 并检查路由键类型，项目再按 `Identity + CmdDescriptor + RouteKey -> PID` 决定归属。
桥接以 `agent.command` 原样投递 BON body、静态命令号和路由键；它不硬编码 role/room/battle kind，不把连接 `Seq`
误作业务幂等键，也不让 Agent 直接承载业务命令 handler。

`txgame/internal/actor.CommandActor` 已提供项目侧消费端：它接收 `actor.Delivery`、以生成
`cmd.DecodeBONCommand` 解码 BON body，复核静态命令号和路由键与解码字段一致后，再按命令名调用通过
`RegisterCommand` 注册的强类型 handler。该适配只在 `txgame` 依赖生成 cmd，不把业务处理器、GD 或 Store 引入
`tx/modules/actor`；未知命令、外层元数据不匹配、解码失败或业务 handler 返回错误只记录结构化日志，不应让 Actor
进程 panic。

`msgq` 下行 payload 采用只读所有权移交：`Pub` 或 `BroadcastTopic` 成功后，调用方不得修改、append、复用或归还传入的 BON `[]byte`。本地广播的会话条目共享一份正文，跨进程 gRPC 在传输边界复制；确认前缀会解除正文引用，清空时释放条目数组，部分确认按低水位整理剩余条目。Agent 的 TCP 长度帧通过 scatter/gather 直接写长度、固定头和正文，WebSocket 以流式 writer 写固定头和正文，不在 tx 内拼接新的完整帧。

Actor 单向投递已收敛为 `Runtime.Tell(pid, delivery)`：它仅表示本地 Runtime 接受投递，不确认远端处理完成，也不自动重试。需要同步响应时，业务显式通过 `ActorSystem().Root.RequestFuture` 处理。真实 etcd 集成回归覆盖 Virtual Actor 请求、`Tell` 投递与成员注销。

Cluster Kind 默认使用 `MailboxCapacity`（默认 `1024`）的非阻塞有界邮箱。满时丢弃新消息、不阻塞发送方，并在目标实例按累计丢弃次数的二次幂限频记录 `actor mailbox overflow`，不建立跨实例容量同步或背压系统。`Runtime.TellConfirmed(ctx, pid, delivery)` 是可选确认式投递：要求 context 带 deadline，以一条 `Acceptance` 回执确认目标 Actor 已取出消息；它不确认业务处理成功，且同一消息不能再由业务 Actor 使用 `ctx.Respond` 返回结果。业务结果仍使用显式 `RequestFuture`。

## Module Layout

当前包结构方向：

```text
tx/
  meta/             # 元数据模型、registry、annotation 输出模型
  anno/             # Go 注释中的 type-level tag 解析、规范化、校验
  astscan/          # Go AST 扫描器，提取 package/type/field/method/comment 信息
  change/           # 生成式投影契约、Path、变更文档和变更模式
  lifecycle/        # 可取消、可等待后台任务结束的运行时上下文
  log/              # 基于 slog 的最小日志门面
  storage/          # 存储抽象：Store mode、route、key、options、runtime metadata
  storage/config/   # 项目 yml 路由配置的强类型承接模型
  storage/redis/    # Redis key builder、route resolver、后续 Redis adapter
  storage/mongo/    # Mongo collection/index/sharding metadata、后续 Mongo adapter
  storage/fusion/   # Fusion 策略与延迟任务抽象
  storage/task/     # Delay task backend interface 和内存实现
  modules/discovery/ # 可选：etcd lease/watch 服务发现与进程实例目录
  modules/control/   # 可选：运维排水控制 gRPC 契约
  modules/msgq/      # 可选：客户端会话 owner 与进程内下行邮箱
  modules/rpc/       # 可选：独立 Service/Instance gRPC 投递
  modules/txn/      # 可选：单机事务、savepoint、undo log 和 after-commit hook
  modules/actor/    # 可选：protoactor-go Actor Cluster 装配
  util/             # 小型通用工具
  tests/            # 跨包行为和公共 API 回归
```

当前实现以 `anno`、`astscan`、`meta`、`storage`、`change`、`util` 和 `tests` 为主要边界；代码生成职责放在独立的 `txgen` 工程。

## Annotation Direction

主入口放在 Go 注释中，并由 Go AST 绑定到准确的 type/field 节点。type 级注释 tag 与 field 级 Go struct tag 都使用反引号承载参数。

第一版建议支持：

```go
// @storage:`mode:"fusion" delaySaveTime:"600s" delayDelTime:"24h"`
// @redis:`routeType:"role" routeBy:"RoleId" idField:"RoleId" key:"role"`
// @mongodb:`routeType:"role" routeBy:"RoleId" idField:"RoleId" collection:"roles" enableSharding:"true"`
// @transaction:`scope:"role" keyField:"RoleId" order:"20"`
type GDRole struct {
    RoleId int64 `json:"roleId" mongodb:"index,unique"`
}
```

字段级继续使用 Go 原生 struct tag：

```go
Items map[int32]*GDItem `redis:"subKey:items"`
RoleId int64 `mongodb:"index,unique"`
```

注释语法设计原则：

- 使用 `// @<tagName>:` 后跟可选反引号参数，保持与 Go `reflect.StructTag` 相近的写法。
- type-level tag 第一版支持：`storage`、`redis`、`mongodb`；后续支持可选的 `transaction`。
- field-level tag 继续读取原生 struct tag，第一版支持：`redis`、`mongodb`。
- 参数使用 struct tag 风格的 `key:"value"` 形式。
- parser 只负责理解单条 tag 内容；注释和 struct/field 的绑定必须由 Go AST 完成。
- 时间字段使用 Go duration 风格字符串：`600s`、`10m`、`24h`。
- Redis hash tag 默认使用 `routeBy` 指向的字段。
- `routeBy`、`idField` 是当前 canonical 写法。
- `transaction` 与 `storage` 相互独立。存在 `transaction` 注解时表示生成 `LockKey()` 和 `LockTarget()`；`scope` 必填，`keyField` 必须绑定当前类型的基础字段，`order` 可选且必须是非负整数。纯内存类型也可以只使用 `transaction`。

## Schema Direction

独立 schema 文件不再作为支持范围。第一版只维护一条元数据链路：

```text
Go 注释 / struct tag -> anno -> astscan -> meta -> storage/gen
```

设计原则：

- 不提供 YAML/JSON schema loader。
- 不提供 schema 导入、导出或冲突合并。
- 外部工具若需要消费元数据，应基于后续 generator 输出，而不是手写 schema 文件。

## Phase 1: Metadata Core

目标：建立新库的核心元数据模型和只允许初始化期注册的 registry。

建议实现：

- `meta.TypeRef`
  - `ImportPath string`
  - `Package string`
  - `Name string`
  - `FullName string`
- `meta.Field`
  - `Name string`
  - `Alias string`
  - `Type string`
  - `Exported bool`
- `meta.Type`
  - `Ref TypeRef`
  - `Fields []Field`
  - `Methods []Method`
- `meta.Registry`
  - `RegisterType(meta.TypeSpec) error`
  - `Freeze()`
  - `Lookup(typeRef) (TypeSpec, bool)`
  - 注册后 freeze，freeze 后不允许隐式修改。
- 元数据必须在 `RegisterType` 前完成规范化和校验；Registry 不提供通用覆盖钩子，避免出现未进入装配链的预留公共 API。

验收：

- Registry 支持初始化期注册、冻结、查找。
- Freeze 后注册返回明确错误。
- 错误包含 type 名和字段名，便于迁移诊断。

测试：

- `tests/meta_registry_test.go`
- 覆盖注册、重复注册、冻结后拒绝、lookup。

## Phase 2: Annotation Parser And Validator

目标：解析 Go 注释中的 type-level tag 和字段上的 struct tag，输出强类型配置。

建议实现：

- `anno.ParseTypeTagLine(text string) (*anno.Directive, error)`
- `anno.ParseTypeDoc(comments []string) ([]anno.Directive, error)`
- `anno.ParseStructTag(tag reflect.StructTag, scope anno.Scope) ([]anno.Directive, error)`
- `meta.Normalize(directives []anno.Directive) (meta.TypeSpec, error)`
- `meta.Validate(spec, astType) error`
- 强类型配置：
  - `StorageSpec`
  - `RedisSpec`
  - `MongoSpec`
  - `FusionSpec`
  - `FieldSpec`
  - `IndexSpec`

校验规则：

- type 必须能在 AST 扫描结果中找到。
- `redis.idField`、`redis.routeBy`、`mongodb.idField`、`mongodb.routeBy` 必须匹配字段 alias。
- `storage.mode` 第一版支持：`redis`、`mongo`、`fusion`。
- `fusion` 模式必须同时具备 redis 和 mongo 配置。
- `redis.key` 缺省可由类型名推导。
- `mongo.collection` 缺省可由类型名或命名策略推导，但建议第一版要求显式配置。
- 未识别参数必须报错，不静默忽略。

验收：

- 能解析 `// @storage:"..."`、`// @redis:"..."`、`// @mongodb:"..."`。
- 能解析字段级 `redis:"..."`、`mongodb:"..."` struct tag。
- 能产生稳定、可序列化的 `TypeSpec`。
- 校验错误可以定位到 type/field/property。

测试：

- `tests/anno_parse_test.go`
- `tests/anno_validate_test.go`
- 测试文件中的 fixture 使用内联字符串或写入 `t.TempDir()`，不要在包目录放测试 fixture。

## Phase 3: Go AST Scanner

目标：使用 Go AST 提取 Go 类型结构并把注释 tag 绑定到准确节点。

建议实现：

- `astscan.ScanDir(root string, opts Options) (*astscan.PackageSet, error)`
- `astscan.ScanFiles(files []string, opts Options) (*astscan.PackageSet, error)`
- 提取：
  - package import path
  - struct type
  - exported/unexported field
  - embedded field
  - field tags: `json`、`bson`、`msgpack`、`alias`
  - method receiver 和 method 名
  - type doc comment 中的 `// @storage` / `// @redis` / `// @mongodb`
  - field struct tag 中的 `redis` / `mongodb`
- 字段名规则：
  - Go 字段名是框架内部字段身份。
  - `alias` 只是输入侧语法糖；写了 `alias` 时不能再写 `json`、`bson` 或 `msgpack`。
  - 不写 `alias` 时，`json`、`bson`、`msgpack` 三套字段名各自独立；缺省按 lower-camel Go 字段名推导。

验收：

- 能扫描 Go module 内指定目录。
- 能解析嵌入字段和常规字段。
- 能稳定输出 type/field alias map。
- 能将 type-level 注释 tag 绑定到准确的 AST type 节点。
- 能将 field-level struct tag 绑定到准确的 AST field 节点。

测试：

- `tests/astscan_test.go`
- 使用 `t.TempDir()` 创建临时 Go module 或临时 package 文件。

## Phase 4: Annotation Boundary

结论：`tx` 只维护当前注解语义，迁移工具不属于 runtime 边界。

当前约束：

- 输入侧统一使用新的 Tx 注解语义。
- 如后续确实需要外部迁移工具，应作为独立工具工程实现，不放入 `tx` runtime。

## Phase 6: Delay Task Backend Abstraction

目标：Fusion 不绑定 Redis ZSet + Lua，抽象可替换任务后端。

建议接口：

```go
type DelayTask struct {
    Type      string
    ID        string
    ShardKey  string
    KeySuffix string
    Action    string
    DueAt     time.Time
}

type DelayTaskBackend interface {
    Enqueue(ctx context.Context, task DelayTask) error
    ClaimDue(ctx context.Context, now time.Time, limit int) ([]DelayTaskLease, error)
    Complete(ctx context.Context, lease DelayTaskLease) error
    Retry(ctx context.Context, lease DelayTaskLease, next time.Time, reason error) error
}
```

第一版实现：

- `storage/task/memory`: 内存 backend，只用于测试和本地开发。
- `lifecycle`: 提供可取消、可等待后台 worker 完成的运行时上下文，供 Fusion 延迟回写和其他后台组件在服务关停时等待活跃任务结束。
- `storage/task/redis`: Redis 持久化 delayed task backend，使用 sorted-set/hash 记录任务内容、到期时间、busy lease、完成与重试状态，并通过 Lua 保证 claim/complete/retry 的原子性。
- Redis delayed task keys 必须按任务 id 分桶，每个 bucket 使用独立 hash tag，例如 `tx:task:{0}:due`、`tx:task:{0}:payload`，保证单次 Lua 脚本访问的 due/payload/busy/token/attempts key 落在同一个 Redis Cluster slot，同时避免所有任务集中到一个 slot。
- Redis delayed task backend 面向多服务实例并发消费：claim/complete/retry 通过 lease token 隔离，但整体语义是 at-least-once，后续 Fusion Mongo 回写必须按存储幂等方式设计。同一幂等任务处于 lease 期间再次 enqueue 时，必须更新最新 payload 并记录 follow-up 到期状态，不能发放第二份并发 lease，也不能让旧 `Complete` 删除新写入；旧 lease 成功后原子重排 follow-up 并重置尝试次数，旧 lease 失败时则以最新 payload 沿原次数继续退避。Redis 实现使用同 bucket、同 hash tag 的 `pending` ZSet 承接该状态，memory backend 保持一致语义。claim 在写入任何 lease 状态前预检整批 due task 的 payload；任一缺失即显式失败并保持整批状态不变，不允许静默删除任务或产生调用方不可见的部分 lease。
- 跨 bucket claim 允许同时返回已成功取得的 leases 与后续 bucket error；Scheduler 必须先结算全部可见 lease，再报告聚合错误。单条 Complete/Retry 失败不能中断本批其他 lease 的处理，避免已经领取的任务在调用方不可见状态中等待超时。该契约不改变业务 facade，仅约束 `DelayTaskBackend` 与高级自定义 Scheduler。
- Redis delayed task payload 的整数定位必须精确往返：`int64` 范围外的非负整数恢复为 `uint64`，不得退化成有损 `float64`，避免 claim 后幂等任务键变化而无法 complete/retry。Mongo 批量读取使用 BSON 类型和值编码恢复输入顺序，不把动态 ID 直接作为 Go map key；`[]byte` 等不可比较 BSON 主键不能触发 panic。
- `log`: 基于标准库 `log/slog` 提供最小 logger 注入入口，默认使用 `slog.Default()`；已提供 `Debug/Info/Warn/Error` 和 Context 版本的包级快捷方法，并提供 `NewLogger(name)` 轻量命名 logger 代理；Fusion Scheduler 失败重试时输出结构化错误日志，失败原因不进入 Redis 队列状态。
- `storage/fusion.TaskQueue` 负责把 Fusion 延迟 operation 转换为 `storage/task.DelayTask`，只保留 record id、route/shard key、action 和 due time；任务类型优先来自 `DelayedOperation.Type`，为空时使用 `RecordKey.TypeName`；Fusion 层不暴露额外 suffix，同一记录的延迟保存按 locator 合并为一条任务；入队支持保存和热数据延迟删除 action，并要求类型和 record id 非空，避免无效任务进入 Redis 队列。`storage/task` 的 memory/Redis backend 也统一校验 Type、ID、Action，Redis claim 解码不完整 payload 时返回错误。延迟任务不携带业务对象或字段变更日志，执行时重新读取 Redis 当前热数据。
- `storage/fusion.StorageTaskHandler` 负责消费 delayed task：保存任务从 Redis 读取完整热数据并 replace 到 Mongo；Redis 热数据延迟删除任务会读取 `<mainKey>@v`、保存 Redis 当前热文档到 Mongo，然后仅在版本未变化时删除 Redis main/subKey/version key，版本变化时重新排队。
- Fusion 显式删除不进入延迟队列：运行时先删除 Redis 热数据，再通过显式 Mongo deleter 立即删除 Mongo 文档；如果业务绕过 Fusion 只删除 Mongo，未过期 Redis 热数据仍可能被延迟保存任务重新写回 Mongo。
- Fusion 延迟保存已通过真实 Redis/Mongo 集成路径验证：Redis 同步写入热数据，Redis delayed task backend 入队，Scheduler claim 到期任务，StorageTaskHandler 读取 Redis 热数据并整文档 replace Mongo。
- Fusion 读取默认在 Redis miss 后从 Mongo 全量加载并回填 Redis 热数据，回填路径运行时要求 `delayDelTime` 大于 0；回填前会获取记录级 Redis 锁并二次读取 Redis，防止并发 miss 时 Mongo 快照覆盖新的 Redis 热数据；显式 `SkipRedisBackfill` 选项才允许直接读 Mongo 且不回填；部分字段读取主要按 Redis main/subKey key 粒度减少热数据读取，只请求 subKey 字段时不强依赖主文档命中。
- Redis 热数据生命周期版本键 `<mainKey>@v` 已作为运行时原语实现并接入 Fusion 延迟删除 handler：Redis delete 可带 expected revision 比对，只在版本未变化时删除 main/subKey/version key。每次 Redis 热写都会递增版本键，并让它与 main/subKey 使用同一有效 TTL 续期；持久化热数据则同步持久化版本键。该版本只用于 Redis 热数据过期/删除窗口的并发保护，不是业务字段，也不是 Mongo 文档字段。
- `txgame` 已用真实 Redis/Mongo 验证两个版本键窗口：热写的 main/subKey/version key 会共同自然过期；延迟删除在 Mongo 回写期间遇到 Redis 新写时，条件删除会失败、保留新热数据并重新排队删除任务。
- `txgame` 已用真实 Redis/Mongo 验证并发 Redis miss 回填：两个请求同时在 Redis miss 后竞争同一记录锁，只有一个请求回源 Mongo；另一个请求在锁内二次读取到已回填的 Redis 文档，不会以第二份 Mongo 快照覆盖热数据。
- Mongo runtime 已提供最小 route/cluster adapter，按 routeType 和 RecordKey.ShardKey 路由单条读写，并在批量读取时按 Mongo shard 分组后还原输入顺序；`storage/mongo/mongodriver` 提供官方 MongoDB driver 的薄适配层。
- Redis runtime 已提供与 Mongo 对等的最小 route/cluster adapter：先按 routeType 和 RecordKey.ShardKey 路由到具体 Redis endpoint/adapter，再在每个 endpoint 内执行批量读取并还原输入顺序；`storage/redis/goredis` 提供 go-redis 的薄适配层。
- Redis 写入和整文档替换 API 直接使用 `storage.ApplyOptions`，不再保留 Redis 包内的 `ApplyOptions` 类型别名；`redis.LoadOptions` 仅表达 Redis 批量读取专用选项。
- `storage.RouteTable` 已提供 Redis/Mongo 共用的运行时路由表：支持 `min/max` 区间命中、多 shard 按 `routeBy % len(shards)` 选 endpoint、单 shard 字符串主键直连，并同时实现 Redis/Mongo cluster router 接口；构造时深复制规则与 shard 切片，调用方后续修改配置对象不会改变运行时路由。后续 yml 配置解析应构造该路由表，而不是在 Redis/Mongo adapter 内重复实现路由规则。
- `storage/config.RouteTableConfig` 已提供项目 yml 路由配置的强类型承接模型：支持标量 endpoint、endpoint 数组和 `min/max/shards` 规则数组；同一 routeType 的规范化区间重叠会在启动配置阶段失败。项目负责读取本地 yml 和 include，tx 负责把解析后的配置对象归一化为 `storage.RouteTable`。
- Redis 批量读取默认贴合生产云存储代理能力：同一个 Redis endpoint 内允许直接批量读取多条不同 hash tag 的记录，由云代理负责跨 slot 分发和结果聚合，避免把生产热路径退化为多次 IO。
- Redis 批量读取应使用客户端层原生 `MGET` 展平 main/subKey key 后读取，再在 Go 侧按固定步长合并文档；不要把跨记录批量读取包进 Lua。云代理能识别 `MGET` 的所有参数都是 key，但不能可靠分析自定义 Lua 中哪些 `ARGV` 是 key，也不能安全拆分一个需要原子执行的脚本。
- 本地原生 Redis Cluster 或直连节点只作为研发 debug 兼容路径：已提供显式可选的 `LoadOptions.ClusterSafe` 模式，按 hash tag 分组后多次读取，保证本地集群可复现和排查跨槽问题，但不作为生产默认策略。
- StorageTaskHandler 把 Redis 当前 JSON 文档原样 replace 到 Mongo；业务 `null` 是合法数据，必须保留。字段删除只由 `OpDelete` / `Document.Delete` 表达，不能再用 map 中的 `nil` 兼作删除标记。
- `delaySaveTime` 表示 Redis 热数据变更后，经过多久触发一次 Mongo 回写；`delayDelTime` 表示 Redis 热数据 TTL，用于释放长时间不活跃记录的热备数据，不代表 Mongo 删除延迟。
- `change.Mode` 已作为生成代码的持久化模式边界：`ModeSnapshot` 用于原生字段修改后的快照 diff 持久化，`ModeStore` 用于生成 Store 的拦截式脏标记持久化；两者在同一变更周期内互斥。当前快照 diff 由 txgen 生成代码实现，tx 只提供共用模式枚举和错误。
- RedisJSON/ReJSON 可作为后续可选 Redis backend 单独评估：当前主线仍保持 Redis string JSON main/subKey 文档模型，后续只有在确认目标云 Redis、本地测试环境、Redis Cluster 行为和 JSON path 局部读写收益后再引入。
- 后续可增加 SQL backend、消息队列 backend；当前优先贴合生产参考流程，不引入 Kafka 等新的持久化队列依赖。

验收：

- Fusion 只依赖接口，不依赖具体 Redis task 实现。
- memory backend 支持按 due time claim。
- 同一任务可定义幂等 key。
- Redis backend 支持任务进程重启后的持久化保留与 leased 超时恢复。
- Redis backend 在 Redis Cluster 下不会因为 Lua 多 key 跨 slot 失败。
- Redis route/cluster adapter 能按 routeType 和 RecordKey.ShardKey 分组到 Redis endpoint；默认批量读取不在 endpoint 内继续拆 slot，显式 `LoadOptions.ClusterSafe` 才在 endpoint 内按 hash tag 拆分。
- 通用 RouteTable 能同时作为 Redis/Mongo router 使用，并贴合现有配置的 `min/max/shards` 规则。
- RouteTableConfig 能从 yml 混合形态归一化出 RouteTable，并暴露 endpoint 集合供业务工程创建和复用具体客户端。
- 有 subKey 的批量读取仍然使用客户端 `MGET`：每条记录展开为 main key 加需要的 subKey key，返回后按输入记录顺序和每条记录 key 数合并。
- Redis backend 按 bucket 分散队列 key，claim worker 会轮询 bucket 消费。
- 多个服务实例同时消费时，同一任务同一时刻只能被一个 lease 持有；lease 超时后允许重新领取。
- Fusion 后台 worker 使用 lifecycle 上下文，在优雅关停时等待已 claim 的 Mongo 回写任务完成；`Scheduler.Start` 在 backend 或生命周期上下文缺失时返回错误，项目启动入口必须处理该错误，不能静默跳过 worker。
- Fusion delayed save worker 可把 Redis 热数据回写到 Mongo，并保留文档中的业务空值。
- Fusion delete 不产生 Mongo 延迟删除任务，显式删除调用会删除 Redis 热数据并立即删除 Mongo 文档。
- Fusion delayed task handler 只接受保存 action，delete 或其他未知 action 都不会被静默吞掉。

测试：

- `tests/task_backend_memory_test.go`
- `tests/task_backend_redis_test.go`
- `tests/lifecycle_context_test.go`
- `tests/fusion_scheduler_test.go`

## Phase 7: Code Generation Integration

目标：代码生成职责迁移到独立的 `txgen` 工程，`tx` 只保留运行时库、注解扫描、元数据归一化和存储基础能力。

Store 生成、Redis key helper、Mongo filter helper、字段编号 resolver 等生成逻辑都在 `txgen` 中实现和测试。

验收：

- `tx` 中没有 `gen` 包。
- 生成侧集成测试迁移到 `txgen/tests`。
- 生成产物仅依赖 `github.com/ngnl47/tx` 的运行时 API。

测试：

- 生成侧测试放在 `txgen/tests`。
- `tx` 只保留 runtime、annotation、metadata 和 adapter 相关测试。

## Phase 8: Storage Adapter Implementation

状态：Redis、Mongo 和 Fusion 的最小真实读写链路已经建立，后续不再把 adapter 视为“待开始”的方向。

当前能力：

- Redis adapter 支持 string JSON main/subKey 文档、Lua merge/replace、TTL 续期、批量读取和可选 `ClusterSafe`；完整替换会保留空 main 文档并删除新文档未包含的已知 subKey，避免陈旧子文档回流；`redis.NewAdapter/NewCluster` 使用 `ScriptClient` 强类型约束，并在启动装配阶段拒绝 nil/typed-nil client、nil router、空 shard 名与空 shard 集合。Redis、Mongo、Fusion 写入口收到 nil/typed-nil `change.Source` 时统一返回 `change.ErrNilSource`；只有有效 Source 明确报告无变更才是合法 no-op。嵌套集合 Clear 已通过 txgame 生成结构及真实 Redis/Mongo 回归，空 slice 在两端均保持数组类型。
- Mongo adapter 支持 load、batch load、change update、replace、delete、route cluster 和官方 driver 薄适配层；生产构造接收完整 `mongo.Client` 组合接口并返回装配错误，缺失能力在编译期暴露，nil/typed-nil 和无效 shard 在启动期失败。
- Fusion strategy 支持 Redis 热写、Mongo miss 回源、Redis 回填、延迟保存任务入队、后台 scheduler 和 Redis 到 Mongo 整文档回写。
- Mongo JSON/BSON 整文档转换已接入生成 `TypeSpec`：不同 JSON/BSON 字段名、Mongo `_id`、嵌套 struct/map/slice 均通过类型对象中转；真实 Redis/Mongo 集成测试已覆盖 Fusion 回填、延迟回写和生成 reader。
- `storage/config.RouteTableConfig` 支持项目 yml 路由配置归一化；项目负责读取 yml 和创建 driver client。

后续重点：

- Redis 热数据版本键 `<mainKey>@v` 已通过真实 Redis 覆盖旧 revision 条件删除与 Mongo 回写期间新热写两个窗口：旧删除不会清掉新文档，版本变化会保留热数据并重新排队删除任务。后续新增 Redis 生命周期写法时必须维持这两个回归场景。
- Fusion 并发 miss 回源的单条与批量真实 Redis/Mongo 场景已验证：批量路径在两个请求都取得旧 Mongo 候选后写入新 Redis 热文档，锁内二次读取会返回并保留新热文档，不能用旧候选覆盖。
- Redis/Mongo/Fusion adapter 围绕 `Source` 继续瘦身，不保留独立变更批次的过渡接口。Redis/Mongo 边界已通过 `Path.FieldNameAt` 直接解析根 Go 字段，只为最终 JSON/BSON 输出构造完整字符串路径；后续优化不能恢复每条变更的重复 Go 路径分配。
- SQL 或其他 backend 只作为未来扩展，不进入当前主线。

## Testing Policy

执行规则：

- 大节点架构审计按 `docs/architecture_audit_standard.md` 执行，先输出 P0-P3 findings、证据和残余风险，不在审计阶段直接混入大规模重构。

- 跨包行为、公共 API 和端到端 runtime 契约放在 `/Users/a47/workspace/tx/tests`，使用 `package tests` 并通过 public API 驱动。
- 只验证未导出状态机、生成脚本、Lua/driver glue 的白盒测试允许与实现包同目录；这类测试不得反向要求扩大生产导出面。
- 临时输入文件用 `t.TempDir()` 生成。
- `tx` 单元测试默认不依赖外部 Redis/MongoDB；真实 Redis/Mongo/Fusion 集成验证放在 `txgame/server/tests/integration`。
- `tx` runtime 单元测试可使用最小手写 `TypeSpec`、fake client 和构造变更源，专注通用算法与故障分支；涉及脚手架字段名、Store、`DocumentFactory`、Redis main/subKey 或 Mongo BSON 映射的契约测试，必须在 `txgame` 使用实际 txgen 生成类型。`GDStorageProbe` 是该层的最小专用输入，覆盖 JSON/BSON 不同名称、Mongo `_id`、map/slice subKey。

## Milestones

### M1: Tag Annotation And AST Foundation

交付：

- `meta`
- `anno`
- `astscan`
- public API and cross-package tests in `tests/`; focused white-box tests may stay with their package

完成标准：

- 已完成。能从 type-level 注释 tag、field-level struct tag 和 Go AST 得到稳定 type metadata。

### M2: Annotation Boundary

交付：

- runtime 只维护当前 Tx 注解语义。
- 外部迁移工具不属于当前 `tx` runtime 边界。

完成标准：

- 已完成。当前输入和测试均使用 Tx 注解语义。

### M3: Runtime Metadata

交付：

- `storage` metadata registry
- Redis key builder
- Mongo filter/index metadata

完成标准：

- 已完成。不连接外部 DB 即可验证 runtime metadata。

### M4: Fusion Task Abstraction

交付：

- `storage/task`
- memory backend
- Fusion scheduler skeleton

完成标准：

- 已完成接口抽象；当前同时具备 memory backend 和 Redis 持久化 backend。Redis backend 使用 ZSet/hash + Lua 作为一种实现，但 Fusion 只依赖 `DelayTaskBackend` 接口。

### M5: Generator Boundary

交付：

- `tx` 内不保留生成包。
- `txgen` 输出 `GD*.go`、`GD*Store.go`、cmd 和类型注册表。

完成标准：

- 已完成基础边界。生成产物只依赖 `github.com/ngnl47/tx`。

### M6: Generated Projection And Collection Store

交付：

- 生成 Store `Source` 主线，以及 `Capture` 同源累计与 `Merge` 独立批次合并能力。
- Store 模式和 snapshot diff 模式互斥。
- Store 树按直接字段、map key、slice index 懒创建路径子节点。
- map wrapper key 级状态与生成对象指针子 Store。
- slice wrapper 稳定 index 级状态与 `[]*GD...` 子 Store。
- 主 Store 使用可变 Store 状态树；StoreView 使用独立稀疏 GD 值和 txgen 生成的强类型视图状态树，不共享根 GD，也不依赖结构漂移回退。
- 生成包提供单份私有基础层，收敛集合状态、字段 undo、加载位图和 Store 读取装配；具体 `GD*Store.go` 只保留公开强类型 API 与捕获热路径的静态字段路由。

完成标准：

- 生成式投影、确定性帧内合并、危险 string key 降级、snapshot diff、路径 Store 树和生成包基础层已完成。当前生成 Store 在导出边界递归访问 dirty 路径；共享引用按路径隔离，StoreView 在捕获边界复制所选值并从类型系统上排除持久化入口。真实 txgame 生成 Store 包总行数已由 `411,893` 收敛到 `303,451`，并由 fixture 体积约束与运行时 benchmark 共同防回退。

## Open Design Details

当前仍需单独讨论或逐步验证的点：

- 是否需要字段级 snapshot 入口，用 benchmark 判断，不能凭感觉提前加 API。
- map/slice wrapper 是否引入更强的可观察容器；如果引入，应保持强类型、低分配、与生成式投影直连。
- RedisJSON/ReJSON 是否作为可选 backend 引入，需先确认目标 Redis 环境和收益。
