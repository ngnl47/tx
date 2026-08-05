# Tx storage metadata implementation plan

本文是 `github.com/ngnl47/tx` 存储元数据与运行时能力的实施计划。

目标是提供一套独立、稳定、可由 `txgen` 生成代码消费的 Go runtime 能力：

- 使用 Go 注释作为元数据主入口，保持类型定义和存储定义靠近。
- 使用 Go AST 读取 Go 类型结构和注释，建立注释元数据与 Go 类型之间的绑定关系。
- 语法形态使用紧凑注解写法，输入侧统一使用 Tx 语义。
- 不支持独立 schema 文件；存储元数据只来自 Go 注释、struct tag 和 AST 扫描结果。
- 运行时元数据只允许初始化期注册；不支持隐式运行时追加 tag。
- `fusion` 作为 Redis 热数据与 Mongo 冷数据组合模式名。
- Redis key 默认使用 `shardBy` 作为 Redis Cluster hash tag。
- Fusion 延迟任务抽象成可替换任务后端。
- 测试用例只放在 `tests/` 目录下。

## Current Stable Decisions

后续实现、代码审查和模型切换后接力均以本节为准；如果代码、使用手册和本节冲突，先停下来核对，不要按记忆继续改。

- `tx` 是 runtime 基础库，不依赖 `txgen`；`txgen` 生成的代码只依赖 `github.com/ngnl47/tx`。
- `txgen` 负责生成 `GD*.go`、`GD*Store.go`、cmd、字段名函数、字段编号和类型元数据；业务模板和集成样例放在 `txgame`。
- `GD*` 是纯数据对象；`GD*Store` 是独立、完整、可组合的变更追踪结构，绑定 `GD*` 后才具备读值、修改和投影意义。Store 只持有业务数据引用，不保存第二份业务数据。
- `GD*` 可以懒持有默认主 Store，作为 `role.SetName(...)` 这类代理入口；Store 本身不依附于某个 `GD*` 实例，可单独创建、绑定、组合和复制。
- Store 树按持久化路径组织：父 Store 持有复合字段、map key 或 slice index 对应的子 Store；子 Store 不向 `GD*` 写入父回调或父路径。`DeltaSet` 只在保存或下行边界从整棵 Store 树投影产生。
- 同一个 `*GD...` 可以出现在多个根对象、集合字段或集合 key 中。每条路径拥有独立 Store 节点和 dirty 状态，业务数据引用可以共享；框架不维护全局反向引用表，也不自动把一次路径内修改扩散到其他引用位置。
- `store.View()` 深复制 Store 的字段状态、集合状态和子 Store 树，浅复制绑定的 `GD*` 引用；不得直接复制含 map 或子 Store 指针的 Store 结构。主 Store 与 viewStore 的内部结构一致，但 viewStore 是只读投影模式：允许读取、`Touch/UnTouch` 和生成文档，不允许修改数据或持久化。
- viewStore 是字段选择状态的副本，不是业务数据快照。`ChangeDocument()` 在边界重新读取当前 `GD*` 值并校验指针、map key、slice index 路径；结构漂移时必须提升为最近安全祖先的整字段写，不能输出已失效的 key/index 路径。需要时刻快照时，业务必须在变更后的下一步立即物化 `ChangeDocument()` 或协议 body。
- 业务输入模型和协议字段不能直接声明生成的 `*Store` 类型；Store 只用于框架持久化和 Provider 投影输入。
- 业务 facade 和框架 API 分层。无状态 handler 推荐写 `db.Save(role)`；有状态服务的写业务通过项目侧 `state.Call(ctx, procedure)` 进入 `state.Manager`，由 Manager 在后续 Flush 内部取得 Store 并调用 `storage.Save`。
- `txgame/server/internal/db` 已提供泛型 `db.Save(role)` facade：它使用服务启动时装配的根 context 与 writer，并只在 facade 内部获取生成数据对象的 Store。
- 通用 `db.Create` facade 不作为当前目标。新建对象、默认字段初始化、初始资源发放属于业务流程；框架层保留 `storage.Create` 表达 upsert/首次缓存语义。
- 后续单机事务能力放在可选的 `tx/modules/txn`，`tx` 核心和普通 Store 路径不依赖该模块。没有进入 `txn.Run(...)` 时，不创建 undo log、不记录旧值，也不增加事务运行时开销。
- `@transaction` 是生成期类型能力声明，不是常驻事务开关。它独立于 `storage` 注解，为持久化 GD 或纯内存 GD 生成统一的本机 `LockKey()` 和 `LockTarget()`；`scope` 是一致性域分类，`keyField` 是域内对象键，`order` 是可选的非负锁层级，默认 `0` 且较小值先获取。
- 固定锁集使用 `txn.Locks(guild, role)`；前置查询后需要循环收集锁时使用 `txn.NewLockSet().Add(...).Build()`。两者只负责收集，重复锁、顺序、完整性和超时仍由 `txn.Run` 统一处理。
- `txn.Run(...)` 只处理单机内存事务：对输入锁对象的锁键去重、按逻辑 Key 全序获取本机锁，并在回调期间临时绑定 undo recorder。锁获取受传入 `context.Context` 控制；超时或取消时释放本次已获取锁，回调不会开始执行。它不解决分布式锁、跨实例一致性或 Redis/Mongo 分布式事务。
- `txn.Config.Debug.CheckCleanLockTargets` 是可选调试期检查。默认关闭；开启后，事务在获取全部锁后检查每个可检查根 Store 是否已有 delta，若有则释放锁并拒绝进入回调，防止事务外 Store 修改混入本次提交边界。项目负责从自己的 yml 配置转换并调用 `txn.Configure`。
- 可复用事务逻辑使用 `Procedure`：根 `txn.Call(ctx, procedure)` 在锁定完整目标集后执行；事务内只能通过 `tx.Call(child)` 调用子 Procedure，并建立 savepoint。子 Procedure 的目标必须是根事务已持有锁键的子集；发现新目标时返回包含 `A -> B` 调用路径的明确错误，不等待形成死锁。直接嵌套 `txn.Run` 不是业务 API，应直接拒绝。
- 根 Procedure 需要在进入事务前声明自身及子 Procedure 的完整锁键并集。若锁键依赖查询结果，先做只读预取，再按完整键集加锁并重新校验；不支持事务运行中动态扩张锁集。
- 需要在根事务提交后开启独立事务时，通过 `AfterCommit` 调度新的 `Procedure`，不与父事务共享原子性或锁。
- 单机事务只保证经 Store 受控 API 发生的修改可回滚；裸字段赋值和 snapshot diff 不参与事务回滚。undo log 属于一次事务的临时状态，不写入 GD，也不作为 Store 的长期业务数据。
- 事务锁对象由生成代码在 GD 与 Store 侧提供：业务通过 `txn.Locks(guild, role)` 收集，GD 的 `LockTarget()` 只代理默认 Store，不复制业务数据。字段 setter、map/slice 的 `Set/Delete/Replace` 和生成对象子 Store setter 在事务内登记 undo；事务成功后的存储写入、消息发送或后续业务动作通过 `AfterCommit` 等机制调度；这表示内存事务已提交，不等价于 Mongo 已可靠落盘。
- 可选 `modules/state.Manager` 服务于有状态运行时。它按 `LockKey` 聚合脏 Store；`manager.Call` 在事务成功后登记 `DirtyTargets`，不执行 I/O；`manager.Flush` 逐目标取得同一把本机锁，在锁内投影并压缩 DeltaSet 后调用 `storage.Save`，成功才清理 delta。Manager 不保存业务副本，不把 Store 或 DeltaSet 作为异步队列项。
- 无状态服务不使用 `modules/state` 和 `modules/txn`：请求内临时加载 GD，直接由项目 facade 调用 `db.Save` 或 snapshot `SaveChange` 写 Fusion。启用有状态运行时后，所有写操作都必须通过 `Procedure + state.Manager`；读取不要求进入事务。
- 当前标脏主线是 `change.DeltaSet`。热路径只保存 `FieldID`、map key、slice index 和 `DeltaKind`，不常驻拼接字符串路径。
- `DeltaEntry` 当前只保存 `Path` 和 `Kind`，不持有字段值；生成 Store 会在输出边界按 DeltaPath 读取当前值。后续如需性能优化，只做局部 fast path，不提前引入自研 value 编码。
- 字符串路径只在输出边界生成，例如 `ChangeDocument()`、Redis JSON patch、Mongo update document 和 cmd Provider body。
- `DeltaSet.Compact()` 只做路径上可确定的压缩：同路径后写覆盖先写，后发生的祖先写清理更早子路径；较早祖先不会吞掉较晚子路径，因为 runtime 不读取当前对象，无法证明祖先值已包含后续修改。不做需要外部生命周期上下文的新建删除抵消。
- `DeltaKindSet` 用于标记当前字段值；`DeltaKindReplace` 用于集合、结构体指针和集合元素的覆盖赋值。`SetItems(value)`、`ItemsStore().Set(key, value)`、`SetAt(index, value)` 都遵循 Go 赋值语义：持久化时不保留旧值中未出现的字段；对子对象的 setter 才保留局部 `DeltaKindSet` 路径。
- `MaxDeltaDepth` 是 Store 树投影和 snapshot diff 共用的真实递归预算，当前为 4 层 `PathNode`。即将超过预算时必须立刻停止向下比较或投影，将最近可解析祖先标为 `DeltaKindReplace`；不能 panic，也不能继续做无效深层 diff。每个生成数据类型通过 `sync.Once` 输出一次告警，提示该类型发生了深度降级。
- `map[string]` 的 key 如果为空字符串、包含 `.` 或 `$`，Mongo 边界不转义 key，而是降级整字段写，保证数据库中保留业务真实 key。
- Store 拦截路径和 snapshot diff 路径同时存在，但同一变更周期互斥。`ModeStore` 用于 setter/Store wrapper 标脏，`ModeSnapshot` 用于 `BeginChange/SaveChange`。
- `BeginChange()` 必须在业务直接修改字段之前调用；`SaveChange()` 成功后清理 snapshot 和本轮 dirty，失败时保留可重试状态。
- Store 路径适合高频对象和大集合字段；保存前可用 `store.View()` 复制下行选择状态，或立即物化下行文档，避免保存成功清理 dirty 后丢失广播字段。
- 当前集合策略：map wrapper 支持 key 级状态和生成对象指针子 Store；slice wrapper 支持稳定 index 级状态和 `[]*GD...` 元素子 Store。`InsertAt`、`Append`、`RemoveAt`、`Replace`、`Clear` 这类长度或整体语义变化按整字段处理；viewStore 发现 map/slice 结构漂移时同样整字段降级。
- 字段名体系本体是 `go/json/bson/msgpack`。Redis 当前选择 JSON 名，Mongo 当前选择 BSON 名，cmd Provider 当前选择 Msgpack 名；`alias` 只是输入侧语法糖。
- `storage.DocumentLoader` 和 `DocumentReplacer` 的公共文档契约统一使用 JSON 字段名。Redis 原样读写该文档；Mongo adapter 在完整文档读取与替换边界通过 `TypeSpec.DocumentFactory` 物化生成类型，再按 BSON tag 转换。Fusion 只传递公共 JSON 文档，不直接搬运 Mongo 的 BSON map。
- Fusion 是 Redis 热数据加 Mongo 冷数据。Redis main/subKey 都使用 string JSON 文档；RedisJSON/ReJSON 仅作为后续可选 backend，不是当前默认路径。
- Redis 局部 patch 只会创建缺失的 JSON object 父节点；已有父节点为 `null`、标量或数组时返回 `storage.ErrPathTypeMismatch`，不做静默类型修复。业务若要有意重建复杂字段，必须走字段级 `Set*` 的 `DeltaKindReplace` 覆盖。
- `delaySaveTime` 表示 Redis 热数据变更后多久回写一次 Mongo；`delayDelTime` 表示 Redis 热数据 TTL 和自动续期时间，不表示 Mongo 删除延迟。
- Fusion 延迟任务队列只保存记录定位信息；到期后读取 Redis 当前最新热数据，整文档 replace 到 Mongo。队列不保存中间态业务对象或本轮字段 diff。
- 从 Mongo 回填 Redis 必须具备 `delayDelTime > 0` 的热数据 TTL；并发 miss 回填要通过 Redis 锁和二次读取避免旧 Mongo 快照覆盖新 Redis 热数据。
- Redis `<mainKey>@v` 是热数据生命周期版本键，只用于 Redis 删除/过期窗口并发保护，不是业务字段，也不是 Mongo 字段；版本键和 main/subKey 必须使用同一 hash tag，并在每次热写时使用同一有效 TTL 续期或同步转为持久化。
- Redis 批量读取默认按生产云代理能力走 endpoint 内原生 `MGET`；本地原生 Redis Cluster debug 才显式打开 `LoadOptions.ClusterSafe` 按 hash tag 分组多次读取。
- Mongo 不内置业务删除策略。物理删除接口存在，但是否调用由业务决定；TTL 索引是 Mongo 自身按字段值删除整条文档的能力，生成阶段限制同一类型最多一个 TTL 字段。
- 后续如发现中间过渡结构、废弃测试或过时文档描述，应持续瘦身；保留稳定决策，不保留已经放弃的实现路线。

## Next Work Items

当前后续实施优先级：

已完成基线：

- 生成 Store 已使用父 Store 路径树组织直接指针、map key 和 slice index 子 Store；`Deltas()` 在边界递归投影并压缩，viewStore 会复制状态树且禁止写入、持久化。
- `map[K]*GD...` 遇到不能表达为 `PathKey` 的 key 类型时，仍可通过子 Store 修改数据，但导出时保守替换整 map；`txgame` 已覆盖枚举 key 的真实输入回归。
- `txgame` 的 `GDRole`、`GDLegion` 已使用 `@transaction` 生成锁键与锁对象，并以 `PGuildContribution` 覆盖多对象锁序、生成 Store undo 和回滚后变更文档清理。
- `txn.Tx.DirtyTargets()` 已按锁顺序暴露本次经受控 Store API 修改且仍有 delta 的根目标；savepoint 回滚或清空 delta 的目标不会出现。`modules/state.Manager` 使用它在事务成功后登记落盘对象；`txgame/server/internal/state.Call(ctx, procedure)` 是有状态项目侧入口，`runtime.state.flushInterval` 控制生命周期内的定时 Flush，`runtime.state.shutdownFlushTimeout` 控制服务退出时的最终 Redis 刷盘窗口。生命周期先用服务全局 `WaitGroupContext` 等待已经进入的业务任务结束，再使用不受运行 context 取消影响的限时 context 进行最终刷盘；它不等待 Fusion 延迟队列回写 Mongo。`runtime.transaction.debug.checkCleanLockTargets` 仅用于临时、无历史 delta 的直接 `txn.Call`；Manager 会显式允许此前未 Flush 的 delta。
- Fusion 批量混合命中先批量读取 Redis；Mongo reader 支持批量读取时，所有 miss 只做一次 Mongo 全文批量读取，再逐条进入记录锁、二次 Redis 读取与带 TTL 回填。锁内命中 Redis 时直接返回新热数据，不能以旧 Mongo 候选覆盖它。

1. 保持 `txgame` 集成验证与生成 Store 行为同步：新增集合、指针、存储边界行为时，补业务侧代码示例及真实 Redis/Mongo 回归。
2. 持续收紧 Redis/Mongo/Fusion adapter 的失败契约：未知脚本协议结果、路径类型冲突和路由错误必须显式返回错误，不能静默成功或做运行时数据修复。
3. 持续清理不再作为主线的字符串路径变更代码、过渡接口和旧测试，只保留必要历史 benchmark 结论。
4. 日志基建后续专题：确定业务项目 logger 枚举或包级变量、名称规范、级别配置和 tx/log 的最小扩展点。
5. RedisJSON/ReJSON 仅作为未来可选 backend 单独评估；SQL、消息队列等其他 backend 也不进入当前主线。
6. 完善可选的 `tx/modules/txn` 与 `tx/modules/state`：`@transaction` 已生成 `LockKey()` 和 `LockTarget()`；受控字段、map、slice 与生成对象子 Store 已登记 undo，并覆盖事务失败后的回滚。事务锁使用按逻辑 Key 管理的本机锁表，按稳定顺序获取，并由 `context.Context` 提供锁等待超时；超时必须释放已获取锁且不得进入回调。`Procedure.Locks`/`tx.Call` 已约束锁集和输出嵌套路径；Manager 已覆盖事务后登记、持锁 Flush、失败重试，以及先等待全局 `WaitGroupContext` 再执行的最终限时刷盘。后续用真实 Redis/Mongo 压测决定是否需要不可变写入批次；第一版不处理分布式锁、跨实例事务或持久化层 ACID。
7. 在状态运行时边界稳定后，评估可选 `tx/modules/actor` 与 RPC。actor 负责业务一致性域的单实例串行执行和跨进程路由；事务模块只负责该实例内的多对象锁定、回滚和提交，Manager 只负责当前进程的脏实体落盘，三者不反向依赖。

## Module Layout

当前包结构方向：

```text
tx/
  meta/             # 元数据模型、registry、annotation 输出模型
  anno/             # Go 注释中的 type-level tag 解析、规范化、校验
  astscan/          # Go AST 扫描器，提取 package/type/field/method/comment 信息
  change/           # DeltaSet、DeltaPath、变更文档和变更模式
  lifecycle/        # 可取消、可等待后台任务结束的运行时上下文
  log/              # 基于 slog 的最小日志门面
  storage/          # 存储抽象：Store mode、route、key、options、runtime metadata
  storage/config/   # 项目 yml 路由配置的强类型承接模型
  storage/redis/    # Redis key builder、route resolver、后续 Redis adapter
  storage/mongo/    # Mongo collection/index/sharding metadata、后续 Mongo adapter
  storage/fusion/   # Fusion 策略与延迟任务抽象
  storage/task/     # Delay task backend interface 和内存实现
  modules/txn/      # 可选：单机事务、savepoint、undo log 和 after-commit hook
  modules/state/    # 可选：有状态实体脏数据聚合与定时 Flush
  modules/actor/    # 后续可选：业务一致性域的单实例串行执行与 RPC 路由
  util/             # 小型通用工具
  tests/            # 所有测试文件，仅此目录下生成 *_test.go
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
- `meta.Overlay`
  - 预留显式覆盖机制，但第一版可以只定义接口，不启用运行期修改。

验收：

- Registry 支持初始化期注册、冻结、查找。
- Freeze 后注册或覆盖返回明确错误。
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

## Phase 5: Runtime Storage Metadata

目标：把 type-level 注释 tag、field-level struct tag 和 AST type 信息组合成运行时可用的 storage metadata。

建议实现：

- `storage.TypeConfig`
  - `StorageMode`
  - `Redis *RedisConfig`
  - `Mongo *MongoConfig`
  - `Fusion *FusionConfig`
  - `Fields map[string]FieldConfig`
- `storage.Resolve(specs, packageSet) (*storage.Registry, error)`
- `storage.Registry` freeze 后用于 runtime。

关键行为：

- `mode=fusion` 要求 redis/mongo 均存在。
- Redis key builder 默认：`namespace:key:{shardBy}:id` 当 `id != shardBy`。
- Redis key builder 当 `id == shardBy`：`namespace:key:{id}`。
- Mongo filter 默认：`{idField: id}`；若 sharding enabled 且 `id != shardBy`，补 `shardBy`。
- 生成 route resolver，不依赖反射临时推导。

验收：

- 给定 type config，能生成 Redis key。
- 给定 type config，能生成 Mongo collection/filter/index metadata。
- 所有 runtime metadata 初始化后不可变。

测试：

- `tests/storage_key_test.go`
- `tests/storage_mongo_filter_test.go`
- `tests/storage_resolve_test.go`

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
- Redis delayed task backend 面向多服务实例并发消费：claim/complete/retry 通过 lease token 隔离，但整体语义是 at-least-once，后续 Fusion Mongo 回写必须按存储幂等方式设计。
- `log`: 基于标准库 `log/slog` 提供最小 logger 注入入口，默认使用 `slog.Default()`；已提供 `Debug/Info/Warn/Error` 和 Context 版本的包级快捷方法，并提供 `NewLogger(name)` 轻量命名 logger 代理；Fusion Scheduler 失败重试时输出结构化错误日志，失败原因不进入 Redis 队列状态。
- `storage/fusion.TaskQueue` 负责把 Fusion 延迟保存 operation 转换为 `storage/task.DelayTask`，只保留 record id、route/shard key、action 和 due time；任务类型优先来自 `DelayedOperation.Type`，为空时使用 `RecordKey.TypeName`；Fusion 层不暴露额外 suffix，同一记录的延迟保存按 locator 合并为一条任务；入队时只接受保存 action，并要求类型和 record id 非空，避免无效任务进入 Redis 队列。延迟保存任务不携带业务对象或字段变更日志，回写时重新读取 Redis 当前热数据。
- `storage/fusion.StorageTaskHandler` 负责消费 delayed task：保存任务从 Redis 读取完整热数据并 replace 到 Mongo；Redis 热数据延迟删除任务会读取 `<mainKey>@v`、保存 Redis 当前热文档到 Mongo，然后仅在版本未变化时删除 Redis main/subKey/version key，版本变化时重新排队。
- Fusion 显式删除不进入延迟队列：运行时先删除 Redis 热数据，再通过显式 Mongo deleter 立即删除 Mongo 文档；如果业务绕过 Fusion 只删除 Mongo，未过期 Redis 热数据仍可能被延迟保存任务重新写回 Mongo。
- Fusion 延迟保存已通过真实 Redis/Mongo 集成路径验证：Redis 同步写入热数据，Redis delayed task backend 入队，Scheduler claim 到期任务，StorageTaskHandler 读取 Redis 热数据并整文档 replace Mongo。
- Fusion 读取默认在 Redis miss 后从 Mongo 全量加载并回填 Redis 热数据，回填路径运行时要求 `delayDelTime` 大于 0；回填前会获取记录级 Redis 锁并二次读取 Redis，防止并发 miss 时 Mongo 快照覆盖新的 Redis 热数据；显式 `DontLoadToRedis` 选项才允许直接读 Mongo 且不回填；部分字段读取主要按 Redis main/subKey key 粒度减少热数据读取，只请求 subKey 字段时不强依赖主文档命中。
- Redis 热数据生命周期版本键 `<mainKey>@v` 已作为运行时原语实现并接入 Fusion 延迟删除 handler：Redis delete 可带 expected revision 比对，只在版本未变化时删除 main/subKey/version key。每次 Redis 热写都会递增版本键，并让它与 main/subKey 使用同一有效 TTL 续期；持久化热数据则同步持久化版本键。该版本只用于 Redis 热数据过期/删除窗口的并发保护，不是业务字段，也不是 Mongo 文档字段。
- `txgame` 已用真实 Redis/Mongo 验证两个版本键窗口：热写的 main/subKey/version key 会共同自然过期；延迟删除在 Mongo 回写期间遇到 Redis 新写时，条件删除会失败、保留新热数据并重新排队删除任务。
- `txgame` 已用真实 Redis/Mongo 验证并发 Redis miss 回填：两个请求同时在 Redis miss 后竞争同一记录锁，只有一个请求回源 Mongo；另一个请求在锁内二次读取到已回填的 Redis 文档，不会以第二份 Mongo 快照覆盖热数据。
- Mongo runtime 已提供最小 route/cluster adapter，按 routeType 和 RecordKey.ShardKey 路由单条读写，并在批量读取时按 Mongo shard 分组后还原输入顺序；`storage/mongo/mongodriver` 提供官方 MongoDB driver 的薄适配层。
- Redis runtime 已提供与 Mongo 对等的最小 route/cluster adapter：先按 routeType 和 RecordKey.ShardKey 路由到具体 Redis endpoint/adapter，再在每个 endpoint 内执行批量读取并还原输入顺序；`storage/redis/goredis` 提供 go-redis 的薄适配层。
- Redis 写入和整文档替换 API 直接使用 `storage.ApplyOptions`，不再保留 Redis 包内的 `ApplyOptions` 类型别名；`redis.LoadOptions` 仅表达 Redis 批量读取专用选项。
- `storage.RouteTable` 已提供 Redis/Mongo 共用的运行时路由表：支持 `min/max` 区间命中、多 shard 按 `routeBy % len(shards)` 选 endpoint、单 shard 字符串主键直连，并同时实现 Redis/Mongo cluster router 接口；后续 yml 配置解析应构造该路由表，而不是在 Redis/Mongo adapter 内重复实现路由规则。
- `storage/config.RouteTableConfig` 已提供项目 yml 路由配置的强类型承接模型：支持标量 endpoint、endpoint 数组和 `min/max/shards` 规则数组；项目负责读取本地 yml 和 include，tx 负责把解析后的配置对象归一化为 `storage.RouteTable`。
- Redis 批量读取默认贴合生产云存储代理能力：同一个 Redis endpoint 内允许直接批量读取多条不同 hash tag 的记录，由云代理负责跨 slot 分发和结果聚合，避免把生产热路径退化为多次 IO。
- Redis 批量读取应使用客户端层原生 `MGET` 展平 main/subKey key 后读取，再在 Go 侧按固定步长合并文档；不要把跨记录批量读取包进 Lua。云代理能识别 `MGET` 的所有参数都是 key，但不能可靠分析自定义 Lua 中哪些 `ARGV` 是 key，也不能安全拆分一个需要原子执行的脚本。
- 本地原生 Redis Cluster 或直连节点只作为研发 debug 兼容路径：已提供显式可选的 `LoadOptions.ClusterSafe` 模式，按 hash tag 分组后多次读取，保证本地集群可复现和排查跨槽问题，但不作为生产默认策略。
- StorageTaskHandler 在 Mongo replace 前会递归移除 Redis 文档中的 nil 删除标记，避免已删除字段以 Mongo `null` 形式重新落库。
- `delaySaveTime` 表示 Redis 热数据变更后，经过多久触发一次 Mongo 回写；`delayDelTime` 表示 Redis 热数据 TTL，用于释放长时间不活跃记录的热备数据，不代表 Mongo 删除延迟。
- `change.Mode` 已作为生成代码的持久化模式边界：`ModeSnapshot` 用于原生字段修改后的快照 diff 持久化，`ModeStore` 用于生成 Store 的拦截式脏标记持久化；两者在同一变更周期内互斥。当前快照 diff 由 txgen 生成代码实现，tx 只提供共用模式枚举和错误。
- RedisJSON/ReJSON 可作为后续可选 Redis backend 单独评估：当前主线仍保持 Redis string JSON main/subKey 文档模型，后续只有在确认目标云 Redis、本地测试环境、Redis Cluster 行为和 JSON path 局部读写收益后再引入。
- 后续可增加 SQL backend、消息队列 backend；当前优先贴合生产参考流程，不引入 Kafka 等新的持久化队列依赖。
- 日志基建后续专题：在 txgen 脚手架主链路跑通后，提前讨论并实施项目级 logger 管理策略，包括业务项目统一定义 logger 枚举或包级变量、logger 名称规范、日志级别配置、handler 初始化、Provider/Factory 是否需要引入，以及是否需要自动推导包名或模块名以减少手写 logger 名称。

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
- Fusion 后台 worker 使用 lifecycle 上下文，在优雅关停时等待已 claim 的 Mongo 回写任务完成。
- Fusion delayed save worker 可把 Redis 热数据回写到 Mongo，并且不会把删除标记落成 Mongo null 字段。
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

- Redis adapter 支持 string JSON main/subKey 文档、Lua merge/replace、TTL 续期、批量读取和可选 `ClusterSafe`；`redis.Cluster` 的 shard 映射使用 `ScriptClient` 强类型约束，实际 I/O 遇到未装配 client 会返回 `redis.ErrClientNotConfigured`，不再静默 no-op。
- Mongo adapter 支持 load、batch load、delta update、replace、delete、route cluster 和官方 driver 薄适配层。
- Fusion strategy 支持 Redis 热写、Mongo miss 回源、Redis 回填、延迟保存任务入队、后台 scheduler 和 Redis 到 Mongo 整文档回写。
- Mongo JSON/BSON 整文档转换已接入生成 `TypeSpec`：不同 JSON/BSON 字段名、Mongo `_id`、嵌套 struct/map/slice 均通过类型对象中转；真实 Redis/Mongo 集成测试已覆盖 Fusion 回填、延迟回写和生成 reader。
- `storage/config.RouteTableConfig` 支持项目 yml 路由配置归一化；项目负责读取 yml 和创建 driver client。

后续重点：

- Redis 热数据版本键 `<mainKey>@v` 在更多写入/删除窗口中的并发保护测试。
- Fusion 并发 miss 回源的批量真实 Redis/Mongo 场景补充验证。
- Redis/Mongo/Fusion adapter 围绕 `DeltaSet` 继续瘦身，清理不再需要的过渡接口。
- SQL 或其他 backend 只作为未来扩展，不进入当前主线。

## Testing Policy

用户已确认：仅在 `tests/` 文件夹下生成测试用例。

执行规则：

- 所有 `*_test.go` 只放在 `/Users/a47/workspace/tx/tests`。
- 包目录如 `meta/`、`storage/` 不放测试文件。
- 测试 package 统一使用 `package tests`。
- 测试通过 public API 驱动，避免为了测试暴露内部实现。
- 临时输入文件用 `t.TempDir()` 生成。
- `tx` 单元测试默认不依赖外部 Redis/MongoDB；真实 Redis/Mongo/Fusion 集成验证放在 `txgame/server/tests/integration`。

## Milestones

### M1: Tag Annotation And AST Foundation

交付：

- `meta`
- `anno`
- `astscan`
- tests only in `tests/`

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

### M6: DeltaSet And Collection Store

交付：

- `change.DeltaSet` 主线。
- Store 模式和 snapshot diff 模式互斥。
- Store 树按直接字段、map key、slice index 懒创建路径子节点。
- map wrapper key 级状态与生成对象指针子 Store。
- slice wrapper 稳定 index 级状态与 `[]*GD...` 子 Store。
- 主 Store 与 viewStore 使用同一追踪结构；viewStore 在边界校验路径，结构漂移时整字段降级。

完成标准：

- `DeltaSet`、确定性 Compact、危险 string key 降级、snapshot diff 与路径 Store 树已完成。当前生成 Store 在导出边界递归收集路径子 Store；共享引用按路径隔离，viewStore 会重新校验结构漂移并拒绝写入/持久化。

## Open Design Details

当前仍需单独讨论或逐步验证的点：

- 是否需要字段级 snapshot 入口，用 benchmark 判断，不能凭感觉提前加 API。
- map/slice wrapper 是否引入更强的可观察容器；如果引入，应保持强类型、低分配、与 `DeltaSet` 直连。
- 日志基建是否需要项目级 logger registry、名称枚举或自动包名推导。
- RedisJSON/ReJSON 是否作为可选 backend 引入，需先确认目标 Redis 环境和收益。
