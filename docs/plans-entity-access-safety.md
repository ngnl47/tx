# GD 访问安全与 World 独占运行时实施计划

> 执行方式：使用 executing-plans 按批推进，每批先固定关键测试，再实现和回归；不自动提交 Git。

**状态：** 2026-10-29 第 1～7 批已实现并完成本地回归，未提交。两组应用已迁移共享读写调用及类型声明；World 独占加载、四网格 Monster 与邮箱保存闭环已验证。整体回归、微基准和未验证范围见 [验收报告](entity-access-acceptance.md)。

**目标：** 区分共享缓存实体与业务运行时独占实体，补齐 Mongo 原生入口、类型缓存排除和共享访问安全，最后完成四网格 World 最小运行示例。

**架构：** EntityCache 共享对象由现有 Procedure 记录锁保护，普通查询返回安全快照。World 独占 GD，直接从 Mongo 加载，网格邮箱串行修改和保存，不经 EntityCache、不持有整份世界副本。两种路径复用存储与生成 Store，不增加第二套锁、不自动包装 Handler。

**技术栈：** Go、tx EntityCache/Procedure/WriteBack、txgen 模板及生成 Store、本地 Redis/Mongo 验收。

**依据：** 本文“访问契约”落实当前确认方向；[统一实体访问](plans-entity-access.md) 已同步为现行契约，不再保留普通调用取得共享可变对象的旧方案。创建、删除、使用期和存储契约保持。

## 事实与范围

第 4 批之前，`modules/entitycache/runtime.go` 的 Find 在没有 Procedure 时仍返回缓存中的同一个指针。操作范围只防止对象被回收，不保护字段；后台 WriteBack 即使取锁，也无法约束绕过锁的 Handler。并发遍历和修改 Go map 可能导致不可 recover 的运行时 fatal。

rj3d/xdb 的参考价值是区分访问方式，而不是为所有 Handler 自动开事务：

- `xdb/TTable.java` 的 get 使用当前事务；select 在记录锁内调用字段选择函数。
- 生成的 `xtable/Bag.java` 的 select 和 selectItems 通过 toData/getItemsAsData 返回独立数据。
- Welfare 等普通查询存在非 Procedure 的 select；事务回调中的访问也不应被误判为无事务。
- 写回与回收使用记录锁。Java 可重入锁不能直接照搬到 Go，事务内快照必须识别并复用当前事务。

以上依据来自本地 `rj3d_server-1.0.0.jar.src/src/main/java` 和 `xdb-0.0.1.jar.src/xdb` 的只读核查；不是全工程静态证明，也不表示其全部调用均无误用。

本轮约束：

- 保留显式 ctx、生成期 `data.entityCache` 开关、按需缓存；少数持久根用 `@noEntityCache` 排除，不维护注册清单，不增加 CMod 或运行期开关。
- txgame 保持工程缓存关闭；txgameslg 保持工程缓存开启，World 独占的持久根显式排除。不把无缓存或运行时独占数据强制改为 Procedure。
- 不恢复 Access、可选 ctx、第二套 db.FindGD*、自动重试 Handler 或隐式协程上下文。
- 不改变 Actor 路由、消息协议、合服、Redis 原始存取及 Fusion 的序列化格式。
- 不运行云测试、不清理共享数据库、不提交已有工作区变更。
- 业务返回值、事件和异步任务不能携带缓存对象或其 map/slice 的别名。Go 导出字段无法阻止人为保留裸指针，本轮提供受控 API、误用检查和示例审查，不宣称类型系统已杜绝一切逃逸。

## 访问契约

下表的“缓存开启”指工程开关开启且类型未被排除；“缓存关闭”包含 `@noEntityCache` 类型。同一记录不得同时拥有缓存可写对象和 WorldRuntime 可写对象。World 可以长期持有其独占 GD 指针，禁止的是在保护范围外任意访问共享缓存对象。

| 操作 | 缓存开启 | 缓存关闭 |
| --- | --- | --- |
| `data.FindGD*(ctx, id)` | 必须在有效 Procedure 中，取得并纳管唯一对象；无事务尽早报错 | 保持现有加载行为和事务接入 |
| `data.SnapshotGD*(ctx, id)` | 优先读取唯一缓存，在记录锁内深复制，返回独立对象 | 返回独立加载结果 |
| Procedure 内调用 Snapshot | 复用当前事务锁，允许读到本事务修改；不是“已提交版本” | 遵守现有事务上下文，不另起事务 |
| `db.Create(ctx, value)` | 新建临时对象可在事务外构造；纳入共享缓存的业务创建在 Procedure 中完成 | 保持原行为 |
| `db.Remove(ctx, value)` | 共享实体的业务删除在 Procedure 内完成 | 保持原行为 |
| `db.Save(ctx, value)` | 不允许以快照或事务外裸对象覆盖共享实体；事务提交交给 WriteBack | 保持显式保存及已有事务限制 |
| 框架写回、回收、条件初始化 | 受控内部路径，使用相同记录锁及既有创建/删除状态；不伪造业务 Procedure | 保持存储语义 |

Snapshot 是独立数据，不是可写缓存代理；不要修改快照后 Save 来覆盖实体。多实体快照默认逐实体一致，不承诺整个批次处于同一时间点；需要跨实体一致性时使用显式 Procedure。

完整身份的快照必须观察缓存中已提交但尚未写回的数据，不能直接从 Mongo/Redis 读旧版本来冒充当前快照。非主键扫描、账号唯一约束、条件回填等存储权威查询仍保留独立语义，不伪装成缓存查询。字段投影必须明确归属这两类之一。

### 类型排除与锁能力

工程开关保留，持久根可声明无参数的 `@noEntityCache`：

```go
// @genData
// @noEntityCache
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"world" routeBy:"WorldId" idField:"Id" collection:"world_buckets" enableSharding:"true"`
type GDWorldBucket struct {
    Id       string
    WorldId  int64
    BucketId uint32
    Units    map[uint64]*GDWorldUnit
}
```

- 有效缓存选择为：工程开关开启、类型具有完整持久身份、未声明排除。
- 排除类型不生成 EntityDescriptor，Find/Load 及 db.Save/Create/Remove 不再因该类型被识别为缓存对象而进入缓存协调。
- 内嵌数据跟随根对象，不单独排除；将注解放在非持久根时生成报错。工程开关关闭时该注解仍合法，不改变存储模式。
- `@transaction` 与缓存选择分别表达能力；排除缓存不隐式删除事务声明，也不强制业务使用事务。已有锁身份不随本次调整变化。
- 只有最终仍纳入 EntityCache 的持久根需要现有事务锁能力，生成时检查，不能据此要求所有 GD 增加事务注解。

World 的 `GDWorldBucket` 和最小身份 `GDWorld` 由 WorldRuntime 独占，显式排除。GDUser/GDServer 先按实际调用核对其存储权威与共享缓存边界：不要机械补事务声明，更不能将两阶段建角 CAS 变为延迟缓存回写。若存储直接写与缓存读取并存，明确收敛到原有权威路径；需要新增失效协议时先报告，不暗中增加双写机制。

### Mongo 原生操作入口

参考 evo-server 的 `server/db/record.go`：取得类型对应数据库/集合，再使用 bson 条件、Find options 等原生能力。tx 复用已装配的进程级 Mongo 连接与路由；应用 db 工具层只提供薄入口，不另建连接，不新增查询 DSL，也不扩展生成 Find/Load 来承接任意查询。

调用：`db.MongoCollection(ctx, data.GDWorldBucketTypeSpec(), worldID)` 返回可使用官方驱动语法的 Collection；worldID 用于现有存储路由。具体公开接口位置在 tx 存储装配层统一，业务不能手动拼接数据库名、连接串或重复维护集合名。

世界加载使用 `Find(ctx, bson.M{"worldId": worldID}, options.Find().SetBatchSize(...))`，按 cursor 逐条解码并接管，不调用 All 聚合全世界再复制。这里读取的是 Mongo 的持久数据，不包含共享缓存尚未写回的状态。原生写操作也不自动同步缓存或加入 Procedure。

解码后须验证 BSON 字段映射、嵌套 map/slice、数值类型和生成 Store 初始化；不能假定原生 Decode 与现有生成加载器等价。优先复用既有类型描述及解码能力，不为每个 GD 再生成一套 Mongo 查询 API。

### World 独占数据与保存

```text
Mongo 游标分批读取
  → WorldRuntime 接管 GDWorldBucket
  → WorldBucket 包装唯一 GD + 必要索引/邮箱，不复制完整字段
  → tick / scheduler / 外部请求投递网格任务
  → 同一网格邮箱串行修改生成 Store
  → SaveTask 在邮箱内等待 db.Save 完成
      成功：按现有保存契约清理变更
      失败：保留变更并报告，后续受控重试
```

周期保存由业务运行时调度，第一版示例可用十分钟周期；它不是框架固定策略。使用有界并发、错峰投递，避免一次性冲击 Mongo；不为一个网格积压重复周期保存任务。只有该网格邮箱会修改/序列化其独占数据，SaveTask 不启动持有裸指针的异步保存 goroutine，主 tick 不直接修改网格字段。

这条路径先复用生成 Store、db.Save 和底层 Mongo 写入，不让 Procedure WriteBack 的后台循环读取独占数据。默认不采用长期完整比较快照，变更通过 Store 记录。若同步保存实测阻塞网格不可接受，再评估有界变更快照和异步保存，本轮不提前实现。

排水：停止 tick/周期任务生产与外部准入 → 等待已接收网格任务完成 → 在各网格执行最终保存并等待结果 → 成功后释放世界。复用既定有限重试/失败退出契约；入队成功不是保存完成，ctx 超时也不表示执行已退出。初始化失败同样收尾已创建的任务和邮箱。

周期保存不承诺进程意外退出前最后一个周期的变更已落盘；逐网格保存也不是跨网格同一时刻的原子世界检查点。不接 Redis，不改为 Fusion。

### 两条调用流程

```text
普通只读 Handler
  → SnapshotGDRole(ctx, id)
  → 保持缓存使用权 → 取得记录锁 → 深复制 → 解锁、释放使用权
  → 返回独立快照 → 业务读取/序列化

修改 Handler
  → 显式 Procedure
  → FindGDRole(tx.Context(), id)
  → 保持使用权 → Enlist/记录锁 → 返回唯一对象
  → Store 修改 → 提交登记 WriteBack 或回滚
  → 事务释放锁和使用权
  → 返回独立响应数据

后台 WriteBack
  → 同一记录锁 → 构造安全写回数据 → 按既有持久化流程执行
```

Procedure 可能按既有规则重试，事务体不引入 RPC、下行发布等不可回滚副作用。ctx 取消不是释放锁或缓存使用权的证明，必须等待实际执行退出。

## 工程职责与文件

| 工程/位置 | 修改职责 |
| --- | --- |
| tx/modules/entitycache/runtime.go、scope.go、cache.go、create.go、remove.go | 限定共享访问、增加快照运行时入口、审查低层 Use/Find 是否能绕过约束、保持使用期 |
| tx/modules/txn/access.go、txn.go | 复用当前事务锁及动态 Enlist，不在持锁事务内嵌套 WithLocks |
| tx/modules/writeback、runtime/persist.go、internal/framework/entity_cache.go | 对齐共享对象创建/删除、写回、释放和快照；保留排除类型的直接存储路径 |
| tx/storage/runtime/runtime.go、storage/mongo/mongodriver、runtime 与 CMod Mongo 访问层 | 复用进程连接暴露 Mongo 原生集合能力，保留启动/关闭与路由边界 |
| txgen/gen/templates/entity_access.go.tmpl、store.go.tmpl、find_object.go.tmpl | Find 准入、Snapshot 类型化入口、投影/批量/别名契约 |
| txgen/gen/model.go、render_data.go、render_store.go 及注解解析/校验入口 | noEntityCache 类型排除、有效缓存根能力校验；复用 snapshotCopy，不增加逐 GD 公共描述类型 |
| txgameslg/gen/data、两组 server/gen/data | 补最小输入声明并重新生成；不手改生成产物 |
| 两组 server/internal/db、internal/process | 统一写入误用检查，保留操作期限与持久化边界 |
| txgameslg/server/services/slggame/modules | 普通查询改快照；Procedure 内查询后修改；移除“先拿裸对象再开事务” |
| txgameslg/server/services/slgworld/registration.go、handler、modules/internal/world | 业务 WorldRuntime、独占网格、Mongo 加载、邮箱保存与四格移动示例；保留加载成功后 Ready |
| 两组 server/services/internal 的身份/目录/建角入口 | 审查存储权威查询，避免被缓存替代；回调/响应不泄漏共享指针 |
| tx/tests、txgen/tests、两组应用测试与文档 | 分别验证框架安全、生成契约、实际业务接入 |

## 第 1 批：按持久根排除 EntityCache

- [x] 在 `txgen/tests/entitycache_test.go` 补工程开关与类型排除组合的生成/编译用例，以及非持久根误用注解的失败用例。
- [x] 在现有注解解析、校验和 Store 模型中支持 `@noEntityCache`，有效缓存选择统一计算，避免查询、描述、保存三处各用不同条件。
- [x] 验证排除类型不提供 EntityDescriptor，Find/Load 不进入缓存，保留 Store 和原有事务声明能力；未排除类型行为保持到第 4 批调整。
- [x] 为 SLG 的 GDWorld 增加排除，重新生成并编译，移除激活期多余的 EntityCache 使用范围；不增加 Redis/Fusion，不手改产物。
- [x] 在应用契约测试中证明 GDWorld 可直接 Save/Create/Remove，不要求缓存登记，保存失败保留变更；仍受管对象不能借此绕过校验。

执行调整：GDWorldBucket 的排除必须与第 3 批迁移同时生效。当前 `loadWorldBucket → FindGDWorldBucketByID → MoveUnit Procedure → WriteBack` 仍依赖唯一对象；若先排除缓存，相同记录再次加载得到另一 Store，在首次写回完成前就可能触发 WriteBack 的 ErrIdentityConflict。不能为了完成单项勾选留下失效的业务链路，也不增加临时缓存兼容层。

本地验证：生成注解识别与 GDWorld 未绑定缓存查询分别先观察到失败，再完成实现；txgen 全量测试通过（含混合/关闭/全部排除的生成包编译）；txgameslg gen 回归通过。server 全量回归中三个包先因沙箱禁止回环端口监听失败，授权本地监听后原包重跑通过；其余包通过。`internal/db` 和 `services/slgworld/...` 的 race 通过。未显式启用真实 Mongo/Redis 集成用例、未跑云测试。按 ponytail-review 检查了注解到生成路径、存储误用检查及 World 生命周期，本批未保留阻塞项。

完成标准：选择在类型定义处可见，无需业务每次查询选择绕过缓存，无需启动登记。

## 第 2 批：Mongo 原生入口

- [x] 在 tx 存储 runtime/mongodriver 测试中固定连接复用、类型集合与路由选择、未启用 CMod/已关闭资源的错误边界。
- [x] 通过现有 Mongo runtime 暴露官方驱动集合能力；两组 `server/internal/db` 增加一致的薄入口。不扩展生成 Find/Load 的过滤语法。
- [x] 使用本地 Mongo 测试原生条件、排序、投影和游标迭代，区分不同 worldId，不一次读取整个集合至临时切片。
- [x] 验证 GDWorldBucket 的 BSON 解码与生成 Store 接合：嵌套单位、整数坐标、uint64 键及既有字段映射不失真；解码后的 Store 为干净状态，修改后能生成预期更新。
- [x] 验证 cursor 出错/取消/正常结束均关闭；应用不关闭进程共享 Mongo Client，原生操作不隐式加入 EntityCache 或 Procedure。

完成标准：高级查询直接使用 Mongo 语法，复用资源、类型信息和解码能力，不新增通用查询 DSL。

2026-10-29 验收：复用 Cluster 路由和 mongodriver 原生 Database，经 CMod/运行时能力边界取得句柄。两组 db 提供一致入口；未增加查询 DSL 或生成查询函数。原生 Decode 使用新 GD 对象，既有 Store 惰性接合即可，无需改生成器；不附加生成 Load 的字段投影状态，投影应解码为独立查询结构。GDWorldBucket 正式缓存/事务路径尚未迁移，本批没有通过原生写入绕过它。

本批测试先确认新增入口未实现时编译失败，再完成连接复用、路由、能力和关闭检查。本机 Mongo 的独立测试库验证 worldId 筛选、排序、批大小 1 的游标、投影、取消/解码错误后关闭；覆盖最大 uint64 map 键、超过 JS 安全整数的单位 ID、负坐标及嵌套 Store 更新。测试库已清理。tx、txgen、两组 server 全量本地回归通过；框架回归首次因沙箱禁止本地监听失败，允许本地监听后重跑通过。新增 tx 路径和 SLG db（含真实 Mongo）的 race 通过。txgen 仅同步原生驱动类型带来的间接依赖清单，没有增加数据库测试。未运行云测试。

按 ponytail-review 自检本轮连接所有权、CMod 准入、原生操作绕过缓存的边界及游标收尾，无阻塞项。取得原生句柄后无法拦截其后续操作，生命周期/健康校验只发生在取得句柄时；关闭后保留句柄的操作由官方驱动返回错误，不宣称逐操作经过 tx 闸门。

## 第 3 批：World 独占加载与邮箱保存闭环

- [x] 在网格执行/保存路径迁移完成时，为 GDWorldBucket 增加 `@noEntityCache` 并重新生成；不得仅改注解而继续通过旧 Procedure/WriteBack 管理独立加载的同一记录。
- [x] 在 `txgameslg/server/services/slgworld/modules/internal/world` 组织 WorldRuntime/WorldBucket 的加载、执行和保存；新增文件按 `runtime.go`、`bucket.go`、`persistence.go` 职责拆分，薄入口仍在 modules，Handler 只投递业务操作。
- [x] 复用或最小调整现有 GridModule 任务执行能力，使同一网格任务严格串行，多个网格可并行；不另造一套全局 Actor 调度器或强制每格一个进程。
- [x] Mongo 游标加载后直接接管 GD，不生成第二份长期字段副本；已加载网格对外可用前完成必要初始化。保留原世界 owner/Ready/异常收尾约束。
- [x] 把既有 MoveUnit 路径迁入网格邮箱，不能一部分通过 Procedure 改同一个 GD、另一部分直接修改；任务内容不得将内部 GD 指针交给外部调用方。
- [x] 周期调度投递 SaveTask，任务内同步保存并等待完成；控制在途保存数量及重复投递。复用 Store 跟踪变更、db.Save 和 Mongo Writer，不注册到 Procedure WriteBack。
- [x] 测试保存阻塞时同格修改等待、异格任务仍可执行、失败保留脏变更且可再次保存；覆盖嵌套单位修改及条件首次创建。
- [x] 测试停止生产、排空、最终保存、失败退出及启动失败清理；取消等待不得提前释放仍使用 GD 的任务。

完成标准：独占数据不进入 EntityCache，邮箱承担执行顺序；周期保存和排水可验证，不以自动锁或自动事务替代归属。

### 第 3 批验证与边界（2026-10-29）

- tx 仅补可选 ServiceFlusher：Actor 和后台任务排空后、CMod 排水前执行；成功阶段不重复，失败保留资源并沿用 Stop/PrepareHandoff 重试。新增测试先复现没有该阶段时失败保存仍被关闭的问题，再验证卸载顺序与重试。
- World 加载只接管一份 GD；144 个执行邮箱按 BucketID 取余映射，支持稀疏编号；目录锁只保护分区索引，不替代同格串行。每 10 分钟错峰扫描保存，每个 World 最多 4 个在途，同分区不重复入队。最终保存失败禁止替换同 WorldID 的旧激活。
- 删除旧 UseBucket/PMoveUnit 路径与 World 的 CModProcedure 依赖；生成排除、MoveUnit 与最终保存一起切换。短时压测/交接验收在正常排水后核对 Mongo，不再依赖旧高频 WriteBack。
- tx、txgen、txgame/server、txgameslg/server 全量与 txgameslg/gen 回归通过。World/网格/db/集群编排针对性 race、通用 Flush/交接 race 通过。
- 本机真实 Mongo 验证加载、条件创建、嵌套 Store 修改、保存后重新加载。隔离 Docker 依赖下 JetStream 可靠任务 race 验证 ACK 早于 Mongo 保存、Stop 后最终值正确；没有跳过该项。
- 关键并发测试覆盖同格保存期间不能修改、异格可推进、取消不释放仍执行对象、失败保留与重试、启动失败关闭队列、周期生产与保存并发上限。
- 按 ponytail-review 复核当前改动，修正了分区编号上限误设与旧压测落盘时机。未运行云压测、长时吞吐或完整多进程集群部署；10 分钟周期使用短周期测试验证调度行为。World 周期未保存数据不承诺强杀恢复。

## 第 4 批：共享 EntityCache 访问安全

### 4.1 失败证据与共享访问准入

- [x] 在 `tx/tests/entity_safety_test.go` 补“缓存模式无 Procedure 查询应拒绝”的失败测试；当前实现应暴露缺口。
- [x] 在 `tx/modules/entitycache/runtime_test.go` 覆盖有效/失效事务、同事务重复查询、取消但仍执行时不释放对象；不要在主测试进程中故意触发不可恢复 fatal。
- [x] 收敛 Runtime.Find 及可达的低层缓存入口：共享 GD 返回前必须成功纳管；纯缓存容器的泛型测试不得成为应用绕过准入的理由。
- [x] 复用 Enlist、根事务结束释放和现有锁顺序；不增加自动 Procedure 或第二套锁。
- [x] 运行上述测试及缓存/事务针对性 race，确认拒绝路径、锁与使用期先稳定。

完成标准：框架正常入口不再向非事务业务返回受管理可变 GD。

### 4.2 独立快照与生成 API

- [x] 在 `txgen/tests/entitycache_test.go` 先补生成/编译契约：Find 与 Snapshot 均显式 ctx，缓存根缺少锁能力时报清晰错误，关闭缓存时不新增此限制。
- [x] 生成 `SnapshotGD*(ctx, 完整主键参数...) (*GD, bool, error)`，放在现有 Store 文件；不另增 db 包包装。共享辅助逻辑放在现有 entity_access 模板。
- [x] 描述增加必要的深复制能力，复用现有 `snapshotCopy`；确认复制不带原 Store、tracker 或事务状态，不以 map/slice 浅拷贝代替深复制。
- [x] tx 快照实现先取得使用权，再使用相同记录锁复制。活动事务内通过现有纳管流程复用锁；已结束事务拒绝，不能回退普通快照。
- [x] 覆盖未写回最新值、事务回滚后读值、事务内读己写、同事务重复快照、删除/创建可见性，以及嵌套 GD、map、slice、字节数组和 int64 不失真。
- [x] 核查完整批量/别名、FindObject/FindObjects 与投影：完整身份走对应受控入口；需要脱离对象的查询明确快照语义；不能留下另一条默认裸访问路径。批量接口不自动宣称全批次原子快照。
- [x] 编译完整生成包并执行核心快照 race；txgen 测试只使用内部夹具/假存储，不接 Redis/Mongo。

完成标准：普通读取不需业务 Procedure，且返回数据与共享缓存彻底脱离。

### 4.3 写入、写回和释放边界

- [x] 在 `tx/tests/entity_access_test.go` 增加快照不能 Save/Remove 共享对象的测试，沿用已有 WriteBack 创建/失败测试；非缓存模式由生成夹具与 txgame 回归覆盖，不重复堆叠相同断言。
- [x] 创建/删除共享对象走有效 Procedure；保留临时对象赋初值、创建读己写、提交发布、回滚不可见及删除失败状态。
- [x] 审查框架条件初始化与持久化内部入口，只允许明确的受控路径；不为了 World 启动扩大普通业务绕过权限。
- [x] 快照、WriteBack、释放、创建/删除共同验证锁顺序：先持有使用权再进入记录操作，等待记录锁时不长期占用缓存全局锁。
- [x] 并发测试事务修改 map、快照导出、WriteBack 和回收，验证 race、无死锁、无旧写回复活；失败持久化保留既有重试/排水语义。

完成标准：不能只修 Find，而让 Save、删除或后台复制继续绕过同一保护边界。

2026-10-29 第 4 批验收：

- 失败证据：无 Procedure 的共享 Find 原先返回对象；启用 EntityCache、缺少 `@transaction` 原先仍生成成功。新增用例先失败，收紧运行时与生成器后通过。
- `tx go test ./...`、`txgen go test ./...` 通过，生成器正式夹具已重新生成。核心缓存/事务/创建/快照测试通过 `-race`；生成包通过 `GOFLAGS=-race` 编译与执行，测试不依赖真实存储。
- 并发 map 测试同时运行 Procedure 修改、深复制快照、WriteBack 与软容量/空闲回收；覆盖写回失败保留 pending，恢复后最终值与已提交次数一致。取消、复制错误与 panic 的使用权释放分别验证。
- 未启用缓存的 txgame server 全量测试通过；SLG World 独占路径通过针对性 race，SLG server 全包编译通过。外部依赖按测试既有开关跳过，不计为本批真实数据库/集群验收。SLG 共享实体业务尚未做第 5 批迁移，未宣称其完整业务验收通过。
- 执行范围：复用既有深复制模板与锁，不增自动 Procedure。共享普通 Save/Remove 协调器移除；错误删除由 WriteBack 重试，独占类型保留直接存取。`ponytail-review` 无阻断发现，收敛了重复删除校验及不可达的旧同步重试分支。

## 第 5 批：两组共享实体应用迁移

实施判定：GDUser/GDServer 是条件更新的存储权威，显式排除缓存；GDRole/GDLegion 保留既有事务锁。World 的独占排除不变。并发业务回归发现首次 StoreTarget 惰性初始化发生在发布之后，tx 冷加载补齐发布前初始化，避免在应用中添加第二套锁。


- [x] 依据有效缓存选择补齐仍受管类型的锁能力，重新生成两组产物；不改已有锁身份，不让已排除类型重新进入缓存。
- [x] `GetBag`、Legion 资料、排行榜装饰等普通读取使用 Snapshot；需跨对象一致性的读取继续使用 Procedure。
- [x] `ClaimBag`、改名、等级等共享实体修改改为在 Procedure.Process 内 Find，移除只为预先提供 StoreTarget 的裸对象查询。保留显式事务，而非自动包裹 Handler。
- [x] 核查 Procedure 回调、事件、响应结构和异步队列，结果在锁内复制；不把共享对象或子集合直接留给后续序列化协程。
- [x] 核对 World 持有的是排除缓存的独占 GD，而非共享缓存引用；缺失记录沿用条件初始化且成功加载后才 Ready，不能以异步写回登记代替持久化成功。
- [x] 检查 GDUser 建角、GDServer 目录及初始化扫描，保留其原子条件/存储权威语义，明确这些路径不等同于缓存 Snapshot。
- [x] txgame 缓存关闭，验证读写行为和既有事务不变；两组模块入口、db 与文档用词一致。
- [x] 实测 Role 查询/领奖、Legion 查询、World 初始化/操作/卸载及创建回滚；启用本地依赖验证写回及排水，清理仅限本轮测试资源。

完成标准：业务没有长期逃逸的缓存引用，普通查询可用，事务修改和生命周期仍成立。

本批验证（2026-10-29）：

- 两组 gen/run.sh 重新生成，gen/server 全包测试通过；两端客户端无生成差异。txgen 全包、tx 全包测试通过。
- tx EntityCache/txn/writeback 与 SLG slggame/db 针对性 race 通过。新增并发领奖与快照修改隔离；测试先发现冷加载 Store 初始化竞争，修复后通过。
- 本机 Redis/Mongo/etcd/NATS 实际验证 Agent→Role 领奖/查包/改名、Legion 查询、World 初始化/操作/卸载、SLG 周期与停服写回；两组 Redis 排行榜和 Mongo 并发建角预留通过。事务创建提交/回滚、首次落盘失败保留及重试由隔离存储夹具验证。
- 未启用本批无关的可靠任务与 Redis 整数专项环境开关；不将默认跳过记为通过。无云压测、无容量结论。
- 最终变更级 ponytail-review 未发现阻断问题；独立审查核对实际对象逃逸、深复制、存储排除与锁初始化边界，未独立重复跑全套测试。

## 第 6 批：最后补齐四网格 Monster 最小示例

本批只在 txgameslg 的 World 业务运行时落地。tx/txgen 不定义世界尺寸、怪物、tick 或随机移动策略，不将此例扩展为战斗框架。基础能力完成后再增加示例行为，不保留此前回血方案。

固定示例参数：

| 项目 | 约定 |
| --- | --- |
| 世界布局 | 2×2 个网格，每格 100×100；世界为 `[0,200) × [0,200)` |
| 网格中心 | `(50,50)`、`(150,50)`、`(50,150)`、`(150,150)` |
| 单位 | 每格唯一一个 Monster，复用 GDWorldUnit 的单位 ID、Kind、PosX/PosY；无血量、战斗或离线补算 |
| 主 tick | 50ms；按运行中的实际经过时间累计，不假设每次调度都精确到达 |
| 移动周期 | 每满 1 秒，向每格邮箱投递一次移动任务 |
| 位置 | 固定网格中心半径 10 的圆内随机 `(x,y)`；不围绕上次位置移动 |
| 坐标类型 | 沿用整数坐标；先按圆面积均匀采样连续点，再取整并确保结果仍在圆内 |
| 保存 | 示例默认每 10 分钟调度保存；测试可控制时间，不真实等待 10 分钟；排水始终最终保存 |

圆内采样可用 `r = 10 * sqrt(U)`、`theta = 2πV`，其中 U/V 在 `[0,1)` 均匀取值；取整后落到圆外则重采样。该规则不声称整数格点概率严格相等。随机源由业务运行时管理，在各网格执行范围内安全使用，测试可固定随机序列。

```text
激活 World
  → 读取最小 GDWorld 身份及当前世界 GDWorldBucket
  → 缺失的示例网格/Monster 按稳定身份条件初始化
  → 四格恢复完成 → World Ready → 主 tick 启动

50ms 主 tick
  → 累计时间满 1 秒
  → 各格邮箱：移动 Monster → Store 记录 PosX/PosY 变更

周期 SaveTask / 排水最终 SaveTask
  → 在同一网格邮箱等待 Mongo 保存
  → 重启先恢复上次位置，不重置为中心，不补发停服期移动
```

- [x] 先写时间推进和移动范围测试，再增加示例 tick；复用现有调度能力，仅提供必要的时间/随机测试入口，不引入通用模拟时钟框架。
- [x] 新世界初始化四个稳定网格身份，每格一只 Monster；已有数据恢复，重复启动不增殖单位、不覆盖已有位置。缺格恢复仅补缺失记录。
- [x] 首次位置为各格中心；运行满 1 秒再移动。测试 50ms 分段与不规则调度间隔累计，结果都遵守每秒一次的规则。
- [x] 确认随机坐标始终在本格固定中心的半径 10 圆内，四个中心正确，主 tick 仅投递而不读取/写入独占 GD。
- [x] 使用 Store 修改位置；保存与重载验证每格单位身份、数量和坐标不变。观察接口在邮箱内复制必要结果，不泄露 GD/map 指针。
- [x] 本地 Mongo 验证完整初始化、移动、周期保存、排水和重载；测试控制时间，有限步数验证，不要求万连或 G 级实际内存数据。

完成标准：示例能证明“Mongo → 唯一运行时数据 → 网格执行 → Mongo”的完整闭环，没有 EntityCache 双份驻留，也没有依赖 Procedure 包装 tick。

本批验证（2026-10-29）：

- 四格初始化、50ms/不规则 elapsed 推进、固定中心圆内采样及边缘取整重采样测试通过；恢复保留单位身份与既有坐标，初始化失败关闭邮箱。
- 本地 Mongo 实际验证条件创建不覆盖、四格初始化落库、移动、周期保存、最终保存及重载。正式 slgworld 启动后，Actor 定时器推进移动，Stop 后回读 Mongo 验证最终坐标。
- txgameslg/server 全包回归通过；World 包针对性 race（包含真实 Mongo）通过。txclientslg 127 项测试、构建通过。
- 客户端行军坐标同步为 2×2；每格 500 人的分配夹具总量随地图收敛为 2000，不作为集群容量结论。tx/txgen 无本批运行时代码变更，不增加 Monster 生成结构。
- 独立变更级 ponytail-review 未发现阻断问题；复核 Actor 定时器与排水顺序、独占字段访问、初始化失败收尾及旧地图调用点，未重复运行主任务测试。
- 第 7 批整体回归和微基准已完成，见下方验收记录；未做云压测、未提交。

## 第 7 批：本地回归、性能与文档

- [x] 先运行受影响测试，再运行 tx、txgen、两组 server/gen module 的完整回归；生成包整体编译。
- [x] 缓存/事务/写回及修改后的业务用例跑针对性 race，不把普通功能测试通过当作并发安全证据。
- [x] 用本地依赖验收 Role/World、创建/删除、写回失败和正常关闭；明确记录跳过的依赖用例。
- [x] 微基准分开记录受管理查询、快照复制、Procedure 修改的时间及分配；快照按实体大小记录，不将深复制成本隐藏在“缓存命中开销”里。不运行万连或云容量测试。
- [x] 更新 tx design/runtime-access、txgen 使用说明、两组 README/设计文档，删除“普通 Handler 可直接修改缓存，由业务承担并发风险”的过期口径。
- [x] 进行变更级 review，检查实际字段逃逸和后台读取，不只检查新增代码；逐工程报告实际测试与未验证范围，不提交。

执行命令参考（在各自 module 根目录，先跑具体新增用例再扩展）：

```sh
/Users/a47/sdk/go1.26.5/bin/go test ./...
# tx 根目录，重点并发回归：
/Users/a47/sdk/go1.26.5/bin/go test -race ./modules/entitycache ./modules/txn ./modules/writeback
```

真实依赖用例按各测试当前的环境变量/入口显式启用，不能将默认 skip 记为通过。基准先验证其路径确实经过锁和深复制，再记录结果。

第 7 批验收（2026-10-29）：六个 Go module 完整回归、两组生成一致性、两客户端测试与构建通过；核心及应用定向 race 通过。本机 Redis/Mongo 验证创建、删除、写回失败保留，正式 Actor/World 验证调用、tick 与关闭。未启用的外部专项与 Luban .NET 导出单独记录，不计为通过。

使用实际生成 GDRole 的单实体热基准，受管查询约 4.8～5.3µs、单字段 Procedure 修改约 8.0～9.3µs；4096 项背包 Snapshot 约 284µs/247KB 累计分配。单次平均值不代表 P99，无旧分支 A/B 或集群容量结论。独立变更复核无阻断项，两组 DB 过期错误提示已修正并回归。完整数据、计时边界和复现方式见 [验收报告](entity-access-acceptance.md)。

## 重点复核与停止条件

1. 持有事务锁再调用 Snapshot：必须复用，不能死锁；第 4.2 批固定测试。
2. 刚提交但未写回：Snapshot 看到缓存最新值；第 4.2、4.3 批固定测试。
3. 事务结束后的返回值：不能共享 map/slice；第 5 批验证响应与后台任务。
4. WriteBack 失败、取消和回收交错：不提前释放或丢弃脏对象；第 4.3、5 批验证。
5. 未声明事务的 GD 与存储权威查询：生成约束需显式调整，不能静默降级或更改业务一致性；第 1、4、5 批验证。
6. World 保存与移动交错：网格串行保存、失败保留变更，不能因另开 goroutine 失去保护；第 3、6 批验证。
7. 排除类型仍被识别为受管对象，或启用缓存后被原生查询旁路写入：第 1、2、5 批检查类型选择与所有权，禁止双重可写权威。

若必须改变两阶段建角、存储 CAS 或现有持久化完成契约才能继续，先提出具体冲突，不用隐式自动事务/双写缓存掩盖。Go 公开字段无法追踪非法裸指针逃逸，文档和最终结论必须保留这一实际限制。

## 后续独立批次

本计划建立管理接口所需的快照与使用期基础；后续确认的统计、元数据列表、仅命中安全快照和尝试回收已实现，见 [管理 API](entity-cache-management.md)。仍不增加管理页面、全量实体导出或调试 HTTP 服务。
