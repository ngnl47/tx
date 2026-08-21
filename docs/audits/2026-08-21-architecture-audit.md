# tx 架构审计报告（2026-08-21）

审计 revision：`7c157dad4060aaa27f8f2874b62da233c5439a55`

审计环境：`go1.26.5 darwin/amd64`，Intel i9-9880H，macOS 15.6

审计范围：`tx` 全部包、公开 API、Delta/Store/Fusion/事务、Agent/msgq/RPC/Actor、txgame 真实 Redis/Mongo 集成链路，以及 etcd 服务发现链路。

本报告按 [`../architecture_audit_standard.md`](../architecture_audit_standard.md) 执行。审计阶段只记录证据和建议，不直接修改实现或把未确认方案写入实施计划。

## Findings

本轮未发现受支持主链路上的 P0 脏写问题。以下条目保留审计发现和整改状态，避免后续只看到结论而失去问题背景。

### [P1][已整改] 投影 Delta 保存成功后会清理 Store 的全部当前变更

- 位置：`storage/changes.go:90`、`storage/changes.go:97`、`storage/changes.go:119`
- 触发条件：高级调用方先取得一份 Delta 投影，随后 Store 又产生投影中不存在的新路径，或者调用方仅传入当前 Delta 的子集，再调用 `SaveDeltasWithOptions`。
- 影响：适配器只落地传入的 Delta，但成功后 `ClearDeltas` 会清理 Store 的全部当前 dirty 状态；未包含在本批次里的变更可能不再被保存。
- 当前主线：`txn.Runtime` 在同一事务锁内投影并保存同一个 Store，当前支持路径不会触发该问题。风险来自公开 API 未在类型中表达“投影代际、锁所有权和清理范围”。
- 整改：已移除公开的 `SaveDeltas/SaveDeltasWithOptions`。`storage.Save` 总是从传入 Store 取得当前投影并在成功后清理；`txn.Runtime` 在持有目标锁期间通过 Runtime 私有的 Store 投影视图复用该批次。普通路径只投影一次；超预算路径会先把 dirty 树明确收敛为顶层 Replace，再取得一次新的降级投影。业务层继续只暴露 `db.Save(role)` 等 facade，不能自行传入 Delta 子集。

示例：

```go
store.SetName("first")
batch := store.Deltas() // 只有 Name 路径
store.SetPower(9000)    // batch 中没有 Power 路径

// 旧接口只写入 Name，却会把 Power 的 dirty 状态一并清掉；该入口现已移除。
err := storage.SaveDeltas(ctx, writer, store, batch)
```

同一路径在投影后再次赋值时，`DeltaValueResolver` 通常会读取 Store 的当前值，因此并不是该问题的最小触发条件；危险的是投影之后新增了尚未包含在批次里的其他路径。

### [P1][已整改] Fusion 健康闸门没有覆盖完整读写边界，写错误分类又过宽

- 位置：`txgame/server/internal/runtime/storage.go:165`、`txgame/server/internal/runtime/storage.go:181`、`txgame/server/internal/runtime/storage.go:263`
- 触发条件一：Mongo 已被后台探针判定不可用，但 Fusion 读取恰好命中 Redis。
- 影响一：`FusionReader` 直接返回原始 Fusion strategy，读取不会检查共享健康状态，和“Fusion 的 Redis/Mongo 任一不可用时，该模式不可读写”的既定契约不一致。
- 触发条件二：Fusion 写入返回 RecordKey、Delta、字段映射等调用方语义错误。
- 影响二（整改前）：writer 会把所有错误都当作 backend 故障，一次无效业务写入也可能关闭整个进程的新事务准入，直到健康探针下一次成功。
- 整改：项目运行时分别维护 Redis/Mongo 健康状态，并用同一个 gate 包装单条/批量 reader 和 mode-aware writer。Fusion 入口要求两端健康；Redis、Mongo 单模式只检查自身依赖并写入各自 backend，原始 Fusion strategy 已从项目运行时公开面收回。错误分类下沉到具体 go-redis/Mongo driver client，通用 adapter 不会把解码或协议错误重新标成 backend I/O；RecordKey、Delta、映射、协议返回值及调用方 context 错误不会关闭健康状态。Probe 使用 backend generation，旧成功结果不能覆盖并发发生的新故障。延迟回写 handler 只观察 backend 错误并继续把失败返回 scheduler，因此任务会保留重试，不会被健康 gate 阻止恢复消费。

### [P1][已整改] Mongo 装配使用 `any`，类型错误被推迟到第一次业务 I/O

- 位置：`storage/mongo/adapter.go:73`、`storage/mongo/adapter.go:78`、`storage/mongo/adapter.go:103`、`storage/mongo/cluster.go:31`
- 触发条件：向 `NewAdapter` 或 `NewCluster` 传入缺少某项能力、nil 或 typed-nil 的 client。
- 影响：构造和服务启动可以成功，直到第一次 Update/Replace/Delete/Find 才通过运行期断言报错；这不符合项目启动装配 fast-fail 和编译期契约优先原则。
- 对比：Redis adapter 已使用 `ScriptClient` 强类型构造；正式 Mongo driver 的 `Database` 已完整实现 Updater、Replacer、Deleter、Finder、BatchFinder。
- 整改：新增组合接口 `mongo.Client`，统一要求 `Updater/Replacer/Deleter/Finder/BatchFinder`；`NewAdapter` 与 `NewCluster` 返回装配错误，Cluster shard 改为强类型 map。官方 driver 增加编译期接口断言，nil、typed-nil、空 router、空 shard 名和空 shard 集合均在启动装配阶段失败；运行时能力断言和逐条 batch fallback 已移除。

### [P1][已整改] `MaxRootDeltaEntries` 对非生成 Store 会静默失效

- 位置：`modules/txn/runtime.go:36`、`modules/txn/runtime.go:427`、`modules/txn/runtime.go:431`
- 触发条件：持久化目标实现 `DeltaStore`，但未额外实现未导出的 `CollapseDeltas` 能力，同时 Delta 数量超过配置预算。
- 影响：文档和配置声明 Runtime 会收敛超额路径，实际代码直接返回原 Delta，既不报错也不告警；高级扩展 Store 可能在持续失败刷盘时突破预期预算。
- 当前主线：txgen 生成 Store 已实现收敛，常规业务路径安全。
- 整改：新增 `storage.CollapsibleDeltaStore` 显式契约。配置非零 `MaxRootDeltaEntries` 时，`Runtime.Call` 在进入 Procedure 前、`Runtime.MarkDirty` 在登记前校验持久化目标；不支持收敛时返回 `txn.ErrDeltaCollapseUnsupported`。Flush 内保留防御校验，不再存在静默绕过容量预算的分支。

### [P1][已整改] race 质量门禁被并发测试日志收集器打红

- 位置：`tests/log_test.go:87`、`tests/log_test.go:101`、`tests/fusion_scheduler_test.go:442`
- 触发条件：Scheduler 后台 goroutine 写入 `recordingSlogHandler.records`，测试 goroutine 同时轮询 slice 长度并读取记录。
- 影响（整改前）：`go test -race ./... -count=1` 失败，无法把 race 作为持续回归门禁；生产实现未在本次审计中观察到对应竞态。
- 整改：测试 handler 使用 mutex 保护记录写入，并只向测试协程暴露快照；Scheduler 回归不再直接并发读取内部 slice。完整 race 结果见本报告验证记录。

### [P2][已整改] 单机事务锁表的全局串行与临时分配远高于基础互斥量

- 位置：`modules/txn/txn.go:522`、`modules/txn/txn.go:545`、`modules/txn/txn.go:575`、`modules/txn/txn.go:624`
- 触发条件：多个无冲突 Procedure 高频获取不同 LockKey。
- 影响：所有 key 都先竞争同一个全局 mutex；每个新 key 还会分配 `lockEntry`、buffered channel 和 held-lock slice。不同业务实体的锁管理在元数据层发生串行。
- 正式基线：`BenchmarkLockTableAcquireRelease` 的单 key 串行为 `341.8-347.9 ns/op, 176 B/op, 3 allocs/op`，四 key 串行为 `1.275-1.364 us/op, 672 B/op, 9 allocs/op`，16 线程离散 key 为 `520.5-556.3 ns/op, 176 B/op, 3 allocs/op`。共享同 key 的并行路径为 `402.2-419.2 ns/op, 48 B/op, 1 allocs/op`，说明并发持有期间会复用 entry，但短生命周期离散 key 仍承担创建成本和全局锁竞争。
- 整改：精确 Key 锁注册表的元数据按稳定 `LockKey` 哈希分为 64 个 shard；哈希碰撞只竞争短暂的注册表 mutex，不会让不同 Key 共用业务锁。带 context 等待能力的 channel entry 在引用归零并从注册表删除后进入 `sync.Pool`；四把以内的常见事务锁集以内联数组持有，更多锁才使用 overflow slice。单锁预处理跳过没有意义的复制、排序和去重 map，但继续执行完整合法性校验。锁顺序、同 key 互斥、context 超时及失败反向释放语义不变。
- 整改结果：锁内核单 key 串行为 `247.6-255.9 ns/op, 0 allocs`，四 key 串行为 `965.6-995.2 ns/op, 0 allocs`，16 线程离散 key 为 `43.99-62.26 ns/op, 0 allocs`；共享同 key 仍为 `403.2-423.4 ns/op, 0 allocs`，没有通过放宽同对象串行语义换取性能。完整 `WithLocks` 单 key 入口为 `282.6-311.7 ns/op, 0 allocs`；四 key 的排序和重复校验仍为 `1.585-1.696 us/op, 360 B/op, 4 allocs/op`，后续只有真实多对象事务基准证明值得时才继续复杂化预处理结构。

### [P2] Delta 的实际物理尺寸较大，边界 Clone 会按条目放大瞬时内存

- 位置：`change/delta.go:22`、`change/delta.go:30`、`change/delta.go:36`、`change/delta.go:194`、`storage/mongo/document.go:61`
- 触发条件：一个根对象积累大量不同 map key/index 路径，并进入 Mongo/Redis 文档构建。
- 影响：定长路径具备良好局部性，但不是“小结构体”。在当前 64 位布局下 `PathKey=40B`、`PathNode=48B`、`DeltaPath=200B`、`DeltaEntry=208B`；256 条 Delta 的 backing array 约 52 KiB，边界 Clone 会再复制一份。
- 证据：`Compact/RepeatedField64` 约 `2.28-2.42 us/op, 0 allocs`；`Compact/MapItemPaths256` 约 `45.2-47.6 us/op, 11560 B/op, 4 allocs`；祖先 Replace 256 条约 `15.2-15.7 us/op, 0 allocs`。正式 `BenchmarkDeltaSetClone` 中，64 条约 `3.19-3.71 us/op, 13568 B/op`，256 条约 `10.47-11.29 us/op, 57344 B/op`，1024 条约 `43.26-49.05 us/op, 212992 B/op`，均为一次大块分配。
- 当前边界：生成 Store 的 `Deltas()` 已返回新建且压实的独立投影，但 `DocumentWithResolver`、Redis/Mongo 文档构建器是公开转换 API，不能默认取得调用方切片的修改所有权。Fusion 只把投影即时交给 Redis，延迟队列不保存 Delta；当前不存在为了 MongoQueue 再复制一份 Delta 的问题。
- 建议：保留公开转换 API 的非修改语义，不为省一次 Clone 暴露“owned delta”定制接口。下一步单独决策是否把互斥的 int/uint key 合并到一个 64 位 numeric slot；预计可把 `DeltaEntry` 从 208B 降到 176B（约 15.4%），但会联动 `PathKey` 公共构造方式、txgen 模板和全部生成文件。第一版继续保留 `any` 值解析，不引入自研值编码或生产 `unsafe`。

### [P3][已整改] 事务创建保留了一次立即被覆盖的 context 分配

- 位置：`modules/txn/txn.go:348`、`modules/txn/txn.go:355`
- 触发条件：每次执行 Procedure。
- 影响：先写入带 typed-nil Tx 的 context，随后立刻覆盖为真实 Tx；前一次 `context.WithValue` 没有语义作用，并在逃逸分析中进入堆分配。
- 整改：先构造 `Tx`，随后只创建一次携带真实 `Tx` 的 context；typed-nil context 节点已移除。

### [P3][已整改] 测试目录规范与当前白盒测试现实冲突

- 位置：`docs/IMPLEMENTATION_PLAN.md:17`、`docs/IMPLEMENTATION_PLAN.md:473`
- 触发条件：按实施文档执行“所有测试只能放 `tests/`”规则。
- 影响：当前已有 `cmd/txctl/main_test.go`、`modules/msgq/mailbox_test.go`、Redis/Mongo script 与 driver 包内测试。强制迁移会让未导出状态机和脚本契约难以测试，或反向促使为了测试扩大生产 API。
- 整改：实施计划已改为跨包行为和公共 API 测试放 `tests/`；未导出算法、生成脚本或 driver glue 的白盒测试允许与实现同包，明确禁止为了测试扩大生产 API。

## 已验证通过

- `change` 只依赖标准库，Compact/路径比较没有反向依赖存储、RPC 或事务锁；纯计算内核边界成立。
- Delta Compact 只做路径可证明的确定性覆盖；Mongo unsafe string key、超深路径和结构漂移均在可读取当前值的边界提升为祖先整字段写。
- Fusion 并发 miss 使用记录锁、二次 Redis 检查和 Mongo 全文档回填，真实 Redis/Mongo 测试证明不会用旧回源结果覆盖更新热数据。
- Fusion 删除与延迟回写按记录锁串行；Mongo 不可用时任务保留并可重试，没有把 Redis TTL 误解为 Mongo 删除。
- msgq 的下行队列按条数和字节双重限制，Ack 后清理引用并按阈值压实；广播共享已序列化只读 payload，所有权转移契约已写入 API 文档。
- Agent、msgq、通用 gRPC 与 Actor 的职责依赖方向保持单向；Actor payload 使用 protobuf 信封，业务 BON body 不在基础设施中反序列化。
- gRPC Receiver 排水只封闭新入口，已接受工作由 lifecycle Admission 计数；下行 msgq 不被错误地绑定到业务排水入口。
- etcd 注册、配置、Slot CAS 与关闭后 slot 保留通过真实 etcd 集成测试。
- Redis `@v`、Lua 路径创建/异常父类型拒绝、map element Replace、Mongo/Fusion Delta 写入和 txn Runtime 延迟刷盘通过真实 Redis/Mongo 集成测试。
- `go vet ./...` 通过；全量普通测试在允许本地端口监听的环境下通过。

## 已知语义与残余风险

### Agent 上行是 at-most-once，不是业务可靠队列

`modules/agent/session.go:154` 在调用业务 Dispatcher 前通过 msgq 推进上行 Seq。业务返回错误或进程随后崩溃时，客户端重发相同 Seq 不会再次分发。这是当前明确记录的协议语义，不作为本轮实现缺陷；充值、发奖等关键动作必须先进入项目选择的可靠消息队列，并使用独立业务幂等键。

### Go 无法提供语言级只读 `[]byte`

msgq 本地零拷贝广播依赖所有权转移契约。发布成功后继续修改或 append 原 payload 会造成竞态或数据污染；高级用户必须遵守该契约。无法证明所有权时，应在项目边界主动复制。

### 真实集群与故障注入尚未覆盖

本轮使用单节点 Redis、单节点 Mongo 和单节点 etcd。尚未覆盖原生 Redis Cluster 跨 slot、本地多 Mongo 分片、gRPC 网络抖动、etcd lease 长暂停、进程强杀与恢复期流量；这些属于发布前集群演练范围。

## 成熟依赖评估

| 能力 | 当前选择 | 审计结论 |
| --- | --- | --- |
| Redis/Mongo/etcd/gRPC/protobuf | 官方或主流 Go 客户端 | 保持；`tx` 只维护最小 adapter 和领域策略。 |
| Actor Cluster | `protoactor-go` | 保持；不在 `tx` 重写邮箱、远程投递和集群成员机制。 |
| BON/MessagePack | 成熟 MessagePack 实现 | 保持；当前性能成本应通过真实协议 benchmark 优化，不自研字典编码。 |
| Delta/Store/Fusion | tx + txgen 领域实现 | 保持自研；它们承载字段映射、整字段降级、热冷一致性和生成代码契约，通用库无法直接替换。 |
| Procedure 多锁与 undo | tx 领域实现 | 暂不直接引入 keyed-mutex 库；现有契约包含锁顺序、动态锁集、context 超时、undo 和状态刷盘。先实施分段/复用基准，再判断第三方是否真正降低总复杂度。 |
| EventBus/DelayQueue | 可选业务工具 | 不纳入存储/RPC 核心替换；后续单独按其并发与严格有序语义评估。 |

## 验证记录

```text
go test ./... -count=1                                  PASS
go vet ./...                                             PASS
go test -race ./... -count=1                            PASS
TX_ETCD_ENDPOINT=127.0.0.1:2379 go test ./tests -run '^TestEtcd' -v
                                                          PASS
TXGAME_REDIS_URI=redis://127.0.0.1:6379?db=15 \
TXGAME_MONGO_URI=mongodb://127.0.0.1:27017/txgame \
go test ./... -count=1                                   PASS
```

## 建议实施顺序

1. 已完成：修复 race 测试 helper，使并发质量门禁恢复可信。
2. 已完成：收敛 Delta 投影批次契约，移除公开 `SaveDeltas`。
3. 已完成：统一 Fusion 读写健康闸门，并区分后端不可用错误与调用方语义错误。
4. 已完成：强类型化 Mongo client/cluster 构造并启动期 fast-fail。
5. 已完成：将 Delta 容量收敛能力纳入持久化事务 Store 契约。
6. 已完成：为事务锁表建立正式基准，并实施精确 Key 锁注册表分片、entry 复用、常见小锁集内联和单锁预处理快路径。
7. 已完成评估：公开转换边界保留 Clone；`PathKey` 的 int/uint numeric slot 合并进入待决策项。
8. 已完成：清理重复 context 分配，并修正测试目录文档规则。
