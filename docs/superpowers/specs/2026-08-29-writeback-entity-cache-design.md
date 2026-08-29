# WriteBackManager 与 EntityCache 体系化设计

## 背景

当前有状态链路存在两个名字相近、职责不同的缓存层：

- `tx/modules/cache.Manager` 接收成功 Procedure 的持久化根对象，维护事务预留、待刷变更、批量 Flush 和关闭排水；它不负责对象加载、命中率或容量淘汰。
- `txgameslg/services/internal/resident.Cache` 合并并发冷加载并保存进程内唯一业务对象；它才是真正的实体工作集缓存，但目前只有角色对象的空闲超时扫描，世界分区没有容量边界。

继续沿用 `CacheManager + residentCache` 会让配置、SPI 和运行日志无法直接表达实际职责。接入成熟的容量淘汰策略前，先完成一次无兼容层的术语迁移，再为有状态示例增加基于 Otter v2 的有界实体缓存。

本设计是后续世界写、WAL 和 checkpoint 性能基线的前置工作。性能基线不能建立在世界分区对象可能无界驻留的实现上。

## 目标

1. 用 `WriteBackManager` 准确描述事务提交后的脏根管理与延迟写回。
2. 用 `EntityCache` 准确描述业务对象加载、唯一实例、热度和容量淘汰。
3. 让配置层级显式表达 CMod 归属，不再把 Procedure 子组件平铺在 `runtime`。
4. 采用 Otter v2 adaptive W-TinyLFU 作为 EntityCache 的准入和淘汰候选策略。
5. 保持 resident map 为业务对象身份的唯一权威，不允许第三方缓存先删除脏对象。
6. 将“暂时不能释放”建模为正常结果，不再使用 error 表示 pending 或事务占用。
7. 保持热点命中路径无额外业务对象复制，并以 benchmark 证明性能没有明确回退。

## 非目标

- 不为旧 Go package、类型、方法或 YAML 路径提供兼容别名。
- 不把 EntityCache 移入 `tx` Core；对象如何加载仍由具体有状态项目决定。
- 不让无状态 `txgame` 装配 Procedure、WriteBackManager 或 EntityCache。
- 不按对象动态估算 Go 堆内存，也不在第一版提供 `maximumWeight`。
- 不实现跨进程实体缓存协调、远程失效通知或分布式锁。
- 不改变 WAL、Fusion、checkpoint 和 Procedure 的提交成功边界。
- 不在本阶段实施世界写性能负载；完成缓存边界后再继续该基线。

## 固定术语

| 术语 | 代码位置 | 职责 |
| --- | --- | --- |
| EntityCache | `txgameslg/server/services/internal/entitycache` | 加载并复用唯一业务对象，合并并发冷加载，使用 W-TinyLFU 管理工作集 |
| WriteBackManager | `tx/modules/writeback.Manager` | 接收事务提交结果，维护根对象身份、pending、预留、变更折叠、Flush 和关闭排水 |
| WriteBack Runtime | `tx/modules/writeback.Runtime` | CModProcedure WriteBack 子组件依赖的稳定 SPI |
| TryRelease | `writeback.Runtime.TryRelease` | 在实体锁内检查根对象能否从写回身份中解除 |
| policy token | EntityCache 写入 Otter 的轻量条目 | 只用于热度、准入和淘汰候选选择，不拥有业务对象生命周期 |
| deferred release | EntityCache 内部延期集合 | 保存暂时不能释放的候选，按退避重新执行 TryRelease |

完成迁移后，生效代码、README、design、配置错误和启动日志统一使用以上术语。已经完成的历史审计、spec 和 plan 不回写；新文档明确取代其中的旧术语。

## 分阶段实施

### 阶段一：纯命名与配置迁移

阶段一不引入 Otter，不改变现有空闲扫描行为。它只完成符号、package、配置层级和释放结果语义迁移，使阶段二在最终命名上开发。

核心迁移如下：

```text
tx/modules/cache                  -> tx/modules/writeback
cache.Manager                     -> writeback.Manager
cache.Runtime                     -> writeback.Runtime
cache.Manager.Release             -> writeback.Manager.TryRelease
ant.CModProcedureCache            -> ant.CModProcedureWriteBack
CModProcedureBuilder.Cache        -> CModProcedureBuilder.WriteBack

txgameslg/internal/cachemgr       -> txgameslg/internal/writeback
services/internal/resident        -> services/internal/entitycache
CacheRuntimeFactory               -> WriteBackRuntimeFactory
CacheRuntime                      -> WriteBackRuntime
```

不保留旧目录或导出别名。`txgen` 的生成契约回归改用新 package；无状态模板和两个客户端只更新仍然引用旧术语的生效说明。

### 阶段二：Otter v2 EntityCache

阶段二在 `txgameslg/server` 固定引入：

```text
github.com/maypok86/otter/v2 v2.3.0
```

Otter 提供 adaptive W-TinyLFU、按条目数的容量边界和可选访问后过期。它只保存 `key -> *cacheEntry` 的 policy token；`EntityCache.values` 继续持有唯一业务对象。

## WriteBack Runtime 契约

### SPI

```go
type Runtime interface {
	txn.CommitParticipant
	Start(lifecycle.Tasks) error
	Shutdown(context.Context) error
	TryRelease(txn.LockTarget) (bool, error)
}
```

`Manager.TryRelease` 取得与 Procedure 相同的实体锁，并返回：

- `true, nil`：目标不存在于 WriteBackManager，或目标已经干净且身份绑定已解除。
- `false, nil`：目标仍有事务预留、pending 或 `Store.HasChanges()`。
- `false, err`：目标非法、同一 LockKey 绑定了不同 Store，或发生其他真实异常。

旧的 `ErrPendingRoot` 不再是公开控制流。容量拒绝、变更折叠不支持等真正的 Procedure 准入错误继续保留明确 error。

项目 facade 收敛为单对象调用：

```go
func TryRelease(value txn.Lockable) (bool, error)
```

EntityCache 每个条目只对应一个根 `LockTarget`，不引入含义不清的批量 bool 聚合。

## 配置体系

### 总体层级

```yaml
runtime:
  lifecycle:
    drainTimeout: 5m
  log:
    level: debug
    format: console
  health:
    interval: 1s
    timeout: 500ms
  cmods:
    actor:
      clusterName: txgameslg-debug
    discovery:
      endpoints: [127.0.0.1:2379]
    fusionStorage:
      dirtyTTL: 720h
      delayTask:
        routeType: fusion
        prefix: txgameslg:task
        bucketCount: 64
        leaseTimeout: 30s
      scheduler:
        batchSize: 100
        pollInterval: 1s
        retryInitialDelay: 1s
        retryMultiplier: 2
        retryMaxDelay: 1m
        retryJitter: 0.2
    procedure:
      transaction:
        debug:
          checkCleanLockTargets: false
      writeBack:
        flushInterval: 10s
        shutdownFlushTimeout: 30s
        maxPendingRoots: 0
        maxRootChanges: 1024
        heapSoftLimitBytes: 536870912
        heapSampleInterval: 1s
      wal:
        backend: mongo
        mongo:
          routeType: wal
          collection: tx_wal

services:
  slggame:
    entityCache:
      maximumSize: 10240
      idleTimeout: 0s
  slgworld:
    entityCache:
      maximumSize: 1024
      idleTimeout: 0s
    checkpoint:
      workers: 4
```

### 层级规则

- `runtime.cmods.*` 只保存 CMod 及其内部组件配置。
- `lifecycle`、`log` 和跨存储共享的 `health` 继续属于进程 runtime。
- Redis、Mongo 路由表继续位于顶层 `storage`，不复制到每个 CMod。
- WAL 是 Procedure CMod 的可选持久化组件，配置从 `storage.wal` 移到 `runtime.cmods.procedure.wal`。
- EntityCache 是具体服务的工作集，不属于 CMod，保留在 `services.<service>.entityCache`。
- world checkpoint 是 `slgworld` 项目管线，保留在服务配置。

所有旧配置路径直接删除。YAML 严格解码继续负责未知字段；项目不增加旧键探测或双路径优先级。

### EntityCache 配置

```go
type EntityCacheConfig struct {
	MaximumSize int
	IdleTimeout time.Duration
}
```

- `MaximumSize` 必须大于零，并显式传给 Otter。
- `IdleTimeout == 0` 表示关闭时间淘汰。
- `IdleTimeout > 0` 时使用 Otter 的 access expiry；不再公开 `sweepInterval`。
- 第一版只按条目数计费，不提供动态对象权重。
- role 和 world 使用独立配置，不共享一个进程级容量。

## EntityCache 数据结构

```go
type Options[V any] struct {
	MaximumSize int
	IdleTimeout time.Duration
	TryRelease  func(V) (bool, error)
}

type Cache[K comparable, V any] struct {
	keyString KeyString[K]
	loader    Loader[K, V]
	options   Options[V]
	loads     singleflight.Group

	mu     sync.RWMutex
	values map[K]*cacheEntry[V]
	policy *otter.Cache[K, *cacheEntry[V]]

	deferredMu sync.Mutex
	deferred   map[K]deferredEntry[K, V]
	wake       chan struct{}
}
```

`cacheEntry` 保存业务对象和单调递增 generation。Otter 中的 value 只是同一 `cacheEntry` 指针，不复制 GD 或 payload。`mu` 只保护业务对象身份，`deferredMu` 只保护候选调度；原子删除回调不取得 `mu`。

不使用 Otter Loader。现有项目 Loader 和 `singleflight.Group` 已经定义了缺失对象不缓存、错误原样返回和同键冷加载合并语义；替换为库 Loader 会扩大行为变化且没有收益。

## 读取与冷加载

### 命中

```text
EntityCache.Load(key)
  -> values 命中
  -> generation++
  -> policy.GetIfPresent(key) 记录热度
  -> policy token 已被淘汰时重新 Set 当前 entry
  -> 返回同一业务对象指针
```

generation 的推进与 values 命中在同一缓存同步边界内完成。policy 允许最终一致；业务对象身份不允许最终一致。

### 冷加载

```text
values miss
  -> singleflight(key)
  -> 二次检查 values
  -> Loader
  -> not found: 不缓存
  -> found: 创建 cacheEntry
  -> 写入 values
  -> policy.Set(key, entry)
```

若 Otter 的准入策略拒绝新 token，该删除事件仍会形成淘汰候选；业务对象是否真正删除由 TryRelease 决定。

## 淘汰与安全释放

Otter 的 `OnAtomicDeletion` 只执行常数时间的候选登记，不在库内部锁路径调用存储、实体锁或 WriteBackManager：

```text
Otter 删除 policy token
  -> 过滤非 eviction 原因
  -> 读取 entry 当前 generation
  -> 将 key/entry/generation 合并进 deferred map
  -> 唤醒单个 release worker
```

release worker 在 EntityCache 锁外调用：

```text
released, err := TryRelease(entry.value)
```

结果处理：

1. `released=true`：重新取得 EntityCache 锁；仅当 key 仍指向同一 entry 且 generation 未变化时删除 `values[key]`。
2. `released=false`：保留对象并按内部退避再次检查。
3. `err != nil`：记录结构化错误并按同一退避保留对象。
4. worker 执行期间对象被访问：generation 改变，旧候选失效；对象重新写入 policy。

TryRelease 成功但 generation 已变化时，WriteBackManager 已解除一个干净对象的身份绑定，EntityCache 仍保留唯一对象；后续 Procedure 会用同一 Store 重新登记，不会产生第二个对象实例。

## Deferred release

deferred release 使用一个按 key 去重的 map 和一个生命周期 worker，不为每个对象创建 goroutine 或 timer。

固定内部退避：

```text
initial: 100ms
multiplier: 2
max: 5s
retries: until released, touched, replaced, or shutdown
```

这些值不是业务策略，不进入 YAML。只有 profile 或真实运行证据证明需要调整时才增加配置。

访问 deferred 对象时：

- 推进 generation；
- 删除旧 deferred 代次；
- 把当前 entry 重新加入 Otter；
- 已经开始执行的旧重试依靠 generation 复核失效。

在容量压力下，无法释放的脏对象会使 `values` 暂时超过 `maximumSize`。这是正确的软上限语义；WriteBackManager 的 `MaxPendingRoots` 继续负责限制新脏根，而不是强制丢数据。

## 生命周期

- EntityCache 在服务构造阶段创建，在 CMod/服务任务容器中启动唯一 deferred worker。
- 取消生命周期后停止 worker，并调用 Otter `StopAllGoroutines`。
- 关闭时不批量 TryRelease 全部 EntityCache 对象；入口已经封口，WriteBackManager 的最终 Flush 才是持久化完成条件，进程退出会统一释放 resident 内存。
- Otter 关闭产生的非 eviction 删除事件不进入 deferred。
- TryRelease 与最终 Flush 使用同一实体锁，不增加第二套状态轮询。

## 服务接入

### slggame

- Role EntityCache 使用 `maximumSize=10240`。
- Loader 继续调用生成的 `FindGDRoleByID`。
- TryRelease 通过项目 `internal/writeback` facade。
- Ping 仍不加载角色对象，也不人为刷新 EntityCache 热度。

### slgworld

- World bucket EntityCache 使用 `maximumSize=1024`。
- Loader 继续走 world WriteBack Runtime 的恢复和 generation fence。
- `worldID + bucketID` 继续作为强类型 key。
- MoveUnit 的 Procedure、WAL 和 checkpoint 成功边界不变。

## 克制的防御性编程

本轮及后续 review 遵守：

1. 只在所有权边界校验影响正确性、持久化安全、资源边界或稳定 API 契约的条件。
2. 不枚举假想非法字符，不复制 YAML、Otter、etcd、Redis 或 Mongo 已有的输入校验。
3. 启动配置或程序员错误允许尽早失败，不用恢复逻辑掩盖。
4. 不为内部已保证的不变量叠加重复 nil 检查和兜底分支。
5. 外部网络帧、持久化身份和可能导致数据丢失的条件仍必须严格校验。
6. 本次只修改命名与 EntityCache 直接触达的代码，不顺手清理无关历史实现。

该原则写入六个工程的 `AGENTS.md`，作为长期协作约束。

## 测试与性能验收

### 阶段一

- 新 `writeback` package 的 Manager、Runtime 和 Procedure 集成测试全部通过。
- `TryRelease` 覆盖已干净、pending、reserved、Store 仍有变化、身份冲突和非法目标。
- 配置测试证明只接受新 `runtime.cmods.*` 与 `entityCache` 路径。
- `tx`、`txgen`、`txgame`、`txgameslg` 完整 Go 测试和相关 race 测试通过。
- 全工作区生效代码、README 和 design 不再引用被替换的公开符号或配置路径；历史 spec/plan 除外。

### 阶段二

- 同键并发冷加载只执行一次 Loader，所有调用得到同一指针。
- not found 和 Loader error 不进入 values 或 policy。
- 热对象在容量压力下优先保留，冷对象成为候选。
- 干净候选成功释放并删除 resident 引用。
- pending、reserved 或仍有变更的候选进入 deferred，Flush 后最终释放。
- TryRelease error 保留对象并重试。
- TryRelease 期间发生访问时旧 generation 不能删除对象。
- `idleTimeout=0` 不触发时间淘汰；正 duration 仍通过安全释放闸门。
- role 和 world 都使用 EntityCache，world bucket 不再无界常驻。
- `go test -race` 不报告 data race，生命周期结束不遗留 EntityCache worker。

### Benchmark

实施前先记录现有 `BenchmarkCacheHit` 和并发命中基线；实施后使用相同 Go 版本、CPU、`-count` 和 `benchstat` 对比：

- 命中路径保持零业务对象复制。
- allocation 不增加时优先以吞吐和 `ns/op` 判断；allocation 增加必须解释并消除。
- 出现稳定、可复现的明显回退时停止，不以功能测试通过替代性能验收。
- Otter 统计默认关闭，避免为未消费指标增加热点原子操作。

完成单元、race 和 benchmark 后，再运行有状态真实集群短验收，确认 RoleActor、WorldActor、WriteBackManager、WAL、checkpoint 和优雅关闭仍形成闭环。

## 文档与后续

阶段一完成后更新 `tx` README/design、`txgameslg` README 和当前实施路线，只描述新术语和新配置。阶段二补充 EntityCache 工作集、软容量、deferred release 和 Otter 依赖说明。

两个阶段分别停下供人工审查和提交，不执行 Git commit。完成后恢复下一项：以 `World_MoveUnit` 为核心的世界写、WAL 和 checkpoint 性能基线。
