# Otter v2 EntityCache 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan.

**Goal:** 在 txgameslg 的 role/world 业务对象工作集上接入 Otter v2 adaptive W-TinyLFU，使两个服务都具备按条目数的软容量边界和可选访问过期，同时通过 WriteBack `TryRelease` 保证脏对象绝不被强制丢弃。

**Architecture:** `EntityCache.values` 继续拥有唯一业务对象，Otter 只保存 `key -> *cacheEntry` policy token。Otter 的原子删除回调只登记候选；EntityCache 唯一 release worker 在缓存锁外调用 WriteBack，依据 bool/error、对象身份和 generation 决定删除、取消或按固定退避重试。role 与 world 使用独立实例和容量。

**Tech Stack:** Go 1.26 module baseline、`github.com/maypok86/otter/v2 v2.3.0`、singleflight、tx lifecycle、WriteBack Runtime、Go test/race/benchmark、真实 Redis/Mongo/etcd 集群验收。

**Spec:** `/Users/a47/workspace/tx/docs/superpowers/specs/2026-08-29-writeback-entity-cache-design.md`

## Global Constraints

- 必须在命名与配置迁移计划完成、验证并由维护者提交后开始；开始前六个工程必须没有未知改动。
- 不执行 Git commit；完成后停下供人工 review，并按实际 diff 给出分项目中文 Angular Commit Message。
- Otter 固定使用 `v2.3.0`，不使用 `@latest`，不引入第二个淘汰库。
- Otter 不是业务对象身份或持久化状态的权威；任何删除都必须经过 `TryRelease`。
- 不使用 Otter Loader、refresh、weight、stats 或 persistence 扩展；第一版只使用 `MaximumSize`、可选 `ExpiryAccessing`、`OnAtomicDeletion` 和 `StopAllGoroutines`。
- 不复制 GD、Store 或消息 payload；policy value 只能是当前 `cacheEntry` 指针。
- capacity 是软上限；脏对象暂时超限属于正确行为，不能用强制删除换取表面容量稳定。
- deferred retry 固定为 100ms 起步、2 倍增长、5s 封顶，不增加 YAML 参数。
- 回调中禁止调用 Loader、WriteBack、存储、实体锁或日志 I/O；只允许常数时间候选登记和 worker 唤醒。
- 防御性校验只覆盖构造契约、资源边界、对象身份和数据安全；不枚举假想非法字符，不重复 Otter 和 YAML 已有校验。

---

## Task 1：记录阶段一基线并固定依赖

**Files:**

- Modify: `/Users/a47/workspace/txgameslg/server/go.mod`
- Modify: `/Users/a47/workspace/txgameslg/server/go.sum`
- Measure only: `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/cache_benchmark_test.go`

### Step 1：确认阶段一基线通过

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/internal/entitycache ./services/slggame/... ./services/slgworld -count=1
go test -race ./services/internal/entitycache -count=1
```

### Step 2：记录迁移前热点命中分布

保持同一 Go toolchain、CPU 和电源状态，至少采样 10 次：

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/internal/entitycache -run '^$' -bench 'BenchmarkCache(Hit|ParallelHit)$' -benchmem -count=10 > /private/tmp/txgameslg-entitycache-before.txt
```

该文件只用于本轮 A/B，不加入仓库。

### Step 3：加入精确依赖

```bash
cd /Users/a47/workspace/txgameslg/server
go get github.com/maypok86/otter/v2@v2.3.0
go mod tidy
```

检查 `go.mod`：Otter 必须是直接依赖且版本精确为 `v2.3.0`；不得顺带升级无关直接依赖。

### Step 4：依赖落盘后先验证未使用状态

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/internal/entitycache -count=1
git diff --check -- go.mod go.sum
```

预期：现有行为仍通过，diff 只包含 Otter 及其必需的 module graph 变化。

---

## Task 2：建立最终 EntityCache 构造契约和 policy token

**Files:**

- Modify: `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/cache.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/cache_test.go`

### Step 1：先写 Options 校验失败测试

最终公开构造输入：

```go
type Options[V any] struct {
	MaximumSize int
	IdleTimeout time.Duration
	TryRelease  func(V) (bool, error)
}
```

测试：

- `MaximumSize <= 0` 返回构造 error。
- `IdleTimeout < 0` 返回构造 error；`0` 合法且关闭时间淘汰。
- `TryRelease == nil` 返回构造 error。
- nil `KeyString` 和 nil `Loader` 继续返回构造 error。
- 不再接受可选变参 `Eviction`，不再公开 `SweepInterval` 或 `Now`。

先将测试改为：

```go
cache, err := NewCache(keyString, loader, Options[*cachedValue]{
	MaximumSize: 16,
	TryRelease:  tryRelease,
})
```

运行：

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/internal/entitycache -run 'TestNewCache' -count=1
```

预期：旧构造函数不能满足新测试，编译失败。

### Step 2：建立对象身份与 policy 的分离结构

实现保持以下所有权：

```go
type cacheEntry[V any] struct {
	value      V
	generation atomic.Uint64
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

使用 `otter.New` 返回构造 error，不用 `Must` 把配置错误变成 panic。构造顺序固定为先创建 EntityCache 本体，再把其回调绑定给 policy：

```go
policyOptions := &otter.Options[K, *cacheEntry[V]]{
	MaximumSize:     options.MaximumSize,
	OnAtomicDeletion: cache.onAtomicDeletion,
}
if options.IdleTimeout > 0 {
	policyOptions.ExpiryCalculator = otter.ExpiryAccessing[K, *cacheEntry[V]](options.IdleTimeout)
}
policy, err := otter.New(policyOptions)
```

不设置 stats recorder、Loader、refresh 或 Weigher。

### Step 3：验证 policy 只保存同一 entry 指针

同 package 测试在一次成功 Load 后读取 `values[key]` 和 `policy.GetIfPresent(key)`，断言两者是同一指针，且业务 value 指针未复制。

运行：

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/internal/entitycache -run 'Test(NewCache|CachePolicyToken)' -count=1
```

预期：构造边界和身份断言通过。

---

## Task 3：实现命中、冷加载和 policy 重登记

**Files:**

- Modify: `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/cache.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/cache_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/cache_benchmark_test.go`

### Step 1：补齐加载行为测试

保留并扩展现有测试：

- 同键并发冷加载只调用一次 Loader，返回同一业务对象指针。
- not found 连续两次调用 Loader 两次，不进入 `values` 或 policy。
- Loader error 原样返回，不进入 `values` 或 policy。
- 命中时 generation 单调推进。
- policy token 被淘汰后再次 Load 返回 `values` 中原对象，并把当前 entry 重新 Set 到 policy。
- 命中 deferred 对象时旧候选代次被取消，当前 entry 重新登记。
- 在固定容量和确定性访问序列下反复命中一个 hot key，再灌入多个只访问一次的 cold key；`policy.CleanUp()` 后 hot token 仍存在且至少一个 cold token 被淘汰。测试不绑定具体 cold victim。

先运行定向测试，确认新场景失败：

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/internal/entitycache -run 'TestCache(Merges|DoesNotRetain|ReturnsLoaderError|Advances|Reinserts|Cancels|RetainsHot)' -count=1
```

### Step 2：实现命中路径

顺序固定为：

```text
values 命中
  -> 推进 generation
  -> 取消同 key 的旧 deferred 代次
  -> policy.GetIfPresent(key) 记录热度
  -> token 不存在或不再指向 currentEntry 时 policy.Set(key, currentEntry)
  -> 返回 currentEntry.value
```

不要在热点命中复制业务对象、创建 goroutine 或调用 WriteBack。`GetIfPresent` 返回的 token 不是身份来源，必须继续返回 `values` 中的对象。

### Step 3：实现冷加载路径

顺序固定为：

```text
values miss
  -> singleflight(KeyString(key))
  -> 二次检查 values
  -> Loader
  -> not found/error 直接返回
  -> 创建 generation=1 的 entry
  -> 写入 values
  -> 在主锁外 policy.Set(key, entry)
```

不使用 Otter Loader，不缓存 not found。主锁内再次确认同 key 未出现，确保业务对象身份只有一个。

### Step 4：保持可比的命中 benchmark

保留阶段一已经建立的 `BenchmarkCacheHit/IdleDisabled`、`BenchmarkCacheHit/IdleEnabled` 和 `BenchmarkCacheParallelHit` 名称、输入与预热方式，只把构造迁移到最终 Options。两个 benchmark 都继续报告 allocation，确保 before/after 可以按同名样本比较。

### Step 5：验证加载和命中

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/internal/entitycache -count=1
go test -race ./services/internal/entitycache -run 'TestCache(Merges|Advances|Reinserts|Cancels|RetainsHot)' -count=1
go test ./services/internal/entitycache -run '^$' -bench 'BenchmarkCache(Hit|ParallelHit)$' -benchmem -count=3
```

预期：对象身份唯一、冷加载合并不变，热点路径没有业务对象复制。

---

## Task 4：实现常数时间候选登记和唯一 deferred release worker

**Files:**

- Modify: `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/cache.go`
- Add: `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/release.go`
- Add: `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/release_test.go`

### Step 1：先写 release 状态机测试

使用 channel 控制 `TryRelease`，不要用不稳定的 wall-clock sleep 表达并发顺序。测试覆盖：

1. `DeletionEvent.WasEvicted()==false` 时不登记候选。
2. overflow 或 expiration 事件按 key 去重；同 key 只保留当前 entry/generation。
3. `true, nil` 且 entry/generation 未变化时删除 `values[key]`。
4. `false, nil` 保留对象，并以 100ms、200ms、400ms 递增重试，最大不超过 5s。
5. `false, err` 记录 error、保留对象并使用相同退避。
6. TryRelease 阻塞期间 Load 命中，旧 generation 完成后不得删除对象。
7. 同 key 已被新 entry 替换时，旧候选不得删除新对象。
8. 多个 key 共用一个 worker，不为每个候选创建 goroutine 或 timer。
9. 所有候选暂时不可释放时，`len(values)` 可以超过 `MaximumSize`，且所有业务指针仍可命中。

Go 1.26 测试使用 `testing/synctest` 推进内部退避时间；涉及 TryRelease 并发窗口的用例继续使用 channel 明确卡点。

先运行：

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/internal/entitycache -run 'Test(AtomicDeletion|DeferredRelease|CacheKeeps)' -count=1
```

预期：release worker 尚未实现，测试失败。

### Step 2：实现回调候选登记

`onAtomicDeletion` 只允许：

```text
event.WasEvicted() 检查
  -> 读取 event.Value.generation
  -> deferredMu 下合并 key/entry/generation/nextAttempt
  -> 非阻塞写入容量为 1 的 wake channel
```

- `CauseInvalidation` 和 `CauseReplacement` 必须忽略。
- 不读取存储，不拿 EntityCache 主锁，不调用 `TryRelease`，不输出日志。
- 新候选初始 delay 为 100ms；相同 entry/generation 的重复事件不创建重复状态。
- 新 generation 覆盖旧 generation；旧 entry 事件不能覆盖 `values` 中已经存在的新 entry 候选。

### Step 3：实现单 worker 调度

`Start(tasks lifecycle.Tasks)` 只登记一个 worker。worker 管理一个 timer，选择 deferred map 中最早到期项；wake 用于候选新增或提前。

对每个到期候选：

1. 在 `deferredMu` 下取得候选快照并标记执行，随后释放锁。
2. 在所有 EntityCache 锁外调用 `options.TryRelease(entry.value)`。
3. `released=true`：取得主锁，仅当 `values[key] == entry` 且 generation 未变化时删除；再从 deferred 删除同代候选。
4. `released=false`：仍是同 entry/generation 时，delay 乘 2 并封顶 5s，更新 nextAttempt。
5. `err != nil`：用 `KeyString(key)` 记录结构化错误，然后按第 4 条处理。
6. entry 被访问、替换或已删除时，丢弃旧候选，不再次调用 WriteBack。

不得在每个 key 上创建 goroutine、ticker 或 timer。

### Step 4：验证 release worker 和 race

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/internal/entitycache -run 'Test(AtomicDeletion|DeferredRelease|CacheKeeps)' -count=1
go test -race ./services/internal/entitycache -run 'Test(AtomicDeletion|DeferredRelease|CacheDoesNotDelete)' -count=1
```

预期：bool/error/generation/identity 四个闸门均由测试固定，race 无报告。

---

## Task 5：接入可选 access expiry 与生命周期关闭

**Files:**

- Modify: `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/cache.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/release.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/cache_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/release_test.go`

### Step 1：先写 expiry 和关闭测试

覆盖：

- `IdleTimeout=0` 时不配置 ExpiryCalculator；清理 maintenance 不能仅因时间删除 token。
- 正 duration 使用 access expiry；重复 Load 刷新 token 的访问时间。
- expiration 候选也必须经过 `TryRelease`，`false,nil` 时业务对象继续存在。
- lifecycle context 取消后 worker 退出，并调用 `policy.StopAllGoroutines()`。
- 关闭产生的非 eviction 删除事件不进入 deferred。
- 未调用 Start 时重复 Start 的契约沿用 lifecycle task 容器；不增加额外状态机或兼容路径。

使用 Otter `CleanUp()` 触发待处理 maintenance，使用 `testing/synctest` 控制时间，避免固定 sleep。

### Step 2：实现 lifecycle 结束顺序

worker 收到 context 取消时：

```text
停止并清空内部 timer
  -> 不再领取 deferred 候选
  -> policy.StopAllGoroutines()
  -> 返回 lifecycle task
```

关闭时不遍历 `values` 调用 TryRelease；WriteBackManager 的最终 Flush 和进程退出负责持久化完成与内存统一释放。

### Step 3：验证 expiry 与 goroutine 生命周期

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/internal/entitycache -run 'Test(CacheExpiry|CacheStart|CacheShutdown)' -count=1
go test -race ./services/internal/entitycache -count=1
```

预期：`IdleTimeout=0` 无时间淘汰，正 duration 走同一安全释放闸门，生命周期结束无 EntityCache worker 泄漏。

---

## Task 6：把 slggame 角色对象接入最终 EntityCache

**Files:**

- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/service.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/service_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/handler.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/handler_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/app/app.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/app/app_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/config/services_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/config/debug/app.yml`
- Modify: `/Users/a47/workspace/txgameslg/server/config/release/app.yml`
- Modify: `/Users/a47/workspace/txgameslg/server/config/test/app.yml`
- Modify: `/Users/a47/workspace/txgameslg/server/tests/cluster/config/debug/app.yml.tmpl`

### Step 1：先改角色服务测试到最终配置

`slggameservice.Config` 使用：

```go
EntityCacheMaximumSize int
EntityCacheIdleTimeout time.Duration
```

测试 `MaximumSize <= 0` 和 `IdleTimeout < 0` 被拒绝，`IdleTimeout == 0` 合法。删除 sweep interval 断言。

handler 测试增加：

- 构造时把 maximum/idle 传给 EntityCache。
- `TryRelease` 使用项目 `internal/writeback` facade。
- pending 角色被淘汰候选命中时仍返回同一指针。
- WriteBack flush 后 deferred worker 最终允许释放，再次 Load 执行 Loader。
- Ping 不 Load 角色，也不刷新 policy 热度。

先运行：

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/slggame/... ./internal/app -count=1
```

预期：旧 Config/handler 装配无法满足新测试。

### Step 2：修改角色 handler 装配

`newRoleHandler` 创建：

```go
entitycache.NewCache(keyString, loader, entitycache.Options[*data.GDRole]{
	MaximumSize: config.EntityCacheMaximumSize,
	IdleTimeout: config.EntityCacheIdleTimeout,
	TryRelease:  writeback.TryRelease,
})
```

继续用 `runtime.Tasks()` 启动缓存唯一 worker。删除旧 `Eviction`、扫描间隔和特殊 error 转换；`TryRelease` 的 bool/error 已经是最终控制流。

### Step 3：切换角色默认 idleTimeout

四份生效 YAML 的 `services.slggame.entityCache.idleTimeout` 改为 `0s`，默认只由 W-TinyLFU 容量管理工作集。README 在 Task 8 同步说明正 duration 是可选策略。

### Step 4：验证角色链路

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/slggame/... ./internal/app ./internal/config -count=1
go test -race ./services/slggame/... -count=1
```

预期：Role_Rename 的事务与 WriteBack 行为不变，角色工作集改由 EntityCache 容量策略管理。

---

## Task 7：把 slgworld bucket 接入独立 EntityCache

**Files:**

- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/service.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/service_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/handler.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/handler_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/app/app.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/app/app_test.go`

### Step 1：先写 world 服务配置与容量测试

新增 slgworld 服务配置：

```go
type Config struct {
	EntityCacheMaximumSize int
	EntityCacheIdleTimeout time.Duration
}

type ConfigProvider func() (Config, error)
```

`Registration` 最终签名明确接收 service config provider、`WriteBackRuntimeFactory` 和 push sender factory。测试 provider 只在 slgworld 被实际构造时执行。

handler 测试覆盖：

- `worldID + bucketID` 强类型 key 仍唯一定位 bucket。
- 同 key 冷加载合并并返回同一 `GDWorldBucket`。
- 超过 `maximumSize` 时产生冷 bucket 候选，干净对象可释放。
- pending/WAL/checkpoint 未完成时 `false,nil` 保留 bucket；完成后最终释放。
- generation fence 或 WAL recovery error 仍由原 loader 原样返回，不缓存失败对象。

先运行：

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/slgworld ./internal/app -count=1
```

预期：world 尚未接收 EntityCache 配置和 lifecycle tasks，测试失败。

### Step 2：修改 world handler 与服务装配

`newWorldHandler` 接收 `lifecycle.Tasks` 和 `Config`，构造独立缓存：

```go
entitycache.NewCache(keyString, loader, entitycache.Options[*data.GDWorldBucket]{
	MaximumSize: config.EntityCacheMaximumSize,
	IdleTimeout: config.EntityCacheIdleTimeout,
	TryRelease:  writeback.TryRelease,
})
```

立即用 service runtime tasks 启动 worker。不得把 world bucket cache 放进 WriteBack Runtime；两者只通过 `TryRelease` 协作。

### Step 3：保持世界持久化成功边界

逐项确认没有改变：

- `loadBucketForActor` 的 WAL recovery 与 slot generation 检查。
- `World_MoveUnit` 的 Procedure 锁目标和 WAL append。
- checkpoint 同 stream 串行、跨 stream 并行。
- WriteBack Drain 等待 checkpoint 的关闭语义。
- stale generation 写入 fence。

### Step 4：验证 world 链路和 race

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/slgworld ./internal/app -count=1
go test -race ./services/slgworld ./services/internal/entitycache -count=1
```

预期：world bucket 不再无界常驻，WAL/checkpoint/generation 行为不变。

---

## Task 8：文档、性能和真实集群验收

**Files:**

- Modify: `/Users/a47/workspace/txgameslg/README.md`
- Modify: `/Users/a47/workspace/tx/docs/design.md`
- Modify: `/Users/a47/workspace/tx/docs/IMPLEMENTATION_PLAN.md`
- Review: `/Users/a47/workspace/txgameslg/server/go.mod`
- Review: `/Users/a47/workspace/txgameslg/server/go.sum`

### Step 1：更新当前稳定文档

README/design 必须说明：

- EntityCache 与 WriteBackManager 的职责边界。
- role/world 各自的 `maximumSize` 和 `idleTimeout` 配置路径。
- Otter 只持有 policy token，`values` 保持唯一业务对象。
- capacity 是软上限；pending/reserved/dirty 对象会延期释放。
- deferred release 固定退避，不公开调参。
- 默认 `idleTimeout=0s`；正 duration 使用 access expiry 且仍经过安全释放。
- Otter 固定版本 `v2.3.0`，stats 默认关闭。

实施路线将本项标记完成，并把下一项恢复为 `World_MoveUnit` 世界写、WAL、checkpoint 性能基线。

### Step 2：完整单元、race 和静态验证

```bash
cd /Users/a47/workspace/txgameslg/server
gofmt -w services/internal/entitycache/cache.go services/internal/entitycache/cache_test.go services/internal/entitycache/cache_benchmark_test.go services/internal/entitycache/release.go services/internal/entitycache/release_test.go services/slggame/service.go services/slggame/service_test.go services/slggame/handler.go services/slggame/handler_test.go services/slgworld/service.go services/slgworld/service_test.go services/slgworld/handler.go services/slgworld/handler_test.go internal/app/app.go internal/app/app_test.go internal/config/services_test.go
go vet ./...
go test ./... -count=1
go test -race ./services/internal/entitycache ./services/slggame/... ./services/slgworld ./internal/writeback -count=1

cd /Users/a47/workspace/tx
go test ./... -count=1

cd /Users/a47/workspace/txgen
go test ./... -count=1

cd /Users/a47/workspace/txgame/gen
go test ./... -count=1

cd /Users/a47/workspace/txgame/server
go test ./... -count=1

cd /Users/a47/workspace/txgameslg/gen
go test ./... -count=1

cd /Users/a47/workspace/txclient
npm test
npm run build

cd /Users/a47/workspace/txclientslg
npm test
npm run build
```

### Step 3：记录迁移后 benchmark 并做 A/B

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/internal/entitycache -run '^$' -bench 'BenchmarkCache(Hit|ParallelHit)$' -benchmem -count=10 > /private/tmp/txgameslg-entitycache-after.txt
go run golang.org/x/perf/cmd/benchstat@v0.0.0-20260825160852-19be9d8e6c70 \
  /private/tmp/txgameslg-entitycache-before.txt \
  /private/tmp/txgameslg-entitycache-after.txt
```

比较 `ns/op`、`B/op`、`allocs/op`：

- 不允许业务对象复制。
- allocation 增加必须定位并消除，不用“第三方库开销”概括带过。
- 稳定、可复现的明显吞吐回退时停止交付，先 profile 热点。
- 单机绝对值只作本机基线，重点看相同环境的变化方向。

### Step 4：运行有状态真实集群回归

确认真实 etcd、Redis、Mongo 和 txclientslg 依赖可用后运行：

```bash
cd /Users/a47/workspace/txgameslg/server
env GOCACHE=/private/tmp/txgameslg-e2e-go-cache \
  go run ./cmd/cluster-e2e \
  --artifact-root /private/tmp/txgameslg-entitycache-e2e \
  --timeout 6m
```

必须继续通过跨进程 Actor/RPC、双 MsgQ、Seq/Ack、断线恢复、Fusion、WriteBack、WAL、checkpoint、generation 接管和优雅关闭。失败保留 artifact，不清理现场。

### Step 5：检查范围并停止交付

六个工程分别运行：

```bash
git status --short --branch
git diff --check
git diff --stat
```

确认：

- `server/gen/**` 没有手工改动。
- 没有旧 `Eviction`、`SweepInterval` 或新的重复下行/持久化路径。
- 依赖仅新增精确 Otter 版本及必要间接项。
- 未执行 Git commit。

汇报单元/race/benchmark/真实集群证据和分项目中文 Angular Commit Message，然后等待人工 review，不开始世界写性能任务。
