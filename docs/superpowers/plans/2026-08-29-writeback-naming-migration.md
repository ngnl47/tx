# WriteBack 命名与配置迁移实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan.

**Goal:** 在不改变现有运行行为的前提下，把事务提交后的脏根管理统一命名为 WriteBack，把业务对象工作集统一命名为 EntityCache，并一次性切换到 `runtime.cmods.*` 配置层级。

**Architecture:** `tx/modules/writeback` 提供通用写回运行时；`CModProcedure` 只通过 WriteBack SPI 管理其生命周期；`txgameslg/server/internal/writeback` 完成项目装配；`services/internal/entitycache` 在本阶段继续沿用当前 map、singleflight 和空闲扫描实现。旧 Go 符号、旧目录与旧 YAML 键直接删除，不提供兼容层。

**Tech Stack:** Go 1.26 module baseline、tx CMod/Procedure/txn/lifecycle、txgen 契约回归、YAML 严格解码、Go test/race。

**Spec:** `/Users/a47/workspace/tx/docs/superpowers/specs/2026-08-29-writeback-entity-cache-design.md`

## Global Constraints

- 当前代码、README、design 和 `AGENTS.md` 是事实来源；实施前重新检查六个工程的工作区状态。
- 不执行 Git commit；本计划结束时停下供人工 review，并按实际 diff 给出分项目中文 Angular Commit Message。
- 不保留旧 package、类型、方法、配置键或别名；编译失败用于找全迁移调用点。
- 本阶段不增加 Otter 依赖，不修改 EntityCache 的现有空闲扫描、加载、唯一实例和淘汰时机。
- `txgameslg/server/gen/**` 只能由生成器更新；本阶段预期不修改生成协议，不能手工编辑该目录。
- 新增或修改的注释、GoDoc 和维护说明使用中文。
- 防御性校验只覆盖正确性、持久化安全、资源边界和稳定 API；不枚举假想非法字符，不重复 YAML 严格解码和依赖库已有校验。
- 每个行为变更先写失败测试，再写最小实现；纯文件移动后立即运行定向测试，避免把多个断点堆到最后。

---

## Task 1：建立 WriteBack Runtime 的释放结果语义

**Files:**

- Move: `/Users/a47/workspace/tx/modules/cache/manager.go` → `/Users/a47/workspace/tx/modules/writeback/manager.go`
- Move: `/Users/a47/workspace/tx/tests/cache_manager_test.go` → `/Users/a47/workspace/tx/tests/writeback_manager_test.go`
- Modify: `/Users/a47/workspace/tx/tests/txn_runtime_manager_test.go`
- Modify: `/Users/a47/workspace/tx/tests/txn_runtime_test.go`

### Step 1：先把释放控制流写成失败测试

在迁移后的 `writeback_manager_test.go` 增加或改写表驱动测试，直接验证：

```go
released, err := manager.TryRelease(target)
```

必须覆盖：

- 未登记或已经干净的目标返回 `true, nil`。
- pending、reserved、`Store.HasChanges()` 返回 `false, nil`。
- 非法 `LockTarget` 返回 `false, ErrInvalidTarget`。
- 同一 `LockKey` 绑定不同 Store 返回 `false, ErrIdentityConflict`。
- 成功释放后再次调用仍为 `true, nil`。

运行：

```bash
cd /Users/a47/workspace/tx
go test ./tests -run 'Test.*TryRelease' -count=1
```

预期：因新 package 或新方法尚未完成而编译失败。

### Step 2：移动 package 并实现最终接口

将 package 声明、错误消息、GoDoc 和 import 统一改为 `writeback` / `writeback manager`。稳定 SPI 必须是：

```go
type Runtime interface {
	txn.CommitParticipant
	Start(lifecycle.Tasks) error
	Shutdown(context.Context) error
	TryRelease(txn.LockTarget) (bool, error)
}
```

删除 `ErrPendingRoot`。`ErrPendingRootCapacity`、`ErrIdentityConflict`、`ErrInvalidTarget`、`ErrChangeCollapseUnsupported` 保留，但错误文本使用新术语。

`TryRelease` 必须在与当前 `Release` 相同的实体锁和 manager 锁边界内判断 reserved、pending、`HasChanges()` 与身份冲突；暂时不能释放只返回 `false, nil`。

### Step 3：迁移 tx 内部测试 import 与命名

- `cachemodule` import alias 改为 `writebackmodule`。
- `newCacheRuntime` 改为 `newWriteBackRuntime`。
- 测试名和失败消息中表示写回职责的 `CacheManager` 改为 `WriteBackManager`。
- 不重命名与 CPU cache 或无关业务缓存有关的词。

### Step 4：验证 Manager 边界

运行：

```bash
cd /Users/a47/workspace/tx
go test ./tests -run 'Test.*(WriteBack|Runtime|TryRelease|Pending|Reservation|Flush)' -count=1
go test -race ./tests -run 'Test.*(WriteBack|TryRelease|Flush)' -count=1
```

预期：测试和 race 均通过，pending/reserved 不再依赖 error 分支。

---

## Task 2：迁移 CModProcedure 的 WriteBack SPI

**Files:**

- Modify: `/Users/a47/workspace/tx/ant/cmod_procedure.go`
- Modify: `/Users/a47/workspace/tx/ant/cmod_procedure_test.go`
- Modify: `/Users/a47/workspace/tx/internal/framework/procedure.go`
- Modify: `/Users/a47/workspace/tx/internal/framework/procedure_test.go`

### Step 1：先将测试改成最终公开 API

测试调用统一改成：

```go
ant.NewCModProcedureBuilder().WriteBack(component).WAL(walComponent).Build()
```

并断言缺少 WriteBack 组件时的构建错误使用新术语。先运行：

```bash
cd /Users/a47/workspace/tx
go test ./ant ./internal/framework -run 'Test.*Procedure' -count=1
```

预期：旧 Builder 尚未迁移，测试编译失败。

### Step 2：修改公开层和 framework 内部层

公开 API：

```go
type CModProcedureWriteBack interface {
	ComponentSpec() ComponentSpec
	Validate(CModProcedureContext) error
	Start(CModProcedureContext) error
	Drain(context.Context) error
	Stop(context.Context) error
}

func (builder *CModProcedureBuilder) WriteBack(component CModProcedureWriteBack) *CModProcedureBuilder
```

内部 API 对称改为 `ProcedureWriteBack`、`ProcedureBuilder.WriteBack`、`procedureWriteBackAdapter`。Builder 字段使用 `writeBack`，组件 Role 使用 `"writeback"`。WAL 注释改为 Procedure WriteBack 管线，生命周期顺序不变。

不要保留 `.Cache`、`CModProcedureCache` 或内部别名。

### Step 3：验证装配与生命周期顺序

运行：

```bash
cd /Users/a47/workspace/tx
go test ./ant ./internal/framework -count=1
go test -race ./ant ./internal/framework -run 'Test.*Procedure' -count=1
```

预期：组件校验、Start、Drain、Stop、WAL 可选装配和事务 participant 绑定顺序保持原样。

---

## Task 3：迁移 txgameslg 项目 WriteBack 适配层

**Files:**

- Move: `/Users/a47/workspace/txgameslg/server/internal/cachemgr/component.go` → `/Users/a47/workspace/txgameslg/server/internal/writeback/component.go`
- Move: `/Users/a47/workspace/txgameslg/server/internal/cachemgr/component_test.go` → `/Users/a47/workspace/txgameslg/server/internal/writeback/component_test.go`
- Move: `/Users/a47/workspace/txgameslg/server/internal/cachemgr/facade.go` → `/Users/a47/workspace/txgameslg/server/internal/writeback/facade.go`
- Move: `/Users/a47/workspace/txgameslg/server/services/slgworld/cache_runtime.go` → `/Users/a47/workspace/txgameslg/server/services/slgworld/writeback_runtime.go`
- Move: `/Users/a47/workspace/txgameslg/server/services/slgworld/cache_runtime_test.go` → `/Users/a47/workspace/txgameslg/server/services/slgworld/writeback_runtime_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/app/app.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/app/app_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/service.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/service_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/handler.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/internal/procedure/p_rename_role_test.go`

### Step 1：先改项目组件测试契约

将 fake runtime 改为实现：

```go
func (runtime *testWriteBackRuntime) TryRelease(target txn.LockTarget) (bool, error)
```

增加 facade 测试：

```go
func TryRelease(value txn.Lockable) (bool, error)
```

测试 facade 在组件未启动、已经停止和 typed nil runtime 时返回真实 error；已启动时原样返回 runtime 的 bool/error。暂时不可释放必须是 `false, nil`。

先运行：

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./internal/writeback ./internal/app ./services/slgworld ./services/slggame/... -count=1
```

预期：新目录和符号尚未实现，编译失败。

### Step 2：完成项目 package 与 facade 迁移

- package 改为 `writeback`，依赖改为 `github.com/ngnl47/tx/modules/writeback`。
- `Component` 实现 `ant.CModProcedureWriteBack`。
- `ComponentSpec.Role` 改为 `"writeback"`。
- factory、dependencies、运行时字段和日志统一使用 WriteBack。
- facade 只保留单对象 `TryRelease(txn.Lockable) (bool, error)`；不提供含义不明确的批量 bool 聚合。
- 角色空闲淘汰调用点在 `released=false` 时保留对象，真实 error 记录错误并保留对象。

### Step 3：迁移世界运行时命名但保持行为

统一改为：

```go
type WriteBackRuntime struct {
	manager    writebackmodule.Runtime
	pipeline   checkpointRuntime
	timeout    time.Duration
	loadBucket bucketLoader
	slots      discovery.SlotStore
	slotID     string
	instanceID string
}

type WriteBackRuntimeFactory struct {
	provider CheckpointConfigProvider
	mu       sync.Mutex
	runtime  *WriteBackRuntime
}

func NewWriteBackRuntimeFactory(provider CheckpointConfigProvider) (*WriteBackRuntimeFactory, error)
```

`WriteBackRuntime.TryRelease` 委托底层默认 manager；WAL 恢复、slot generation fence、checkpoint drain 和 shutdown timeout 不改变。

### Step 4：验证项目装配

运行：

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./internal/writeback ./internal/app ./services/slgworld ./services/slggame/... -count=1
go test -race ./internal/writeback ./services/slgworld ./services/slggame/... -count=1
```

预期：默认有状态逻辑服与 slgworld WAL 管线都通过新 SPI 装配，释放结果不再使用普通 error 表示忙碌。

---

## Task 4：把 resident package 迁移为 EntityCache，行为保持不变

**Files:**

- Move: `/Users/a47/workspace/txgameslg/server/services/internal/resident/cache.go` → `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/cache.go`
- Move: `/Users/a47/workspace/txgameslg/server/services/internal/resident/cache_test.go` → `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/cache_test.go`
- Move: `/Users/a47/workspace/txgameslg/server/services/internal/resident/cache_benchmark_test.go` → `/Users/a47/workspace/txgameslg/server/services/internal/entitycache/cache_benchmark_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/handler.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slgworld/handler.go`

### Step 1：记录迁移前行为与 benchmark

运行并保存终端结果到阶段记录，不把临时 benchmark 文件提交到仓库：

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/internal/resident -count=1
go test -race ./services/internal/resident -count=1
go test ./services/internal/resident -run '^$' -bench '^BenchmarkCacheHit$' -benchmem -count=5
```

### Step 2：只移动 package 和符号

- package 改为 `entitycache`。
- `Cache`、`Loader`、singleflight、map 和现有空闲扫描结构暂时不变。
- role/world handler import 改为 `services/internal/entitycache`。
- GoDoc 使用“实体缓存”“业务对象”，不再把写回管理器和业务对象缓存混为一谈。
- `BenchmarkCacheHit` 子项稳定命名为 `IdleDisabled`、`IdleEnabled`；新增使用同一预热 key 和 `b.RunParallel` 的 `BenchmarkCacheParallelHit`，作为阶段二前后共同基线。
- 不在本 Task 引入容量、policy token、deferred worker 或新 goroutine。

### Step 3：验证行为和基线未变化

运行：

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./services/internal/entitycache ./services/slggame/... ./services/slgworld -count=1
go test -race ./services/internal/entitycache -count=1
go test ./services/internal/entitycache -run '^$' -bench 'BenchmarkCache(Hit|ParallelHit)$' -benchmem -count=5
```

预期：测试语义一致，benchmark 只受 package 移动噪声影响；若出现稳定回退，先检查迁移是否意外改动热路径。

---

## Task 5：一次性迁移 `runtime.cmods.*` 与服务 EntityCache 配置

**Files:**

- Modify: `/Users/a47/workspace/txgameslg/server/internal/config/model.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/config/framework.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/config/services.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/config/loader_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/config/framework_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/config/services_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/config/debug/app.yml`
- Modify: `/Users/a47/workspace/txgameslg/server/config/release/app.yml`
- Modify: `/Users/a47/workspace/txgameslg/server/config/test/app.yml`
- Modify: `/Users/a47/workspace/txgameslg/server/tests/cluster/config/debug/app.yml.tmpl`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/app/app.go`
- Modify: `/Users/a47/workspace/txgameslg/server/internal/app/app_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/service.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/service_test.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/handler.go`
- Modify: `/Users/a47/workspace/txgameslg/server/services/slggame/handler_test.go`

### Step 1：先写只接受新层级的配置测试

构造最小 YAML，断言以下路径被解析并转换：

```text
runtime.cmods.actor
runtime.cmods.discovery
runtime.cmods.fusionStorage.dirtyTTL
runtime.cmods.fusionStorage.delayTask
runtime.cmods.fusionStorage.scheduler
runtime.cmods.procedure.transaction
runtime.cmods.procedure.writeBack
runtime.cmods.procedure.wal
services.slggame.entityCache
services.slgworld.entityCache
services.slgworld.checkpoint
```

再以旧键样本断言 YAML 严格解码报 unknown field；不要在业务代码增加旧键探测。

先运行：

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./internal/config ./internal/app -count=1
```

预期：新模型尚未实现，测试失败。

### Step 2：重组配置模型

模型使用明确的组合结构：

```go
type RuntimeSection struct {
	Lifecycle LifecycleSection `yaml:"lifecycle"`
	Log       LogSection       `yaml:"log"`
	Health    HealthSection    `yaml:"health"`
	CMods     CModsSection     `yaml:"cmods"`
}

type CModsSection struct {
	Actor         ActorSection         `yaml:"actor"`
	Discovery     DiscoverySection     `yaml:"discovery"`
	FusionStorage FusionStorageSection `yaml:"fusionStorage"`
	Procedure     ProcedureSection     `yaml:"procedure"`
}
```

`FusionStorageSection` 包含 `DirtyTTL`、`DelayTask`、`Scheduler`；`ProcedureSection` 包含 `Transaction`、`WriteBack`、`WAL`。删除顶层 `storage.wal` 字段。

服务配置统一为：

```go
type EntityCacheSection struct {
	MaximumSize int    `yaml:"maximumSize"`
	IdleTimeout string `yaml:"idleTimeout"`
}
```

本阶段为了保持当前扫描行为，可在转换后的内部配置中保留非公开扫描间隔常量；YAML 不再公开 `sweepInterval`。role 和 world 都必须要求 `MaximumSize > 0`，但本阶段不据此触发容量淘汰。

角色服务的项目配置同步改为：

```go
type Config struct {
	WorldSlot             string
	WorldCallTimeout      time.Duration
	EntityCacheMaximumSize int
	EntityCacheIdleTimeout time.Duration
}
```

`IdleTimeout == 0` 表示不启动当前空闲扫描，正 duration 继续使用固定的内部扫描间隔；不再把扫描频率作为业务配置。`MaximumSize` 在阶段一完成资源边界校验和透传，但不提前实现容量淘汰。

### Step 3：迁移转换函数与错误路径

- `CacheManagerConfig` 改为 `WriteBackManagerConfig`，返回 `writeback.Config`。
- `SLGGameResidentCacheConfig` 改为 `SLGGameEntityCacheConfig`。
- 新增 `SLGWorldEntityCacheConfig`。
- `slggameservice.Config` 和 handler 字段统一改用 EntityCache 术语；正 idle duration 继续构造当前空闲淘汰，扫描间隔使用 package 内固定常量。
- actor、discovery、Fusion、scheduler、transaction、WriteBack、WAL 转换全部从新结构读取。
- 错误文本精确指向新 YAML 路径。
- 不检查非法字符集合；只保留必要的非空、数值范围、duration 和组件依赖约束。

### Step 4：迁移全部生效 YAML

四份配置使用设计规范中的层级。保留各环境现有值；服务配置采用：

```yaml
services:
  slggame:
    entityCache:
      maximumSize: 10240
      idleTimeout: 15m
  slgworld:
    entityCache:
      maximumSize: 1024
      idleTimeout: 0s
```

阶段一必须可独立运行，因此角色暂时保留迁移前的有效空闲时长；阶段二接入 Otter 时再将默认值改为 `0s`。world 尚未接入容量淘汰，但必须先具有最终配置位置。

### Step 5：验证配置与应用启动

运行：

```bash
cd /Users/a47/workspace/txgameslg/server
go test ./internal/config ./internal/app -count=1
go test ./... -run 'Test.*Config|Test.*App' -count=1
```

预期：所有环境只使用新键，旧键测试稳定失败，新配置可以装配各服务组合。

---

## Task 6：同步 txgen 契约回归和跨工程编译

**Files:**

- Modify: `/Users/a47/workspace/txgen/tests/txn_runtime_retention_test.go`
- Modify: `/Users/a47/workspace/txgen/tests/txn_runtime_benchmark_test.go`
- Review only: `/Users/a47/workspace/txgame/gen/**`
- Review only: `/Users/a47/workspace/txgame/server/**`
- Review only: `/Users/a47/workspace/txclient/**`

### Step 1：迁移 txgen 测试 helper

- import 改为 `github.com/ngnl47/tx/modules/writeback`。
- `newGeneratedCacheRuntime` 改为 `newGeneratedWriteBackRuntime`。
- fixture 和失败消息使用 WriteBack 术语。
- 不改变生成数据生命周期和 benchmark 负载。

### Step 2：运行 txgen 与两个模板编译测试

```bash
cd /Users/a47/workspace/txgen
go test ./... -count=1

cd /Users/a47/workspace/txgame/gen
go test ./... -count=1

cd /Users/a47/workspace/txgame/server
go test ./... -count=1

cd /Users/a47/workspace/txgameslg/gen
go test ./... -count=1
```

预期：无状态工程不新增 Procedure WriteBack 依赖，有状态生成契约继续通过。

---

## Task 7：同步当前文档并清除生效旧符号

**Files:**

- Modify: `/Users/a47/workspace/tx/README.md`
- Modify: `/Users/a47/workspace/tx/docs/design.md`
- Modify: `/Users/a47/workspace/tx/docs/IMPLEMENTATION_PLAN.md`
- Modify: `/Users/a47/workspace/txgameslg/README.md`
- Modify: `/Users/a47/workspace/txclientslg/README.md`
- Modify: `/Users/a47/workspace/txclientslg/AGENTS.md`

### Step 1：更新稳定说明

- `tx` README/design 描述 `modules/writeback.Runtime`、`WriteBackManager`、`CModProcedureWriteBack` 和 `.WriteBack(...)`。
- `txgameslg` README 更新目录树、调用示例、配置表和生命周期说明。
- `txclientslg` 只把服务端证据边界的旧术语替换为 `WriteBackManager`；客户端能力边界不扩张。
- 设计文档说明本阶段 EntityCache 仍沿用当前淘汰实现，Otter 接入属于下一阶段。
- 历史 audit/spec/plan 保持不可变；新 spec 与当前 README/design 是后续事实来源。

### Step 2：扫描生效代码和当前文档

运行：

```bash
cd /Users/a47/workspace
rg -n 'modules/cache|CModProcedureCache|ProcedureCache|CacheManager|\.Cache\(|cachemgr|CacheRuntime|runtime\.cache|storage\.wal|residentCache|ResidentCache|internal/resident|cache manager' \
  tx txgen txgame txgameslg txclient txclientslg \
  --glob '!**/docs/superpowers/**' \
  --glob '!**/docs/audits/**' \
  --glob '!**/docs/archive/**' \
  --glob '!**/server/gen/**' \
  --glob '!**/node_modules/**'
```

预期：无生效引用。若命中的是通用 CPU cache 或第三方文档，逐项确认语义，不做机械替换。

---

## Task 8：阶段一完整验收与人工检查点

### Step 1：格式与静态验证

```bash
cd /Users/a47/workspace/tx
gofmt -w modules/writeback/manager.go ant/cmod_procedure.go ant/cmod_procedure_test.go internal/framework/procedure.go internal/framework/procedure_test.go tests/writeback_manager_test.go tests/txn_runtime_manager_test.go tests/txn_runtime_test.go
go vet ./...
go test ./... -count=1

cd /Users/a47/workspace/txgen
gofmt -w tests/txn_runtime_retention_test.go tests/txn_runtime_benchmark_test.go
go vet ./...
go test ./... -count=1

cd /Users/a47/workspace/txgame/gen
go test ./... -count=1

cd /Users/a47/workspace/txgame/server
go test ./... -count=1

cd /Users/a47/workspace/txgameslg/gen
go test ./... -count=1

cd /Users/a47/workspace/txgameslg/server
gofmt -w internal/writeback/component.go internal/writeback/component_test.go internal/writeback/facade.go internal/app/app.go internal/app/app_test.go internal/config/model.go internal/config/framework.go internal/config/framework_test.go internal/config/services.go internal/config/services_test.go internal/config/loader_test.go services/internal/entitycache/cache.go services/internal/entitycache/cache_test.go services/internal/entitycache/cache_benchmark_test.go services/slggame/handler.go services/slggame/handler_test.go services/slggame/service.go services/slggame/service_test.go services/slggame/internal/procedure/p_rename_role_test.go services/slgworld/writeback_runtime.go services/slgworld/writeback_runtime_test.go services/slgworld/handler.go services/slgworld/service.go services/slgworld/service_test.go
go vet ./...
go test ./... -count=1
```

### Step 2：重点 race 验证

```bash
cd /Users/a47/workspace/tx
go test -race ./tests ./ant ./internal/framework -count=1

cd /Users/a47/workspace/txgameslg/server
go test -race ./internal/writeback ./services/internal/entitycache ./services/slggame/... ./services/slgworld -count=1
```

### Step 3：检查工作区和 diff

对六个工程分别执行：

```bash
git status --short --branch
git diff --check
git diff --stat
```

确认没有生成目录手改、依赖变化或与迁移无关的清理。

### Step 4：停止并交付 review

不要开始 Otter 接入，不执行 Git commit。汇报：

- 每个工程的实际变更文件与验证命令。
- `TryRelease` 六类结果覆盖情况。
- 配置旧键拒绝和新键装配证据。
- 生效旧符号扫描结果。
- 分项目中文 Angular Commit Message。
