# cacheMgr 提交 SPI 与刷盘职责拆分实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 Procedure 的内存事务与有状态服务的缓存刷盘解耦，并通过提交 SPI 把最终变脏根对象可靠登记到 txgame cacheMgr。

**Architecture:** `txn.Runtime` 只执行 Procedure，并在修改前向 `CommitSink` 预留根对象身份，提交后以不可失败的确认把最终 dirty roots 交给 sink。通用 cache Manager 位于 tx 的 L2 模块，负责身份唯一性、Pending、容量收敛和 Flush；txgame 通过自定义 CMod 实例化 Manager、绑定 SPI 并管理生命周期。

**Tech Stack:** Go 1.26、tx `modules/txn`、tx `modules/cache`、`storage.ChangeApplier`、WaitGroupContext、txgame CMod。

**Spec:** `/Users/a47/workspace/tx/docs/design.md`

## Global Constraints

- 不保留 `Runtime.MarkDirty`、`Executor.MarkDirty`、公开 `Tx.DirtyTargets` 等过渡 API。
- `Prepare` 在 Procedure 修改前完成全部可失败校验；提交确认不返回错误。
- 相同 `LockKey` 在同一 cache Manager 内不能绑定不同 Store。
- Flush 成功才清理 Store 变更与 Pending；失败保留并重试。
- 无状态业务继续使用 `db.Save(GD*)`，不依赖 cache Manager。
- 新增或修改的注释、GoDoc 和维护文档使用中文。
- 不提交 Git，由用户手动提交。

---

### Task 1: 定义事务提交 SPI

**Files:**
- Modify: `/Users/a47/workspace/tx/modules/txn/runtime.go`
- Modify: `/Users/a47/workspace/tx/modules/txn/txn.go`
- Test: `/Users/a47/workspace/tx/tests/txn_runtime_test.go`
- Test: `/Users/a47/workspace/tx/tests/transaction_test.go`

**Interfaces:**
- Produces: `CommitSink.Prepare(context.Context, []LockTarget) (CommitReservation, error)`。
- Produces: `CommitReservation.Commit([]LockTarget)` 与 `Abort()`。
- Produces: `Runtime.BindCommitSink(CommitSink) error`，仅供框架组装。

- [x] 先写失败测试，锁定根/动态子 Procedure 的 Prepare、Commit、Abort 与 typed-nil 校验。
- [x] 运行定向测试，确认因 SPI 尚未实现而失败。
- [x] 实现最小 SPI，并把最终 dirty roots 收为 txn 包内部结果。
- [x] 删除公开 `MarkDirty`、`Pending` 与 `DirtyTargets`。
- [x] 运行 txn 定向测试和 race 测试。

### Task 2: 抽离通用 cache Manager

**Files:**
- Create: `/Users/a47/workspace/tx/modules/cache/manager.go`
- Create: `/Users/a47/workspace/tx/modules/cache/manager_test.go`
- Modify: `/Users/a47/workspace/tx/tests/txn_runtime_manager_test.go`

**Interfaces:**
- Consumes: `txn.CommitSink`、`txn.CommitReservation`、`storage.ChangeApplier`。
- Produces: `cache.Manager`、`cache.Config`、`Start`、`Flush`、`Shutdown`、`Pending`、`Release`。

- [x] 先写失败测试，覆盖身份冲突、提交登记、回滚释放、重复根去重、Flush 成功/失败、容量收敛和关闭排空。
- [x] 运行定向测试，确认新 package 尚不存在。
- [x] 从旧 txn Runtime 迁移 Pending、generation、Heap 观测、变更预算和 Flush，不复制第二份业务数据。
- [x] 删除 txn 中的 writer、后台循环和缓存容量配置。
- [x] 运行 cache、txn 和 storage 定向测试。

### Task 3: 调整 server CMod 边界

**Files:**
- Modify: `/Users/a47/workspace/tx/server/cmod.go`
- Modify: `/Users/a47/workspace/tx/server/cmod_transaction.go`
- Modify: `/Users/a47/workspace/tx/server/assembly.go`
- Modify: `/Users/a47/workspace/tx/server/context_access.go`
- Modify: `/Users/a47/workspace/tx/server/config.go`
- Modify: `/Users/a47/workspace/tx/server/runtime.go`
- Test: `/Users/a47/workspace/tx/server/context_test.go`
- Test: `/Users/a47/workspace/tx/server/cmod_builtin_test.go`

**Interfaces:**
- Produces: `CModContext.BindTransactionCommitSink(txn.CommitSink) error`，普通 `Context` 不暴露。
- Changes: `CModTransaction` 不再依赖 Fusion，也不负责 Flush。

- [x] 先写失败测试，证明普通业务 Context 看不到绑定接口，而自定义 CMod 能绑定一次 sink。
- [x] 运行 server 定向测试并确认失败。
- [x] 实现 CMod SPI，收敛 Transaction 配置和生命周期。
- [x] 运行 server 定向测试。

### Task 4: txgame 组装 cacheMgr CMod

**Files:**
- Create: `/Users/a47/workspace/txgame/server/internal/cachemgr/cmod.go`
- Create: `/Users/a47/workspace/txgame/server/internal/cachemgr/cmod_test.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/app/app.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/config/model.go`
- Modify: `/Users/a47/workspace/txgame/server/internal/config/framework.go`
- Modify: `/Users/a47/workspace/txgame/server/services/game/service.go`
- Modify: `/Users/a47/workspace/txgame/server/config/debug/app.yml`
- Modify: `/Users/a47/workspace/txgame/server/config/test/app.yml`
- Modify: `/Users/a47/workspace/txgame/server/config/release/app.yml`

**Interfaces:**
- Produces: txgame `CModCacheManager`，直接依赖 Transaction 与 FusionStorage。
- Changes: cache Manager 配置从 `runtime.txn` 迁移到 `runtime.cache`；`runtime.txn` 只保留事务调试配置。

- [x] 先写失败测试，证明有状态 game 必须选择 cacheMgr，且缺失依赖启动失败。
- [x] 运行 txgame 定向测试并确认失败。
- [x] 实现 cacheMgr CMod、惰性配置转换和 lifecycle 对接。
- [x] 用真实生成 GDRole 验证外部创建对象经 Procedure 自动进入 Pending 并 Flush 到 Fusion。
- [x] 运行 txgame 单元与集成测试。

### Task 5: 文档、瘦身与性能回归

**Files:**
- Modify: `/Users/a47/workspace/tx/README.md`
- Modify: `/Users/a47/workspace/tx/docs/design.md`
- Modify: `/Users/a47/workspace/tx/docs/IMPLEMENTATION_PLAN.md`
- Modify: `/Users/a47/workspace/txgame/README.md`

**Interfaces:**
- Documents: 无状态 `db.Save` 与有状态 `Procedure -> cacheMgr -> Flush` 双运行时。

- [x] 删除旧 txn Flush、MarkDirty、Pending 和 DirtyTargets 文档与测试残留。
- [x] 补充外部创建 GD 自动接纳、身份冲突、Release 和关闭排空示例。
- [x] 运行 `git diff --check`、三工程 `go test ./... -count=1`、`go vet ./...` 和事务/cache benchmark。
- [x] 对照审计标准检查依赖方向、公开 API、锁边界和失败保留语义。
