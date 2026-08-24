# Fusion 本地延迟任务分发器实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 移除 Fusion 持久 handoff 协议，以进程内去重重试分发器发布延迟保存任务，并保证发布失败可观测且不会反向改变已经成功的 Redis 业务写结果。

**Architecture:** `Strategy` 只同步写 Redis 热文档，成功后把 `TypeSpec + RecordKey` 登记到进程内 `DelayTaskDispatcher` 并返回。分发器通过既有 `DelayQueue` 发布 save/delete task，失败时输出结构化错误日志并按有上限的指数退避持续重试；进程异常退出会丢失尚未持久化的本地登记，这是本方案明确接受的可靠性边界。

**Tech Stack:** Go 1.26、现有 `storage/fusion`、Redis delayed task backend、`WaitGroupContext`、`slog` 封装。

**Spec:** 本任务对话中已确认的 Fusion 简化方案，并同步更新 `docs/implementation_plan.md`、`README.md`、`docs/design.md`。

## Global Constraints

- Redis 热写失败时 `db.Save` 返回错误；热写成功并完成本地登记后返回 `nil`。
- delay task 发布失败不能静默，必须输出可被日志中心采集的结构化错误并持续重试。
- 本地 pending 按根记录去重，不设置需要业务估算的容量上限。
- 优雅关闭由 `WaitGroupContext` 管理分发器生命周期。
- 删除 handoff Redis Hash、Lua、启动 lease、全槽恢复、配置、测试和文档，不保留兼容入口。
- 不修改或回退当前工作区中与生命周期排水相关的既有变更。

---

### Task 1: 锁定 Strategy 与分发器契约

**Files:**
- Modify: `tests/storage_fusion_strategy_test.go`
- Create: `tests/fusion_delay_dispatcher_test.go`

**Interfaces:**
- Produces: `DelayTaskSubmitter.Submit(meta.TypeSpec, storage.RecordKey)`。
- Produces: `NewDelayTaskDispatcherWithOptions(DelayQueue, DelayTaskDispatcherOptions) *DelayTaskDispatcher`。

- [x] 编写 Strategy 热写成功后只登记本地任务并返回成功的失败测试。
- [x] 编写发布失败后保留 pending、输出日志并最终重试成功的失败测试。
- [x] 运行定向测试并确认因接口尚不存在而失败。

### Task 2: 实现本地分发器并切换 Fusion 写路径

**Files:**
- Create: `storage/fusion/dispatcher.go`
- Modify: `storage/fusion/strategy.go`
- Modify: `storage/fusion/errors.go`
- Modify: `server/storage.go`
- Modify: `server/storage_workers.go`
- Modify: `server/cmod_fusion.go`

**Interfaces:**
- Consumes: 既有 `DelayQueue.Enqueue` 与 `enqueueDelayedWrites`。
- Produces: 进程内按根记录去重、失败退避、结构化日志和生命周期启动能力。

- [x] 实现最小分发器，使 Task 1 测试通过。
- [x] 把标准 server 装配改为启动分发器，再启动 delayed task Scheduler。
- [x] 运行 `storage/fusion`、`server` 和顶层定向测试。

### Task 3: 删除 handoff 协议和配置

**Files:**
- Delete: `storage/fusion/handoff.go`
- Delete: `storage/fusion/handoff_recovery.go`
- Delete: `storage/redis/handoff.go`
- Delete: `storage/redis/handoff_test.go`
- Delete: `storage/redis/scripts/confirm_handoff.lua`
- Delete: `storage/redis/scripts/read_handoffs.lua`
- Delete: `storage/task/redis/startup_lease.go`
- Delete: 对应 handoff/startup lease 测试
- Modify: Redis adapter、cluster、Lua、server config 与 txgame config 转换。

**Interfaces:**
- Produces: 不再暴露 handoff、startup lease 或 `MaxPendingPerSlot` 的干净 API。

- [x] 删除 Redis 热写中的 marker/token/revision 交接分支。
- [x] 删除 server 和 txgame 的 handoff 配置与启动恢复装配。
- [x] 删除只服务于 handoff 的启动 lease；保留 Scheduler 自己的 task lease。
- [x] 运行编译和定向测试，清理所有生产代码引用。

### Task 4: 同步文档并验证真实链路

**Files:**
- Modify: `docs/implementation_plan.md`
- Modify: `README.md`
- Modify: `docs/design.md`
- Modify: `txgame/README.md`

**Interfaces:**
- Produces: 面向使用者和维护者一致的本地重试可靠性说明。

- [x] 删除已经淘汰的 handoff 主线说明，保留必要的技术取舍结论。
- [x] 补充 delay task 发布失败日志、重试和进程异常边界示例。
- [x] 使用 `127.0.0.1:6379` 验证 Redis 热写、后台 task 发布和 Mongo 回写。
- [x] 运行 tx、txgen、txgame 全量测试、vet 和残留符号扫描。
