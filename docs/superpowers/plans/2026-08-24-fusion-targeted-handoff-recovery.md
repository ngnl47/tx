# Fusion 定点交接恢复实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 Fusion 交接恢复从常驻全 slot 遍历改为“启动全量恢复 + 运行期失败 marker 定点重试”，消除微服务 worker 身份分配和持续重复扫描。

**Architecture:** Fusion 热写仍以同槽 marker 封住 Redis patch 与 delayed task 入队之间的故障窗口。`HandoffRecoverer.Start` 在服务开放前完整遍历一次固定 slot 并恢复遗留 marker；服务运行后只维护当前进程明确入队或确认失败的 marker，使用根生命周期 context 按退避重试，不再周期性调用 `ReadHandoffs`。delayed task 继续按既有幂等任务键覆盖或合并，Scheduler 的多实例 lease 语义不变。

**Tech Stack:** Go 1.26.5、Redis Lua、go-redis、现有 Fusion delayed task backend、`lifecycle.WaitGroupContext`

**Spec:** `docs/design.md`

## Global Constraints

- 不新增第二套持久化 outbox、任务队列或微服务 worker 身份。
- 启动恢复和定点重试都不得执行 Redis `SCAN`/`KEYS`。
- 任务入队或交接确认失败必须返回原始错误并保留 Store 变更；定点重试登记不能依赖请求 context。
- marker 确认继续同时校验 Redis revision 与本次热写 token。
- 不提交 Git，由项目维护者审核后手动提交。

---

### Task 1: 锁定 Strategy 失败交接契约

**Files:**
- Modify: `tests/storage_fusion_strategy_test.go`
- Modify: `storage/fusion/strategy.go`

**Interfaces:**
- Produces: `type HandoffRetryer interface { QueueHandoffRetry(redis.HandoffEntry) error }`
- Consumes: `redis.HandoffReceipt{Revision, Token}` 与稳定 marker。

- [x] 添加失败测试：delayed save/delete 任一步入队失败时登记 `Marker + Token`，并保留原始错误。
- [x] 添加失败测试：`ConfirmHandoff` 返回错误时同样登记定点重试。
- [x] 运行聚焦测试，确认因缺少 retryer 接口和登记行为失败。
- [x] 在 `Strategy` 中注入必需的 `HandoffRetryer`，只在 Redis 热写成功后的交接失败路径登记。
- [x] 运行聚焦测试并确认通过。

### Task 2: 将 Recoverer 改为启动恢复与定点重试

**Files:**
- Modify: `storage/fusion/handoff_recovery.go`
- Modify: `tests/fusion_handoff_recovery_test.go`

**Interfaces:**
- Produces: `QueueHandoffRetry(redis.HandoffEntry) error`
- Produces: `RecoverStartup(context.Context) (int, error)`
- Produces: `RunPendingOnce(context.Context) (int, error)`，作为高级测试与受控运行入口。

- [x] 添加失败测试：`Start` 在注册 goroutine 前完成一个全 cursor 周期，启动遗留 marker 恢复失败则 fast-fail。
- [x] 添加失败测试：运行期只处理 `QueueHandoffRetry` 登记的 marker，不再调用 `ReadHandoffs`。
- [x] 添加失败测试：失败 marker 保留并退避；同 marker 的较新 token 不会被旧重试完成逻辑移除。
- [x] 运行聚焦测试并确认按预期失败。
- [x] 用互斥保护的 marker 去重表和单元素 wake channel 实现进程内定点队列。
- [x] `Start` 同步执行启动恢复，再注册只等待 wake/退避 timer 的后台循环。
- [x] 删除 `PollInterval` 和常驻 slot cursor 轮询逻辑；启动读取批量继续受 `SlotsPerRun/EntriesPerSlot` 约束。
- [x] 运行聚焦测试并确认通过、race 契约稳定。

### Task 3: 收敛 server 装配与 Redis 启动检查

**Files:**
- Modify: `server/storage.go`
- Modify: `server/storage_workers.go`
- Modify: `server/storage_workers_test.go`
- Modify: `storage/redis/handoff.go`
- Modify: `storage/redis/cluster.go`
- Modify: `tests/storage_redis_adapter_test.go`

**Interfaces:**
- Consumes: `HandoffRecoverer` 同时作为启动恢复器和 `Strategy.HandoffRetryer`。
- Removes: 独立 `ProbeHandoffs`，因为完整启动恢复已经使用真实读取脚本覆盖所有 endpoint。

- [x] 添加/调整 server 测试：启动恢复失败时 Scheduler 与业务服务均不启动。
- [x] 先构造 Recoverer，再注入 Fusion Strategy；`Strategy.Validate` 必须检查 retryer。
- [x] 删除仅为上一过渡步骤增加的 `ProbeHandoffs` 接口、实现与测试。
- [x] 保持 Recoverer 成功后才启动 Scheduler 的顺序。
- [x] 运行 `go test ./server ./tests -count=1` 的受影响用例；完整回归见 Task 4。

### Task 4: 收敛当前方案文档与完整验证

**Files:**
- Modify: `README.md`
- Modify: `docs/design.md`
- Modify: `docs/IMPLEMENTATION_PLAN.md`

**Interfaces:**
- Documents: 启动恢复、定点重试、任务幂等合并和故障示例。

- [x] 删除“常驻轮询全部 slot”“需要 Recoverer leader/worker 身份”的当前方案描述。
- [x] 补充入队失败但进程存活、进程在交接窗口宕机、重复任务合并三个示例。
- [x] 运行 `git diff --check` 和固定术语检查。
- [x] 使用 Go 1.26.5 运行 `tx go test ./... -count=1`，并设置 `TX_REDIS_ENDPOINT=127.0.0.1:6379`。
- [x] 运行 `txgen go test ./... -count=1`。
- [x] 使用 Redis 6379、Mongo 27017 运行 `txgame/server go test ./... -count=1`。
