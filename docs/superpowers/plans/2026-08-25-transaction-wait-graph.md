# Transaction Wait Graph Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 允许子 Procedure 在运行时声明并获取新的直接锁，并在形成循环等待前返回包含完整等待链和 Procedure 路径的可诊断错误。

**Architecture:** 保留按 `LockKey` 精确注册和 64 分片索引；每个锁条目记录当前事务所有者和等待队列，无竞争路径只进入条目局部临界区。争用时由外层锁管理器维护单出边等待图，新增等待边前沿直接阻塞者链做指针追踪；发现环时拒绝当前请求，不让图进入有环状态。根事务负责有界重试，子 Procedure 继续使用 savepoint，所有锁遵循严格两阶段锁并在根事务结束时释放。

**Tech Stack:** Go 1.26、标准库 `sync`/`sync/atomic`/`context`、项目现有 `change.UndoRecorder` 与代码生成 Store；不引入通用图库。

**Spec:** `/Users/a47/workspace/tx/docs/design.md` 的“单机事务”和“事务精确 Key 锁注册表”。

## Global Constraints

- `tx` 不实现分布式事务或跨进程锁。
- 新增和修改的注释、GoDoc、维护文档统一使用中文。
- 无竞争单锁和四锁热路径必须保留独立 benchmark，并报告 `ns/op`、`B/op`、`allocs/op`。
- 死锁错误必须能还原完整等待环，环中每一步包含等待事务、直接阻塞事务、锁键和等待发生时的 Procedure 路径。
- Procedure 重试期间不得执行提交后副作用；外部副作用必须登记到 `AfterCommit`。
- 不提交 Git，由项目维护者人工审核后提交。

---

### Task 1: 纯等待图与诊断模型

**Files:**
- Create: `modules/txn/wait_graph.go`
- Create: `modules/txn/wait_graph_test.go`

**Interfaces:**
- Produces: `TransactionID`、`DeadlockStep`、`DeadlockError`、`waitGraph.add`、`waitGraph.remove`。
- Consumes: `LockKey`。

- [x] **Step 1: 写入等待边、无环链和两事务环的失败测试**
- [x] **Step 2: 运行 `go test ./modules/txn -run 'TestWaitGraph' -count=1`，确认因类型缺失失败**
- [x] **Step 3: 实现单出边 `map[TransactionID]waitEdge` 与指针追踪检测**
- [x] **Step 4: 只在发现环时分配完整 `[]DeadlockStep`，并实现稳定错误文本**
- [x] **Step 5: 重跑定向测试并确认图在拒绝死锁边后仍保持无环**

### Task 2: 可取消的所有权锁条目

**Files:**
- Modify: `modules/txn/txn.go`
- Modify: `modules/txn/lock_test.go`
- Modify: `modules/txn/lock_benchmark_test.go`

**Interfaces:**
- Consumes: Task 1 的等待图与诊断类型。
- Produces: 以 `TransactionID` 获取和释放的锁条目；超时取消不会残留等待边或 waiter。

- [x] **Step 1: 写入所有权转移、等待取消、死锁边拒绝和条目复用的失败测试**
- [x] **Step 2: 运行 `go test ./modules/txn -run 'TestLock' -count=1`，确认旧 channel 锁无法满足所有者诊断**
- [x] **Step 3: 将 `contextMutex` 改为条目局部状态、FIFO waiter 与等待图协调**
- [x] **Step 4: 保留 64 分片注册表、entry pool 和四锁内联持有集**
- [x] **Step 5: 重跑锁测试和 baseline benchmark，记录无竞争与争用成本**

### Task 3: 动态子 Procedure 直接锁

**Files:**
- Modify: `modules/txn/txn.go`
- Modify: `tests/transaction_test.go`

**Interfaces:**
- Consumes: Task 2 的事务身份锁管理器。
- Produces: `tx.Call(child)` 可获取 child 新声明的直接锁，并绑定对应 Store undo recorder。

- [x] **Step 1: 将“子过程新增锁被拒绝”测试改为动态获取成功测试**
- [x] **Step 2: 增加相同 Key 不同 Store、动态锁 child 回滚、父提交和锁继续持有测试**
- [x] **Step 3: 运行定向测试，确认旧实现失败**
- [x] **Step 4: 为 `Tx` 增加事务身份和可扩展持锁集，动态锁按批次稳定排序后获取**
- [x] **Step 5: 动态 Store 绑定 recorder；子失败只回滚 savepoint，锁与 recorder 保留到根结束**
- [x] **Step 6: 重跑 `tests/transaction_test.go` 全部事务测试**

### Task 4: 并发死锁诊断与根事务有界重试

**Files:**
- Modify: `modules/txn/config.go`
- Modify: `modules/txn/txn.go`
- Modify: `tests/transaction_test.go`

**Interfaces:**
- Produces: `ErrDeadlock`/`DeadlockError` 可由 `errors.Is`/`errors.As` 识别；根 `Call` 对内部死锁受害者做有界重试。

- [x] **Step 1: 用两个根 Procedure 和屏障构造 `A -> B`、`B -> A` 的确定性失败测试**
- [x] **Step 2: 断言错误链含两个锁键和两条完整 Procedure 路径，且锁等待 timeout 仍是 context 错误**
- [x] **Step 3: 增加受害者回滚后另一事务可完成、等待图无残留测试**
- [x] **Step 4: 增加根 Procedure 重跑测试，并断言 `AfterCommit` 只在最终成功时执行一次**
- [x] **Step 5: 实现根调用有界重试；不重试业务错误、context 错误或 after-commit 错误**
- [x] **Step 6: 运行竞争测试、`go test -race` 定向事务测试**

### Task 5: 文档、基准与最终审计

**Files:**
- Modify: `README.md`
- Modify: `docs/design.md`
- Modify: `docs/IMPLEMENTATION_PLAN.md`
- Modify: `modules/txn/lock_benchmark_test.go`

**Interfaces:**
- Documents: Procedure 只声明直接锁、动态锁流程、严格两阶段锁、死锁链、重试和副作用边界。

- [x] **Step 1: 用父 Procedure 动态调用子 Procedure 的完整代码示例更新 README**
- [x] **Step 2: 在 design 记录静态完整锁集、Wait-Die、Wound-Wait、通用图库和单出边等待图的取舍**
- [x] **Step 3: 补充无竞争、动态 child、争用无环和成环检测 benchmark**
- [x] **Step 4: 运行 `go test ./... -count=1`、定向 `-race`、benchmark 五轮和 `git diff --check`**
- [x] **Step 5: 检查公共 API、错误文本、注释术语和旧“完整锁集”描述没有残留**
