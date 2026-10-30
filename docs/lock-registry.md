# Procedure 精确锁注册表

包结构：`modules/txn/lockey.go` 定义逻辑锁，`lock_table.go` 管理精确条目、取得释放和等待关系。公共声明与调用见 [Lockey](procedure-locks.md) 和 [Procedure](procedure-calls.md)。本篇只说明实现，不把注册表分片数量当业务并发上限。

## 事务精确 Key 锁注册表

### 进程级全局结构

事务精确 Key 锁注册表是进程级全局对象，不属于单个 Procedure：

```go
var transactionLocks lockTable

type lockTable struct {
    shards     [64]lockShard
    entryPool  sync.Pool
    waiterPool sync.Pool
    waitsMu    sync.Mutex
    waits      waitGraph
}

type lockShard struct {
    mu      sync.Mutex
    entries map[Lockey]*lockEntry
}
```

同一进程的所有 `txn.Run`、根 Procedure 和 `txn.WithLocks` 共享它。另一个进程拥有自己的注册表，因此不能提供分布式互斥。

### 64 分片的准确含义

64 个 `lockShard.mu` 只保护注册表 map，不是固定业务锁。真正业务锁是每个完整 Lockey 对应的动态 `lockEntry`：

```text
shard 18
  |
  +-- role:1001 -> entry A -> owner / waiters A
  +-- role:9009 -> entry B -> owner / waiters B
```

即使两个 Key 哈希到同一个 shard，它们只在 reserve/release map 的极短临界区内竞争；事务执行期间分别由 entry A 和 entry B 记录所有权，可以并行。

这与固定分段业务锁不同：

```text
固定分段业务锁：hash(key) -> Mutex，碰撞 Key 共用整段事务锁
当前精确 Key 注册表：hash(key) -> map shard -> map[key] -> 独立 entry
```

分片数量只控制注册表元数据竞争和固定 mutex 成本，不限制可同时存在的业务 Key 锁数量。

### 锁所有权与可取消等待队列

```go
type lockEntry struct {
    mu      sync.Mutex
    owner   TransactionID
    waiters *lockWaiter
    tail    *lockWaiter
    refs    int
}
```

`owner == 0` 表示空闲。无竞争获取只短暂进入该条目的局部 mutex 并设置 owner，不进入全局等待图；不同 Key 的条目互不阻塞。发生争用时，请求者才创建或复用一个 waiter，登记等待边并进入条目的 FIFO 队列：

```text
transaction 18 --等待 role:1002--> transaction 23
                                          |
                                          +-- lockEntry.owner = 23
```

waiter 使用容量一的 `signal` channel 接收所有权转移，同时通过调用方 context 响应取消。等待图按 FIFO 前驱关系建边：队首等待当前 owner，后续 waiter 等待自己的前一个 waiter。释放者只需删除队首等待边、转移 owner 并发送 signal；中间 waiter 取消时，也只需把直接后继改接到自己的前驱。这样既不会保留指向已释放事务的陈旧关系，也避免每次授权遍历整条队列形成 O(n²) 成本。取消和授权同时发生时，条目状态决定唯一结果；若 context 已取消但所有权已经转移，请求者会立即释放该锁，不进入 Procedure。

`signal` 只负责唤醒，不代表锁本身；锁的唯一事实来源是 `lockEntry.owner`。因此死锁诊断不需要猜测 channel 中是否还有 token，也不依赖 goroutine ID。

### reserve、refs 与回收

reserve 流程：

1. 根据完整 Lockey 哈希选择 shard。
2. 短暂获取 shard mutex。
3. 精确查询 `entries[key]`。
4. 不存在时从 pool 取得或创建 entry。
5. `refs++`。
6. 释放 shard mutex。
7. 尝试取得 owner；只有发生争用时才进入等待图和队列。

`refs` 统计已经 reserve 的持有者和等待者。它保证仍有人引用 entry 时不会从 map 删除或回收到 pool；owner 和等待队列由 entry mutex 单独保护。

释放所有权后再在对应 shard 下 `refs--`。归零后删除精确 Key 映射，并把 entry 放入 `entryPool`。争用 waiter 也由 `waiterPool` 复用，稳定争用路径不会持续制造 channel 和节点分配。

### 单次事务持锁结构

全局注册表只负责定位锁。单次事务使用 `heldLockSet` 保存已经取得的 entry：

```go
type heldLockSet struct {
    inline   [4]heldLock
    overflow []heldLock
    count    int
}
```

四把以内的常见锁集直接放在值数组中，不分配 held-lock slice；更多锁才使用 overflow。失败或结束时按逆序释放，确保中途超时不会遗留已持有锁。

### 为什么不使用成熟可重入锁

当前需求是 Procedure 直接锁、动态扩锁、context timeout 和子 Procedure 复用，而不是任意嵌套重入。第三方可重入锁通常需要识别 goroutine 或改变调用模型，也不能自动发现多个根事务之间的动态扩锁死锁。禁止嵌套根事务，并让 child 通过同一个 `Tx` 复用 owner 身份，边界更明确。

### 等待图与技术选型

采用单出边等待图：事务同步执行时，同一根事务至多等待一把锁。新增边沿 blocker 链检查循环，无竞争锁不访问等待图。此结构服务于动态扩锁，不要求调用方静态展开全部子过程。

等待图本身是无锁纯算法；`lockTable.waitsMu` 在争用边界保护它。无竞争锁不访问 `waitsMu`。发现环时，本次新增等待边不会写入图，请求当前边的根事务成为受害者：

```text
T1 [PGuildTrade -> PGrantItem] --role:1002--> T2
T2 [PRefreshGuild -> PLoadOwner] --guild:7----> T1
                                      |
                                      v
拒绝 T2 的新等待边，返回 DeadlockError.Cycle
```

`DeadlockError.Cycle` 的每个 `DeadlockStep` 都包含 `Waiter`、`Blocker`、`Lock` 和 `ProcedurePath`。`Blocker` 可能是当前锁持有者，也可能是 FIFO 队列中必须先推进的前驱事务；它准确表达等待图中的直接依赖，不把排队者误报为锁 owner。正常链路不构造诊断切片；只有确认成环时才分配完整链。错误可通过 `errors.Is(err, txn.ErrDeadlock)` 和 `errors.As(err, *txn.DeadlockError)` 识别。

标准 `txn.Call` 与 `txn.Runtime.Call` 最多自动重试三次死锁受害者。每次重试前完整 undo、解绑 recorder、释放锁和 Runtime 容量预留，并输出带完整等待链的 warning。`CallWithOptions` 供高级调用方设置 `DeadlockRetries`；零表示不重试。锁等待超时仍返回 context deadline/cancel 错误，不混同为死锁。

框架不会跨 goroutine 强制回滚另一个正在执行的根事务。当前请求者只让自己的根事务失败，符合 Go 同步调用和局部 undo 的所有权边界。Procedure 如果捕获 `tx.Call` 的死锁错误，根事务仍被标记为必须回滚；业务应立即返回该错误，不应继续外部副作用。

### 性能验证

锁基准覆盖无竞争、离散 key 并发、同 key 争用、动态锁和等待环。owner、FIFO 公平唤醒及等待图是当前锁契约的一部分，不能为改善争用数字而移除。只有实际业务 profile 显示瓶颈时才调整分片或索引结构；复测入口为 `modules/txn/lock_benchmark_test.go`。

