# 热点路径 I/O 检查

范围：Agent 上行、Game/Actor、MsgQ 下行、实体缓存与持久化写回。本文区分实际命令计数和代码风险；没有延迟采样的数据不宣称为已发生的慢查询。未改业务协议、持久化保证或缓存策略。

## 已验证的 Redis 边界

`TestRoleMailboxRedisOwnershipLifecycle` 使用真实 Redis 和该用例客户端的命令 Hook，复用角色归属生命周期用例，不运行 MONITOR、KEYS 或 SCAN，不修改 Redis 全局统计。

- 一次角色冷寻址执行 1 条 Redis 命令。
- 预热后 100 次 RoleRouter 寻址、角色双邮箱下行以及两个邮箱的 Fetch/Rec/Confirm，新增 Redis 命令为 0。
- Session 续期到期后连续 100 次活动，仅首个活动执行 1 次续期。
- 校验实际下行内容、各邮箱独立序号及双邮箱分发数量，不只检查计数器。

计数覆盖真实 Redis + 进程内 RoleRouter/MsgQ，不等于独立集群所有进程的总命令数，也不是吞吐或延迟基准。两套示例已有多 Agent、多 MsgQ 验收；跨进程统一命令归因仍待补齐。

## 寻址延迟与写回隔离验证

`BenchmarkRoutingRedisIO` 使用真实 Redis，分别测角色预热寻址和精确 Session 寻址；在客户端命令边界注入额外 5 ms 等待，不调整 Redis 服务配置。2026-09-08 本机 darwin/amd64、Intel i9-9880H，100 次操作采样：

| 寻址路径 | 无注入 | 每命令额外等待 5 ms | Redis 命令/次 |
| --- | ---: | ---: | ---: |
| 角色热寻址 | 307 ns | 290 ns | 0 |
| 精确 Session | 0.331 ms | 6.467 ms | 1 |

这些数据证明延迟传播与命令次数，不代表生产吞吐；注入场景包含 Go 定时器调度开销，不能把差值全部归为 Redis 延迟。短采样下纳秒数差异不作性能提升结论。

复用 `TestWriteBackManagerFlushHoldsTransactionLockUntilStorageWriteFinishes` 的阻塞存储注入，并补充独立角色事务验证：同根对象等待存储写入完成；另一个根对象的内存事务可继续。该测试不证明不同根对象的刷盘并行，当前 Flush 仍按队列串行保存。

复现命令（在 tx 根目录）：

```sh
TX_REDIS_ENDPOINT=127.0.0.1:6379 go test ./modules/msgq -run '^$' -bench '^BenchmarkRoutingRedisIO$' -benchtime=100x -count=1
go test -race ./tests -run '^TestWriteBackManagerFlushHoldsTransactionLockUntilStorageWriteFinishes$' -count=1
```

## 优先跟进项

### P1：精确 Session 寻址逐次读取 Redis

`modules/msgq/session.go` 的 `ResolveSession` 每次调用 `store.Find`。`Sender.PubSession`、`JoinTopics`、`BroadcastTopic` 都经过它。普通成功业务响应走 RoleRouter，不受此影响；精确错误响应或高频使用这些接口时会放大 Redis 读负载。

建议下一批先测错误响应压力与精确下行量，再选择复用已知绑定或增加有界缓存。不能直接套用角色缓存而忽略 ConnectionID 在恢复时变化，以及 Topic 操作需要当前连接的约束。

### P1：后台持久化仍会占用 GD 根对象锁

`modules/writeback/manager.go` 的 `Flush` 串行处理待刷根对象；`flushTarget` 在 `txn.WithLocks` 内执行 `storage.Save`。慢 Redis/Mongo 写入会同时延长当前对象锁占用和后续根对象的排队时间。Procedure 提交登记本身是内存操作，但后续同对象操作仍可能等待刷盘锁。

`storage/fusion/strategy.go` 的保存同步写 Redis，随后提交本地延迟任务，并非完全无 I/O。`txgameslg/server/services/slgworld/pipeline.go` 使用异步 WAL Writer；`wal/writer/writer.go` 仍同步 Append，之后才提交 checkpoint。Mongo WAL 使用 majority 和 journal 写入保证。这些保证不能为了消除等待而直接去掉。

建议先用慢存储注入测量同根对象阻塞与跨根对象排队，再讨论有界并行刷盘或不可变变更快照；快照方案必须同时解决失败重试、版本与清理已保存变更的边界。本轮不实施。

### P2：邮箱续期到期时，Redis 等待占用邮箱锁

`modules/msgq/mailbox.go` 的 `touch` 在邮箱锁内调用 `store.Refresh`。Redis 实现按 TTL/2 节流，不是逐消息续期；但到期请求遇到慢 Redis 会阻塞同邮箱的消息操作。同角色共享分发逐个访问邮箱，一个被续期占锁的邮箱可能拖慢本次分发到其他邮箱。

角色租约维护只遍历本地角色，按 TTL/2 续期，不扫描 Redis。维护循环逐角色等待 Redis；需通过延迟注入检查大量角色同步续期时的尾延迟，不宜仅因存在网络操作就改为无限并发。

已复现邮箱阻塞范围：`TestSlowRenewalKeepsOtherMailboxesUsable` 在 SessionStore.Refresh 边界暂停续期，执行真实邮箱分发逻辑。相同角色的另一邮箱、不同角色邮箱均可完成精确下行；固定共享快照为“续期邮箱在前”时，共享分发至少等待 20 ms，释放续期后两个邮箱均成功入队。该等待是人工注入，不是 Redis 实测慢查询；实际角色快照来自 map，其他邮箱是否已经入队取决于遍历顺序，但整次共享调用仍会等待受阻邮箱。

测试只将其他邮箱隔离与最终分发正确性作为稳定断言，等待时间作为诊断输出，不把阻塞固定为必须保留的契约。

据此优先考虑将续期网络等待与队列锁分离：保留每 Session 的续期合并，并在续期返回后重新校验 ConnectionID 与邮箱活动状态；恢复、终止和旧续期失败不得误删新连接。不能只在 Refresh 前后机械地解锁/加锁，也不通过逐邮箱创建无界 goroutine 绕开等待。该生产改造尚未实施。

### P2：开启逐消息调试日志时有同步输出风险

`modules/msgq/mailbox.go` 的接收、发布和确认路径调用 Debug 日志。`log/log.go` 同步执行 Handler；实际是否磁盘或标准输出等待取决于装配的 Handler。关闭 Debug 时会在 Enabled 判断后返回。本轮未测日志设备延迟，不建议因此引入无界异步日志队列。

## 当前合理保留的 I/O

- Agent 与 MsgQ 的 Rec、Fetch、Confirm 以及 Game 的定向下行是既定可靠通信，不改为 UDP。
- 有状态角色和世界实体通过项目侧实体缓存加载，命中为内存读取；首次激活、缓存冷加载、世界恢复需要存储读取，不要求零 I/O。
- Login 鉴权、账号映射、建角和号段申请属于登录或初始化链路，不能与每帧游戏请求混算。
- etcd 租约续期、服务发现与槽位所有权属于后台或冷寻址；应分开统计，不能把整个 Redis/etcd 实例的后台活动归因到单次角色下行。

## 后续验收顺序

1. 补独立集群的进程级存储命令计数，分别记录 Game、Agent、MsgQ 的冷启动、稳定请求和续期窗口。
2. 精确 Session 下行和邮箱续期注入延迟，测量阻塞范围。
3. 写回注入慢 Redis/Mongo，测量同对象、不同对象的等待，再确定改造范围。

以上未完成项不标记为已经消除的瓶颈；六个项目仍独立维护，不引入跨示例引用。
