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

复用 `TestWriteBackManagerFlushHoldsTransactionLockUntilStorageWriteFinishes` 的阻塞存储注入：同根对象等待存储写入完成；另一个待刷根对象的内存事务可继续，其未保存变更仍保留。解除阻塞后，两次存储调用完成，pending 和已保存变更均清空。当前 Flush 按队列串行保存，这不是不同根对象的刷盘并行验证。

2026-09-08 补齐双待刷根对象验证，既有用例在 race 下重复 10 次通过，`go test ./... -count=1` 通过；未新增测试函数或改变生产写回策略。此处注入位于存储 SPI，不等同于真实 Redis/Mongo 网络故障注入，也未给出刷盘延迟分位数。

复现命令（在 tx 根目录）：

```sh
TX_REDIS_ENDPOINT=127.0.0.1:6379 go test ./modules/msgq -run '^$' -bench '^BenchmarkRoutingRedisIO$' -benchtime=100x -count=1
go test -race ./tests -run '^TestWriteBackManagerFlushHoldsTransactionLockUntilStorageWriteFinishes$' -count=1
```

## 优先跟进项

### 已处理：精确邮箱下行缓存归属

`SessionRouter.ResolveMailbox` 使用有界 Otter 缓存 SessionID 对应的 MsgQ InstanceID，合并同键冷查询。
`Sender.PubSession` 使用该边界：命中时不查询 Redis，不缓存 ConnectionID；每次从本地发现目录核对实例可用性。
同一 Session 不跨 MsgQ 迁移，邮箱丢失后建立新 Session。实例退出或明确邮箱失效时清缓存，传输超时不清缓存且不重发载荷。

`ResolveSession` 仍读取完整当前绑定，供 `JoinTopics` 与 `BroadcastTopic` 使用，避免把过期连接身份或已经终止的会话当成 Topic 操作锚点。
这条低频控制路径仍有每次一次 Redis 读取，不宣称所有 Session 接口均零 I/O。
两套示例独立传入 65536 个邮箱归属的容量上限，`NewSessionRouter` 要求显式容量。

真实 Redis 基准（同机、100 次操作）新增 `mailbox-warm`：无注入约 280 ns/次、注入 5 ms 后约 412 ns/次，均为 0 命令/次。
`session-current` 仍约 0.325 ms/次、注入后约 6.270 ms/次，均为 1 命令/次。
纳秒级短采样只用于说明不随 Redis 延迟等待，不作吞吐承诺。

2026-09-08 再次运行同一真实 Redis 基准（100 次）：角色热寻址无注入/注入 5ms 分别为 308/447 ns，邮箱热寻址为 466/553 ns，均为 0 命令/次；完整绑定查询为 0.327/6.209 ms，均为 1 命令/次。当前共享下行和精确邮箱下行均走缓存边界，不能再把早期精确 Session 查询结果理解为当前普通下行的逐消息成本。

### P1：后台持久化仍会占用 GD 根对象锁

`modules/writeback/manager.go` 的 `Flush` 串行处理待刷根对象；`flushTarget` 在 `txn.WithLocks` 内执行 `storage.Save`。慢 Redis/Mongo 写入会同时延长当前对象锁占用和后续根对象的排队时间。Procedure 提交登记本身是内存操作，但后续同对象操作仍可能等待刷盘锁。

`storage/fusion/strategy.go` 的保存同步写 Redis，随后提交本地延迟任务，并非完全无 I/O。`txgameslg/server/services/slgworld/pipeline.go` 使用异步 WAL Writer；`wal/writer/writer.go` 仍同步 Append，之后才提交 checkpoint。Mongo WAL 使用 majority 和 journal 写入保证。这些保证不能为了消除等待而直接去掉。

建议先用慢存储注入测量同根对象阻塞与跨根对象排队，再讨论有界并行刷盘或不可变变更快照；快照方案必须同时解决失败重试、版本与清理已保存变更的边界。本轮不实施。

真实适配器写回基准（2026-09-08）：`txgameslg/server/tests/integration/BenchmarkWriteBackStorageIO` 使用生成的 `GDRoleStore`、两个根对象和默认串行 Manager，结束后从数据库回读验证最终名称。每组 20 轮，同机 darwin/amd64、Intel i9-9880H：

| 后端 | 无注入（每轮两角色） | 每次适配器调用前等待 5ms |
| --- | ---: | ---: |
| Redis | 0.877ms | 13.265ms |
| Mongo | 1.081ms | 13.461ms |

计时包含变更登记和 Flush；每轮两次等待累加，另含调度及真实存储耗时。该微基准不是数据库服务端慢查询、网络故障或 Fusion/WAL 全链路注入，不提供生产吞吐和延迟分位数结论。两组均完成真实写入、回读及精确记录清理；有状态示例全量 Go 测试通过。复现命令与专用范围见 txgameslg README 的“有状态写性能基线”。

### 已处理：邮箱续期与队列锁分离

`modules/msgq/mailbox.go` 的 `touch` 只在队列锁内检查身份和登记在途续期，Redis Refresh 在锁外执行。
同邮箱到期活动等待同一在途请求，等待者可通过自身 context 取消；成功续期后继续按 TTL/2 节流。
下行队列访问不等待续期 I/O。恢复和终止可以在续期期间完成，旧续期返回后必须重新校验活动状态、ConnectionID 和角色归属有效期，
不得更新新连接的活动时间或删除新连接。该改造不增加逐消息 Redis 命令，也不引入后台无界任务。

`TestSlowRenewalKeepsOtherMailboxesUsable` 覆盖慢续期期间的精确下行和双邮箱共享下行；
既有重连测试改为验证旧续期结果不会破坏新连接，另覆盖等待者取消及终止后不复活。
并发活动测试验证 32 个到期活动只执行一次成功续期。

角色租约维护仍只遍历本地角色，按 TTL/2 续期，不扫描 Redis。维护循环逐角色等待 Redis；
大量角色同步续期时的尾延迟仍需独立验收。重连 Rebind 和终止 Unbind 的存储等待不属于本次 Refresh 锁拆分范围。

角色归属续期阻塞复现：`TestSlowRoleRenewalKeepsDeliveryUsable` 在两个本地角色都到达续期时刻后阻塞首个 `RefreshRole`。
阻塞期间两个角色均可精确下行，但维护循环尚未调用第二个角色的续期；释放后两个角色均完成续期。
这是存储 SPI 阻塞验证，不是完整 48h 租期实测。维护续期现已移出初始化分片锁：先用同角色新邮箱初始化复现等待超时，再验证拆锁后初始化和业务下行均能继续。
同角色在途请求合并，等待者可独立取消；新增迟到成功响应的停止/归属换代用例，防止旧请求恢复已失效归属。核心并发用例在 race 下重复 20 次通过，真实 Redis MsgQ race 回归及 tx 全量测试通过。
当前维护循环仍串行，后续角色续期仍会等待慢请求；首次申请、条件释放及初始化自身 I/O 的锁范围未变。不能把此改造解释为整个 MsgQ 不受慢存储影响；是否引入有界并行续期仍需单独评估，不增加逐消息 Redis 查询。

### P2：开启逐消息调试日志时有同步输出风险

`modules/msgq/mailbox.go` 的接收、发布和确认路径调用 Debug 日志。`log/log.go` 同步执行 Handler；实际是否磁盘或标准输出等待取决于装配的 Handler。关闭 Debug 时会在 Enabled 判断后返回。本轮未测日志设备延迟，不建议因此引入无界异步日志队列。

## 当前合理保留的 I/O

- Agent 与 MsgQ 的 Rec、Fetch、Confirm 以及 Game 的定向下行是既定可靠通信，不改为 UDP。
- 有状态角色和世界实体通过项目侧实体缓存加载，命中为内存读取；首次激活、缓存冷加载、世界恢复需要存储读取，不要求零 I/O。
- Login 鉴权、账号映射、建角和号段申请属于登录或初始化链路，不能与每帧游戏请求混算。
- etcd 租约续期、服务发现与槽位所有权属于后台或冷寻址；应分开统计，不能把整个 Redis/etcd 实例的后台活动归因到单次角色下行。

## 后续验收顺序

### 预热后空闲与稳定请求对照（2026-09-08）

两套角色/消息压测已使用客户端屏障，将预热、空闲、正式请求和关闭分开。并发 2、预热 1s、空闲及正式窗口各 3s 的真实依赖验收均通过：

- txgame：`/private/tmp/txgame-io-windows/20260908-110459.434136000`，正式完成 5664 次 System_Ping。
- txgameslg：`/private/tmp/txgameslg-io-windows/20260908-110615.838334000`，正式完成 6210 次 Role_Rename，Fusion/Mongo 最终角色验证通过。

两套 Agent/MsgQ 在两组窗口均仅观测到 Redis PING，没有 GET 等寻址命令；该结论只覆盖当前负载、窗口和 Runtime 客户端，不代表所有协议永远零存储访问。
无状态两个 Game 的每组窗口分别约 120 次 EVAL。有状态 slggame 空闲窗口 139 次 EVAL、2 次 Mongo update，正式窗口 144 次 EVAL；说明预热写回可落在空闲期，不能把两组计数直接相减后认定为业务开销。
延迟任务 ClaimDue 对空队列也逐桶执行脚本。验收配置使用 8 桶、200ms 轮询，3s 约 15 轮，即每实例 120 次 EVAL，与无状态观测一致。此为代码与配置归因，尚未逐调用标记，不能据此断言所有负载的 EVAL 都来自该路径。本批不调整后台调度或生产配置。

双世界负载已接入同一组屏障与采样器，按实际拓扑覆盖两台 slgworld。真实验收目录为 `/private/tmp/txgameslg-world-io-windows/20260908-111522.768321000`，并发 2、预热 1s、空闲及正式窗口各 10s；完成 17851 次移动，失败 0，两个世界最终 Mongo 数据、checkpoint、代际隔离及 WAL 清理通过。7 个进程的前后 InstanceID、计数增量已复核。

- Agent/MsgQ：两组窗口均仅 Redis PING，各 20 次，没有逐消息 GET。
- slggame：两组均为 400 次 EVAL，与空轮询量级一致；正式窗口另有 pipeline/other 计数，不从类别推断具体业务来源。
- 每个 slgworld：空闲窗口 Mongo find/insert/delete 各 1 次、update 2 次；正式窗口分别为 20/20/20/40 次，说明预热持久化尾部也会落在空闲窗口。

本轮仍不覆盖完整 Session 续期周期；后续应按真实租期安排更长窗口，不缩短生产租期来替代验收。

### 运行时快照接口

`storage/runtime.Runtime.IOStats()` 返回该运行时创建的 Redis/Mongo 客户端累计统计。固定类别记录 `Calls`、`Commands`、`Errors`、
`TotalDuration` 和 `MaxDuration`；不保存端点、key、载荷、账号或命令参数。未知命令归入 `other`，标签数有固定上限。
Redis 启动 Ping 前安装 Hook，Mongo Connect 时安装命令 Monitor，因此包含启动协商和后台 Ping；关闭连接不清空累计数据。

- Redis 单条 Hook 计一次调用，底层透明重试不能按此推算网络发送次数。Pipeline 单列，Calls 为批次数，Commands 为批内命令数，耗时为整批等待。
- Redis `Nil` 是正常缺失结果，不计失败；其他错误按驱动返回记录，包含可选启动协商或脚本回退中的失败，不能直接解释为业务失败。
- Mongo 按驱动命令完成事件计数，含驱动重试产生的各次命令事件；耗时口径不同于 Redis，不能直接比较为两个数据库的服务端执行耗时。
- 两次快照可对调用数、命令数、失败数和累计耗时作差；最大耗时是运行期最大值，不能相减得到阶段最大值。不提供清零操作，避免并发采样丢计数。
- 内存互斥仅保护固定统计项更新和快照复制，不包围网络 I/O。计数核心并发基准约 68 ns/次、0 分配（本机 i9-9880H），不含完整 Hook 的计时开销。

真实 Redis/Mongo 验证覆盖运行时客户端装配、Ping 和只读 GET。统计仅覆盖由该 Runtime 创建的客户端，不包括项目自行创建的外部客户端。
控制面已接入独立的 `StorageIOService.Snapshot`，宿主自动提供当前 InstanceID 和存储运行时快照。
`txctl --endpoint <控制地址> storage-io` 输出 Protobuf JSON，64 位整数使用十进制字符串，耗时字段单位为纳秒。
只查询内存，不触发 Redis/Mongo I/O，不改变排水状态或清零计数。必须确认两次 InstanceID 相同后才能对累计值相减；
未装配服务时 RPC 返回未实现，不将其当作零 I/O。两套示例编排器已独立接入每轮客户端场景的前后快照及增量产物，校验 InstanceID 和计数器单调性；包含登录、初始化及后台活动，不等同于纯热路径测量。故障后、重启后的场景只采样当前存活进程，各自建立基线，不跨重启相减。故障注入、排水、启动过程和独立 Login 验收不在这些窗口内，不能补采已宕机进程尚未导出的计数。纯热路径与后台 I/O 的分离归因仍未完成，不宣称集群 I/O 验收全部完成。
该查询沿用控制面的内网隔离边界，不新增公网入口或认证机制。

1. 补独立集群的进程级存储命令计数，分别记录 Game、Agent、MsgQ 的冷启动、稳定请求和续期窗口。
2. 精确邮箱热寻址已通过真实 Redis 延迟注入；邮箱续期已通过存储 SPI 阻塞验证。后续补真实链路的续期延迟窗口，不以缩短 48h 生产 TTL 代替完整租期验收。
3. 写回 SPI 阻塞下的同对象等待、其他待刷对象内存事务及最终刷空已验证；后续补真实 Redis/Mongo 慢写延迟测量，再确定改造范围。

以上未完成项不标记为已经消除的瓶颈；六个项目仍独立维护，不引入跨示例引用。
