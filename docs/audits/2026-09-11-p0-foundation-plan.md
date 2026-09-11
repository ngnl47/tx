# 基础能力 P0 验证与实施计划（2026-09-11）


## 当前结论与验收矩阵

本节是当前状态；下方逐批记录中的“下一项”仅描述当时进度。复核实际日志、集群 summary 和当前 Git 差异后，四项 P0 的既定关键路径均已有证据，未发现本批需要修改生产实现的新缺陷。六工程全量测试入口与客户端构建已有证据，默认跳过的 47 项真实依赖测试现已全部单独启用并通过；此结论不是所有集成场景认证或故障窗口无损保证。

| P0 | 已验证证据 | 结论与限制 |
|---|---|---|
| GD Redis/Mongo/Fusion | 最新真实存储回归 15 pass；Mongo 迟到写/确认丢失探针；真实世界写回；两套集群套件 | 版本保护、失败留脏、发布恢复及落地关键路径有证据；Fusion 本地 pending 的进程崩溃窗口仍按既定契约接受 |
| 单机 Procedure | 定向 84 pass、race 73 pass；真实 GD 回滚与 Manager 写回失败恢复；生成 Store 回滚 2 pass | 多根锁、动态子过程、panic 预约撤销和旧 pending 保留有分层证据；不承诺跨库 ACID 或裸字段撤销 |
| ProtoActor RPC | 定向 46 pass；真实 Remote 请求生命周期和双进程准入排水；此前本地 Actor race 29 pass | 明确错误、等待/请求超时、迟到响应、关闭清理有证据；真实 Provider 生命周期未纳入本轮 race 成功结论 |
| 消息与多 Session | 最新定向 race 67 pass；gRPC 重连隔离、双 Agent 内存链路；两套真实跨 Agent 共享/恢复场景 | Seq/Ack 与精确/共享投递有分层证据；内存错误载荷用例不能当作所有业务错误协议的端到端验证 |

表中计数来自各自最终定向运行，均无 skip/fail；存在范围重叠，不相加成“总通过数”。两套集群 summary.status 均为 passed；SLG 两个预期 SIGKILL 符合场景，不计为优雅退出。

| 剩余事项 | 分类 | 处理决定 |
|---|---|---|
| ProtoActor etcd Provider 的 shutdown 普通 bool 并发读写 | 已知依赖风险 | 真实 etcd 定向 race 再次失败：Shutdown 写入与 keepAliveForever 读取竞争；处置复核已完成，继续保留风险，不直接改 module cache、加延时或关闭检测 |
| Fusion 热写后、任务持久化前进程退出；MsgQ owner 内存正文丢失 | 已接受可靠性边界 | 不自动增加 outbox、持久化 MQ 或业务重投；需求升级时单独设计 |
| SLG --shutdown-failure | 本轮已补齐的关闭验收 | 实测四次失败、自行退出码 1、新 owner 接管及最终清理通过；详见末节，不再作为待验收项 |
| 六工程全量入口、完整集群 race、性能复测 | 全量入口已执行，仍有验证限制 | 六个 Go 模块与两客户端测试/构建成功，默认跳过的 47 项已全部补验通过；未跑完整集群 race 或新 benchmark，详见末节 |

此前测试及文档变更已提交：tx `f4f3d38`、txgame `ea47a44`。测试代码分别净增 360、291 行，包含资源清理顺序修正；生产及生成物变化 0 行。本次从六工程干净工作区开始，仅补真实依赖验证记录，不作为瘦身或性能收益统计。

当前四项关键路径验收阶段收敛，已知 Provider 竞态的处置复核也已完成，详见末节。没有已确认的新编码项；仅在出现具体缺陷、可验证修复版本或第三方维护边界调整时重新开启，不重复评估同一风险或堆叠零碎测试。

## 范围与顺序

用户明确四项 P0 工作优先级：GD 的 Redis/Mongo/Fusion 持久化管道、单机 Procedure 事务、基于 ProtoActor 的 RPC、上下行消息与 Session 管理及多 Session 单角色扇入扇出。本表的 P0 表示工作优先级，不表示已发现四类 P0 缺陷。

按依赖关系先存储，再 Procedure，再 Actor RPC，最后消息与 Session。当前设计把 Fusion/Transaction/Actor Runtime 归为 L2；本计划将其视为用户指定基础能力的必要运行时，覆盖 L0/L1 及这些直接相关 L2，不改变依赖层级，不扩展为 L3/L4 业务功能建设。

| 顺序 | 验收重点 | 当前实现入口 | 首要证据 |
|---|---|---|---|
| 1 GD 持久化 | Create/Save/Load、字段及集合语义、失败留脏、路由键、版本冲突；Fusion 热写、任务发布、租约、回填与 TTL | storage/changes.go、storage/redis、storage/mongo、storage/fusion；txgen 生成加载/变更模板 | adapter 单测、真实 Redis/Mongo 条件写与故障恢复、真实 GD 往返 |
| 2 Procedure | 多根锁顺序、并发冲突、子过程/savepoint、error/panic/cancel 回滚、提交预约及最终脏根；失败写回保留 | modules/txn、modules/writeback | 受控并发/取消测试、真实生成 Store 撤销及写回验证 |
| 3 ProtoActor RPC | 请求/响应及错误、超时取消、消息所有权、停止准入、已受理消息排水、目标恢复和定位 | modules/actor、modules/agent/actor_dispatcher.go；相关普通 modules/rpc 边界 | 本机与双进程调用、明确拒绝与超时、关闭和恢复窗口 |
| 4 消息与 Session | 上行 Seq 去重/处理完成确认、下行 Seq/Ack、会话恢复与旧连接拒绝、角色归属、多 Session 扇入及共享/精确下行 | modules/agent、modules/msgq、protocol、两个客户端及独立服务适配 | 多 Session 同角色并发、断线重连、迟到 Ack/解绑、部分失败与慢会话验证 |

两套示例作为真实生成结构和端到端验收载体，各自维护边界；不因本计划建立跨示例源码依赖。第三项以 ProtoActor 链路为主，不用普通 gRPC RPC 测试替代 Actor 验收。

## 保留的契约

- storage.Save 仅在 writer 成功后清理 Store；失败保留变更。Mongo 条件写使用版本保护，冲突返回，不让旧增量换版本盲重试。
- Fusion 成功是 Redis 热写及本地任务登记完成，不是 Mongo 已落盘，也不是跨库事务。当前设计明确接受热写成功至任务持久化之间进程异常退出的窗口；先验证设计内的发布重试、非静默失败和正常关闭排空。若要提升为异常退出后也必落盘，属于单独的可靠性契约决策，不能默默引入 outbox。
- Procedure 成功是单机受控内存提交；不承诺跨库 ACID，也不覆盖裸字段写入。写回成功和回收另有边界。
- RPC 只承诺正常运行时调用语义，超时不证明业务未执行，不自动重放业务。异常后的可靠完成继续属于 MQ、幂等和恢复设计。
- SessionID 是逻辑会话，ConnectionID 是物理连接；恢复必须拒绝旧连接的迟到操作。多会话共享角色归属，成功业务响应可按 RoleID 扇出，请求级错误精确回请求 Session。
- 当前 MsgQ 下行正文在 owner 内存，实例崩溃允许未确认下行丢失并由业务状态重同步；不把现有内存邮箱视为持久化消息队列。

例如两个 Session 同属一个 Role：两侧上行必须携带各自可信会话身份进入同一角色处理边界；角色共享响应分发到对应邮箱，各邮箱独立推进 Seq/Ack；一条旧连接的 Ack 不得确认另一条新连接的数据。

## 推进与退出条件

1. 每项先核对当前调用链、声明契约及已有测试，不以历史通过记录当成本轮验证。
2. 先执行已有关键测试，再补足有实际风险的故障窗口；发现问题先复现，修复后回归。不为覆盖率或接口实现数量新增任务。
3. 区分实现缺陷、验收证据缺口与契约升级。契约升级和新能力先明确取舍，已有契约内的可复现缺陷按授权范围修复。
4. 每批报告触发条件、影响、证据和验证限制；串行实施，不提交 Git，验证后执行 ponytail-review。Ponytail 只审复杂度，不替代正确性检查。
5. 每条需有明确通过证据或剩余缺口，才进入阶段结论。集成依赖缺失、skip、race 风险须单独记录，不能按通过处理。性能仅在热路径实际变更时做前后对照，停止边际微优化。

## 首批当前证据

开始时六工程工作区干净。核对 tx README、设计文档、当前实施计划与存储/事务/消息关键入口后，在 tx 执行：

```sh
GOCACHE=/private/tmp/tx-go-build go test ./storage/... ./tests \
  -run 'Test(Redis|Mongo|Fusion|StorageSave)' -skip 'Integration' -count=1 -json
```

退出码 0，294 个测试及子测试 pass、1 个 skip，没有 fail。skip 为 TestMongoLateWriteAfterClientTimeout，缺少 TX_MONGO_LATE_WRITE_ADDRESS。名称含 Integration 的用例被筛选排除，不能计为通过。

原始日志：/private/tmp/tx-p0-foundation-20260911/storage-baseline.jsonl。本轮不是完整存储链路认证，没有运行真实 Redis/Mongo 故障窗口、Procedure、Actor 或 Session 全量验收，也未跑 race 或新 benchmark。代码未修改。

下一项：检查现有真实存储验收入口及隔离配置，验证 Redis 原子 patch、Mongo 版本冲突/迟到写，以及 Fusion 发布/落地/回填的失败恢复；使用真实 GD 结构串联读取与保存。先执行现有覆盖，只有明确缺口才新增测试或修改代码。

本轮文档及六工程 diff 检查通过。后置 ponytail-review：Lean already. Ship. 无新增实现抽象或测试框架。计划变更未提交。


## 真实存储首批进展

本轮 driver 故障探针、真实 GD 三后端及 SLG 世界写回共 16 个顶层测试、含子测试 23 项通过，均无 skip/fail。发现并修复 txgame 集成测试先关闭连接后清理数据的问题，真实残留从 roles=12/storage_probes=1 降为 0/0；生产代码未修改。命令、隔离和限制见 [真实存储报告](2026-09-11-p0-storage-real-validation.md)。

下一项补真实 DelayTaskDispatcher 与 Redis 任务 backend 的发布失败/恢复排空联合证据，再推进 Procedure。现有测试中的直接 submitter 或内存队列不能替代该链路；本轮不宣称全部存储故障已认证。

## 实际分发器发布恢复进展

已新增 TestFusionDispatcherRecoversRedisPublicationFailure，使用实际 DelayTaskDispatcher、Redis task backend、Mongo 和生成 GDRole。任务发布因 Redis WRONGTYPE 失败，Drain 超时后无需重新 Submit 即可恢复；连续热写的最新状态最终落到 Mongo，发布恢复不改变热数据 revision，再次排空不产生任务。本次 14 项真实存储回归通过、0 skip/fail，未修改生产代码。详细命令与证据追加于真实存储报告。

下一项进入单机 Procedure：先核对多根锁顺序、子过程提交/回滚、错误与 panic 恢复的现有测试，再补明确缺口。Fusion 进程崩溃窗口保持原契约。

## Procedure 首批验证进展

核对当前 txn.Run/Call、Runtime 提交预约、锁注册表和 README 单机事务契约。现有覆盖已有稳定锁序、动态锁死锁受害者完整回滚及重试、等待取消后释放已取得锁、子过程 error/savepoint、写回容量预约与 Flush 持锁。此次补充的明确缺口是动态子过程 panic 与 Runtime 提交预约的联合验证。

新增 TestTxnRuntimeChildPanicRestoresPendingChangesAndAbortsReservations：父子根进入事务前各有待写变更；子过程取得新锁并修改后 panic，验证原 panic 向上传播、父子待写状态恢复、undo recorder 解绑、两份预约各 Abort 一次且不 Commit。同一 Runtime 随后重新调用父子过程，限时取得原锁并提交两份新预约。本次未发现生产实现缺陷，未修改生产代码；新增测试 58 行。

例如上一轮已提交内存但未 Flush 的变更不能因下一轮子过程 panic 被清空；本测试保存进入前的变更集合，并在回滚后比较。

本次验证（测试与子测试计数，不叠加重复运行）：

- 修改前 tx 定向基线 83 pass；修改后 84 pass，均 0 skip/fail。
- tx 事务/写回定向 race 73 pass、0 skip/fail，未报告数据竞争；该结论不覆盖 Actor 第三方 Provider。
- txgen 现有生成 Store 事务回滚测试 2 pass、0 skip/fail，覆盖嵌套集合恢复和既有 key delta 保留。此次未修改 txgen。

```sh
# tx，修改前后相同命令
GOCACHE=/private/tmp/tx-go-build go test ./modules/txn ./modules/writeback ./tests ./internal/framework ./ant \
  -run 'Test(Transaction|TxnRuntime|WriteBackManager|LockTable|WaitGraph|ManagedProcedure|Procedure|CModProcedure)' -count=1 -json
# tx
GOCACHE=/private/tmp/tx-go-build go test -race ./modules/txn ./modules/writeback ./tests \
  -run 'Test(Transaction|TxnRuntime|WriteBackManager|LockTable|WaitGraph|ManagedProcedure)' -count=1 -json
# txgen
GOCACHE=/private/tmp/tx-go-build go test ./tests -run '^TestGeneratedStoreTransactionRollback' -count=1 -json
```

日志在 /private/tmp/tx-p0-procedure-20260911：baseline.jsonl、regression.jsonl、race.jsonl、generated-store.jsonl。一次尝试在 workspace 根运行 go test 因无 go.mod 未启动测试，随后已在正确 tx 模块目录完成上述回归，不计为测试失败或通过。

后置 ponytail-review：Lean already. Ship. 复用现有 Store、父子 Procedure 和提交参与者测试桩；没有新公共抽象、生产配置或故障注入框架。本批仅测试与文档，未跑 benchmark；全部变更未提交。

下一项：用真实生成 GD 与默认 writeback.Manager 联合检查子过程回滚、旧 pending 保留及写回失败重试，避免仅凭桩测试宣称完整 Procedure 验收。context 取消现有证据限于锁等待和返回错误，不宣称任意运行中的 Process 会被强制中断。

## Procedure 与真实 GD/Fusion 联合验证

新增 TestFusionProcedureRollbackPreservesPendingAcrossFlushFailure，串联生成 GDRole、txn.Runtime、默认 writeback.Manager、实际 Fusion Strategy/Dispatcher、Redis task backend 和 Mongo。测试显式驱动 Flush/Drain/RunOnce，不启动后台周期调度；故障仅注入测试角色根键，不新增生产配置或 writer 桩。

先提交名称 A 留待写回，再在父子过程内分别改名并返回子过程错误；回滚后名称仍为 A，Store 仍脏且 Manager.Pending 为 1。保存原始 Redis 根键后，临时改为 hash，让真实 GET 在 patch 写入前返回 WRONGTYPE。Flush 失败保留脏状态及 pending，TryRelease 拒绝释放；恢复原键再次 Flush 成功，实际任务最终把 A 写入 Mongo，清理 pending 后 TryRelease 成功。

本次新测试单独通过，相关真实存储回归 15 项通过、0 skip/fail，独立运行不重复计数。本次未发现生产实现缺陷，生产及生成物变化 0 行；新增测试代码 156 行，未跑 benchmark。累计 txgame 未提交代码为新增 312、删除 21、净增 291 行，包含此前清理顺序和发布恢复测试。

```sh
# txgame/server
TXGAME_REDIS_URI='redis://127.0.0.1:6379?db=15' \
TXGAME_MONGO_URI='mongodb://127.0.0.1:27017/tx_p0_procedure_20260911' \
GOCACHE=/private/tmp/tx-go-build go test ./tests/integration \
  -run '^Test(Redis|Fusion|GeneratedStorageProbeDocumentContracts|GeneratedChangesApplyToRedisMongoAndFusion)' \
  -count=1 -timeout=120s -json
```

日志目录 /private/tmp/tx-p0-procedure-storage-20260911；focused.jsonl 为单测、regression.jsonl 为回归。测试仅使用唯一角色 ID、任务前缀及独立 Mongo 库，清理备份键和自身数据，不执行 FLUSHDB。

本例覆盖同根父子过程错误回滚；多根锁和动态子过程 panic 证据在上一节，不把两者描述成同一条真实多根端到端用例。过程内存提交、Manager 热写成功和 Mongo 落地仍是三个独立完成边界，不承诺跨库 ACID 或进程崩溃无损。

下一项进入 ProtoActor RPC：先复核请求/响应、明确错误、超时与停止准入/排水的现有测试，再执行有边界的本机及远端验收；具体代码修复尚需评估，不加入自动业务重投。

补充验证：使用相同依赖环境，将上面的测试命令改为 `go test -race ./tests/integration -run '^TestFusionProcedureRollback' -count=1 -timeout=60s -json`，1 项通过、0 skip/fail，未报告竞争；见 race.jsonl。cleanup-check.txt 实测 Mongo roles=0、storage_probes=0。此轮未重跑六工程全量或 Actor race。

后置 ponytail-review：Lean already. Ship. 保留直接测试 Procedure 契约所需的两方法测试适配，未新增公共抽象。六工程 diff 检查通过，所有累计变更保持未提交。

## ProtoActor RPC 首批验证

核对当前 Actor README、Delivery/Respond、Future/pending 表和邮箱准入实现。新增 TestActorRemoteAwaitTimeoutAndBusinessError：两个实际 Runtime 经回环 Remote 通信，目标处理由通道屏障阻塞；20ms 的 Await 返回 DeadlineExceeded，释放目标后同一 Future 仍取得完整业务结果。第二次显式调用返回错误码 47 及原始错误信息，两条请求各处理一次。该用例验证 Await 的局部等待超时，不把它等同于 CallFuture 创建时的请求 deadline 到期。

现有 TestActorDrainAcrossProcesses 本次真实运行通过：管道只控制屏障，业务经过双进程 Remote；两条已准入请求在关闸后完成，迟到 Call/TellAccepted 返回明确拒绝。新增 Remote 用例复用已有启动助手，无新增运行时协议、依赖或自动重投。

首次定向回归为 45 pass、1 fail、0 skip：TestActorRuntimeLogsMailboxOverflow 把第一条正常的 actor system started 日志当成溢出日志。专用 actorLogHandler 现在仅采集 actor mailbox overflow，保留原溢出断言，避免启动日志占满容量为 4 的采集通道。修复后最终定向回归 46 pass、0 skip/fail（含该筛选命中的固定 Session router 既有测试）。本地 Actor race 29 pass、0 skip/fail，未报告竞争；真实 etcd/Remote 两项显式排除在本次 race 外，未重验或修复历史第三方 etcd Provider Shutdown/keepAliveForever 竞态。

```sh
# tx，功能回归含真实依赖
TX_ETCD_ENDPOINT=127.0.0.1:2379 GOCACHE=/private/tmp/tx-go-build \
 go test -p 1 ./modules/actor ./tests \
 -run '^Test(Actor|PendingCalls|Future|NewCallError|DrainedMailbox|Fixed|RuntimeVirtual|IdlePassivation|Kind|LifecycleContext|Config|VirtualPlacement)' \
 -count=1 -timeout=120s -json
# tx，本地并发检查，显式排除真实 Provider 生命周期
GOCACHE=/private/tmp/tx-go-build go test -race ./modules/actor \
 -skip '^TestActor(DrainAcrossProcesses|RemoteAwaitTimeoutAndBusinessError)$' -count=1 -json
```

日志在 /private/tmp/tx-p0-actor-20260911：regression.jsonl 保留首次失败；final.jsonl 为最终回归；local-race.jsonl 为本地 race。运行使用唯一 etcd 测试前缀与临时回环端口。remote.jsonl 是早先取消等待版本的验证，不替代最终超时版本证据。

本轮新增测试 96 行、日志采集修正 3 行，生产及生成物变化 0 行，未跑 benchmark。后置 ponytail-review：Lean already. Ship. 屏障仅用于测试，未新增公共抽象；六工程 diff 检查通过，全部累计改动未提交。

下一项仍在 RPC：检查请求自身 deadline 到期、迟到响应与 Runtime 关闭时 pending 清理的联合证据；现有本地表测试不直接替代真实 Remote 验收。具体生产修复尚需评估，仍不增加业务自动重投。

## ProtoActor 请求过期与关闭清理

将上一节的真实 Remote 测试扩展并更名为 TestActorRemoteCallLifecycle，复用同一组 Runtime、业务 Actor 和测试屏障，未增加公共接口。追加三个检查：

1. CallFuture 的请求 context 在业务阻塞期间到期，以仍有效的外层 context Await，确认结果为 DeadlineExceeded 且 pending 表为空。
2. 在释放旧业务前发起新请求；旧响应迟到后，新 Future 仍取得自身的业务错误码，旧 Future 保持过期结果，不能被迟到成功响应改写。
3. 目标业务已开始后关闭调用方；未完成 Future 返回 ErrRuntimeStopped，pending 表清空，新调用明确拒绝。解除目标屏障后仍能完成远端业务与排水，不把调用方关闭当作远端撤销。

例如第一次请求已执行但调用方超时，第二次请求不能误收第一次的迟到成功响应。本例只发起明确的独立请求，不加入业务自动重投。调用方退出后，目标回送响应产生 EndpointWriter 连接拒绝/重连日志是预期故障注入现象，属于传输连接行为，不是业务自动重放证据。

原始记录在 /private/tmp/tx-p0-actor-cleanup-20260911：focused.jsonl 为扩展用例单测，regression.jsonl 为定向回归。测试使用独立 etcd 前缀与临时回环端口。

```sh
# tx；单测改用 -run '^TestActorRemoteCallLifecycle$'
TX_ETCD_ENDPOINT=127.0.0.1:2379 GOCACHE=/private/tmp/tx-go-build \
 go test -p 1 ./modules/actor ./tests \
 -run '^Test(Actor|PendingCalls|Future|NewCallError|DrainedMailbox|Fixed|RuntimeVirtual|IdlePassivation|Kind|LifecycleContext|Config|VirtualPlacement)' \
 -count=1 -timeout=120s -json
```

本次未修改生产或生成物，测试净增 75 行，未跑 benchmark，也未重跑 race。历史 Provider 关闭竞态继续单列，功能验收不等于 race 已解决。

下一项进入上下行消息与 Session：先核对 Seq/Ack、旧连接拒绝、同角色多 Session 扇入扇出及断线恢复的现有实现和关键测试，具体新增用例或生产修复需要以当前缺口评估。

最终结果：扩展单测 1 pass；定向回归 46 pass、0 skip/fail，单测不重复累计。后置 ponytail-review：Lean already. Ship. 复用既有 Remote 测试，检查私有 pending 表只发生在测试且持锁读取，没有新增生产观测接口。六工程 diff 检查通过，累计变更仍未提交。

## Session 首批序号与重连隔离验证

核对 MsgQ gRPC 契约、错误映射及已有 Agent/Session/Redis 绑定测试，扩展 TestMsgQSenderPublishesExactAndSharedRoleWithReusedConnection。复用原有 Sender、两个同角色 Session 和 bufconn gRPC 链路，验证两会话上行 Seq 独立；第一会话重连恢复已收 Seq，旧连接的 Confirm、Rec、TerminateSession 均返回 codes.Aborted，未确认下行仍可重放。第一会话确认至 Ack=2 后，第二会话重连仍收到自己的未确认 Seq=1；新连接正常继续 Seq=2。

例如同角色的桌面端确认了共享响应，不会替手机端确认该响应；旧桌面连接也不能确认或终止新桌面连接。此用例验证邮箱及 gRPC/Sender 边界，不等同于真实 TCP/WebSocket 客户端经 Agent 到业务 Actor 的完整扇入扇出。

当前修改前定向基线 66 pass、0 skip/fail；扩展单测 1 pass；修改后相同范围 race 66 pass、0 skip/fail，未报告竞争。包含显式开启的真实 Redis Session 绑定及并发 Rebind 测试；新扩展用例本身使用内存绑定存储和 bufconn，不把它称作真实 Redis 端到端测试。

```sh
# tx；修改前基线不带 -race，修改后回归带 -race
TX_REDIS_ENDPOINT=127.0.0.1:6379 GOCACHE=/private/tmp/tx-go-build \
 go test -race -p 1 ./modules/agent ./modules/msgq ./tests \
 -run '^Test(Agent|MsgQ|SessionClient|LocalSessionClient|GRPCSessionClient|FixedSessionClient)' \
 -count=1 -timeout=120s -json
```

日志目录 /private/tmp/tx-p0-session-20260911：baseline.jsonl、focused.jsonl、race.jsonl。Redis 用例使用自身唯一前缀及键清理，无全库清理。本轮未修改生产或生成物，测试净增 44 行，未跑 benchmark；未运行六工程全量或真实客户端验收。

后置 ponytail-review：Lean already. Ship. 直接扩展既有链路，无新测试框架或公共抽象；六工程 diff 检查通过，全部累计变更未提交。下一项检查两个 Agent 连接经可信身份进入同一角色、共享响应与请求级错误精确返回的联合覆盖；具体缺口仍需评估。

## 双 Agent 连接身份与下行隔离

新增 TestAgentSameRoleSessionsPreserveIdentityAndIsolateDownMessages：两份 Agent Server 使用相同角色、不同可信 SessionID/ConnectionID，通过既有内存 Transport 和本地 MsgQ 接入同一个测试 Dispatcher。两个连接各自发送 Seq=1，断言认证主体与连接身份完整保留、两侧分别收到处理完成 Ack。随后 MsgQ 精确发送 session 99 的错误载荷，再按 Role 广播共享载荷：第一会话依次收到 Seq=1 错误、Seq=2 共享；第二会话只收到 Seq=1 共享。

本测试中的错误正文是不透明测试字节，由测试直接 PubSession；没有模拟业务协议解码或真实业务 Handler，也不把测试 Dispatcher 当作 ProtoActor 执行证据。覆盖的是 Agent 可信身份传递、MsgQ 精确/共享投递与两条连接实际帧输出，尚不等同于真实 TCP/WebSocket 客户端端到端验收。

新用例单独 1 pass；包含该用例的定向 race 回归 67 pass、0 skip/fail，未报告竞争。回归中现有 Redis Session 绑定用例显式连接本机 Redis，新增用例本身无外部存储。原始记录：/private/tmp/tx-p0-agent-fanout-20260911/focused.jsonl、race.jsonl。

```sh
# tx
TX_REDIS_ENDPOINT=127.0.0.1:6379 GOCACHE=/private/tmp/tx-go-build \
 go test -race -p 1 ./modules/agent ./modules/msgq ./tests \
 -run '^Test(Agent|MsgQ|SessionClient|LocalSessionClient|GRPCSessionClient|FixedSessionClient)' \
 -count=1 -timeout=120s -json
```

本次生产/生成物变化 0 行，测试新增 84 行，未跑 benchmark。后置 ponytail-review：Lean already. Ship. 复用已有 Transport、SessionStore 与公共本地路由能力，无新增公共抽象或测试框架。六工程 diff 检查通过，全部累计变更未提交。

下一项评估两套示例现有真实 TCP/WebSocket 和业务 Actor 验收入口，优先执行跨进程多会话场景；具体补充目标仍需根据现有覆盖确认，不继续为独立小分支堆叠测试。

## 两套示例真实多进程验收

按两工程 README 顺序执行现有 cluster-e2e；无需新增重复测试。两套命令均退出 0，summary.status=passed。成功状态只在各进程收尾及专用依赖 Cleanup 成功后写入；未以客户端单个事件代替整套成功。

```sh
# txgame/server
GOCACHE=/private/tmp/tx-go-build go run ./cmd/cluster-e2e \
 --artifact-root /private/tmp/tx-p0-game-cluster-20260911 --timeout 5m
# txgameslg/server
GOCACHE=/private/tmp/tx-go-build go run ./cmd/cluster-e2e \
 --artifact-root /private/tmp/tx-p0-slg-cluster-20260911 --timeout 6m
```

- txgame：真实 Login、TCP/WebSocket、跨进程 Actor/RPC、MsgQ、Fusion 写回、Seq/Ack、重连及 Game/MsgQ/Login 排水重启。initial.jsonl 的 shared_role 记录 RoleID=470101，两 SessionID 为 367019542814720、367019542814723，对应不同 Agent txgame-3121-19825、txgame-2746-19830，restored=true；共享载荷一致且不会确认另一 Session 的上行。12 份进程退出记录均为 0。
- txgameslg：generation-1.jsonl 的 shared_role_world 记录 RoleID=480001、WorldID=490001，两 SessionID 为 367021072941058、367021089718272，对应不同 Agent txgameslg-815-19943、txgameslg-1546-19938，restored=true。RoleActor 与 WorldActor 的共享响应、跨 Actor 调用、Mongo 拒写窗口及接管、MsgQ 故障恢复等标准场景完成。slgworld-a、msgq-a 按场景设计为 signal: killed，其余 11 份退出记录均为 0；不能将两个预期故障进程描述为优雅退出。本次未启用 --shutdown-failure 分支。

完整 artifacts：

- /private/tmp/tx-p0-game-cluster-20260911/20260911-124039.722667000
- /private/tmp/tx-p0-slg-cluster-20260911/20260911-124148.280653000

两目录保留 summary.json、client JSONL、processes 日志及退出记录。隔离与清理沿用带 ownership 校验的专用 Redis DB12/13、Mongo txgame_cluster_e2e/txgameslg_cluster_e2e、etcd /tx/e2e/txgame 与 /tx/e2e/txgameslg 范围；本次未清理日常 DB14/15。SLG summary.redisKeys=17 是最终清理前的采样值，不是残留数量。

本轮代码变更 0 行，仅追加当前验收证据；未运行 benchmark、全量单测或 race，不把进程故障下的功能成功当成第三方 Provider 竞态已解决。后置 ponytail-review：Lean already. Ship. 无新增抽象或重复测试。全部累计改动保持未提交。

四项 P0 已有从存储/事务故障检查到真实客户端多会话的本轮证据，但不是所有失败窗口的完整认证。下一项收敛验收矩阵，区分已接受可靠性边界、现有风险与真正需要修复的缺陷；具体新编码项尚需评估，停止按零碎分支持续增加测试。

## SLG 有限关闭重试验收补齐

执行已有 --shutdown-failure 分支，完整命令退出 0，summary.status=passed、shutdownRetries=3、worldWritebackVerified=true、slgGameGeneration=2。

```sh
# txgameslg/server
GOCACHE=/private/tmp/tx-go-build go run ./cmd/cluster-e2e \
 --shutdown-failure --artifact-root /private/tmp/tx-p0-slg-shutdown-20260911 --timeout 8m
```

Artifacts：/private/tmp/tx-p0-slg-shutdown-20260911/20260911-153524.394642000。slgworld-a.log 在 15:36:40、15:36:56、15:37:12、15:37:28 记录 attempt=1..4 的 server shutdown failed，随后明确记录重试耗尽和未刷数据可能丢失；失败期间均保留 1 个 pending 根。slgworld-a.exit 为 exit status 1，套件通过 SIGTERM 触发，由服务自行退出，未对该进程使用 SIGKILL。

解除 Mongo validator 后，新 owner 从最后已落盘状态接管，generation-2、msgq-a-crashed、msgq-restarted 等客户端场景完成。13 份进程退出记录中，slgworld-a 为预期状态码 1、msgq-a 为预期 signal: killed，其余 11 份均为 0。标准专用范围的最终 Cleanup 成功后套件才返回 passed；summary.redisKeys=11 是清理前采样，不能当作残留。

此结果证明既有有限重试和明确失败退出契约，不承诺重试耗尽后的未刷内存恢复。本轮没有代码或生成物修改，没有新增测试，没有运行 benchmark 或 race。后置 ponytail-review：Lean already. Ship. 复用现有验收入口，仅补证据。六工程 diff 检查通过，全部累计变更未提交。

四项 P0 关键路径及本轮明确列出的关闭分支验收已收敛，不继续按零碎分支增加测试。已接受崩溃窗口不转化为新需求；下一项先评估已知 ProtoActor etcd Provider 竞态的可维护处理路径，是否需要升级或其他改动仍待当前证据评估。


## Provider 关闭竞态处置复核

2026-09-11 使用当前锁定的 protoactor-go `v0.0.0-20240822202345-3c0e61ca19c9` 和真实本机 etcd 重跑已有复现：

```sh
# tx
TX_ETCD_ENDPOINT=127.0.0.1:2379 GOCACHE=/private/tmp/tx-go-build \
  go test -race ./tests -run '^TestActorRuntimeDrainWaitsForQueuedMessages$' \
  -count=1 -timeout=45s -json
```

退出码 1，顶层测试 1 fail、0 pass、0 skip，报告 1 处 DATA RACE。`etcd_provider.go:117` 的 `Provider.Shutdown` 写入 shutdown，与 `etcd_provider.go:168` 的 `keepAliveForever` 读取竞争。原始日志：`/private/tmp/tx-p0-provider-review-20260911/race.jsonl`。例如业务排水完成后，测试清理触发 Runtime.Shutdown，Provider 的后台续租仍可能并发读取关闭标记；功能排水通过不能证明依赖关闭路径无竞争。

处置沿用 [既有 Actor 决策](2026-09-08-actor-runtime-decision.md)：

| 路径 | 本次判断 |
|---|---|
| tx 外层加锁或包装 Shutdown | Provider 内部协程不取得外层锁，公开接口也未提供续租取消并等待退出的能力，不能解决该竞争 |
| 直接升级依赖 | 本次读取的上游 dev 页面仍含普通 shutdown bool；页面样本不代表最新 HEAD 或全部版本，没有可验证的修复候选，不盲目升级 |
| 修改第三方源码或替换 Provider | 超出现有“不增加第三方源码维护、不替换成员发现适配器”的决定；单改 atomic 也不等于完整生命周期已验收 |

上游页面样本：[etcd Provider 源码](https://raw.githubusercontent.com/asynkron/protoactor-go/dev/cluster/clusterproviders/etcd/etcd_provider.go)。本次只更新实施结论与证据，生产、生成物和测试代码新增 0 行，未跑 benchmark；未重跑全量回归。竞态仍未修复，保留为已知依赖风险；本次处置评估结束，当前没有已确认的新编码项。


## 六工程全量回归收尾

在累计变更未提交的当前工作区串行执行全量入口，所有命令退出码均为 0。Go 计数包含子测试，仅统计带 Test 字段的终态；无测试文件的包不计入 skip。

| 工程/模块 | pass | skip | fail | 构建 |
|---|---:|---:|---:|---|
| tx | 1102 | 39 | 0 | 随 go test 编译 |
| txgen | 506 | 0 | 0 | 随 go test 编译 |
| txgame/gen | 1 | 0 | 0 | 随 go test 编译 |
| txgame/server | 235 | 3 | 0 | 随 go test 编译 |
| txgameslg/gen | 1 | 0 | 0 | 随 go test 编译 |
| txgameslg/server | 329 | 5 | 0 | 随 go test 编译 |
| txclient | 79 | 0 | 0 | npm run build 通过 |
| txclientslg | 101 | 0 | 0 | npm run build 通过 |

六个 Go 模块分别在各自 go.mod 目录运行 `GOCACHE=/private/tmp/tx-go-build go test ./... -count=1 -timeout=180s -json`。txgame/server 额外指定 `TXGAME_REDIS_URI='redis://127.0.0.1:6379?db=15'`、`TXGAME_MONGO_URI='mongodb://127.0.0.1:27017/tx_p0_full_20260911'`，使用测试自有键及测试清理逻辑；其余沿用测试默认环境。两客户端分别运行 `npm test` 和 `npm run build`，未修改依赖、输入定义或手工更新生成物。

47 项 Go skip 均为显式开关未开启：tx 39 项涉及 etcd/Remote、Redis 与 Mongo 故障探针；txgame/server 3 项为真实 Mongo 号段及两项 Actor 应用测试；txgameslg/server 5 项为真实 Mongo 账号/号段/建角、Redis 整数及关闭刷盘。不能把默认全量命令退出成功当作这些场景本次通过，也不以此前定向证据填充本次计数。例如 tx 的 TestActorRemoteCallLifecycle 本次 skip，此前通过的记录仍单独保留。

原始日志、全部跳过测试名称及 Go 统计在 `/private/tmp/tx-p0-full-regression-20260911`（各模块 JSONL、客户端 test/build.log、summary.json）。本次未运行 race、集群 E2E 或 benchmark，不改变上一节已复现的 Provider 竞态结论。仅补回归证据，生产、生成物与测试代码变化 0 行；当前没有已确认的新编码项，不自动为跳过项新建测试或恢复微优化。


## 默认跳过项的真实依赖补验

确认六工程工作区干净后，读取上一节 JSONL 中全部 47 项 skip，以精确测试名白名单启用对应环境开关，串行运行模块和包（`go test -p=1 <packages> -run '^(<测试名白名单>)$' -count=1 -timeout=180s -json`）。逐项比对选中名单和 pass 事件，无遗漏；没有新增测试或修改生产实现。

| 模块 | 选中顶层测试 | 顶层 pass | 含子测试 pass | skip/fail | 退出码 |
|---|---:|---:|---:|---|---:|
| txgameslg/server | 5 | 5 | 7 | 0/0 | 0 |
| txgame/server | 3 | 3 | 3 | 0/0 | 0 |
| tx | 39 | 39 | 46 | 0/0 | 0 |

本次启用 `TX_ETCD_ENDPOINT=127.0.0.1:2379`、`TX_REDIS_ENDPOINT=127.0.0.1:6379`、`TX_MONGO_URI=mongodb://127.0.0.1:27017`、`TX_MONGO_LATE_WRITE_ADDRESS=127.0.0.1:27017`、`TXGAME_LOGIN_MONGO_TEST_URI=mongodb://127.0.0.1:27017`、`TXGAMESLG_LOGIN_MONGO_TEST_URI=mongodb://127.0.0.1:27017`、`TX_WRITEBACK_IO=1` 和 `TXGAMESLG_SHUTDOWN_TEST=1`，统一使用 `GOCACHE=/private/tmp/tx-go-build`。测试沿用专属库、键或前缀及现有清理逻辑，不执行全 Redis 库清理。

补验覆盖真实账号唯一身份、号段并发分配、建角恢复、Redis 整数精度、关闭/周期刷盘，以及 etcd 租约与发现、Remote 排水、Redis Session/任务和 Mongo 迟到写。例如角色已落库但账号投影缺失时，建角重试补齐投影并保留已存名称；仅修改名字或一个道具时，其他字段的正负安全整数边界值在 Redis 和 Mongo 中保持一致。

原始日志、精确命令和测试白名单（各模块 `*-command.json`）、逐模块计数在 `/private/tmp/tx-p0-enabled-regression-20260911`。总计 47 个顶层测试、含子测试 56 项通过；这是单独补验结果，不改写上一节默认运行的 47 skip。未运行 race、benchmark 或新一轮集群 E2E，已知 Provider 关闭竞态仍未修复。

本次仅更新验证状态与提交基线。既定 P0 验收及默认跳过项已收尾，下一项仍需具体需求、缺陷或性能负载依据，不自动进入待决策能力建设。
