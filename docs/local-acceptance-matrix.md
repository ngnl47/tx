# 本地整体验收矩阵

本页以现行契约组织验收，不重复维护测试实现。框架异常边界在 tx 定向测试中构造，两组应用使用已有真实集群入口验证接入；不向业务 Handler 增加故障开关。历史报告只证明对应构建，本轮结果单独记录。

## 2026-11-03 手写结构体组织整理

本轮只调整 tx、txgen 和两组应用手写 Go 结构体的职责分组、锁邻接和必要组注释。依据[字段组织约定](architecture_audit_standard.md#结构体字段组织)读取实际使用关系；例如事务锁条目的引用数由外层分片锁保护，EntityCache 的冷加载、唯一对象目录和回收目录分别同步，World 分区内容由网格邮箱独占。没有修改这些同步机制。

| 核验项 | 结果 |
| --- | --- |
| 语法等价 | 对四仓全部 143 个改动 Go 文件与 HEAD 比较；忽略注释、位置及 struct 字段顺序后语法树一致，方法、字段类型、标签与字面量保持不变 |
| 字段移动 | 核对相关类型的位置字面量、unsafe/固定偏移及序列化使用；被调整的运行时或测试辅助体无此类顺序依赖，协议/GD DTO 不重排 |
| 生成边界 | tx protobuf、txgen 模板/输出、两组 server/gen 及脚手架输入均无差异，未运行生成器来制造格式变更 |
| tx | `GOCACHE=/private/tmp/tx-go-cache go test ./...` 和 `go vet ./...` 通过 |
| txgen | 同缓存配置的 `go test ./...` 通过，生成契约包按既有测试编译 |
| txgame、txgameslg | 各自 server 与 gen module 的 `go test ./...` 通过 |
| 格式与差异 | 四仓 `gofmt -l` 无输出，`git diff --check` 通过 |

应用首次沙箱回归中临时回环监听受限，经授权重跑；最终两组 server 使用最终 tx 工作区再次全包回归通过。最终 tx、txgame/server、txgameslg/server 测试日志及 tx vet、语法核验记录保存在 `/private/tmp/tx-struct-style-20261103/`；txgen 和两个 gen module 的结果已在本轮工具执行中核对。

本轮不新增格式断言测试或正式运维探针；不运行 race、外部存储专项、真实集群或云压测，不把条件跳过的依赖用例计为存储验收，也不以分组声称性能改善。独立 ponytail-review 已完成，新增锁、Once、协程所有权注释及字段移动均未发现待修问题；本轮不提交。

## 2026-11-03 模块边界后续修复

本轮修改 tx 和两组 server 的实际接入，保留此前验收工具工作区变更，不提交。环境为 macOS/amd64、Go 1.27.0，构建缓存使用 `/private/tmp/tx-go-cache`。

| 边界 | 验证结果 |
| --- | --- |
| MsgQ 共享建流 | Mailbox/Publish 并发请求只建一条流；各调用独立取消等待，Close 取消共享建流，迟到结果清理且不复用 |
| 收尾期限 | 无调用方期限时，Service Flush、CMod Drain、Service Stop 仍使用配置期限；未完成阶段保留依赖供重试，已完成阶段不重复 |
| 初始化失败回滚 | 最终错误跨重试保留，已结束的 Stop 不重复；任务未退出、其他服务未完成时继续保留依赖；最终失败状态为 Stopped 并保留 Failure |
| 实例退役 | 有效目录确认实例消失或换址后回收；状态变化与 Watch 失联不误关连接；慢 Close 不阻塞 Watch，缓存 Close 等待观察任务退出；通过正式只读目录视图验证接线 |
| Agent 控制广播 | 使用项目注册名称选择全部 Running 实例，不固定为 agent |
| GD 直接保存 | 框架统一返回 `txruntime.ErrProcedureWrite`；结束后的事务仍为 `txn.ErrInactive`，两组 db.Save 不重复守卫，SaveChange 保留必要检查 |
| 应用 Stop 接入 | Agent 连接、Room 通知、World 加载/保存、Sched 在途任务及管理 HTTP 未完成时明确 deferred；已退出的调度记账错误保持最终结果 |

tx `go test ./...`、txgame/server 与 txgameslg/server `go test ./...` 最终均通过，日志为 `/private/tmp/{tx,txgame,txgameslg}-boundary-complete-full-20261103.log`；tx `go vet ./...` 通过，日志 `tx-boundary-complete-vet-20261103.log`。实例缓存、Discovery、MsgQ、Agent 控制、保存契约定向三轮 race，框架关闭与回滚三轮 race，真实 TCP/WS Agent 回滚三轮 race，以及两组 db、Room/Sched、World 目标 race 均通过。三仓 `git diff --check` 通过；后置审查发现的回滚问题已修复并只读复核。

首次全量验证发现槽位刷新测试依赖 60ms/100ms 的真实调度间隔，机器繁忙时把正常重查误判为相互唤醒；改用 `testing/synctest` 后十轮 race 通过，生产轮询不变。后续完整回归发现内建 Agent 的未完成关闭仍返回普通 DeadlineExceeded，已补齐 deferred 标记后重跑通过。旧回滚实现通过 Go overlay 复现 Stop 被重复调用，日志 `tx-service-rollback-red-20261103.log`；关闭已结束仍显示 Draining 的失败记录为 `tx-service-phase-red-20261103.log`，两者修复后的定向证据为 `tx-boundary-framework-race-20261103.log`。

本轮未运行正式包真实集群、云压测或单独启用外部数据库专项；依赖环境缺失的既有用例按原条件跳过，不计为实际存储验收。取消表示取消等待或发出协作信号，不证明无视 ctx 的传输替身已退出；迟到结果另行清理。未测云端吞吐，不宣称热路径性能改善。

## 2026-11-03 普通正式包会话服务生命周期

两组新增 `--session-lifecycle`，复用程序包、进程控制和真实客户端，均不带 `txbench`。完整范围、包摘要与证据见[验收报告](session-service-lifecycle-acceptance-20261103.md)。

| 场景 | 结果 | 边界 |
| --- | --- | --- |
| Login 逐台替换 | 两组存活及替换节点签票、原 TCP/WS 连接保持；探针各 11 个 SessionID 无重复 | 有限请求，不推算持续签票容量 |
| Agent 在线停止与恢复 | 两种传输恢复原 Session，ConnectionID 换新、MsgQ owner 保持，离线通知接续；新登录进入替换实例 | 不重投结果未知的上行 |
| MsgQ 正常停止与新登录 | 旧角色租约释放、原 Session 明确恢复拒绝；同 Role 新 Session、新 owner/generation；存活会话不变 | 旧 Session Redis 记录可留至 TTL，不迁移邮箱 |
| SLG 业务归属 | 10 阶段实际核对 Game 分组与 World 槽位，owner/generation 不变 | 不改变有状态业务的停服发布策略 |
| 收尾 | 最终 24 个服务进程全部退出 0，专用 Redis/Mongo/etcd 与匹配进程无残留 | 正常关闭；未模拟宕机或写回失败 |

Go 定向回归、客户端 118/129 项测试与构建通过。修复了验收工具的终阶段退出、延迟日志观察竞态并重新运行两组；无框架运行时代码修改。本轮不跑云测试、不提交，不将有限步骤全部通过等同于 99.99% 可用率证明。

## 2026-11-03 当前构建正式包验收

tx `84f095e`、txgame `3261fb8`、txgameslg `8753304`，复用正式打包和现有客户端入口，三组顺序运行。无运行时代码修改，不跑云测试、不提交。完整指标、原始路径和限制见[本轮报告](runtime-boundary-rollout-20261103.md)，机器结果见[验收记录](validation/runtime-boundary-rollout-20261103.json)。

| 场景 | 实际验证 | 结果与限制 |
| --- | --- | --- |
| txgame 在线 A→B→A | 50 TCP 连接×5/s，持续 180 秒；四步交接、配置生效、背包连续性与坏包拒绝 | 44,997/45,000，99.993333%，未发送 0；3 次交接期失败保留，子窗口不各自满足 99.99%；未等待正式 Fusion 600 秒最终 Mongo 回写 |
| SLG Role 停服 A→B→A | 4 并发，各 180 秒；每轮最终名称 Mongo 回读 | 共 1,085,930 次，每轮计数均保留在报告，三轮零失败；摘要仅保留末轮存储计数，Redis 仅核对键存在 |
| SLG World 停服 A→B→A | 两个指定 World，4 并发，各 180 秒；最后全停后 Mongo 单位坐标回读 | 共 3,295,181 次，三轮零失败；4 个单位/4 桶、文档版本合计 21；不证明重启后的首次只读结果 |
| World 存档重载补验 | `TX_MONGO_URI` 启用 `TestOwnedBucketsMongoRoundTrip` | 真实 Mongo 通过、未跳过；新写入前断言恢复旧坐标，属于运行时重建而非正式包跨进程首次读回 |
| 归属与收尾 | etcd 每 10 秒只读采样、退出记录与独立 ps、专用数据检查 | SLG 各槽位稳定阶段 1→3→5 且 owner 更新；45 次正常退出、2 次预期坏包拒绝，无对应存活进程；Redis DB12/13、两专用 Mongo 库和 etcd 前缀已清理 |

SLG 停服发布步骤单列，不冒充在线滚动；七个 180 秒测量窗口均达到 99.99%。txgame 另有一次业务探针角色的旧绑定下行拒绝，后续业务断言通过，保留在报告；不声称零错误日志。本轮不替代此前定向故障和 race 回归，不据此推算云端吞吐。

## 2026-11-02 剩余三项 P1 边界

本批只修改 tx，保留此前 EntityCache 与 DelayQueue 等未提交工作。txgen、生成协议与两组应用业务实现不需要迁移。环境为 macOS/amd64、Go 1.27.0，构建缓存为 `/private/tmp/tx-delayqueue-go-cache`；没有启动云压测或整轮真实集群。

先复现的三个入口与修复：

- **内建 CMod 启动回滚**：真实 Fusion scheduler 领取既有任务后阻塞，后续 CMod 启动失败；旧接线未等 worker 就关闭存储。改用所属 CMod Tasks 后，超时保留连接，worker 实际退出后显式重试清理成功。测试使用官方 Redis/Mongo 客户端及真实 CMod、Host、Fusion 调用链，只以本地协议 fixture 替换数据库 I/O，不等同于真实数据库最终写回验收。红测为 `/private/tmp/tx-cmod-storage-red.log`，定向十轮及五轮 race 为 `tx-cmod-storage-repeat.log`、`tx-cmod-storage-race.log`。
- **共享流操作取消**：旧实现不能中断发送锁等待、实际 Send 或首次开流；红测 `/private/tmp/tx-rpc-cancel-red-20261102.log`。修复覆盖 Tell/Call、健康流的等待取消、完成后迟到取消、并发开流等待与 Close；bufconn 使用真实 HTTP/2 小窗口、不消费服务端流，证实流控阻塞能按期限解除。发送后等待 Call 结果的逐项取消、Tell 无回执、原有顺序与断流不重投用例继续保留。
- **Procedure 根回滚**：正式 Runtime.Call 普通错误曾在持锁且 active 时执行回调。修复后，错误和 panic 均在根清理后回调；子 savepoint 仍即时持锁回调。另补子回调 panic 不重复执行，及根回调 panic 不二次解绑后续事务记录器的异常边界。证据为 `/private/tmp/tx-rollback-red.log`、`tx-rollback-callback-red.log` 及本批后置审查补验日志。

已完成的基础回归：

| 验证 | 结果 / 本地日志 |
| --- | --- |
| tx `go test ./...` | 通过；`/private/tmp/tx-remaining-p1-final-all-20261102.log` |
| txgame/server `go test ./...` | 通过；`/private/tmp/txgame-remaining-p1-final-all-20261102.log` |
| txgameslg/server `go test ./...` | 通过；`/private/tmp/txgameslg-remaining-p1-final-all-20261102.log` |
| rpc、actorrpc、agent、framework、txn、tests 六包 race | 通过；`/private/tmp/tx-remaining-p1-race-20261102.log` |
| 新增流取消用例十轮 race | 通过；`/private/tmp/tx-rpc-cancel-race-20261102.log` |
| 最终 RPC 整包三轮 race | 通过；`/private/tmp/tx-rpc-final-race-20261102.log` |
| 根回调 panic 补验 | txn 整包及相关 tests race 通过；`/private/tmp/tx-rollback-reentry-package-race.log`、`tx-rollback-reentry-tests-race.log` |
| tx `go vet ./...` | 通过；`/private/tmp/tx-remaining-p1-final-vet-20261102.log` |

测试需回环端口的部分在授权的本机测试环境执行；外部依赖测试沿用原有显式启用条件，默认跳过不计为实际依赖验收。后置审查发现的根回调 panic 在旧实现下稳定导致后续事务修改不回滚（测试值为 2），修复后恢复为 0，保留原 panic 与 OnFinish；红测 `/private/tmp/tx-rollback-reentry-red.log`。补齐后独立 ponytail-review 复核无剩余必修项，`git diff --check` 通过。

发送热路径对照使用 `GOMAXPROCS=4`、128 字节载荷、`-benchmem -benchtime=1s -count=3`；基线为 HEAD 原 Sender，实现差异之外输入一致：

| 路径 | 修改前 ns/op | 最终 ns/op | 分配 |
| --- | --- | --- | --- |
| 无阻塞传输替身，仅观察 Sender | 183.6～186.5 | 423.4～435.8 | 均为 240 B/op、3 allocs/op |
| 真实 gRPC bufconn 发送/接收 | 1260～1385 | 1482～1601 | 均为 19 allocs/op；1322～1338 B/op |

日志为 `/private/tmp/tx-rpc-send-before-matched-20261102.log`、`tx-rpc-send-final-20261102.log`。每条共享流复用一个取消监视协程，正常逐消息发送不增加 goroutine 或取消回调分配；多出的可取消串行准入与状态核对有实测时间成本。此对照验证保护成本，不能当成真实网络、端到端吞吐或生产容量结论。未扩大到其他 MsgQ 专用流的统一重构。

## 2026-11-02 业务本地 DelayQueue

仅修改 tx 的 `util.DelayQueue`、对应测试与文档，保留工作区此前的其他未提交变更。按用户确认保留业务工具职责，不接入 CMod 或持久化；环境为 macOS/amd64、Go 1.27.0，构建缓存为 `/private/tmp/tx-delayqueue-go-cache`。

- 修复前：100ms 周期、20ms 回调在前 60ms 执行了 3 次；11,000 个同时到期任务中，首个回调新增任务触发持锁投递与加锁登记互等，三秒测试超时并给出对应阻塞栈。
- 修复后：按到期时间串行执行、相同期限顺序、回调完成后间隔续排、原 TaskID 取消、自取消、积压时回调新增、同批次待执行任务取消、并发 Stop 等待和停止后拒绝任务均通过。
- `go test -race ./util -count=20 -timeout=60s` 通过，日志 `/private/tmp/tx-delayqueue-race-20261102.log`。首次 race 发现测试读取时间记录与后续回调写入缺少同步，改用 channel 传递观测值后重跑通过，不将首次结果记为通过。
- `go test ./...` 通过，日志 `/private/tmp/tx-delayqueue-all-local-20261102.log`。首次沙箱执行被本机端口监听权限阻止，原日志 `/private/tmp/tx-delayqueue-all-20261102.log`；允许本机测试监听后重跑通过。
- `go vet ./util` 与 `git diff --check` 通过；独立差异 review 无待修项。

不测实时调度精度或生产容量，不运行云压测和真实集群。Stop 等待回调实际结束，不强制中断，不能从本队列回调内同步调用；终态队列不支持重启。使用边界见[设计文档](design.md#业务本地有序调度)。

## 2026-11-02 能力收窄与共享冷加载取消

基线为 tx `241f460`、txgame `4ba3b64`、txgameslg `8753304`。生产修改仅在 tx；txgame 修正 AGENTS 中将业务对象归属通信 Actor 激活的旧描述。txgen、两组生成产物和客户端无变化。环境为 macOS/amd64、Go 1.27.0，使用 `GOTOOLCHAIN=local` 与 `/private/tmp/tx-codex-go-build`。

两处权限用例先证实旧行为：缓存业务视图可断言为创建/删除预留 SPI，Placement 的准入视图可断言为共享闸门管理接口。修复后仅所有者保留这些能力；正式创建/删除、宿主排水及已接纳工作计数继续通过。

冷加载先复现两个失败：首请求取消使仍有效的等待者收到 `context canceled`；后加入的请求取消后仍阻塞等待 Loader。原始失败见 `/private/tmp/tx-entity-cancel-repro-20261102.log`。修复后同键由宿主共享一次独立期限加载，每个调用者只取消自身等待；测试覆盖 Provider 能力值保留、事务/使用期屏蔽、关闭超时后继续等待、加载期限到达后保留占位直至实际退出，以及 Loader panic 不残留占位。创建查重不改为共享后台任务。

| 验证 | 结果 / 本地日志 |
| --- | --- |
| 权限收窄及创建/删除回归 | 通过；`/private/tmp/tx-permission-storage-20261102.log`、`tx-permission-race-20261102.log` |
| tx `go test ./...` | 通过；`/private/tmp/tx-entity-cancel-all-20261102.log` |
| txgame/server、txgameslg/server `go test ./...` | 通过；`/private/tmp/txgame-entity-cancel-all-20261102.log`、`txgameslg-entity-cancel-all-20261102.log` |
| `go test -race ./internal/framework ./modules/entitycache ./modules/txn ./tests` | 通过；`/private/tmp/tx-entity-boundaries-race-20261102.log` |
| EntityCache 全包 `-race -count=5` | 通过；`/private/tmp/tx-entity-cancel-race-fixed-20261102.log` |
| tx `go vet ./...` | 通过；`/private/tmp/tx-entity-boundaries-vet-20261102.log` |

新增超时用例首次 race 暴露了测试计数变量的读写竞争：超时本身不与 Loader 完成建立同步。计数改用原子操作后，重新执行上述五轮 race、关键包 race 和全量回归通过；保留原失败日志 `/private/tmp/tx-entity-cancel-race-20261102.log`，不将其计为通过。

相同机器、参数 `-run '^$' -bench 'BenchmarkCache(Hit|ParallelHit)$' -count=3` 的热路径对照：

| 模式 | 修改前 ns/op | 修改后 ns/op | 分配（前后） |
| --- | --- | --- | --- |
| 禁用空闲过期的命中 | 71.16～72.50 | 60.21～62.82 | 0 B/op、0 allocs/op |
| 启用空闲过期的命中 | 234.0～235.1 | 217.9～223.2 | 0 B/op、0 allocs/op |
| 并发命中 | 176.4～191.8 | 167.2～173.9 | 0 B/op、0 allocs/op |

日志为 `/private/tmp/tx-entity-cancel-before-bench-20261102.log`、`tx-entity-cancel-after-bench-20261102.log`。热命中没有新增任务或上下文包装；短时观测未见回退，不据此宣称集群吞吐提升。独立差异 review 无待修项。未显式启用可选外部设施集成场景，未跑真实多进程集群或云压测；本轮不提交。

## 2026-11-02 RPC 调用命名统一

延续上一批工作区，统一发送侧命名：Agent 使用 `actor.Teller.Tell`；低层 gRPC 使用 `TellStream` 和等待接收回执的 `TellAccepted`。`Messenger` 嵌入同一个 Teller，不新增业务调用模式。传输、线协议、路由、流控、排水、错误及重投边界保持不变；接收侧 Deliver/SubmitStream、普通邮箱和 Procedure 不机械改名。两组应用和文档同步；txgen 模板及生成 GoDoc 已满足调用模式说明，无需重新生成。

增强现有 `TestTellStreamReturnsBeforeAdmissionWithoutPerItemResponse`：服务端暂不接纳首条消息，发送端三条单向投递仍能返回；解除阻塞后核对三条顺序和零逐项回执。`TestDirectStreamTellPreservesMailboxOrderAndDrain` 继续覆盖邮箱有序及已接纳工作的排水。

| 验证 | 结果 | 本地日志 |
| --- | --- | --- |
| tx `go test ./...` | 通过 | `/private/tmp/tx-tell-naming-full-local-20261102.log` |
| txgame/server `go test ./...` | 通过 | `/private/tmp/txgame-tell-naming-full-local-20261102.log` |
| txgameslg/server `go test ./...` | 通过 | `/private/tmp/txgameslg-tell-naming-full-local-20261102.log` |
| tx `go test -race ./modules/agent ./internal/actorrpc ./internal/framework ./modules/rpc ./bee ./ant` | 通过 | `/private/tmp/tx-tell-naming-race-20261102.log` |
| tx `go vet ./...` | 通过 | `/private/tmp/tx-tell-naming-vet-20261102.log` |

初次默认沙箱运行因禁止 loopback 监听失败，放行本地监听后按原入口重新执行通过；不能将首轮失败计为通过。未显式启用外部依赖集成测试，未跑云压测或整轮集群。三仓差异检查及独立契约 review 无待修项；本轮未提交。

## 2026-11-02 能力契约与懒装配收敛

基线为 tx `6e57e4e`、txgame `569f7e2`、txgameslg `cf72bd1`，保留本轮之前已有的模块组合与 Mongo 原生句柄文档修改。Bee/Ant 门面政策及 L4 注册适配允许独立实现，不抽取公共政策层；其底层连接和运行时仍由既有装配内核统一管理。

本批修复标准宿主首次共享 Create 未初始化 EntityCache/WriteBack 关联的缺口，Agent 公开直连接入改为单向投递能力（现名 `actor.Teller`），并复用 Discovery 已装配的 SlotOwnership。`DispatchTimeout` 只覆盖目标解析后的投递，默认期限仍为 5 秒；正常 Actor Call 和底层同步直连协议不变。低层 RoutedCaller 的可选传输能力检查保留，不为 Agent 接口收敛强制所有同步调用器实现 Tell。

先复现再修复：新增 `TestRuntimeFirstCreateInitializesEntityCache` 通过正式 Ant 宿主取得 Provider，没有预先查询缓存或手动绑定 resolver；修改前提交、回滚两支都报 `writeback: creation cache is not bound: RuntimeTest`，修改后首次创建、提交可见性、回滚无残留及写回均通过。Agent 复用既有分发测试，移除测试桩 Call 后旧接口因缺少 Call 编译失败，改为最小单向投递接口后通过，不新增协议或测试专用生产入口。

环境为 macOS/amd64、Go 1.27.0，使用 `GOTOOLCHAIN=local`、`GOCACHE=/private/tmp/tx-codex-go-build`：

| 验证 | 结果 / 日志 |
| --- | --- |
| tx `go test ./...` | 通过；`/private/tmp/tx-capability-full-unrestricted-20261102.log` |
| txgame/server `go test ./...` | 通过；`/private/tmp/txgame-capability-full-unrestricted-20261102.log` |
| txgameslg/server `go test ./...` | 通过；`/private/tmp/txgameslg-capability-full-unrestricted-20261102.log` |
| tx `go vet ./...` | 通过；`/private/tmp/tx-capability-vet-20261102.log` |
| Agent、framework、actorrpc、discovery、Bee、Ant `go test -race -count=1` | 通过；`/private/tmp/tx-capability-race-20261102.log` |
| 首次 Create 与既有创建/查询/回滚/删除/重建用例 `-race -count=1` | 通过；`/private/tmp/tx-capability-create-race-20261102.log` |

首次全量运行因沙箱禁止本地 TCP/HTTP 监听失败，原始日志为上述三个全量日志去掉 `-unrestricted` 的版本；允许本地临时测试端口后重新运行通过，不修改测试规避监听。本轮未显式启用真实 Redis/Mongo/etcd/NATS 依赖，相关可选用例沿现有规则跳过；没有重新跑正式包集群、云压测或吞吐基准，不外推性能结论。生成器与客户端无变更，不重新生成协议。

独立 ponytail-review/契约审查未发现本轮代码阻塞项，发现 API 文档继续描述旧的逐请求 10ms 轮询；已改为引用现行同服务池/槽位共享刷新契约。三仓 `git diff --check` 通过，本轮新增/修改行中的本地文档链接路径已核对。全部修改未提交。

## 2026-11-02 vet 告警收敛

在上一批 Reader/元数据变更之上完成，仍未提交。先执行 `go vet ./...`，复现 5 条告警：MsgQ 发布时整值复制 protobuf 内部状态，Sender 夹具三个值接收器复制互斥锁，槽位夹具使用未具名字段。

生产路径只调整 `grpcPublishStream.send` 的消息复制：为本进程构造的请求新建 protobuf 消息，显式复制寻址字段和独立 Payload，不复制内部状态。私有入口不转发外部解码后的未知字段；公共 SendRole/SendSession、协议、队列准入、投递结果及关闭语义不变。没有增加深复制通用层或公共选项。既有准入测试补充寻址字段及提交后修改的隔离断言。

验证环境为 macOS/amd64、Go 1.27.0，`GOTOOLCHAIN=local`、`GOCACHE=/private/tmp/tx-codex-go-build`：

| 验证 | 结果 / 日志 |
| --- | --- |
| 全量 `go vet ./...` | 修改前退出 1、修改后退出 0；`/private/tmp/tx-vet-before-20261102.log`、`tx-vet-after-20261102.log` |
| tx `go test ./...` | 通过；`/private/tmp/tx-vet-full-20261102.log` |
| `go test -race ./modules/msgq -count=1` | 通过；`/private/tmp/tx-vet-msgq-race-20261102.log` |
| Sender 夹具定向普通及 race | 通过；`/private/tmp/tx-vet-fixture-msgq-test.log`、`tx-vet-fixture-msgq-race.log` |
| `TestStopReleasesOnlyAfterServiceBackgroundQueueExits` | 显式连接本机 etcd/NATS，普通与 race 均通过且未跳过；`/private/tmp/tx-vet-fixture-slot-test.log`、`tx-vet-fixture-slot-race.log` |

新增的 `BenchmarkPublishStreamLocalAdmission` 只测 128 字节载荷的本地入队/取出与复制，不包含网络。相同机器和参数 `-benchtime=500ms -count=3 -benchmem`：修改前 264.4～272.7 ns/op，修改后 274.9～283.8 ns/op，均为 256 B/op、2 allocs/op。日志为 `/private/tmp/tx-vet-publish-before.log`、`tx-vet-publish-after.log`；这是短时微基准观测，不宣称集群吞吐改善或统计意义上的性能等价。

本轮四个代码/测试文件的独立差异 review 无待修项，`git diff --check` 通过。本批不修改生成器、应用接口或客户端产物；未重复两组应用全量回归、正式包集群和云压测。普通 tx 全量测试仍按既有规则跳过未配置的可选依赖；明确启用的外部依赖验证只有上述本机槽位测试。

## 2026-11-02 Reader 与元数据装配收敛

基线为 tx `b5bf39d`、txgen `9123dc7`、txgame `6124559`、txgameslg `5ac1a0e`，叠加本轮未提交变更。环境为 macOS/amd64、Go 1.27.0，统一使用 `GOTOOLCHAIN=local` 和 `/private/tmp/tx-codex-go-build` 构建缓存。

| 契约 | 本轮验证 |
| --- | --- |
| 默认查询唯一资源来源 | 缺 Provider 的单条/批量 Find 返回错误；不同 ctx 的 Provider 分别读取自己的后端，没有全局 Reader 回退 |
| 缓存准入一致 | 修复前热 Find/Snapshot、SnapshotCached、TryRelease 可绕过缺 Provider；修复后现有 CheckAccess 返回错误，冷/热、事务快照、创建、管理路径均不额外加载、复制、回收或泄漏使用权 |
| 显式存储读取 | Load 直接使用显式 Reader；本机 Redis/Mongo 验证生成 Role 的 Fusion 热写、读取、延迟落库、卸载热数据后回源，无跳过 |
| 元数据与资源分离 | GeneratedData 仅登记 meta.TypeRegistry；Fusion 不再要求 Reader 绑定，仍校验缺失元数据和持久化参数 |
| SLG 事务接入 | 创建夹具改用测试 Provider，保留唯一缓存；提交、回滚、失败写回/排水、并发创建和子事务回滚后重建通过 race |
| 生成与应用 | 两组正式 gen/run.sh 再生成；完整生成包与 server/gen 测试通过；txclient、txclientslg 工作区无变化 |

实际验证命令与本机日志：

- tx：`go test ./...`，`/private/tmp/tx-reader-full-final.log`；新增元数据/缓存准入关键测试 `-race`，`/private/tmp/tx-reader-race.log`。
- txgen：`go run .` 后 `go test ./... -count=1`，`/private/tmp/txgen-reader-full.log`；包括完整 `tests/output` 与临时 EntityCache 开/关生成包。修复前缺 Provider 失败证据为 `/private/tmp/txgen-reader-red.log`。
- txgame、txgameslg：`GO_BIN=go bash gen/run.sh generate`，各自 `server/` 和 `gen/` 下 `go test ./...`。日志为 `/private/tmp/<工程>-reader-generate.log`、`/private/tmp/<工程>-server-reader-full.log`、`/private/tmp/<工程>-gen-reader-full.log`。
- txgame/server：显式指定本地 debug 配置，`go test ./tests/integration -run '^TestGeneratedRoleFusionRoundTrip$' -count=1 -v`，`/private/tmp/txgame-reader-fusion.log`。只写入并清理测试独有记录，不清理数据库。
- txgameslg/server：`go test -race ./internal/db -run '^(TestProcedureCreateGeneratedRootCommitRollbackAndDrain|TestConcurrentCreateUsesOneCachedRoot|TestCreateAfterChildRollbackRestoresUndoBinding)$' -count=1 -v`，`/private/tmp/txgameslg-reader-create-race.log`。日志中的 offline 是写回失败注入，不是遗漏的测试失败。

四仓 `git diff --check` 与独立差异 review 通过；本轮只修正职责链和资源选择，没有新增配置、包装查询入口或 CMod。普通全量测试保留既有可选依赖跳过，不能视为所有外部集成场景都执行；本轮明确启用的真实存储验证仅上述 Fusion 往返。未运行云测试、正式包多进程集群或吞吐压测，不将此前能力检查微基准作为本轮性能对比。

## 2026-11-02 模块边界修复回归

基线为 tx `9870690`，本批变更未提交。环境为 macOS/amd64、Go 1.27.0；只跑本地包级回归，不运行云测试或正式包多进程集群，不额外启用可选外部依赖场景。

| 边界 | 实际覆盖 |
| --- | --- |
| Agent 监听异常 | TCP/WebSocket 非预期退出上报 Server；正常 Drain/Stop 不误报；启动中上报失败终止启动 |
| Agent 连接退出 | TCP/WebSocket 的身份校验、Acquire、Init 阻塞时，Stop 超时保留 router；实际退出后才能关闭依赖；父取消和后续服务 Start 失败会关闭监听器并等待连接 |
| 业务排水 | 已接纳的消息继续执行；空闲连接不计入业务消息排水，但其连接工作必须在资源释放前退出 |
| 共享资源 | EntityCache 访问视图、Redis Cmdable 和 gRPC ServiceRegistrar 均不暴露底层共享资源的关闭方法；原操作与协议注册仍可使用 |
| 缓存声明 | 冷热 Snapshot/Find、事务创建/删除和管理快照/回收检查当前 Provider；删除预留遇到授权 panic 也会 Abort；检查不构建 Reader 或发起存储 I/O |
| 启动顺序 | Procedure 在已选 Redis/Mongo/Fusion 后启动；弱依赖不启用未选存储 |

修复前已分别复现监听异常未传播、连接未退出便关闭依赖、权限检查随缓存冷热变化、删除预留泄漏及 Redis 启动顺序缺失。红灯证据位于 `/private/tmp/tx-agent-*-red.log`、`/private/tmp/entitycache-*-red.log`、`/private/tmp/tx-boundary-views-red-20261102.log`。最终结果：

| 入口 | 结果与日志 |
| --- | --- |
| tx `go test ./...` | 通过；`/private/tmp/tx-boundary-full-20261102.log` |
| txgen `go test ./...` | 通过；`/private/tmp/txgen-entitycache-all-green.log`，包括生成包编译与权限契约 |
| 两组应用 `bash gen/run.sh generate` | 通过；`/private/tmp/txgame-boundary-generate-20261102.log`、`/private/tmp/txgameslg-boundary-generate-20261102.log` |
| txgame/txgameslg 的 server 与 gen `go test ./...` | 均通过；`/private/tmp/txgame-boundary-full-20261102.log`、`/private/tmp/txgameslg-boundary-full-20261102.log`、`/private/tmp/txgame-gen-boundary-20261102.log`、`/private/tmp/txgameslg-gen-boundary-20261102.log` |
| tx 相关包 `go test -race -count=1` | Agent 全子包、EntityCache、WriteBack、RPC、Redis 适配、framework、tests 通过；`/private/tmp/tx-boundary-race-20261102.log` |
| Agent 最终异常分支 `-race -count=3` | 通过；`/private/tmp/tx-agent-final-race.log` |
| 独立差异 review | 未发现新的必修项；没有把未运行的真实集群或云端场景算作通过 |

缓存能力检查三次微基准为 61.06/80.55/70.09 ns/op，0 B/op、0 allocs/op。已有受管实体基准中，Snapshot 从 1.601～1.688 µs/op 到 1.586～1.903 µs/op，仍为 449 B/op、14 allocs/op；Procedure 从 4.278～4.454 µs/op 到 4.193～4.386 µs/op，仍为 2722 B/op、36 allocs/op。记录见 `/private/tmp/entitycache-capability-benchmark.log`、`/private/tmp/entitycache-benchmark-before.log`、`/private/tmp/entitycache-benchmark-after.log`。同机存在构建噪声，这些数据仅支持没有新增分配，不宣称集群吞吐提高或 P99 改善。

API 调整：自定义 `txruntime.Provider` 实现需补 `RequireStorage(mode)`；自定义缓存 Provider 返回 `*entitycache.Access`。标准应用装配自动对接，已重新生成两组工程；业务 Find/Snapshot 和缓存管理调用方式不变。txgame 未启用 EntityCache，生成结果无差异；txgameslg 的缓存描述增加访问检查；两组客户端无变更。

额外 `go vet ./...` 仍有 5 条既有告警：MsgQ protobuf 消息值复制、测试含锁值接收器和槽位测试无字段名结构字面量，见 `/private/tmp/tx-boundary-vet-20261102.log`。没有将普通回归通过写成 vet 通过，也没有扩大本批修复范围。

## 验收分层

```text
框架边界：关闭 / 取消 / 超阈值 → 状态与资源断言、race
    → 真实依赖：NATS + etcd → 归属交接、命中缓存复用
    → 应用集群：正式 Login / TCP / WebSocket / Actor / MsgQ
        → 业务返回、存储回读、故障恢复、正常排水分别核验
```

| 编号 | 范围 | 通过条件 | 复用入口与证据 |
| --- | --- | --- | --- |
| F1 | 服务后台任务关闭重试 | Stop / PrepareHandoff 等待遵守调用方或配置期限；未退出不释放资源；实际退出后重试仅关闭一次 | `internal/framework/TestServiceTaskShutdownDeadlineRetainsResources` |
| F2 | 共享冷查询取消 | 首个与后续请求均可独立取消；不取消其他等待者的共享解析；成功结果可缓存 | `internal/actorplacement/TestFirstWaiterCancellationDoesNotCancelSharedQuery`、`TestSharedQueryWaiterCancelsIndependently` |
| F3 | 查询期限与实际工作 | 三秒到期只结束等待；底层未退出不得删除在途登记、重复启动查询或提前释放依赖 | `TestQueryDeadlineDoesNotForgetBlockedWork`、`TestCanceledSlotLoadStillBelongsToLifecycle`、framework 的 `TestPlacementCloseDeadlineRetainsWorkForRetry` |
| F4 | 冷寻址软告警 | 1026 个不同身份同时在途均可完成；只产生一次 Warn；热缓存继续可用 | `internal/actorplacement/TestColdQueryWarningDoesNotRejectOrSpam`；1024 是观察阈值，不是发奖/在线人数上限或内存保护 |
| F5 | Procedure 使用方权限 | 业务执行视图可 Call/Submit，不暴露 Drain 所有权 | `internal/framework/TestProcedureAccessDoesNotExposeOwner` |
| I1 | 本地/远端放置与 owner 交接 | 失败不缓存；排水拒绝新冷查询；替换提供方仍可查询；owner 交接后复用身份缓存且实际请求到新 owner | `TestPlacementLocalRemoteAndProviderReplacement`（回环 gRPC）、`TestPlacementGroupHandoffKeepsIdentityCache`（真实 NATS/etcd） |
| A1 | txgame 接入与恢复 | 既有登录、角色、排行榜、跨进程上下行与 Session 恢复断言通过；存储回读与进程退出分别核验 | `txgame/cmd/cluster-e2e.sh`，Redis DB12、Mongo `txgame_cluster_e2e`、`/tx/e2e/txgame/` |
| A2 | SLG 接入、关闭失败与接管 | Role / World 两条路径、分服放置和状态回读通过；写回拒绝时按有限重试失败退出，接管后继续服务；其他进程正常退出 | `txgameslg/cmd/cluster-e2e.sh --shutdown-failure`，Redis DB13、Mongo `txgameslg_cluster_e2e`、`/tx/e2e/txgameslg/` |
| P1 | 放置缓存命中性能 | 相同参数前后对照、记录分配，不从单机微基准推导集群容量 | [本轮修复前后基准](actor-placement-acceptance.md#2026-10-30-生命周期与冷查询边界修复) |

F4 在框架层确定性构造不同身份查询，不等同于完成排行榜发奖业务验收。A2 的持久化失败重试与 F1 的后台任务等待超时是两种不同失败原因，不能互相替代。两组应用不重复实现框架测试。

## 2026-10-30 本地集群验收（生命周期补修前）

状态：F1～F5、I1、A1、A2 均通过，SLG 三处过期夹具与断言已修正并完成整轮重跑。本轮不跑云测试、不提交；P1 沿用同日上一轮实测，未重复运行。

1. 启用本地 NATS/etcd，F1～F5、I1 运行三轮 race 测试，不使用测试缓存。
2. A1、A2 顺序运行现有多进程入口；先核对依赖与 ownership，不覆盖无归属数据。失败保留 artifact。
3. 分别记录功能、存储、排水及残留进程结果；本轮有限请求不作为 99.99% 持续流量达标证据。

本机原始证据根目录：`/Users/a47/workspace/tx-artifacts/local-boundaries-20261030/`。源码基线为 tx `9280ef6`（叠加上一轮未提交修复）、txgen `bf98c21`、txgame `9e3bd55`、txgameslg `52c1065`、txclient `c147a69`、txclientslg `1a59f3e`。`source-state.json` 保存初始版本，`tx-runtime.patch` 保存框架差异。

- **F1～F5 / I1 通过**：`framework-race.log` 记录三轮共 36 次顶层用例通过，无 skip、race 报告或失败。包含回环 gRPC、本地 NATS/etcd 下的两个 Runtime 交接；它们不是独立 OS 进程测试。
- **A1 通过**：`txgame/20261030-184353.039668000/summary.json` 为 passed。TCP/WebSocket、等级榜、双 Session、Agent 重启续接、Game 槽位交接、MsgQ 排水重启及 Login 停服/重启断言通过；13 个进程退出记录均为 0。`storageDocuments=1` 证明 StorageProbe 文档落到 Mongo，不代表最后一次角色等级已完成 600 秒 Fusion 回写。
- **A2 首次失败已定位**：`txgameslg/20261030-184500.470138000` 在登录停服场景报 `placement: origin not found`。旧夹具只发布 GDUser 已有角色投影，未准备新增角色的 GDRole；现由同一夹具方法先补 GDRole 再发布投影，`$setOnInsert` 保留已存在角色的业务状态。
- **A2 第二次失败已定位**：`txgameslg/20261030-184808.101872000` 已通过 Login 停服/恢复，但 World 首次加载被整集合拒写规则阻断。当前 World 在 Ready 前创建四格 Monster，旧故障夹具早于此实现；现只拒绝目标世界网格 7 写回，保持该网格旧值、失败退出与恢复断言，允许其他网格初始化。没有放宽运行时加载或持久化契约。
- **A2 第三次执行发现旧日志断言**：`txgameslg/20261030-185134.953539000` 已完成首次关闭失败加三次重试，并以状态码 1 退出；工具仍要求出现旧 WriteBack 路径的日志字样。现按本次注入的 `Document failed validation` 核验 Mongo 拒写原因，保留四次失败、重试耗尽和非零退出断言。
- 修复均限定在 SLG 验收工具，`go test ./cmd/... -count=1` 通过。失败 artifact 保留，重跑仅由原工具清理 ownership 匹配的专用数据。
- **A2 最终通过**：`txgameslg/20261030-185311.927564000/summary.json` 为 passed，完整日志为 `txgameslg-e2e-verified.log`。Role 交接到 generation 2，World 交接到 generation 3；两个 World 的最终网格坐标通过 Mongo 回读。15 条进程记录中，13 个正常退出为 0，World 持久化拒写后的退出码 1 和 MsgQ 的强制终止均为预期故障。关闭失败共四次（首次加三次重试），没有绕过持久化错误继续晋升。
- **清理核对通过**：`process-verification.json` 独立检查全部运行的 56 条 PID 记录，无残留进程；Redis DB12/13 均为 0 个 key，两组专用 Mongo 库均无集合，两组 etcd 专用前缀均无 key。仅检查已有工具的清理结果，没有扩大删除范围。

可复用命令（分别在对应工程根目录执行）：

```sh
# tx：真实依赖开关仅对集成用例生效；其他用例构造确定性的取消/阻塞场景。
TX_ACTORTRIAL_NATS_URL=nats://127.0.0.1:4222 TX_ETCD_ENDPOINT=127.0.0.1:2379 \
  go test -race ./internal/framework ./internal/actorplacement \
  -run 'Test(ServiceTaskShutdownDeadlineRetainsResources|ProcedureAccessDoesNotExposeOwner|Placement.*|.*Query.*|.*SlotLoad.*)$' \
  -count=3 -v
# txgame
./cmd/cluster-e2e.sh --timeout 6m --artifact-root /absolute/artifacts/txgame
# txgameslg
./cmd/cluster-e2e.sh --shutdown-failure --timeout 8m --artifact-root /absolute/artifacts/txgameslg
```

## 其他能力的现有证据与缺口

| 范围 | 证据 | 本轮边界 |
| --- | --- | --- |
| 共享 GD、EntityCache、Procedure | [实体访问](entity-access-acceptance.md)、[缓存管理](entity-cache-management-acceptance.md)、[应用 Submit](https://github.com/ngnl47/txgameslg/blob/main/docs/procedure-submit-acceptance.md) | 保留原报告日期；A2 只覆盖其实际触达路径 |
| Role / Room / World 正式包生命周期 | [正式包验收](local-package-rollout-acceptance.md) | 本轮不重复 Room 60 秒玩法验收 |
| 持续流量配置发布、回滚 | [发布报告](procedure-rollout-acceptance.md) | 既有三分钟 99.99% 结果不是本轮重跑；大量 Submit 与 Login/Agent/MsgQ 全量滚动仍未覆盖 |
| 合服及 World 寻址 | [txgame 在线合服](https://github.com/ngnl47/txgame/blob/master/docs/acceptance/timed-merge-20261030.md)、[SLG 停服合服与 World](https://github.com/ngnl47/txgameslg/blob/main/docs/acceptance/merge-world-20261030.md) | 本轮不改合服业务，也不将目录成功视为旧榜/World 数据已迁移 |
| 持久化 | 各应用摘要中的 Redis/Mongo 读回 | 按实际字段和阶段记录；不把响应、任务 ACK 或 Redis 最新值当作 Fusion 最终 Mongo 写回 |
| 生产容量与故障域 | [历史七机 20/50](audits/2026-09-28-seven-host-2050-acceptance.md) | 不跑云测试；跨宿主容灾、长期稳态、慢客户端积压仍需独立验收 |

## 2026-10-30 生命周期补修定向回归

本批仅定向回归，没有重跑上述整轮集群。证据目录：`/Users/a47/workspace/tx-artifacts/lifecycle-review-20261030/`。

| 场景 | 验证入口 |
| --- | --- |
| CMod 正常关闭、Start 失败及服务 Init 失败中的阻塞任务 | TestLifecycleCleanupDeadlineAndRetry |
| Placement/CMod 及其任务回调不暴露所有权 | TestPlacementAndCModTasksHideOwnership |
| 释放重试不重复排水、Flush、服务 Stop 和绑定清理 | TestCModStopRetryDoesNotRepeatFinishedStages |
| 失败组件不调用正常 Drain | TestFailedCModStartupDoesNotDrainPartialComponent |
| 并发 Stop 等待者各自遵守期限 | TestHostConcurrentStopHonorsEachWaiterDeadline |
| 已完成关闭的 DeadlineExceeded 不阻断依赖清理 | TestHostTerminalDeadlineDoesNotBlockDependencyCleanup |
| 服务发送器关闭先于根传输取消 | TestServiceStopPrecedesRootCancellation |

初始用例在旧实现下暴露无限等待和权限泄漏，见 `red.log`；状态转换补测见 `states-red.log`。应用回归曾稳定复现 SLG 关闭 Sender 时 EOF（`slg-recheck.log`）：根取消提前关闭同进程 MsgQ 传输，现已恢复服务 Stop 在先的顺序并补回归。最终顺序修正后的结果如下。

复审更正：撤回将 CMod.Start → Submit → Start 失败列为既有 P0 的判断。该判断仅依据扩展接口可构造的调用，未找到当前内置组件和两组应用的实际用法，也没有对应故障复现；不作为本轮验收阻塞项，不增加实现。真实路径的后续问题另见[启动与关闭复审](audits/2026-10-30-start-stop-review.md)。

最终回归全部通过：

- tx：`go test ./...`，证据 `tx-all-final.log`。
- tx：`go test -race ./lifecycle ./component ./internal/framework ./bee ./ant -count=3`，证据 `race-final.log`；包括关闭期限、重试、失败状态与权限用例。
- txgame/server、txgameslg/server：分别执行 `go test ./internal/app ./services/... -count=1`，证据 `txgame-assembly-final.log`、`txgameslg-assembly-final.log`。这是应用装配及包级接入回归，包含本机可用依赖下的集成用例，不是正式包多进程集群验收。
- `git diff --check` 通过。没有运行云压测、整轮集群或重复热路径基准；本批新增逻辑位于装配、任务登记和关闭路径，没有改动缓存命中分支。

## 2026-10-30 启动失败定向回归

证据目录：`/Users/a47/workspace/tx-artifacts/startup-failfast-20261030/`。本批没有运行正式包多进程集群或云压测。

| 场景 | 入口 / 证据 |
| --- | --- |
| 启动中致命错误、预先取消、后续 CMod 不再启动 | startup_failure_test.go；startup-red.log、cmod-red.log |
| RPC/控制面异常退出上报，正常 Stop 不误报 | 两个传输包的 ReportsUnexpectedServeFailure / NormalStopDoesNotReportFailure |
| 恢复消费者非法 MaxDeliver 在 CMod 准备时失败 | TestTaskRecoveryPolicyFailsDuringCModPreparation；task-red.log |
| 准备不消费、启动不重复静态查询 | TestPreparedConsumerSeparatesValidationFromConsumption |

验证结果均通过：

- tx：`go test ./...`，`tx-all.log`。
- txgame/server、txgameslg/server：`go test ./internal/app ./services/... -count=1`，`txgame.log`、`txgameslg.log`。
- tx：显式设置 `TX_ACTORTRIAL_NATS_URL=nats://127.0.0.1:4222`，对 framework、taskqueue、rpc、control 的新增启动分支与 `TestMaxDeliverRecovery` 运行 `go test -race -count=3`，`race-final.log`。JetStream 使用独立测试资源并清理，不运行完整集群。

全量包测试不等于所有外部集成场景均已执行；本批显式启用的外部故障回归仅上述本机 JetStream 用例。启动/关闭路径调整不改缓存命中热路径，本批没有重复性能基准。

## 2026-10-30 补修后正式包滚动验收

复用原入口，Role/Room 生命周期、World 指定启动/交接和保存后重载均通过；txgame Game、SLG Role、SLG World 三轮 180 秒完成率分别为 99.993333%、99.997530%、99.999261%。39 条进程记录全部退出，3 个故意失败进程符合预期，专用数据清理已只读核对。详情见[本轮报告](lifecycle-rollout-acceptance.md)与[机器可读证据](validation/lifecycle-rollout-20261030.json)。

这是前述启动/关闭补修之后的新实测；没有重跑 A1/A2 整套故障场景，不替换其历史版本说明。Login/Agent/MsgQ 只覆盖启动与最终关闭，不宣称全服务滚动或云端容量。
