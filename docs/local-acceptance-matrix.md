# 本地整体验收矩阵

本页以现行契约组织验收，不重复维护测试实现。框架异常边界在 tx 定向测试中构造，两组应用使用已有真实集群入口验证接入；不向业务 Handler 增加故障开关。历史报告只证明对应构建，本轮结果单独记录。

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
