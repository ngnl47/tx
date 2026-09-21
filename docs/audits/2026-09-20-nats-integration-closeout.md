# NATS 迁移整合收益与 API/SPI 收口

## 当前结论

最新决定：ProtoActor 旧实现、专属测试及比较程序已退役，代码仅保留于 Git 历史，性能摘要在 design.md。下列分批记录中的“保留旧代码”属于当时决策，已被本文末尾退役记录取代。当前工程不支持 ProtoActor 后端。

采用 Core NATS 作为实体间 RPC 传输，采用可选 JetStream 能力承接可靠任务；上下行及无实体高频计算保留直连 gRPC，MsgQ 和 etcd 保留当前职责。T5 工程功能闭环已验证，不等于生产容量、同步策略或物理容灾完成。本轮只核对当前源码、依赖图和公开能力，不重复性能结论，也不删除历史对照。

开始时六工程工作区均干净。生产入口核验为两个 server 的 `go list -deps ./cmd/server`，不是仅搜索 go.mod；两者均无 `github.com/asynkron/protoactor-go` 与 `internal/actortrial`。SLG 的 `go list -deps -test ./services/internal/actorcmd` 仍含 ProtoActor，因此 go.mod 中保留依赖不能解释成正式运行时尚未切换。

## 五项收益结算

| 方面 | 已落地 | 尚未实现或仍需承担 |
| --- | --- | --- |
| 通信配套 | 正式 Actor RPC 使用 NATS，Agent 实体入口及会话上下行走共享 gRPC；业务不管理 PID | 两种传输有各自连接、编解码和运维成本；没有证明跨主机生产总成本下降 |
| 请求等待 | 同一调用契约支持 Call/CallFuture，NATS 共享响应订阅，关闭和失败清理等待项 | `internal/actorrpc/nats.go` 仍维护 pending、超时和上限；不能宣称完全交给官方 SDK 或内存泄漏风险为零 |
| 发现与归属 | RPC 与可靠任务共享 Kind、稳定槽位、加载准入、容量和生命周期 | NATS 不承担实体权威归属；etcd 的租约、CAS、Watch 及 SessionID 等依赖仍保留；不能宣称移除 etcd |
| RPC 与持久任务 | 同一 NATS 产品体系内使用 Core 与 JetStream，已有 Publisher、消费确认、重投、死信和耗尽恢复 | 两套完成语义和资源配置仍独立；持久流、通知捕获、容量和同步策略需要部署维护；MsgQ 不是 JetStream 的替代目标 |
| 扩展接入与观察 | 新 Kind 复用实体能力，普通服务可以通过共享 gRPC 注册入口扩展；NATS 提供传输层集中观察位置 | 没有新增可直接调用任意 Actor 的跨语言公共协议、管理端鉴权或统一业务追踪面板；不能把 broker 指标当业务执行成功率 |

冻结性能基线与 T5 测量继续保留原口径；本轮未执行 benchmark，未估算节约服务器数量或云费用。历史依据见 [默认迁移收口](2026-09-14-nats-default-closeout.md)、[可靠任务与实际 GD 验收](2026-09-20-generated-task-command.md)。

## 业务 API 与高级 SPI

| 使用者需求 | 当前入口 | 框架承担 / 业务保留 |
| --- | --- | --- |
| 实体间请求 | `CModActor()`，业务只保存 `actor.Caller` 或 `actor.Messenger` | tx 查归属、加载并投递；业务只给 Kind、Key、Delivery，不组装 NATS 主题 |
| 客户端上行直达 | `CModActorDirect()` 返回 `actor.Caller` | Agent 适配保存窄 Caller；业务 Handler 不按 NATS/gRPC 分叉 |
| 持久任务 | `CModTaskQueue()` 返回 `tasks.Publisher` | tx 管理消费者、续期、ACK、死信与排水；Publish 只等待持久发布确认 |
| 实体生命周期 | `actor.Kind`、BehaviorFactory、Behavior 的 `On…Before/After`，回调 Context 的 Post/After/Deactivate | 业务判断是否可加载及何时卸载；邮箱和回调串行、激活隔离由 tx 管理 |
| 激活状态与显式重试 | `actor.Runtime` 的 Probe/EnsureActivation/RetryActivation | 保留加载中的可观察状态；不是自动迁移或任意业务重试 |
| 稳定分组与世界归属 | `WithSlotMapping`；Ant 的 CModSlots/CModSlotResolver | 业务声明分服、分战区或世界映射；普通调用不自行查权威存储 |
| 运维与高级归属控制 | discovery 的 SlotReader/SlotStore/SlotRouter、SlotOwnership；Server.PrepareHandoff | 高级能力继续保留在低层和控制面；不向普通 Handler 暴露 CAS、关闭连接或消费者控制 |
| 高频无实体计算 | `CModRPC()` 的 Registrar 与服务发现 | 可注册普通 gRPC，不强制建立 Actor；协议及业务权限由服务适配负责 |

例如角色通知世界移动时，业务使用 `Identity{Kind:"world", Key:"i:490001"}` 与生成命令；即时结果走 `Call`，要求跨重启继续处理的独立任务走 `Publisher.Publish`。后者仍需正确的命令路由键，不把普通 Call 超时当作自动切换到可靠任务的依据。任务身份是发布时快照，不因持久化自动获得权限；是否执行前复核由业务决定。

这些窄接口和高级 SPI 各有职责，不能因当前只有一个实现而删除。本轮没有足够证据要求合并 Caller/Messenger/Runtime，或再加一个通用业务 facade。

## 历史代码与延期边界

- ProtoActor 源码快照和基准、生成命令的历史分派测试仍在使用。保留冻结证据；本轮不删测试换取依赖清单变短。
- 旧虚拟 Placement/SetPlacement 仍在 `internal/actorhost`，因此生产包级依赖图仍可见 go-rendezvous；正式框架使用 BindSlots，并不执行该旧成员快照路径。不能声称历史实现已完全从正式包隔离，也不能用包依赖图证明它在运行时被调用。
- MsgQ 按既定决策保持会话邮箱职责，允许与 Agent 同进程组合；不开展 NATS 替换或重复传输对比。
- etcd 不替换。若未来独立立项，必须重新评估租约失效、CAS、Watch 快照、身份分配和故障语义，不能把 NATS KV 的存在当成等价证明。
- 自动扩 Pod、自动选择交接目标、网络分区接管、单实体热迁移和管理端接入均不随迁移收口自动实现。

下一项先评估旧虚拟 Placement 路径能否与冻结对照隔离，列明调用者、可复现性和对正式归属代码的影响；若收益很小则保留，不为消除一个间接依赖重造另一套宿主。生产部署参数与长期验收仍是独立待决策项。

## 本轮验证与后置检查

- txgame/server 正式入口依赖 567 个包、txgameslg/server 575 个包，其中 ProtoActor/actortrial 包均为 0；计数仅表示本机当前 Go 依赖图，不是二进制体积指标。
- `go test ./bee ./ant -run 'TestPublicAPIDoesNotExposeStatefulConcepts|TestStatefulConfigurationIsExplicit' -count=1` 通过。
- 本轮只修改文档，无生成物、客户端协议或生产代码变更；未重复全库测试、真实集群或 benchmark，不把此前结果当作本轮实测。
- 交付前 Ponytail review 按纯文档差异检查：未引入新抽象、未删除必要 SPI 或历史证据，无额外删减建议。差异及本次新增相对链接检查通过。未提交 Git。

## 接续：旧虚拟 Placement 隔离取舍

结论：当前保留，结束这一方向的隔离优化。没有生产正确性缺陷或性能回退证据，不为减少一个包级依赖引入新宿主 SPI、复制宿主或改变历史试验语义。

调用者核对：

- `internal/actorhost/placement_test.go` 的两项测试验证成员顺序稳定，以及归属变更关闸、保留排队工作、清理完成前不得重新激活。
- `internal/actortrial/nats_test.go` 的 TestNATSPlacementChangeRejectsStaleRoute，通过 runtime_aliases_baseline_test.go 复用宿主。它记录早期成员快照切换的边界：B 已能响应时，A 的旧请求仍可在执行，不能将单机关闸当成跨实例交接屏障。
- 全仓 NewPlacement/SetPlacement 的调用均在上述测试；正式框架只 BindSlots。冻结性能基准没有直接调用这些方法，保留理由是历史行为对照，并非声称每个性能基准都依赖它。

隔离方案比较：

| 方案 | 代价与结论 |
| --- | --- |
| 仅改为 `_test.go` | 正式 Runtime 仍引用 Placement，且 actortrial 无法访问其他包仅测试编译的方法，不能直接成立 |
| 移到试验包并增加宿主通用准入/退休回调 | 为历史路径扩大生产 SPI，仍需维持准入锁与清理占位的原子性，复杂度不划算 |
| 复制宿主或使用专用构建标签 | 增加实现分叉或构建矩阵，不符合当前维护规模 |
| 改写/删除旧试验，改用 SlotStore | 能移除旧路径，但不再复现原先无交接屏障的成员快照反例；不在本轮擅自替换证据 |

链接核验也没有将问题掩盖成“已被编译器全部消除”：本机 `go build -o /tmp/tx-placement-inspect-server ./cmd/server` 后，`go tool nm -size` 仍可见 Placement.Target、Runtime.SetPlacement 及 rendezvous 的部分符号；NewPlacement 未出现。符号存在不代表正式代码会调用，符号大小也不能当整体可节省的二进制大小或吞吐收益。未进行候选删除或 benchmark，不作性能改善声明。

本次 `go test ./internal/actorhost -run '^TestPlacement' -race -count=1` 通过（1.038s）；正式 txgame 构建通过。旧 NATS 试验本轮仅核对源码，未作为新运行结果。生产 Go 文件增减均为 0，既有 SPI 和测试不变。Ponytail review 按纯文档差异检查通过；这次的最小方案是保留有用途的旧路径，而不是增加隔离机制。

T6 本次范围已完成收益结算、API/SPI 边界与历史保留决策。下一项建议评估 demo 的可复现部署与任务资源预创建流程，具体缺口尚需核对现有部署文档；生产参数和长期故障域验收不由这次源码收口代为决定。

## 接续：可复现 demo 与测试准备收敛

已按确认范围补充 `deploy/task-demo`：Compose 管理独立 NATS/Redis/Mongo/etcd，健康就绪后由独立 `cmd/task-resources` 使用现有 NATS SDK 创建清单资源，再由 Python 标准库脚本从两个项目 debug 资产生成新的配置目录。正式项目启动只绑定已有资源，未加入自动创建流或测试管理能力。资源清单包含两套各三流/两消费者、有限 MaxDeliver 及专用通知恢复；创建遇到不匹配配置失败，不更新或自动回滚。

实测四服务健康就绪，资源创建及同配置重复执行成功；源流 MaxMsgs 冲突、恢复 consumer MaxAckPending 冲突均失败，再执行原清单仍成功。资产生成不覆盖已有目录。两个实际 cmd/server 使用生成资产启动 game / slgworld，日志确认 task-queue 能力加载，SIGTERM 后均成功退出。这一步验证部署与装配，未向这些常驻 demo 资源发送业务任务；业务行为由下面独立流的真实用例验收。

测试准备仅在 txgame 内提取两处稳定相同的资源创建：文件源流、独立死信流、owner 标记、单条在途与无限重试参数。各用例仍保留自己的清理策略：成功 owner 校验清理，或保留数据供检查；失败现场不被辅助包删除。SLG 不导入此辅助包，不为了统一而跨示例耦合。生产 Game 依赖闭包确认不含 internal/testsupport。

本轮验证：

- 新 Compose 上命令重投验收 race 通过：0.44s；业务完成前不确认、首次失败与再次成功断言保留。
- Fusion GD 任务 race 通过：2.62s；世界 Procedure 任务 race 通过：0.27s。
- tx 与 txgame/server 全量通过；SLG 仅改 README，另执行真实世界任务及正式入口构建/启动，不重复其全量。
- 差异检查和文档相对链接检查通过。Ponytail review 未发现需增加或删除的生产抽象；准备命令保持一次性部署工具，不增加公共库 SPI。

行数口径：两个 txgame 测试文件合计净减 15 行，新增专用辅助包 32 行，测试相关 Go 总体净增 17 行；收益是队列参数只维护一处，不宣称净删行。新部署命令与资产属于新增功能，原生产运行路径没有修改，不跑吞吐 benchmark、不宣称性能改善。

验收日志保存在 `/tmp/tx-task-demo-validation`。本轮 demo 容器停止，专用数据卷与生成配置保留，可按 README 再启动；未触碰以前的失败容器或默认依赖。下一项评估部署环境下的端到端业务投递演示是否需要独立入口，目前无新增业务协议或奖励功能需求，不自动扩展为通用运维平台。

## 接续：向独立 Game 进程投递 demo 任务

补充 txgame `cmd/task-demo`，使用项目配置启动只发布的 Bee 服务，通过正式 CModTaskQueue.Publisher 投递既有 System_StorageProbe。它不注册 Role、不开消费者、不导出内部信封格式，也不等待或伪装业务成功。失败不自动补发，退出执行框架 Stop。入参 ID 限制为 Fusion JSON 已有安全整数范围，超界在连接/投递前报错。

配置副本生成器仅对 Game 增加 storageProbe 路由，正式资产不变。使用此前保存的 demo 资源，Game 离线时命令成功退出，GAME_TASKS 消息数为 1；随后启动独立 cmd/server，Mongo txgame.storage_probes 的 930001 记录 n=queued-live-demo，源流清空、死信为空，SIGTERM 正常退出。这次使用预创建的 GAME_TASKS/workers，而非测试随机流。结果观察由验收脚本完成，不将轮询代码塞进生产 Publisher。

本轮 txgame/server 全量通过；超界 ID 拒绝验证通过。Ponytail review 检查新增 CLI 与资产脚本，未增加通用运维抽象或重复 SLG 发布器；差异检查通过。库运行路径没有修改，本轮不跑 benchmark；此前独立测试和历史现场保留。日志追加至 `/tmp/tx-task-demo-validation`，demo 依赖再次停止、数据卷和配置保留。

复现步骤已补入 task-demo README，并明确保留卷重启后旧稳定归属不会自动改派：沿用正式交接流程，或确认仅为可丢弃 demo 后清理专用卷从头启动，不能删除共享归属绕过检查。本节点部署、实际投递及关键测试准备收敛完成；下一实施项尚未确认，生产负载、同步策略和物理部署目标需在容量验收前明确。


## 接续：主线整体验收

用户明确暂不建设运维投递工具或远程脚本执行。六工程起点工作区干净，本轮回到近期可靠任务接线后的整体回归，不新增生产接口、运维能力或性能微优化。

版本：tx `cc7c569`、txgame `cfabc74`、txgameslg `9e30400`；txgen `8bc6c97`、txclient `5514f2a`、txclientslg `4b663cf`。前三工程仍在 `feature/nats-actor-runtime`。

本轮串行执行结果：

- tx、txgame/server、txgameslg/server 的 `go test ./... -count=1` 全部退出 0，分别有 38、16、21 个测试包通过；环境变量控制的真实依赖用例仍按各自条件运行，不将全量命令通过解释为零跳过或全库 race。
- 两个正式 server 的 `go list -deps ./cmd/server` 均无 ProtoActor 与 internal/actortrial。
- txgame 现有真实 cluster-e2e 通过，覆盖 Login、跨 Agent 多 Session、MsgQ、Game 重启和存储等既有断言。
- txgameslg 现有真实 cluster-e2e 通过，世界正常交接到 generation 3，490001 与 490002 分别归属不同实例；本轮未追加 shutdown-failure 扩展分支。
- 显式设置本机 NATS/etcd 后，`go test -race ./internal/framework -run '^TestJetStreamFormalActorTaskLoop$' -count=1 -v` 通过，耗时 13.912s；覆盖离线持久等待、正式 Actor 完成后 ACK、进程退出重投、失败分类、续期、背压与排水续领。不是全部 JetStream 故障矩阵重新验收。

本轮日志、版本清单、依赖图及两套集群 summary 保存在 `/tmp/tx-mainline-20260920`。成功集群用例按原 ownership 约束清理专用数据；未改动共享依赖配置。没有修改 Go/生成产物，未运行 benchmark，不提出新的性能或生产 SLA 结论。

本轮后置 ponytail-review 按纯文档范围检查：无新增抽象或可删除包装，保留必要 SPI 和历史证据。主线迁移工程范围维持收口，运维脚本暂缓。下一项尚需确定生产候选同步策略、任务负载和部署条件后再开展长期容量及故障域验收；没有已确认的后续编码项，不为延续实施而追加通用管理平台或重复测试。


## 接续：两个示例移除 ProtoActor 测试适配

用户确认开始收敛 ProtoActor 残留。本批只处理 txgame、txgameslg，tx 的冻结对照和旧运行时仍保留，未删除历史基线。

两个示例删除 protocol_fixture_test.go 中的 ProtoActor Receive/Result 转换与工厂，命令测试直接调用当前 commandActor.execute。保留协议 Delivery 解码，验证真实 Call/Tell 标记；测试转换 helper 只负责解码和失败报告，不再模拟邮箱或封装结果。可靠任务测试直接构造现有实体，生产代码与生成产物均未修改。

原有命令测试分别 16、9 项，迁移后名称及数量不变。保留角色响应、Session 错误、下行失败不确认、生命周期 Context、发布完成屏障、内部调用与单向命令模式等业务断言。首次迁移测试曾错误地用 errors.Is 判断 NewCallError 的原始业务错误；按现有错误码和消息契约修正后通过，没有修改生产错误语义。

两个 server 均完成 go mod tidy：移除 ProtoActor 直接依赖及不再使用的间接依赖，已有 NATS 依赖按实际引用提升为直接依赖。全工程含测试的 go list -deps -test ./... 均不含 ProtoActor/actortrial；tx 自身的模块依赖与历史对照仍在，不能描述为整个多仓系统已彻底删除 ProtoActor。

验证：两个 server 的 go test ./... -count=1 均通过；各自 services/internal/actorcmd 的 -race -count=1 均通过。日志与依赖图位于 /tmp/txgame-proto-clean-{full,race}.log、/tmp/txgameslg-proto-clean-{full,race}.log 和对应的 -deps.txt。本轮不重跑此前已通过的多进程集群验收；正式运行路径未修改，不运行 benchmark，也不宣称线上性能提升。

Go 测试代码净减少：txgame 67 行、txgameslg 43 行，合计 110 行；两工程 go.mod/go.sum 另净减少 194 行。后置 ponytail-review 无额外删减建议，独立示例边界、关键业务断言和必要 SPI 保留。

下一项评估 tx 内纯历史 ProtoActor 测试、旧运行时及对照入口的归档边界：先将仍适用的契约与当前运行时覆盖对应，再决定哪些只保留在明确历史提交中。当前未授权直接删除冻结基线或其可复现依据。


## 接续：tx 正式协议契约与历史运行时测试分离

本轮发现 tests/actor_runtime_test.go 的四项 Delivery 协议测试仍导入旧运行时副本。已提取为 tests/actor_delivery_test.go，直接验证正式 modules/actor 的载荷复制、身份字段、内部身份无上行序号与非法协议拒绝；四项断言保留。

其余旧 Runtime/配置/生命周期测试及 Actor Remote 基准迁入 internal/actortrial/testdata/protoactor/public_*_test.go，原测试正文保留，改为外部测试包；基准仅包名及 gofmt 导入顺序变化。当前宿主的 TestBeginDrainRetainsAcceptedWork、TestDrainWaitsForCleanupAndCancelledCallerDoesNotCancelWork 和框架生命周期测试继续承担正式排水契约，旧 PID/Provider 行为不作为当前运行时要求。没有通过删除关键测试来减少依赖。

历史目录 README 与 design 的复跑入口已更新，完整迁移前快照为 cc7c569445b583e0f46343365f37ffcb6c5fb300；既有冻结报告与比较负载未改变。testdata 需要显式运行，不再由 go test ./... 枚举。tx/tests 的 go list -deps -test ./tests 已无 ProtoActor/actortrial；比较程序仍需要旧运行时，tx/go.mod 不变。

本轮验证：四项正式协议测试及定向 race 通过；tx 全量 go test ./... -count=1 通过；历史目录显式 go test ./internal/actortrial/testdata/protoactor -count=1 通过。未显式开启历史 etcd 用例，也未重测旧后端 benchmark，不宣称旧 Provider 的 race 已解决。日志为 /tmp/tx-proto-clean-full.log、/tmp/tx-proto-clean-contract-race.log、/tmp/tx-proto-archive-tests.log，正式测试依赖图为 /tmp/tx-proto-clean-contract-deps.txt。

Ponytail review：本轮仅迁移既有断言与测试文件，没有新增运行时抽象或兼容层。计入新增与迁移文件，tx Go 行数净增 11 行（拆分文件导入）；此前两个示例净减 110 行，累计 Go 净减 99 行。生产路径未变，无性能提升声明。下一项仍是是否退役整个冻结比较程序；只有接受改为从历史提交复现后，才可进一步删除对应源码及 tx 的 ProtoActor 依赖，不能仅删 go.mod 项。


## 最终决定：ProtoActor 代码仅保留于 Git 历史

用户明确要求当前工程不保留已淘汰后端。删除 internal/actortrial/testdata 中旧 Runtime、旧 Agent 和上行/合批构建脚本，以及 comparison*_test.go 性能比较驱动；此前迁入历史目录的旧公开测试及远程基准也随之删除。完整旧代码仍可在提交 cc7c569445b583e0f46343365f37ffcb6c5fb300 的独立检出复现，已核对该提交存在比较入口。design.md 保留历史性能表及 I2 完整链路选型数据，明确当前不支持 ProtoActor；历史审计报告中的旧命令仅适用于历史检出。

仍适用的四项 Delivery 契约继续直接验证当前 modules/actor。NATS、稳定槽位交接、真实写回和租约失效测试继续保留，没有整目录删除 actortrial。该目录的剩余测试只使用 NATS/当前实体宿主，不提供 ProtoActor 后端。

删除无调用的 Instance/RouteInstance.ActorEndpoint、ActorEndpointDirectory、LookupActorInstance、地址反查索引和框架转发方法及专属测试。保留 Directory/RouteDirectory、Endpoint 和 SlotStore 等当前 SPI；这是明确退役旧地址 API，不增加兼容层。Bee/Ant 配置注释改为当前本地宿主和 Core NATS。

三工程 go mod tidy 完成；六工程 Go 源码及 go.mod/go.sum 未发现 ProtoActor 导入/依赖，tx 全部正式和测试包的 go list -deps -test ./... 不含 ProtoActor。tx、txgame/server、txgameslg/server 全量均通过。显式设置本机 NATS、etcd、Redis 后，actortrial、actorhost、actorrpc、framework、discovery 五包按 -p 1 串行 race 全部通过。没有在本轮重复多进程集群套件。日志位于 /tmp/tx-proto-retire-full.log、/tmp/tx-proto-retire-race.log、/tmp/txgame-proto-retire-full.log、/tmp/txgameslg-proto-retire-full.log；依赖图为 /tmp/tx-proto-retire-deps.txt。

缓存路由微基准：用临时 Go overlay 仅恢复 HEAD 的 directory.go 作为清理前对照，当前测试输入保持相同；采集不进入生产路径。-cpu=4、五轮中位数，全部 0 B/op、0 allocs/op。首轮 200ms 窗口前→后为串行 160.7→162.1 ns/op、并行 78.44→103.3 ns/op。其余验证完成后反向采样，500ms 窗口前→后为串行 163.4→160.7 ns/op、并行 86.52→100.7 ns/op（约 +16.4%）。记录局部并行回退，不能宣称清理提升运行性能，也不能据此推导端到端吞吐同幅下降。未为维持旧内存布局添加无用填充或保留已退役字段。完整样本和 overlay 位于 /tmp/tx-proto-retire-bench。

累计未提交 Go 行数（包含新增协议测试）净减少：tx 7419、txgame 67、txgameslg 43，合计 7529 行；三工程依赖文件另净减 293 行。主要减少的是历史实现和比较测试，不能等同于生产热路径瘦身。Ponytail review 无新增抽象或额外删减建议；差异检查通过。

本次 ProtoActor 退役范围完成。后续如评估路由性能，应先验证该局部并行回退在代表性链路中的影响；不自动重开已停用后端、运维平台或无目标的微优化。生产容量和故障域验收仍需先明确部署条件。


## 接续：缓存路由清理的正式链路回测

复用 txgame cluster-perf，32 TCP 会话、2 秒预热、GOMAXPROCS=4、八个服务进程；每会话最多一个在途 System_Ping，覆盖正式上下行、Session、MsgQ，不含 GD 数据 I/O。清理前仅用 Go overlay 恢复 HEAD 的 directory.go，已核对文件一致；没有把旧后端放回源码。性能采集与生产路径隔离。

| 执行顺序 | 窗口 | 请求/s | P99 ms | 完成数 / 失败数 |
| --- | --- | ---: | ---: | ---: |
| 清理前 1 | 10 秒 | 8607.1 | 9.527 | 86071 / 0 |
| 清理后 1 | 10 秒 | 8637.1 | 9.677 | 86371 / 0 |
| 清理后 2 | 10 秒 | 4626.9 | 23.199 | 46269 / 0 |
| 清理前 2 | 10 秒 | 5770.9 | 13.466 | 57709 / 0 |
| 清理前，修复采集器 | 20 秒 | 8198.8 | 10.084 | 163976 / 0 |
| 清理后，修复采集器 | 20 秒 | 8404.15 | 9.672 | 168083 / 0 |

六个成功窗口的 Session owner 及排水退出检查全部通过，共完成 608479 次请求，零失败。短窗口版本内波动明显；20 秒配对吞吐 +2.5%、P99 -4.1% 仅为单组实测，不能宣称性能提升。当前未确认局部并行微基准回退造成稳定的完整链路退化，也没有证明所有负载无回退；保留微基准记录，停止无目标的布局优化。

延长到 30 秒时发现 txclient 采集器将十余万样本展开传给 Array.push，超过 JS 参数数量限制后被结果计数校验掩盖。新增 20 万请求回归测试先复现，再改为直接接收窗口返回的只读数组，成功与失败分支共用同一方式；没有引入分批复制 helper。npm test 80 项全通过，npm run build 通过。第二次 30 秒尝试在等长空闲窗口触发 Agent 读取超时，日志明确 i/o timeout；未调整生产超时，改用 20 秒完成配对。两次失败均保留，不计入成功性能样本。README 记录现有工具的空闲窗口限制。

本轮只有采集器和文档变更，不重复上一轮 Go 全量与 race，也不将上一轮结果记为本轮。日志、汇总、参数与来源哈希在 /tmp/tx-route-impact-20260920；客户端验证日志为 /tmp/txclient-route-impact-{test,build}.log。累计未提交 Go 净减仍为 7529 行；本轮客户端增加 19 行回归测试，采集实现行数不变。

后置 ponytail-review：没有新增生产抽象、兼容层或多余复制；保留必要契约与失败证据，无额外删减建议。下一项若继续长窗口容量验证，先评估测试侧空闲窗口和保活边界；生产容量验收仍需明确部署与业务负载，不自动恢复已退役后端或扩展运维能力。


## 接续：长窗口空闲采样边界修复

原 samplePerfIO 用正式 Duration 等长等待空闲窗口，30 秒无心跳时已在上一轮触发 Agent 读取超时。本轮改为 min(Duration, 10s)，正式请求窗口不变；不新增配置项、保活线程或生产路径逻辑。两窗口时长可能不同，不能直接比较累计 I/O，txgame、txclient README 与 STORAGE_IO.md 同步说明。例如 --duration 45s 先空闲 10 秒，再测量 45 秒。

本轮 go test ./internal/clustere2e -count=1 通过。GOMAXPROCS=4、32 TCP 会话、2 秒预热、45 秒正式窗口的现有 cluster-perf 通过：229678 次完成、0 失败，约 5103.96 请求/s、P99 30.194ms，Session owner 检查通过，八个服务进程均正常排水退出。idle 快照文件写入间隔约 10.009 秒，仅作为等待时长旁证，不解释为存储调用耗时。此次是故障修复验收，没有同期前后配对，不用于宣称吞吐提升或退化，也不是长期生产容量结论。

证据：/tmp/txgame-perf-idle-test.log；/tmp/tx-perf-idle-cap-20260920/20260920-181150.556573000/summary.json 及同目录客户端结果、I/O 快照和进程日志。上轮两次失败证据保留。客户端实现未再次修改，不重复其上轮 80 项测试及构建，也未重跑全库 Go/race。本轮仅测试工具 Go 净增 1 行，累计未提交 Go 净减 7528 行。

后置 ponytail-review 无额外删减建议：使用内置 min，不引入包装、配置或线程；差异检查通过。长窗口工具障碍已处理，下一项尚需评估明确的业务负载与部署条件，再决定容量或故障域验收，不自动扩展监控平台或重开微优化。


## 接续：正式工程多 Agent 与目标频率负载

客户端不再为命中单个 Agent 重试 Login，接受每次实际返回的实例，agentOwners 记录 Login 分配结果。新增 --rate-per-session，每会话按绝对时间轴错开发送；最多一个在途，响应慢或调度落后时跳过错过的时刻，不无限积压或补发。默认 0 保留闭环。planned 是每会话 floor(durationMs * rate / 1000) 乘连接数，notSent = planned - attempted；passed 仍指已发送请求及集群检查通过，不代表达到目标到达率。客户端和 Go 编排器同时记录并校验计数。沿用正式 System_Ping、AgentClient、Login、Agent、MsgQ、Game，不复制业务处理实现。

验证：客户端 npm test 81 项通过及 npm run build 通过；Go 的 internal/clustere2e、cmd/cluster-perf 测试通过。定向测试覆盖多 Agent Login 不重试、固定时间轴落后时记录未发送、单在途边界、Go 不接受被隐藏的未发送数量。

真实集群均 GOMAXPROCS=4：32/64 会话、每会话 10 次/s、3 秒窗口，分别计划并完成 960/1920 次，未发送 0、失败 0；两个 Agent 分别分配 16/16、32/32 会话，两个 MsgQ 均有会话。32 会话闭环 2 秒完成 16734 次；32 会话每秒 1000 次目标、2 秒窗口，planned 64000、attempted/completed 16992、notSent 47008、failed 0。后者没有达到目标负载，不能用已完成请求的延迟宣称容量达标。全部窗口正常排水退出，未改变生产配置或加入 NATS 上下行转发。

日志及 summary：/tmp/tx-paced-20260920、/tmp/tx-paced-closed-20260920、/tmp/tx-paced-overload-20260920；测试日志 /tmp/txclient-paced-{test,build}.log、/tmp/txgame-paced-test.log。未重跑 tx/txgameslg 全量与 race，未采集 pprof，不宣称性能提升。本轮扩大的是可复现负载能力，不是生产路径瘦身。

后置 ponytail-review 无新增框架或业务包装：复用既有负载循环、客户端和正式服务，仅增加目标频率与必要结果计数。两个服务端工程 AGENTS 明确长期压测功能边界。当前仍依次登录后统一建连，没有完成一万连接、连接爬升、全链路指标或 Actor RPC/JetStream 服务端发生器；下一项先检查大规模连接建立、票据时效及 ready 等待预算，再逐级扩大，避免立即将本机推至满载。需要服务端发生器时作为对应服务的可选业务 modules 实现，目标端仍调用正式业务。


## 接续：渐进建连、即时握手与准备阶段预算

本轮起点六工程 git status 均干净，前批变更已提交。txgame 新增 --connect-rate（默认每秒 10 次登录，0 为顺序不限速），每个角色完成登录后立即握手，再申请下一张票据，避免全部登录完成前囤积 5 分钟有效票据。握手单独受 request-timeout 约束，超时后清理连接；ready 与 Session 证据收集改用整轮 timeout 预算，不再复用 5 秒单请求预算。

准备期间每 10 秒对已连接角色发送正式 System_Ping 保活，不另造协议或业务 Handler。进入预热前取消等待并显式 await 所有在途保活完成；失败与结束也清理，正式统计不包含准备请求。空闲 I/O 对照仍不保活。负载发生器保持独立于生产服务实现，未修改登录访问控制或票据 TTL。较大连接规模必须自行给足整轮预算，速率也不能绕过实际 Login 限流。

验证：txclient npm test 83 项通过、npm run build 通过，新增即时握手顺序与握手无响应清理检查；txgame internal/clustere2e、cmd/cluster-perf 测试通过。本轮无共享协议或生产 tx 代码修改，未重复全库与 race，未采集 pprof。

真实集群 GOMAXPROCS=4：

- 40 角色、每秒 1 次登录，准备爬升超过旧的 5 秒预算和 30 秒空闲期限；随后每会话 10 次/s、3 秒窗口，计划与完成均 1200，未发送和失败均 0，两个 Agent 各 20 会话。
- 256 角色、每秒 10 次登录，同样 10 次/s、3 秒窗口，计划与完成均 7680，未发送和失败均 0，两个 Agent 各 128 会话，P99 5.054ms。
- 512 角色、每秒 10 次登录，计划 15360，实际发起并完成 15334，未发送 26、失败 0，两个 Agent 各 256 会话，P99 73.314ms。建连及已发送请求检查通过，但目标负载没有完整发出，不称为容量达标；不据单个短窗口给出回退归因。

全部档位的八个服务进程正常排水退出。证据：/tmp/tx-ramp-20260920/20260920-190553.745329000/summary.json、/tmp/tx-ramp-scale-20260920/20260920-190729.952267000/summary.json，及对应客户端、I/O 和进程日志；单元验证日志 /tmp/txclient-ramp-{test,build}.log、/tmp/txgame-ramp-test.log。

后置 ponytail-review 无额外删减建议：没有新增测试专用业务实现，保活复用正式 Ping，停止时等待完成而非仅取消，未添加公共框架抽象。README 已同步默认速率与超时口径，差异检查通过。一万连接、长期容量、全链路分段统计尚未验收；下一项评估并复用正式 Actor RPC/JetStream 业务入口，需要的服务端发生器放入示例服务可选 modules，不继续无目标微优化。


## 接续：正式业务模块的 Actor RPC 与可靠任务发生器

前批爬升变更未提交，本轮继续累积。txgame 在 services/loadgen 实现可选业务服务，cmd/service-perf 经 app.WithLoadGenerator 显式装配；普通 app.New 与 cmd/server 不注册发生器。加载自身只准备 CMod 能力，Server.Start 后才显式 Run。目标 Game 是独立的正式服务，没有压测专用 Handler，也未新增生成协议。

rpc 复用 System_Ping 与正式 Core NATS Caller，验证 Pong 才计入 rpcCompleted；task 复用 System_StorageProbe 与 CModTaskQueue.Publisher，publishAcknowledged 仅表示 PubAck。角色目标轮转，任务探针 ID 使用专属起始 ID 加计划序号。任务发生器关闭自己的 Consumer，资源仍由部署工具预创建。速率是总速率，最大在途受 workers 约束，错过或名额不足记为 notSent，不积压补发。取消停止新增工作，WaitGroup 等待已交出的工作按单次超时返回后再关闭运行时；不把本地 Call 超时当作远端停止。

首次真实运行发现首个时间槽因固定 worker 尚未接收而漏发：6 次任务发出 5 次、60 次 RPC 发出 59 次，均如实记录。改为有界在途名额后复验，保留首轮证据，不为压测补发漏掉的业务。新增测试覆盖单次计划不因启动时序漏发，以及取消后仍等待在途发布、不把发布确认记成消费完成；loadgen 和 app 定向 race 通过。txgame/server 全量 go test ./... 通过，随后对调度修复重跑定向 race 与真实链路。未重复客户端上一轮验证及 tx/SLG 全库测试。

实际验收使用本机现有依赖、独立应用/发现前缀和本轮独立队列 LG20260920A_TASKS、LG20260920A_TASK_DEAD、LG20260920A_TASK_EVENTS，不修改共享依赖配置。配置副本在 /tmp，所有目标与发生器使用正式二进制：

- Game 离线时发布 6/6 个任务，failed=0、notSent=0；源流保留 6 条，待消费 6、待 ACK 0，对应独立 Mongo 库尚无探针记录。
- 启动实际 Game 后，源流、待消费、待 ACK 均为 0，Mongo 对应范围出现 6 条记录。这里只证明本轮独占资源内的低负载闭环，不以汇总队列状态推导一般情况下的逐任务业务结果。
- Actor RPC 计划并完成 60/60 次、failed=0、notSent=0。
- SIGTERM 中途停止 30 秒发送窗口：planned=600，attempted/rpcCompleted=39，notSent=561、failed=0、stopped=true；发生器正常退出，目标 Game 继续运行，最后单独正常排水关闭。

最终证据 /tmp/tx-loadgen-20260920-b（offline-task.log、rpc.log、stopped-rpc.log、before.json、after.json、game.log）；首轮证据在同名 -a 目录。只读验收脚本是测试夹具，不是业务发生器实现。独立队列和专属 Mongo/Redis 测试记录保留；所有临时业务进程已退出。测试日志 /tmp/tx-loadgen-full.log、/tmp/tx-service-perf-race.log。

后置 ponytail-review 无额外删减建议：复用正式 Caller/Publisher 与 Handler，使用标准库计时器、原子计数和 WaitGroup，不增加 tx 公共接口或通用管理平台。本轮功能验证不是性能提升或容量验收。下一项评估接入现有集群编排及消费证据采集，SLG 的真实跨世界业务负载保持独立，尚未自动实现。


### 发生器目录归属修正

loadgen 拥有独立 ServiceSpec、注册和生命周期，将其从 game/modules 迁至 services/loadgen，与 game 同级；同步组合根、命令入口引用及 txgame 目录约定。仅移动现有实现和测试，不增加模块包装或修改运行行为。迁移后 go test -race ./services/loadgen ./internal/app ./cmd/service-perf -count=1 通过（命令包无测试，完成编译）；差异检查通过，后置 ponytail-review 无额外删减建议。本次未重跑真实集群压测，不产生新的性能结论。


### 旁路压测服务命名统一

服务及包名统一为 loadtest，目录为 txgame/server/services/loadtest；组合根选项为 WithLoadTest，RPC 消息类型同步为 loadtest.command，命令入口保留 service-perf。中文统一使用“旁路压测服务”，同步 README、目录约定和当前实施计划。此前段落中的 loadgen 为当时名称，历史证据路径及测试日志保持原样。

重命名后 go test -race ./services/loadtest ./internal/app ./cmd/service-perf -count=1 通过；txgame 内旧名称引用已清理，差异检查通过。后置 ponytail-review 无额外删减建议；没有增加兼容别名或公共抽象。本次仅调整命名，未重跑集群压测。

## 接续：旁路压测服务接入正式双 Game 编排

此前变更已提交，本轮新增 cluster-service-perf，复用 clustere2e 的配置渲染、双 Game 进程规格、生命周期控制和 ownership 数据区保护。旁路服务作为第三个独立进程运行，未复制目标 Handler。loadtest 新增 rpc-storage 模式，复用正式 System_StorageProbe 的 InstanceId 响应累计各实例实际处理数；service-perf 支持单独输出 JSON 文件。编排器核对本轮 ID 与名称的 Mongo 延迟回写结果，不把 RPC 完成直接等同于落盘完成。尚未执行回读的失败结果省略 mongoRecords。

默认验收 32 个角色、100 请求/秒、3 秒；仅使用既有专用 Redis DB 12、Mongo txgame_cluster_e2e、etcd /tx/e2e/txgame/*。两个 Game 都必须有实际响应，且计划完整执行；不强制均分归属。成功后正常排水并清理专用数据；失败保留数据和日志，停止本轮进程。下一次准备仍须通过 ownership 检查。

本轮实际验证：

- 50 请求/秒、3 秒：150/150 完成，两个 Game 分别 73/77，Mongo 150 条匹配，零错误和漏发。证据 `/tmp/tx-cluster-service-perf-20260920/20260920-201200.913933000/summary.json`。
- 单角色反例：2/2 RPC 完成但只命中一个 Game，整轮正确失败，三个进程退出。证据 `/tmp/tx-cluster-service-perf-single-20260920/20260920-201321.591183000/summary.json`。该次结果结构尚使用默认 mongoRecords=0，实际未进入存储回读阶段；随后改为未检查时省略字段，避免误解为已观察到零条。
- 最终代码 100 请求/秒、3 秒：300/300 完成，game-a 158、game-b 142，Mongo 300 条匹配，零错误和漏发，三个进程正常退出、专用数据清理完成。证据 `/tmp/tx-cluster-service-perf-final-20260920/20260920-201357.659563000/summary.json`。
- txgame/server 全量 `go test ./... -count=1` 通过，日志 `/tmp/txgame-multi-loadtest-full.log`；loadtest、clustere2e、app 定向 race 通过。新增测试约束实例身份、全部 Game 命中、计数一致和发布确认不能代替 RPC 完成；最后仅调整未回读字段的 JSON 表示，已由最终真实运行重新编译验证。

后置 ponytail-review 无额外删减建议：复用现有正式 Handler 和进程管理，没有引入第二套业务实现、公共调度抽象或分布式压测控制器。差异检查通过。本轮是带存储的低负载链路验收，不是纯 Ping benchmark，也未测生产容量。JetStream 模式仍可由 service-perf 独立发布，下一项将共享消费者、队列状态及消费存储证据接入该多实例编排；SLG 跨世界业务链路尚未覆盖。

## 接续：双 Game 共享 JetStream 消费验收

前批双 Game RPC 变更未提交，本轮继续累积。cluster-service-perf 增加 `--mode task`，复用同一个旁路服务、正式 Game、StorageProbe Handler 和进程管理；未修改 tx 生产消费实现。每轮创建随机 TXGAME_PERF 前缀的独立 R1 文件任务/死信/耗尽通知流，配置有容量上限；只允许清理本轮创建且 ownership 标记匹配的资源，失败保留清单与日志，不修改现有业务队列。README 与 AGENTS 同步资源边界。

流程先保持 Game 离线，loadtest 持久发布后退出，核对 PubAck、队列积压和 Mongo 零记录；通过 JetStream 原生暂停先保留任务，等待两个 Game 就绪且共享 durable 有两个拉取请求，再确认队列仍完整积压后恢复消费。分别观察源队列/待投递/待 ACK、投递和 ACK 序号、死信与耗尽通知，再回读本轮 Mongo ID 和名称。snapshot 的 redeliveredUnacked 是未确认重投消息的时点数，不是累计重试计数。task 不填实例处理数，不声称两个 Game 均分、每个 Game 的具体消费量或恰好一次执行。

本轮真实验证：

- 150 条 task：离线 pending/messages=150，Mongo=0；共享拉取 waiting=2 时仍有150条；消费后队列与待 ACK 全零、ACK 序号150、Mongo150。证据 `/tmp/tx-cluster-task-perf-20260920/20260920-205951.229065000/summary.json`。
- RPC 共用编排回归：150/150 完成，两个 Game 分别77/73，Mongo150，零错误与漏发。证据 `/tmp/tx-cluster-rpc-regression-20260920/20260920-210114.428908000/summary.json`。
- 最终 task 代码：100请求/秒、3秒，300/300 PubAck，rpcCompleted保持0。离线messages/pending=300、Mongo=0；ready waiting=2且300条仍未投递；消费后messages/pending/ackPending=0，投递和ACK序号300，死信与耗尽通知0，Mongo300。三个进程正常退出，三个本轮流删除成功、专用数据清理完成。证据 `/tmp/tx-cluster-task-final-20260920/20260920-210147.393807000/summary.json`。
- txgame/server 全量测试通过，日志 `/tmp/txgame-task-loadtest-full.log`；最后加强消费者恢复前的完整积压检查后，clustere2e/loadtest/app 定向 race 与上述最终真实 task 验收通过。测试涵盖发布不等于消费、离线无业务数据、待ACK/ACK不足/死信/耗尽通知不能通过。

后置 ponytail-review 无额外删减建议：复用原生 Pause/Resume、既有进程管理和正式业务，未增加生产采集钩子或分布式控制器。差异检查通过。这是低负载闭环验收，不是 JetStream 吞吐上限或 R3 高可用测试。下一项评估 txgameslg 正式 role 到 world 跨服务链路的负载入口和必要证据；不能将当前旁路服务到角色的调用等同于已覆盖该业务链路。

## 接续：SLG 正式跨 Actor 行军重复验收

前批 RPC/JetStream 旁路压测变更继续保留未提交。本轮核对现有业务发现 Role_StartMarch 已经经 remoteWorldMarcher 调用 WorldActor，World_StartMarch 再通过 MsgQ/Agent 发送 System_Notify；StartMarch 不修改 GD，因此没有将这条链路错误建模为存储压测。

复用现有 txgameslg cluster-e2e 与 txclientslg 协议驱动，新增 --march-rounds（默认1），每个世界的 Role→World 和 Agent→World 路径重复指定轮数。每轮使用不同单位编号，同时等待接受响应和包含 world/unit 的通知，JSONL 记录轮次、单位编号与来源实例。world-pool 的权威归属校验同时要求所有轮次完整且不重复，拒绝错误 owner、越界轮次和缺失路径。没有复制 SLG 旁路服务、替代 Handler 或修改生成协议。

本轮真实命令 `go run ./cmd/cluster-e2e --march-rounds 20 --artifact-root /tmp/txgameslg-march-20260920` 通过，证据 `/tmp/txgameslg-march-20260920/20260920-222047.445024000`：

- generation-1、generation-2、generation-3 各20次跨 Actor调用与20次直连调用，通知来源分别对应世界归属三代实例。
- world-pool 两个世界各20轮，两条路径各40条证据，来源匹配两个权威实例。
- 合计100次 role→world RPC、100次直达世界调用，逐次响应和通知匹配通过；同时完成原有 Login、MsgQ 重启、世界故障恢复、正常交接与存储回读验收。
- 15个进程均已退出，其中 slgworld-a、msgq-a 为原套件规定的故障注入 SIGKILL，其余13个退出码0；专用数据按原套件边界清理。
- txclientslg 101项测试通过、构建通过；txgameslg/server 全量测试通过（/tmp/txgameslg-march-full.log），clustere2e/cluster-e2e 定向 race 通过。真实运行后补充 summary 的 marchRounds 元数据和重跑命令参数，已由随后全量测试编译验证；历史 artifact 的轮次以 JSONL 为准。

后置 ponytail-review 无额外删减建议：仅扩展现有驱动循环和必要证据校验。差异检查通过。本轮是串行重复调用验收，不报告吞吐提升；两路径客户端分别使用 TCP/WebSocket，不能用于 NATS/gRPC 性能横向比较。下一项评估将已核验的行军链路纳入固定频率和并发压测，明确响应、通知均到达的完成统计口径。

## 接续：SLG 双路径定频与完成统计

前批变更继续累积未提交。本轮在同一真实集群驱动上增加 --march-rate（每路径计划速率，默认0闭环）与 --march-concurrency（1或2，默认1），没有再建服务或替代业务处理器。两个固定角色会话各保留最多一个在途请求；并发2表示两条路径同时运行，不代表同一 WorldActor 并行或多角色容量。每个世界分别开启负载窗口。

定频使用绝对时间槽，调度迟到或处理过慢时跳过已错过的槽，未发出计入notSent，不补发追赶。march_load JSONL 记录计划、发送、完成、失败、未发送、实际耗时及延迟分位数；仅发送成功、业务接受响应和对应世界通知全部抵达才计完成。发送、响应、通知的失败分别收敛到该次失败，等待已开始的等待项及另一条并发路径收尾。出现失败或漏发时保留计数并让验收失败；漏发不混入请求延迟样本。继续复用现有延迟汇总函数，不在生产路径添加采集。

真实命令 `go run ./cmd/cluster-e2e --march-rounds 20 --march-rate 10 --march-concurrency 2 --artifact-root /tmp/txgameslg-march-rate-20260920` 通过。证据 `/tmp/txgameslg-march-rate-20260920/20260920-223233.097712000`：3个归属代次加双世界阶段，共10个路径窗口，每窗口20次，planned/attempted/completed合计200，failed/notSent均0。每路径每窗口只有20个延迟样本，观察到role→world窗口P99为7.579–12.531ms，直达世界为3.430–8.810ms；两路径传输分别为TCP/WebSocket且无容量负载，不能据此推导NATS与gRPC性能差距。原世界交接、权威归属、MsgQ重启与存储检查均通过；13个进程正常退出，2个为原套件预期故障注入，专用数据正常清理。

txclientslg 103项测试及构建通过，日志 /tmp/txclientslg-march-rate-{test,build}.log；txgameslg clustere2e/cluster-e2e 定向 race 通过。本轮未重复运行整个服务端测试集，相关路径由定向测试与完整真实集群验收覆盖。新增确定性测试约束超时槽跳过、不补发、操作未完成不计完成及失败/未发送分离。

后置 ponytail-review 无额外删减建议，差异检查通过。下一项评估多角色会话负载：当前两个固定会话的并发不是可任意扩容的客户端群，不能宣称SLG容量验证已完成。

## 接续：SLG 多角色独立会话负载

前批变更继续累积未提交。march-concurrency 从固定1/2扩展为正整数：1保持原双路径串行，N≥2创建N个不同角色会话，交替执行role→world与Agent→world直连，各角色最多一个在途请求、独立定频。前两个会话保留原TCP/WebSocket覆盖，新增会话通过正常Login分配Agent并立即TCP握手；没有复制生产Handler或新增压测子服务。

编排器复用既有角色夹具、ready文件协议和Redis证据读取，新增完整行军会话ready文件。证据等待受整轮context约束，不再受固定10秒限制；逐角色核对Session/MsgQ绑定。world-pool按世界、角色、轮次检查两种路径的权威owner及完整性，拒绝重复、越界角色和错误路径。关闭阶段收尾所有已创建会话，采集只在客户端驱动与测试编排中。

真实命令 `go run ./cmd/cluster-e2e --march-rounds 20 --march-rate 10 --march-concurrency 8 --artifact-root /tmp/txgameslg-march-roles8-20260920` 通过，证据 `/tmp/txgameslg-march-roles8-20260920/20260920-224144.533764000/summary.json`。3个归属代次及双世界阶段共40个角色窗口，planned/attempted/completed均800，failed/notSent均0；两条业务路径各400次，完成均要求业务接受响应及对应下行通知。核对36条Session/MsgQ绑定，其中4个行军阶段各8条、2个消息恢复阶段各2条。15个进程全部退出，13个退出码0，2个为原套件预期故障注入。

txclientslg 103项测试及构建通过，日志 `/tmp/txclientslg-march-multi-test.log`、`/tmp/txclientslg-march-multi-build.log`；txgameslg clustere2e/cluster-e2e定向race通过。本轮未重复服务端全量测试。后置ponytail-review：Lean already. Ship. 保留独立示例边界及会话/归属关键校验，没有新增公共抽象。

本轮为8角色、每角色10次/秒的低负载闭环验收，不是容量benchmark，不宣称性能提升。尚未增加建连爬坡和准备期心跳，不能直接将参数扩大解释为万连接能力。下一项评估将持续压测窗口与完整故障注入验收分开，复用现有编排及真实业务驱动，再进行分档扩容测量。

## 接续：独立持续行军窗口

前批变更继续累积未提交。本轮给既有cluster-e2e增加显式 `--march-only` 模式，复用8个正式服务进程、正常Login、多角色行军、Session/MsgQ证据及权威owner校验，默认完整验收不变。客户端跳过错误响应、断线恢复、改名、世界移动及通知广播场景；服务端跳过Login重启、写回阻断、故障注入和交接。单世界窗口结束后复用正常排水、实例退出及发现清空检查，成功清理专用数据，失败保留证据和数据。未增加旁路服务、替代Handler或生产统计逻辑。

真实命令 `go run ./cmd/cluster-e2e --march-only --march-rounds 300 --march-rate 10 --march-concurrency 16 --artifact-root /tmp/txgameslg-march-only-20260920` 通过，证据 `/tmp/txgameslg-march-only-20260920/20260920-232334.245584000/summary.json`：16角色窗口各约29.90秒，总planned/attempted/completed均4800，failed/notSent均0；role→world与直达world各2400次，16条Session/MsgQ绑定与世界归属检查通过。8个服务进程均退出码0，无故障注入；summary.marchOnly为true。

客户端新增用例先因未知march-only参数失败，实施后104项测试及构建通过，日志 `/tmp/txclientslg-march-only-{test,build}.log`。txgameslg/server全量测试通过（`/tmp/txgameslg-march-only-full.log`），clustere2e/cluster-e2e定向race通过（`/tmp/txgameslg-march-only-race.log`）。

这是总目标160次/秒的持续链路验证，不是吞吐上限或性能提升证据。延迟为实际发送至响应和通知均到达，包含首次懒加载；没有独立预热。storage-io快照覆盖客户端进程整体，包含登录准备，不解释为纯行军窗口。未增加准备阶段心跳和建连爬坡，不宣称一万连接或长期容量已验收。下一项评估复用客户端既有连接准备能力，再进行分档测量。

默认完整故障验收回归通过，证据 `/tmp/txgameslg-march-only-default-20260920/20260920-232509.791462000/summary.json`，marchOnly=false；15个进程退出，13个正常、2个为原套件预期SIGKILL。后置ponytail-review：Lean already. Ship. 共用既有驱动和排水，无新增公共抽象或依赖；差异检查通过。

## 接续：万会话准备屏障与首次目标负载尝试

用户明确：小于一万同时在线会话的运行只作冒烟。此前8/16角色结果只证明链路正确性，不作为容量或优化收益。本轮保留全部未提交变更，增加connect-rate（默认200次登录/秒）、即时握手和准备期正式Ping保活；按连接建立时间分散约10秒一次保活，不在测量窗口叠加准备流量。独立SLG客户端参照txclient既有流程实现，不跨示例导入。

仅行军模式在完整ready会话列表写出后等待start屏障，编排器逐Session回读Redis绑定后放行。客户端停止保活并等待在途Ping完成，核对全部连接在线，输出march_ready再开始测量。准备等待使用整轮剩余预算。客户端失败仍保留已经核验的Session证据，重跑命令保留建连速率与超时参数；没有修改生产准入、邮箱大小或请求超时。

实跑命令：`go run ./cmd/cluster-e2e --march-only --march-concurrency 10000 --connect-rate 200 --march-rate 10 --march-rounds 300 --timeout 20m --artifact-root /tmp/txgameslg-march-10k-20260921`。目录标签为20260921，实际本地开始时间为2026-09-20 23:33，证据 `/tmp/txgameslg-march-10k-20260921/20260920-233310.248655000`。

- 准备成功：march_ready expected=connected=10000；summary保留10000条已核验Session/MsgQ绑定，不是累计建连数。
- 目标负载失败：planned=3000000，attempted=37581，completed=28472，failed=9109，notSent=2962419。每角色仍最多一个在途，失败后停止该角色后续投递，因此notSent包含失败后未继续执行的计划，并非全是调度漏槽。
- role→world：发送10687，完成5870，失败4817；直达world：发送26894，完成22602，失败4292。完成要求响应与关联通知均抵达，客户端等待超时不能证明远端未执行。没有用失败前少量完成量计算稳定吞吐。
- Agent日志中有4292条 `actortrial: mailbox full` 异常关闭证据。当前全部请求集中到一个WorldActor，且每角色首请求集中起发，不能把该结果推广为多世界集群容量，更不能归因为NATS单独性能。
- 隔离于生产的ps旁路采样保存在process-resources.jsonl：客户端主Node进程CPU峰值99.7%、RSS峰值415432KiB；slgworld进程CPU峰值175.3%、RSS47340KiB；slggame进程RSS峰值691528KiB。百分比以逻辑核计，非整机利用率；采样跨准备与测量，不是精确窗口profile。Mongo健康探针也出现deadline超时。单客户端、单世界热点及同机依赖压力混合，尚未量化各自占比。
- 8个服务进程均退出码0。失败日志和统计保留；后续默认回归按ownership检查重新准备同一专用数据区，原数据库内容不作为长期证据，artifact保留。汇总见load-analysis.json。

客户端104项测试及构建通过，屏障用例确认放行前不投递行军；clustere2e/cluster-e2e定向race通过。下一项优先评估投递相位分散、压测端开销与单世界热点隔离；连接数继续保持至少一万，不通过降低连接数、放大邮箱或扩大请求超时掩盖失败。容量目标尚未通过。

本轮txgameslg/server全量回归通过（`/tmp/txgameslg-10k-full.log`）；默认完整故障验收冒烟通过，证据 `/tmp/txgameslg-10k-default-smoke/20260920-233740.661211000/summary.json`。该小规模回归仅验证未破坏原行为，不参与容量结论。后置ponytail-review：Lean already. Ship. 保留证据屏障、保活收尾及真实会话校验，未增加生产采集或新依赖；差异检查通过。

## 接续：按500连接步长定位负载拐点

用户本轮明确授权在问题未定位时从1000连接开始、每档增加500，作为诊断；此前一万连接为最终容量目标的要求不变。本轮不修改负载驱动或生产代码，不提交Git。两档均保持connect-rate=200、每角色10次/秒、300轮、单世界及同一两路径分配、timeout=10m；不改变邮箱容量和请求超时。

| 同时在线连接 | 计划 | 发送 | 双结果完成 | 未获得完整结果 | 未发送 |
|---:|---:|---:|---:|---:|---:|
| 1000 | 300000 | 49114 | 49114 | 0 | 250886 |
| 1500 | 450000 | 40591 | 40116 | 475 | 409409 |

两档march_ready及Redis绑定都确认完整在线数。1000档全部已发送操作完成，但因慢于时间槽导致大量未发送，整体失败；各角色窗口约30.00–30.54秒、各角色P50为600.39–620.55ms（并非合并后的全局P50）。role→world完成24500，直连完成24614，均无失败。1500档role→world完成17979、未获完整结果291；直连完成22137、未获完整结果184，Agent日志明确记录184次mailbox full关闭；超时不解释为远端未执行。未发送也包含失败后停止该角色后续计划。两档各8个服务进程退出码0。

证据目录：

- `/tmp/txgameslg-step-1000/20260920-235527.271213000`
- `/tmp/txgameslg-step-1500/20260920-235711.628420000`

每档均保存summary.json、client/march.jsonl、load-analysis.json与process-resources.jsonl。客户端CPU峰值分别87.5%/73.8%，world进程153.0%/161.8%；ps百分比按逻辑核计且跨准备与测量阶段，不能据此量化独立瓶颈份额。失败后数据按既有ownership流程用于下一档重新准备，artifact保留。

链路核对发现明确的串行限制：`tx/internal/actorhost/runtime.go` 的activation.run逐条调用Receive并等待返回；`txgameslg/server/services/slgworld/handler.go` 的StartMarch同步等待push.PubRole；后者经MsgQ Sender同步gRPC调用目标MsgQ。当前示例没有世界内部协程池。两种上行路径最终进入同一个WorldActor，且每角色首请求集中起发，不能将此次结果推广为NATS容量或整个实例的连接容量。世界归属检查使用Watch缓存，不是每条请求直接查询etcd；MsgQ热路由已有缓存，也不应无证据归因为逐请求Redis查归属。

1000档已无法达到目标速率，1500档出现邮箱溢出且完成量未增长，本轮停止继续增加到2000，不用更高失败档重复证明同一现象。代码和证据已经定位单世界串行等待这一限制，但尚未量化同步MsgQ、起发突发、压测端及同机依赖开销各自占比。下一项先评估同档均匀错开起发对照，再评估多世界分流；不直接修改Actor串行语义、业务并发模型或放大邮箱掩盖问题。

本轮仅执行上述真实诊断并更新文档，未重复代码单测。后置ponytail-review按文档变更简化：无新增代码抽象或依赖；保留失败证据及必要职责边界。差异检查通过。


## 接续：独立 RoleActor 邮箱的 Game 上下行基线

用户确认保留每个 RoleActor 独立邮箱协程，先测现有 Game 链路，不引入144 worker。此次只运行现有 cluster-perf 并更新证据，不修改生产或客户端代码；SLG网格分派的此前未提交改动不参与此结果。上行经过 Agent 的 Seq/Ack 处理与 gRPC 直达 Game，正常 Handler 同步返回 Pong，经 MsgQ 下行到客户端；不包含 NATS Actor RPC 或 JetStream 任务负载。1000/1500连接是定位档位，不代表万连验收完成。

复现命令（在 txgame/server 执行，第二档将1000改为1500）：

```sh
go run ./cmd/cluster-perf --concurrency 1000 --rate-per-session 10 --duration 30s --warmup 2s --timeout 12m --artifact-root /tmp/tx-role-step-1000
```

两档均启动两组 Login、Agent、MsgQ、Game，按现有每秒10次正常登录准备，会话证据屏障后预热2秒、空闲对照10秒、正式测量30秒。请求超时5秒，每会话单在途，错过的时间槽不补发；不改变邮箱容量、GOMAXPROCS或生产限流。环境：Go1.27.0、Node26.0.0、darwin/amd64、16逻辑CPU，客户端与八个服务及依赖同机。验收二进制沿用txbench标签隔离存储统计，不作为无采样生产性能承诺。

| 连接 | 计划 | 发送/完成 | 失败 | 未发送 | 完成/秒 | P50 ms | P99 ms |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000 | 300000 | 177272 | 0 | 122728 | 5909.07 | 165.85 | 312.60 |
| 1500 | 450000 | 165081 | 0 | 284919 | 5502.70 | 254.33 | 494.73 |

两档正常登录分别形成500/500和750/750的Agent与MsgQ归属；八个服务均正常停止，命令退出码0。工具status=passed仅证明实际发出的请求和集群检查通过，notSent分别为40.91%/63.32%，两档目标速率都未达成。1500档吞吐未增长，因此暂停继续增加2000档，不以更大超时、邮箱或降低请求频率掩盖结果。延迟只统计实际发送请求；当前统计不能直接量化各服务的队列深度。

证据：

- `/tmp/tx-role-step-1000/20260921-003739.906467000`
- `/tmp/tx-role-step-1500/20260921-004058.887105000`

每档保留summary.json、client.json、存储I/O窗口和服务日志；进程外ps采样在process-resources.jsonl，按start.measure与ready.json.measured文件时间筛选的汇总在load-analysis.json。CPU为ps滚动估值、100%约一个逻辑核，不能解释为精确窗口CPU时间：两档每个Agent均值约212%–219%，每个MsgQ约159%–162%，每个Game约138%–145%，客户端主进程约65%–71%。单个Game RSS峰值从约97MiB增至约123MiB。采样没有证明Actor协程数量本身是瓶颈，也未测量各角色邮箱积压。

1000档正式窗口的Agent/MsgQ Redis业务命令计数为0，Game各有1208次eval，Mongo除健康ping外为0；这是窗口累计而非逐请求分解，不能据此宣称整个链路零存储成本。下一项优先在相同真实负载下定位Agent/MsgQ/gRPC的调用放大、编码及调度开销，并结合客户端与服务端profile区分压测端限制；不先改成共享144协程池，不把结果当作NATS吞吐上限。

本轮仅新增文档证据，未重复代码单测。交付前ponytail-review按文档范围检查：复用现有压测入口，未增加生产抽象、依赖或采样路径。此前未提交的SLG网格功能仍需单独结算，不能由此次Game压测替代其验收。


## 接续：上下行 profile 与日志、调度单变量对照

本轮保持RoleActor独立邮箱、正式System_Ping/Pong、1000会话、每会话10次/秒、30秒测量、2秒预热、5秒请求超时、正常Login每秒10次及八服务拓扑。没有改业务Handler、队列、生产并行度或传输语义。新增txgame诊断能力：cluster-perf的`--log-level`默认为debug并记录实际配置；summary记录编排器有效goMaxProcs；`cmd/server/profile_txbench.go`仅在txbench且显式设置TXGAME_PROFILE_DIR时监听本机随机端口。普通构建go list仅包含main.go，诊断构建另含profile_txbench.go。采集与生产构建隔离。

源代码复核：当前连续Ping以下一条上行捎带下行Ack，Rec同步确认；客户端仅尾部显式Ack触发Confirm，未发现每条请求多发一次Confirm。主要RPC类型为Agent→MsgQ Rec、Agent→Game Dispatch、Game→MsgQ PubRole与Agent→MsgQ FetchSome。此结论来自调用链和客户端行为，不把四类RPC说成实测的每请求精确调用次数；FetchSome有长轮询及批量语义。真实链路不经NATS。

服务CPU采样20秒，争用样本使用测量开始/采样结束的mutex、block差分，另保存goroutine与heap；客户端通过临时Node inspector采样正式测量窗口，未改正式客户端代码。每档都有同样的诊断开销，不能与无profile结果直接归因为代码优化。

| 日志 | GOMAXPROCS | 发送且完成 | 未发送 | 完成/秒 | P50 ms | P99 ms |
|---|---|---:|---:|---:|---:|---:|
| debug | 未设置，默认环境 | 183804 | 116196 | 6126.80 | 158.59 | 313.95 |
| warn | 未设置，默认环境 | 186308 | 113692 | 6210.27 | 155.81 | 308.58 |
| warn | 2 | 286413 | 13587 | 9547.10 | 28.77 | 196.06 |
| debug | 2 | 278128 | 21872 | 9270.93 | 53.63 | 232.32 |

每档计划300000，已发送请求错误均为0，均核验1000条会话绑定且八服务退出码0。实际默认环境为16逻辑CPU主机；前三份旧版本summary尚未包含goMaxProcs，P=2由运行命令显式设置，末档新summary记录goMaxProcs=2。没有修改历史summary字段冒充自动采集。

对应证据目录依次为：

- `/tmp/tx-role-profile-1000/20260921-005911.817428000`
- `/tmp/tx-role-warn-1000/20260921-010328.828944000`
- `/tmp/tx-role-p2-1000/20260921-010655.058727000`
- `/tmp/tx-role-p2-debug-1000/20260921-011054.560422000`

汇总 `/tmp/tx-role-profile-comparison.json`；各档tiers/concurrency-1000/profiles保存六个服务CPU、heap、mutex/block前后样本及goroutine栈，旁边保存客户端cpuprofile。采集脚本 `/tmp/tx-role-profile.py` 与 `/tmp/txclient-profile.cjs` 为本地分析工具，不进入生产包。复现服务诊断用：

```sh
GOMAXPROCS=2 TXGAME_PROFILE_DIR=/private/tmp/txgame-profile go run ./cmd/cluster-perf --concurrency 1000 --rate-per-session 10 --duration 30s --warmup 2s --timeout 12m --log-level debug --artifact-root /private/tmp/txgame-profile-run
```

该命令只开启入口，采样须在start.measure之后用各PID.addr地址请求标准pprof；本轮还通过NODE_OPTIONS加载上述临时客户端采样脚本，以保证四档相同采样方式。

调查结论：

1. 热路径日志有真实成本。原1000档Agent日志合计约66MiB、MsgQ约42MiB；profile中Agent consoleHandler累计CPU约10.77%、MsgQ约15.80%，MsgQ-a采样锁延迟主要指向consoleHandler。Agent命令完成日志为Info且有现有测试契约，本轮不擅自改级别。debug→warn吞吐仅+1.36%，单轮差异不足以认定稳定收益，更不能解释全部卡点。
2. 同机多进程调度配置是已证实的重要放大因素。保持warn时P=2吞吐+53.73%；恢复debug后相对原debug仍+51.32%。同样约20秒CPU采样，warn的Agent findRunnable累计由10.27s降至2.04s，Game由7.86s降至2.55s，同时请求完成量增加。P限制同时执行Go代码的并行度，不减少Actor数量或改成两协程邮箱池。
3. 剩余热点主要包含gRPC loopyWriter、网络系统调用与调度；这些累计CPU类别有调用栈重叠，不能相加成瓶颈份额。客户端也有writeBuffer、微任务和编码成本，未通过分离客户端部署排除其上限。profile结果不支持先把RoleActor改为144 worker。
4. 仍未达标：P=2的warn/debug分别未发送4.53%/7.29%，不能宣布达到10000次/秒或万连通过。这是同机部署参数实验，不是库默认值调整，也不是多轮统计学结论或生产最优P值。下一项先固定日志、记录每进程并行度，在1000档做无profile复核与客户端/服务端资源隔离评估，再按500步长继续增压；不盲目改RPC协议或合并MsgQ。

验证：txgame/server全量go test通过，txbench构建的cmd/server、cmd/cluster-perf、internal/clustere2e定向race通过；四轮真实运行均正常排水。客户端未改代码，不重复其单测。交付前ponytail-review聚焦本轮诊断差异：复用标准pprof、现有模板和编排，无新公共SPI或生产采集；必要显式开关保留，不增加一套监控服务。差异检查通过。此前SLG网格未提交改动继续保留，不把本轮验证扩大到SLG。


## 接续：固定P=2的无profile分档复核

本轮只运行现有工具并记录证据，不修改运行时、客户端或生产默认配置。未设置TXGAME_PROFILE_DIR、NODE_OPTIONS或其他profile开关；保留既有txbench存储计数和进程外ps旁路采样，因此“无profile”不等于完全无采集。沿用debug日志、每进程GOMAXPROCS=2、正常Login每秒10次、每角色每秒10条、2秒预热、30秒测量、5秒请求超时。1000与1500档各自重建八服务集群，Session/角色区间不重叠；一角色一邮箱协程不变。

```sh
GOMAXPROCS=2 go run ./cmd/cluster-perf --concurrency 1000,1500 --rate-per-session 10 --duration 30s --warmup 2s --timeout 15m --log-level debug --artifact-root /tmp/tx-role-p2-unprofiled
```

| 连接 | 计划 | 发送且完成 | 失败 | 未发送 | 完成/秒 | P50 ms | P99 ms |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000 | 300000 | 288657 | 0 | 11343 | 9621.90 | 31.73 | 203.71 |
| 1500 | 450000 | 323637 | 0 | 126363 | 10787.90 | 123.73 | 306.87 |

未发送分别3.78%/28.08%。1000档与上一轮P=2的提升方向一致，但跨轮环境与采样条件不同，不把差值单独解释为profile开销。1500档吞吐仍增长约12.1%，但连接/目标速率增长50%，不能认定达标，也不能据此宣布硬吞吐上限。两档均核验完整会话，Agent及MsgQ分别500/500、750/750；16次服务退出均为0，编排命令退出0。工具passed不替代目标负载判断。

证据 `/tmp/tx-role-p2-unprofiled/20260921-093817.639641000`：summary.json、两档client.json、storage-io、process-resources.jsonl及load-analysis.json。runtime记录goMaxProcs=2、16逻辑CPU、Go1.27.0/Node26.0.0/darwin-amd64。各档按start.measure与ready.json.measured文件时间筛选ps样本，1000/1500档分别25/26份；百分比按逻辑核计且为滚动估计，不能当成精确CPU时间。

- 客户端主Node进程CPU均值约101.72%/94.62%，峰值116.4%/107.9%，RSS峰值177.10/209.11MiB。进程内还包含V8辅助线程，因此超过100%不表示单JavaScript线程超过一核，也不能单凭这个数值证明事件循环已饱和。
- 每个Agent均值约177%/170%，MsgQ约137%/123%，Game约127%/113%。客户端与服务端共享本机资源；两者限制尚未分离，不能把全部漏发归给tx服务端。
- 单个Game RSS峰值约92/120MiB，没有由此次证据推导每个Actor固定内存或万连内存上限。未运行到一万角色，也没有推翻既定RoleActor邮箱模型。

本轮暂停增加2000连接，避免在客户端供给能力未隔离时继续堆负载。下一项明确为：让同一套正式客户端协议驱动支持多个独立客户端进程，在相同1500会话、总登录速率及总请求计划下做1进程/多进程对照；共用准备和测量屏障，合并完整会话证据，分开保留各进程结果。全局P99必须合并原始样本或可合并直方图后计算，不平均各进程P99；任一子进程失败必须使整轮失败并收尾其余进程。该能力属于旁路压测工具，不增加压测专用Handler，也不改变Actor/MsgQ职责。多进程仍同机，不能等同于物理机器隔离；如仍受共享资源约束，再安排独立压测机。

本轮未改代码，不重复单测。真实链路两档与排水检查完成；ponytail-review按文档范围执行，未新增代码抽象或依赖，差异检查通过。全部未提交变更保留。


## 接续：共享 gRPC 连接并发诊断

复核代码：Agent 每连接独立上行协程，另有下行协程；写锁属于单个 Session。RPC 连接缓存锁不覆盖远端调用，ActorHost 的短锁不覆盖 Handler 执行，RoleActor 仍各有邮箱协程。没有发现全实例单协程处理所有 Session 的实现。

仅在 txgame 的 txbench 诊断文件中注册官方 gRPC Channelz 服务，复用已有 grpc 依赖，监听本机随机端口，地址写入 `<PID>.channelz.addr`。普通构建的依赖列表不包含 Channelz 服务或 pprof；官方 Channelz 包的 init 会启用计数，因此 txbench 即使未设置 TXGAME_PROFILE_DIR 也有计数成本，环境变量只控制诊断监听和争用采样。本轮不增加生产拦截器、公共 SPI 或连接池。

真实链路复测：1500 会话，每会话每秒10条，30秒、预热2秒、debug、GOMAXPROCS=2；开启六服务20秒 CPU 采样及 Channelz 五轮快照，未开启客户端 inspector。计划450000，完成289254，已发送失败0，未发送160746（35.72%），9641.8次/秒，P50 147.61ms、P99 333.42ms。完整1500会话、Agent/MsgQ各750/750，八服务退出码均0。目标负载仍未达标；诊断条件不同，不能把与前轮无profile的差值认定为性能退化。

证据：`/tmp/tx-grpc-channelz/20260921-100418.781043000`，各服务快照及分析位于 `tiers/concurrency-1500/channelz`，CPU、goroutine及争用样本位于 `profiles`。采样工具为本地 `/tmp/tx-channelz-sample.go` 和 `/tmp/tx-role-profile.py`，不复制被测业务。

- 每个Agent到每个MsgQ/Game均复用一条业务socket。单条Agent→MsgQ连接快照中存在约361～470个未结束流，证明多个RPC可以同时在途。这里包含FetchSome长轮询，不能把未结束流当成业务积压。
- 各业务连接的连接级流控窗口在五轮快照均有余量，窗口最小值约43KB；不能排除瞬时耗尽，也不能据此排除单流或TCP流控。未拿到的peerMaxConcurrentStreams字段不解读为0或无限。
- goroutine快照未发现NewStream流配额、writeQuota或controlBuffer.throttle等待栈。采样不证明全窗口从未等待；不能据此断言传输完全没有瓶颈。
- 20秒CPU样本中所有loopyWriter累计占Agent约10.82%、MsgQ-a约23.87%、Game-a约30.65%。这是多个连接的合计，不能推导单writer已饱和；也不与嵌套网络栈比例相加。暂不引入连接池，未进行连接池A/B实验。
- block差分只扣除已记录旧样本，跨越窗口起点的等待可能包含准备阶段。邮箱空闲、FetchSome长轮询等正常阻塞不等于瓶颈，累计阻塞秒数也不等于墙钟耗时。

本轮验证：txgame/server全量go test通过；txbench下cmd/server、cmd/cluster-perf、internal/clustere2e定向race通过。ponytail-review检查本轮诊断与说明，复用官方能力，无需新增封装或删除必要诊断。下一项仍是相同1500总会话和负载下的客户端多进程对照，先隔离客户端供给限制，再决定是否调传输连接；本轮不扩大到SLG容量验收。


## 接续：1500会话客户端分进程与日志成本对照

本轮新增的是旁路负载能力，不修改生产 Actor、Session、MsgQ、RPC 或日志等级。txgame `cluster-perf --client-processes N` 传给 txclient 的 `--processes N`，默认1；角色区间不重叠，总会话和请求计划不变，总建连速率按分片比例拆分。所有分片汇总 ready、warm、measured 后才交给原 Go 编排器，共用 start 屏障及完整 Redis 会话证据。子进程复用 runClusterPerf/AgentClient；原始延迟样本在测量后保存并合并，不平均分位数。失败时停止其他子进程并等待退出，Go 直接管理 Node 父进程，避免 npm 包装层截断终止信号；超时未确认退出会明确报错。

固定拓扑为Login、Agent、MsgQ、Game各两个独立进程，全部与客户端、依赖运行于同一台8物理核/16逻辑核Intel机器。固定1500会话、每会话10次/秒、30秒测量、2秒预热、每秒总计10次登录、每个Go进程GOMAXPROCS=2。不启用pprof或Node inspector，保留txbench已有计数及进程外ps采样。每Session最多一个在途Ping：等待Pong期间错过的时间槽计入notSent，不能将漏发直接解释为连接建立失败。

| 客户端进程 | 日志 | 完成 | 未发送 | 完成/秒 | P50 ms | P99 ms |
|---:|---|---:|---:|---:|---:|---:|
| 1，首次 | debug | 228336 | 221664（49.26%） | 7611.20 | 192.74 | 433.62 |
| 2 | debug | 340790 | 109210（24.27%） | 11359.67 | 126.20 | 256.12 |
| 1，复跑 | debug | 309211 | 140789（31.29%） | 10307.03 | 137.01 | 282.33 |
| 2 | warn | 440752 | 9248（2.06%） | 14691.73 | 74.01 | 183.18 |

四轮计划均450000，已发送失败均0，各轮完整1500会话和八服务退出码0。单客户端两轮明显波动，不能将第一次到双进程的49.25%视为稳定收益；相对单进程复跑，双进程约增加10.21%。首次仍通过npm启动，后三轮已直接Node启动；服务二进制未改变。双进程warn相对debug增加29.33%，支持在客户端限制减轻后重新评估逐消息日志成本，但这是单机单轮对照，不宣称生产稳定增益。仍有2.06%漏发，未达到15000次/秒，更不代表万连验收。

证据目录：

- `/tmp/tx-client-process-one/20260921-102615.544574000`
- `/tmp/tx-client-process-two/20260921-103014.294937000`
- `/tmp/tx-client-process-one-repeat/20260921-103549.709587000`
- `/tmp/tx-client-process-two-warn/20260921-103952.518409000`

各目录保存summary、client、process-resources.jsonl和analysis.json；双进程另存分片结果、PID、原始latencies。已独立合并debug档340790个原始样本，P99与最终汇总一致。四连接双客户端真实冒烟仅验证编排，不作为容量证据。

资源与链路定位：

- 单客户端复跑主Node CPU均值约100.64%，双客户端debug各约81.04%/81.74%，warn各90.82%/91.38%；进程包含辅助线程，不能将进程CPU直接等同JS事件循环占用。两个Agent在后三轮均约179～180%，每个MsgQ约118～129%，每个Game约111～146%。增加同机实例不等于增加物理CPU；未分离独立压测机与服务端资源。
- 单客户端复跑Agent业务转发日志的durationMs为P50 12ms/P99 68ms，而客户端完整Ping/Pong为P50 137.01ms/P99 282.33ms。该日志只包围Dispatch，不含此前Rec以及完整下行/客户端处理。不同分位数不能相减推导某阶段耗时，后续应沿边界采样，不能仅把延迟归给RoleActor串行邮箱。
- MsgQ FetchSome无消息等待时已释放Session锁；没有发现持有全局锁等待下行的路径。各Session与Actor独立执行的设计保持。
- 核验日志位置时曾误将txgame/services/agent/actor_dispatcher.go与tx/modules/agent/actor_dispatcher.go混淆；前者确实在125行记录成功命令Info日志。GODEBUG=gocacheverify=1重编译成功，产物哈希与双进程实验一致，排除该疑点，不以错误文件定位认定缓存失效。

下一项：继续拆分Agent接收/Rec、业务Dispatch、MsgQ下行及客户端读取的等待，重点评估逐消息日志的编码与输出成本。成功命令Info日志目前有示例契约，未擅自改级别；warn仅作隔离实验。先定位剩余漏槽和可接受的日志策略，再按500连接步长增加负载，不直接升级到万连。

验证完成：txgame/server全量go test及txbench下cmd/server、cmd/cluster-perf、internal/clustere2e定向race通过；txclient 86项测试及构建通过。关键新测试覆盖角色分片、总建连速率、原始样本合并以及一个子进程退出后其他子进程被终止且已退出。交付前ponytail-review检查本轮差异：复用Node原生fork、现有客户端驱动和屏障，无新增依赖或生产抽象，未发现需要删除的多余封装。三个本轮修改工程diff检查通过，全部变更不提交；此前SLG未提交功能保留，本轮结果不作为其容量验收。


## 接续：Pong时间戳分段与事件循环对照

新增客户端显式诊断 `TXCLIENT_PERF_TIMINGS=1`：正式窗口每64次尝试抽样一次，读取现有Pong.serverTime，保存开始发送→Game生成Pong、Game生成Pong→客户端Ping等待结束的两个墙钟阶段；同时使用Node标准monitorEventLoopDelay（10ms分辨率）与eventLoopUtilization。只在测量结束后写入各负载进程的 `.timings.json`，不改协议、Handler或服务端默认配置。负值保留并计数，不截断为0；客户端和Game必须共享已核验时钟。前段包含客户端发送、Agent接收/Rec、路由与Actor等待，后段包含响应编码/发布、MsgQ/Agent下行及客户端解码/调度，不能等同于纯网络或纯业务计算。标准事件循环直方图不是每请求延迟，利用率也不等于CPU百分比。

继续固定1500会话、每会话10次/秒、30秒测量、2秒预热、总登录速率10/s、warn、GOMAXPROCS=2、八服务同机；两轮均开启相同客户端阶段诊断，无服务端pprof。只改变客户端进程数。

| 客户端进程 | 完成 | 未发送 | 完成/秒 | 全量P50 ms | 全量P99 ms |
|---:|---:|---:|---:|---:|---:|
| 2，每进程750会话 | 441884 | 8116（1.80%） | 14729.47 | 57.29 | 175.90 |
| 4，每进程375会话 | 431213 | 18787（4.17%） | 14373.77 | 81.84 | 198.14 |

两轮均已发送失败0，完整1500会话，八服务各自正常退出，共16次退出码0。仍未达到15000次/秒，不将passed或小规模冒烟替代目标负载达标。两轮为同机单次对照，不用于推导独立机器部署的容量上限。

| 阶段抽样 | 双客户端平均/P50/P99 ms | 四客户端平均/P50/P99 ms |
|---|---|---|
| 开始发送→Game生成Pong | 37.32 / 35 / 108 | 56.70 / 53 / 156 |
| Game生成Pong→客户端Ping等待结束 | 22.59 / 19 / 81 | 23.88 / 20 / 74 |

双/四客户端分别保留6905/6740个样本，负值样本均0；每64次抽样不是全量统计，不直接与全量P99相减求阶段占比。双客户端事件循环利用率各约93.6%，循环直方图P99约31.0/31.8ms；四客户端各约69.0%～69.9%，P99约12.4～12.9ms。客户端循环等待确实下降，但吞吐下降约2.41%，Pong前段等待反而增加，不支持继续堆同机客户端进程。它也不证明Rec或Actor队列已被单独定位。

进程外CPU均值：双客户端负载进程合计约178%，四客户端合计约237%；双/四客户端时各Agent约176～177%/171～172%，各Game约139～142%/135～137%，各MsgQ约115～116%/107～109%。这是按逻辑核计的滚动采样，不能转成物理核配额或直接证明CPU饱和；各进程共享同一物理机器，增加客户端并不是无成本的独立供给隔离。

代码复核：Agent的Rec完成后进入Dispatch，下行FetchSome独立协程；MsgQ续期网络等待与FetchSome无消息等待均释放会话锁。没有发现固定sleep或全Session单协程处理。当前停止增加同机客户端数量，后续诊断沿用两个客户端；生产默认进程数仍为1，Actor与Session并发模型不变。

证据：`/tmp/tx-stage-timings-two/20260921-110518.450909000`、`/tmp/tx-stage-timings-four/20260921-110918.817780000`。各目录保存summary、各进程timings、timing-analysis.json、process-resources.jsonl、cpu-analysis.json及服务退出文件。

下一项需要评估服务端分段采样的接入点，区分Agent接收/Rec与Dispatch的等待。现有墙钟阶段不能完成这一步，也不以两个独立分位数相减代替实测。优先复用已有诊断或SPI，不能为了压测在业务装配中复制默认路由逻辑，或新增生产公共接口。继续保留现有日志契约，不将降低日志等级当作已实施的生产优化。

验证：txclient完整87项测试及构建通过；新增测试验证时钟偏差负值保留、异常计数与抽样数量。两轮真实txgame链路和16次服务退出检查通过。服务端Go代码本轮未修改，不重复其单元测试。交付前ponytail-review检查本轮新增诊断，复用现有Pong字段、标准Node指标及已有结果文件流程，无新增依赖、公共SPI或重复业务实现；未发现需要删除的多余封装。三个本轮修改工程差异检查通过，全部变更不提交。

## 2026-09-21 RPC 分段采样：区分确认状态更新与远程等待

用户提出 Agent↔MsgQ ACK 是否形成吞吐上限，先核验 rj3d 参考源码：普通请求交给 Executor、场景请求交给 MapThread 后网关返回，下行直接 Netty writeAndFlush；已检查的通用帧没有 tx 式 Seq/Ack，channel 不可写时发送直接返回。该链路不具备与 tx 相同的通用补发契约，不能据此删除 MsgQ 或确认语义。

本轮只增加 txgame `cmd/server/rpc_timings_txbench.go`：通过当前 gRPC 版本的官方实验性 Binary Log Sink 获取事件，固定过滤 MailboxService 与 DeliveryService/Call，h:0/m:0 禁止保留元数据与正文；普通构建排除整个文件。每方法、每端每64个头事件抽样一次，同进程单调时钟配对终止事件并直接落盘；取消后释放等待项，trailer 后取消不重复计入。不新增 tx API/SPI，不覆盖默认工厂或复制路由，不改 ACK、Handler、Actor 邮箱、日志等级默认值。两轮都未开启 pprof 或锁采样，保留已有 txbench Channelz 包及客户端阶段诊断。

需注意：官方日志仍为所有选定调用构造事件，1/64仅降低保留样本和落盘量，不代表总开销也是1/64。客户端头事件晚于 transport 选择和 newStream，因此不覆盖前置寻址/建流等待；服务端区间含消息接收、解码及处理，不能当作纯 CPU 时间。FetchSome 正常长轮询必须单独解释。gRPC 状态不是响应体业务状态，采样成功不能推导所有调用成功；真实完成和失败以客户端结果为准。

采集轮：`/tmp/tx-rpc-stages/20260921-114705.938949000`。8个正式进程、1500会话、2个客户端进程、总建连10/s、每会话10/s、预热2s、测量30s、GOMAXPROCS=2、warn；计划450000、完成352590、失败0、未发送97410（21.65%），完成11753/s，完整P50 121.20ms/P99 258.57ms。8个服务退出码均0，会话归属证据完整。沙箱内首次启动因监听权限被拒，未进入负载；获准本机监听后重跑，上述数据来自实际运行。

正式屏障放行后第1～29秒取28秒内部窗口，以调用开始时间选样，完整保留其终止耗时；排除准备和收尾，不用独立分位数相减。

| RPC | 调用端样本 | 调用端P50/P99 ms | 服务端P50/P99 ms |
|---|---:|---:|---:|
| Rec | 5154 | 32.727 / 94.150 | 0.0209 / 0.1344 |
| PubRole | 5145 | 12.377 / 77.037 | 0.0243 / 0.0965 |
| Actor Call | 5141 | 67.944 / 185.877 | 12.614 / 81.822 |
| FetchSome | 5149 | 121.512 / 257.421 | 85.225 / 199.498 |

各端独立抽样，不能配成同一次调用。内部窗口四类客户端样本乘64约329024～329856次，各类规模相近，支持当前闭环负载下接近每次Ping各一次的调用放大判断；这不是精确调用计数，也不能据此否定FetchSome的批量能力。窗口内采样没有gRPC错误或取消，低频Confirm未采到不能解释为全程零次。

结论：本轮样本不支持“MsgQ 内部更新 ACK 状态耗时主导”的判断，Rec 与 PubRole 服务端区间均很短；调用端等待显著更长，RPC往返、传输及调度值得继续拆分。Game端Call还包含等待PubRole，不能当作RoleActor纯计算成本；FetchSome含等待下一条业务下行，不可与其他串行步骤简单相加。采集会扰动负载，必须结合关闭采集对照，不能将诊断轮数值直接当作原链路瓶颈份额或生产容量。

原始样本归档在上述目录 `rpc-timings/`，分析结果 `rpc-analysis.json`，重算脚本 `analyze_rpc_stages.py`；运行时输出原目录 `/tmp/tx-rpc-stage-samples`。对照同一进程PID，分析无需业务消息内容。

关闭采集对照：`/tmp/tx-rpc-stages-control/20260921-115110.413734000`，去掉 `TXGAME_RPC_TIMINGS_DIR` 和 `GRPC_BINARY_LOG_FILTER`，其他参数及客户端诊断相同。计划450000、完成431513、失败0、未发送18487（4.11%）、14383.77/s，完整P50 79.29ms/P99 213.44ms；8服务均退出0，1500会话证据完整。采集轮相对对照吞吐低18.29%，说明全量事件构造与诊断本身有显著扰动；只有一组顺序对照，不能把全部差值精确归因于采集，更不能用诊断毫秒数计算未采集链路的瓶颈份额。这是诊断工具开关差异，没有生产代码退化或性能优化的结论，容量仍未达标。

本轮验证：普通及txbench的cmd/server测试通过；`go test -race -tags txbench ./cmd/server`通过。`go list`确认普通构建只有main.go，txbench构建才包含profile与rpc_timings文件。本轮没有修改其他业务代码，未重复全库测试或SLG压力验证。后置ponytail-review已检查本轮Sink、单项配对/取消测试及文档，无需增加公共拦截器接口，也无额外可删包装；差异检查通过。

下一项需要评估：在既有同进程直调能力上，保持确认/补发契约和负载不变，设计Agent+MsgQ同进程对照，固定总体CPU预算并关闭Binary Log，量化消除Rec/FetchSome远程传输的收益。进程数变化会改变调度和资源分配，不能把差值全当作网络成本。不擅自合并服务职责、不跳过Rec，也不先添加连接池；本轮未实施这个部署对照。

## 2026-09-21 Agent+MsgQ 合部署对照

本轮沿既有能力增加 `cluster-perf --colocate-agent-msgq`，默认仍启动八个进程。合部署为两个 agent,msgq、两个 game、两个 login，共六进程；复用 Agent service 的 LocalMsgQ/FixedSessionClientRouter，Rec、FetchSome、Confirm 等会话操作本地调用，Game→MsgQ 仍通过 gRPC。普通上行仍由 Agent 通过缓存寻址后 gRPC 调用 Game，不增加 NATS 上行，不跳过序号和确认，不修改 tx 业务路径。

合部署时每个 agent,msgq 的 GOMAXPROCS 取编排器有效值两倍，其他实例不变；默认分进程也显式传入该值，各进程 services/goMaxProcs 写入summary。基准为八进程×2，合部署为两进程×4加四进程×2，总并行度均16。GOMAXPROCS不是硬CPU配额或物理核隔离，两种模式仍有调度、进程数、连接与首次会话归属选择的差别，不能把全部收益归因于网络I/O。存储窗口采集、成员数量和成功排水使用实际拓扑；同一role跨Agent重连不在本次新角色压测范围内。

16会话冒烟：`/tmp/tx-colocate-smoke/20260921-120319.189366000`，320请求全部完成、零未发送/错误，两组Agent与MsgQ归属各8会话且实例ID一致，六进程正常停止。这只是接线验证，不是容量证据。

正式合部署：`/tmp/tx-colocate-1500/20260921-120401.663577000`。1500会话、两个客户端、每会话10/s、总建连10/s、2s预热、30s正式、warn、TXCLIENT_PERF_TIMINGS=1；不启用RPC Binary Log、pprof和锁采样。计划450000，完成447692、未发送2308（0.513%）、已发送错误0，14923.07/s；完整P50 5.273ms、P95 47.058ms、P99 116.492ms。目标到达率为15000/s，吞吐接近输入上限，不能视为最大处理容量。

正式窗口第1～29秒的ps资源采样共27个点：两个agent,msgq均值约250.14%/250.74%，两个Game约169.10%/164.87%，两个客户端约89.91%/90.84%。CPU百分比按逻辑核计，ps值有平滑，不能当作精确CPU时间；各进程RSS是观察值，不把总RSS解释成去重物理内存。采集脚本在旁路读取进程，不向生产路径增加计数。证据保存于各轮process-resources.jsonl、cpu-analysis.json。

同版本八进程对照：`/tmp/tx-separate-1500/20260921-120750.115549000`，负载和开关保持相同，八进程各GOMAXPROCS=2。计划450000、完成415636、未发送34364（7.636%）、已发送错误0、13854.53/s；完整P50 76.836ms/P95 171.673ms/P99 234.171ms。两个Agent的CPU均值174.81%/175.84%，两个MsgQ为112.71%/112.91%，两个Game为140.69%/138.11%，两个客户端88.68%/88.56%。两轮均有1500会话证据与完整归属计数，合部署6/分进程8个服务全部退出0，各轮sum(goMaxProcs)=16。

合部署相对本轮分进程完成吞吐+7.71%，P50下降93.14%，P99下降50.25%。这是同机单轮对照，没有稳定生产收益或最大容量的结论；合部署仍有0.51%未发送，不能宣布目标完全通过。Agent+MsgQ合计CPU约500.88%，分进程对应四个进程合计约576.29%；合部署Game同时更忙，整体CPU不能只取一组进程估算节省。低延迟变化支持继续关注跨进程调用与调度组合成本，但也可能受执行槽从Agent2+MsgQ2变为共同4影响；本轮不能分离这两项贡献，不把ACK状态更新算法判为根因。

验证：txgame/server全量go test通过；internal/clustere2e与cmd/cluster-perf定向race通过；准备/清理/成员计数的原有用例扩展覆盖两种部署模式，新增一个拓扑与总并行度预算检查。沙箱内端口测试先因监听权限失败，获准本机监听后重跑通过。Ponytail-review未发现额外可删包装，无需新增生产工厂或拦截器SPI，差异检查通过。本轮只修改压测编排和文档，无生产实现变更，不报告删行收益。

下一项需评估固定Agent并行度的补充对照，或低扰动Go调度采样，进一步区分远程传输与执行槽共享；保持默认分进程拓扑和确认契约。暂不直接修改ACK协议、放大连接池或以合部署结果替代分进程容量验收。

## 2026-09-21 分层容量：固定总请求量改变会话数

本轮采用用户确认的本机分层排查，不能将缺乏原始报告的商业游戏指标当作统一行业门槛，也不按核数把本机容量等比例外推至生产服务器。机器核验为i9-9880H、8物理核/16逻辑线程、32GiB内存；服务、两个Node负载进程与Docker依赖同机，测试描述的是这套部署的容量。

短轮统一为默认八服务进程、各GOMAXPROCS=2、两个客户端、总建连50/s、warmup=2s、duration=30s、warn、TXCLIENT_PERF_TIMINGS=1，未开启Binary Log或pprof。连接准备不计入正式结果；与此前10/s建连的数据不混为严格单变量对照。各档计划300000次，业务仍是原有System_Ping/Pong，不能代表广播、实际AOI或战斗负载。

| 会话×每会话频率 | 完成 | 未发送 | 已发送失败 | 完成/s | P50 ms | P99 ms |
|---|---:|---:|---:|---:|---:|---:|
| 1000×10/s | 299933 | 67 | 0 | 9997.77 | 3.718 | 100.036 |
| 5000×2/s | 299972 | 28 | 0 | 9999.07 | 17.172 | 231.459 |
| 10000×1/s | 299999 | 1 | 0 | 9999.97 | 7.703 | 198.631 |

证据分别为 `/tmp/tx-fixed-qps-1000/20260921-123822.966038000`、`/tmp/tx-fixed-qps-5000/20260921-124024.241238000`、`/tmp/tx-fixed-qps-10000/20260921-124400.643067000`。每档会话归属证据完整，24个服务进程均退出0。第一档旁路CPU脚本误用旧1500目录，没有资源样本；仅吞吐/延迟/归属有效，不能填入历史CPU值。脚本已按实际tier识别，后两档保留process-resources.jsonl及fixed-qps-analysis.json。

5000/10000档正式窗口中，两个Agent CPU均值分别约174/173%与172/172%，MsgQ分别约131/131%与134/137%，客户端约78/79%与74/75%。万连观察到Game各约586MiB RSS、Agent约274～281MiB、MsgQ约121～122MiB；这些是含运行时的进程RSS，不是每角色GD大小，也不是去重物理内存。30秒内RSS增长不能直接认定泄漏，尚未完成长期稳态验证。

三档都接近目标吞吐，但没有满足建议的“P99≤100ms且零未发送”筛选条件，不能标为容量验收通过。1000档也不能把100.036ms四舍五入后宣称达标。每Session仍只有一个在途Ping，500ms/1000ms请求间隔比100ms更能容纳延迟，所以后两档漏发较少并不代表响应更快。5000与10000的延迟没有单调随人数增长，单轮结果不足以作线性推断。

已核验PubRole按角色会话集合发布，并未发现逐消息遍历全体会话的路径；验收模板的过期会话清理间隔为1s，会话增加确实增加后台扫描量，但本轮未量化其CPU份额，不据此修改扫描策略。后续保留万连，降低请求量寻找尾延迟余量。

为支持万连低频档，现有 `--rate-per-session` 改为有限非负浮点数（Go侧0保持闭环；客户端正频率参数拒绝0，未传仍为闭环）。例如0.5表示每会话两秒一次；两端统一按每会话 `floor(durationMs*rate/1000)` 后乘人数计数，不先汇总再取整。不增加压测业务或改协议。客户端解析及Go计划取整关键用例通过，txclient 88测试及构建、txgame/server全量测试及编排定向race通过；首次客户端tsx因沙箱IPC监听权限失败，获准后重跑通过。

万连低频短轮：`/tmp/tx-10000-rate-half/20260921-125158.801747000`，其他条件不变，频率0.5/s、总5000/s；计划150000，完成150000，未发送0、已发送错误0，完整P50 1.304ms/P95 5.700ms/P99 21.653ms/max 76.056ms。10000会话归属完整，8个服务均退出0。通过本轮短筛选门槛，但此时不能宣称长期稳态通过；同档十分钟验证结果见下文。

这一结果支持降低活跃负载后可保留万连接，不能证明当前机器的极限等同于生产硬件极限，也不能据此给MMO/SLG作人数换算。后续提升请求量时仍需独立验证；Ping响应没有扇出，下行广播与实体间Core NATS/JetStream压测保持独立。


### 万连低频十分钟：计数通过，长尾未达标

实测产物：`/tmp/tx-10000-half-steady/20260921-125742.867635000`。保持上述默认八进程、双客户端、每进程 GOMAXPROCS=2、warn、建连50/s及客户端阶段采样；关闭 gRPC Binary Log 和 pprof。10000会话、每会话0.5次/s，正式测量600秒。计划与完成均3000000，未发送0、已发送错误0，吞吐5000/s；完整P50 2.392ms、P95 76.810ms、P99 189.819ms、最大670.554ms。10000会话证据完整，八个服务退出码均0。

短轮P99为21.653ms，长轮升至189.819ms，**未通过本轮暂定P99≤100ms的稳态门槛，不能建立已达标的本机容量基线**。计数和延迟应分别验收；进程正常退出不代表延迟目标通过。这只是当前同机Ping链路，不代表生产SLG/MMO、广播、Core NATS或JetStream容量。

将每个客户端完成顺序的延迟样本等分十段、合并对应段后，首段P99约87.87ms，后九段约169.10～261.32ms；这些是等完成量分段，并非严格一分钟时间窗。没有持续单调恶化趋势。进程资源按测量时间采集，Agent RSS约285～292MiB保持平稳，MsgQ约120～126MiB、Game约504～585MiB增长后趋稳；客户端保存原始延迟样本，内存不可与生产服务直接比较。RSS趋稳不能单独证明不存在泄漏。

客户端阶段抽样46876条，生成Pong之前P99由短轮16ms升至长轮145ms，生成之后由7ms升至49ms；客户端事件循环P99约15ms。不同区间分位数不能相减，这些结果只能缩小排查范围，尚不能在Rec、Dispatch、Actor排队或调度之间归因。服务端CPU采样均值：每个Agent约159%，MsgQ约138%，Game约137%；ps百分比不是硬配额或精确CPU耗时。

代码核查：PubRole按目标角色的Session集合发布；TerminateDeadSessions按配置每秒扫描会话并调用maintainRoles，后者遍历本地角色租约、到期时续租。周期维护是待测候选，尚无证据将本轮长尾归因于它；不先修改租约或ACK契约。

下一项明确为同档长窗口的低扰动时序诊断：围绕Pong生成前路径关联调度/GC及周期维护时点，保留关闭诊断对照；先区分持续运行与同机资源竞争，不继续增加会话或修改协议。必要时再评估客户端/服务端物理隔离。

本轮验证：txclient 88项测试及构建通过；txgame/server全量测试通过，internal/clustere2e与cmd/cluster-perf定向race通过。本轮未修改tx生产代码，也未重新运行其全量测试。ponytail-review检查小数频率改动：复用现有负载循环及标准数值解析，无需新增调度抽象，保留跨语言计划计数契约测试；未发现需要删减的复杂度。


### 2026-09-21 长窗口时序诊断：系统性能限制与调度排队

本轮仅修改txclient旁路采集：在既有TXCLIENT_PERF_TIMINGS开关下增加窗口墙钟起点、阶段样本相对时间和每秒完成数，依次切分已有原始延迟数组得到时间窗分位数，不另存每条请求时间戳或复制生产Handler。两个客户端起点相差约1ms；尾部在途完成单独计入第600秒，分窗样本合计2999999，与实际完成数一致。服务端复用Go原生`GODEBUG=schedtrace=1000,gctrace=1`，未新增tx接口、未开启pprof/Binary Log。每秒采集ps、每十秒采集macOS `pmset -g therm`，脚本与分析只保存在本轮artifact。

产物：`/tmp/tx-10000-time-diagnostic/20260921-150115.377472000`，含summary、time-analysis、原始服务日志、thermal.jsonl与thermal-after.txt。保持默认八服务、双客户端、GOMAXPROCS=2、warn、建连50/s；万连×0.5/s持续600秒。计划3000000，完成2999999，已发送错误0、未发送1，吞吐4999.998/s；P50 2.793ms、P95 139.257ms、P99 313.338ms、最大919.792ms。10000会话证据完整，八服务退出0；仍未通过延迟和零漏发门槛。

| 正式分钟 | 完成样本P99(ms) | Pong生成前抽样P99(ms) | 两个Agent可运行队列均值 | 系统CPU_Speed_Limit采样范围 |
| --- | ---: | ---: | --- | --- |
| 1 | 394.07 | 226 | 11.80 / 20.15 | 35～100 |
| 2 | 475.79 | 324 | 30.85 / 42.02 | 35～52 |
| 3 | 154.89 | 121 | 11.10 / 7.66 | 45～50 |
| 8 | 451.71 | 325 | 47.08 / 67.93 | 37～45 |
| 10 | 276.53 | 217 | 28.56 / 26.20 | 41～50 |

完整十分钟分窗保存在time-analysis.json；以上分位数按完成时点归窗。Go日志相对时间以PID文件创建时刻近似对齐，适合秒级比较，不用于精确因果追踪。可运行队列统计合并全局与每P队列；不是邮箱长度，也不是正在运行的协程数。

系统准备期CPU_Speed_Limit为100，正式窗口60次采样中59次低于100，首次记录限制约在正式第15秒；停止负载后恢复100。CPU_Scheduler_Limit保持100，可用CPU为16，未报告热警告。因此确认的是系统报告的CPU性能限制，不能换算成实际GHz、直接断言温度原因或线性外推服务器容量。此证据支持将同机持续资源竞争列为优先验证方向，尚不证明它是唯一原因。

Agent/Agent-b可运行队列峰值533/717，GC停顿和最大4.320/3.705ms；两MsgQ最大停顿5.008/3.940ms，两个Game最大1.450/1.690ms。样本不支持单次GC停顿直接解释百毫秒长尾，但不排除并发GC和分配成本。角色租约TTL=10m、半租期刷新，普通消息不逐条续租；当前未采集维护起止事件，不能排除或证明其贡献。

与上一轮关闭Go诊断的P99 189.82ms相比，本轮313.34ms更高；系统状态和采集同时变化，不能认定代码性能退化或量化诊断开销。下一项保持万连×0.5/s和相同诊断，只将每服务GOMAXPROCS从2改为1，检验同机并行度过量的假设；随后需要关闭Go诊断复核。均属压测参数实验，不改变生产默认值。如果仍受系统限制，应评估压测客户端/依赖与服务端物理隔离，不能继续据此修改ACK协议。

本轮验证：txclient完整88项测试与构建通过，变更涉及工程diff --check通过。Go生产代码未改，未重复运行Go全量测试。ponytail-review：复用已有采样开关和原始样本，未新增依赖或通用监控框架，未发现本轮需删除的复杂度。全部变更保持未提交。


### 2026-09-21 单变量并行度对照：P=1未解决长尾

保持万连×0.5/s、600秒、八服务/双客户端、warn、50/s建连及原生Go调度/GC诊断，仅将每服务GOMAXPROCS从2改为1。产物：`/tmp/tx-10000-p1-diagnostic/20260921-152017.818417000`。计划/完成均3000000，错误0、漏发0，吞吐5000/s；P50 2.314ms、P95 97.913ms、P99 287.481ms、最大972.489ms。10000会话证据完整，八服务退出0。

相比上一轮同诊断P=2的P99 313.338ms，本轮约降低8.25%，仍未通过≤100ms门槛，不能据此调整生产默认值。CPU采样均值：两个Agent约83.6%/83.4%，两个MsgQ约63.5%/62.7%，两个Game约67.8%/67.5%；此百分比是ps统计，不能代替精确CPU耗时。系统速度限制60次采样中58次低于100，范围47～100，限制仍存在。

真实分钟P99依次约167、367、338、160、232、238、529、222、187、201ms；不存在简单的持续单调恶化。Agent可运行队列峰值400/426；所有服务GC停顿和最大低于0.7ms，仍有明显长尾。这进一步削弱单次GC停顿主导的假设，但不排除分配、并发GC或调度成本。单轮P99小幅差异不作为稳定收益，后续先关闭Go诊断复核，不继续零碎调并行度。


关闭Go诊断复核产物：`/tmp/tx-10000-p1-control/20260921-153529.838656000`。仅取消GODEBUG，保留P=1、客户端时间分窗、ps及系统限制采样，其余参数与上轮一致。计划/完成均3000000，错误0、漏发0，吞吐5000/s；P50 8.399ms、P95 280.401ms、P99 629.860ms、最大1585.688ms。10000会话证据完整、八服务退出0；客户端分窗样本总数正确，两worker起点相差约5ms，无负时钟阶段样本。服务日志未产生Go调度/GC输出。

此轮正式59次系统采样全部报告CPU_Speed_Limit低于100，范围41～93，停止负载后恢复100。真实分钟P99依次约596、701、480、424、945、520、648、1062、386、367ms。CPU均值：Agent87.0%/87.3%，MsgQ75.0%/74.8%，Game77.2%/76.7%；不能将这些平滑CPU均值与单请求延迟直接换算。

| 轮次 | 完成 / 未发送 / 错误 | P99(ms) | 系统限制低于100的采样 |
| --- | --- | ---: | --- |
| P=2，Go诊断开启（前轮） | 2999999 / 1 / 0 | 313.34 | 59/60 |
| P=1，Go诊断开启 | 3000000 / 0 / 0 | 287.48 | 58/60 |
| P=1，Go诊断关闭 | 3000000 / 0 / 0 | 629.86 | 59/59 |

结论：两轮P=1均未通过延迟门槛，关闭诊断也没有消除长尾；不能把差值全部解释为采集成本，也不能认定P=1存在稳定改善。系统性能限制、单服务可运行排队及其他共享资源竞争仍然混合，尚未定位唯一根因。停止在此环境下继续零碎调并行度，不修改生产默认值、ACK协议、Actor邮箱或gRPC连接策略。本轮只记录实验，没有代码优化或删行收益。

下一项需要评估可用的独立压测机器或稳定CPU配额的部署环境：优先把客户端负载与服务端分离，再验证万连×0.5/s；测试依赖仍可能竞争资源，也应记录部署边界。当前没有已确认的外部机器、连接地址或授权部署目标，不擅自部署或购买资源。若只能继续使用当前Mac，可保留万连、降低总请求率寻找本机延迟达标区间，但这是新的负载验收范围，不代表原5000/s目标通过。当前没有充分证据应修改哪段生产实现。

本轮验收为两轮真实十分钟链路，共6000000次成功请求，均正常收尾；未改源代码，因此未重复运行Go/客户端单元测试。文档diff --check通过，按纯文档范围简化ponytail-review，未增加诊断框架或推测性优化；全部变更未提交。
