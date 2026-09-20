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
