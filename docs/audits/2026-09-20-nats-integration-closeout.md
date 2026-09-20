# NATS 迁移整合收益与 API/SPI 收口

## 当前结论

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
