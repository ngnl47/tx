# 示例生成命令的可靠任务完成确认

接续 T5 示例接线评估。txgame/txgameslg 的命令适配器原本要求内部 Call 必须对应有响应体的 Handler；而任务消费者固定使用 Call 等待业务执行。因此，只有 error 返回值的任务 Handler 会在分派阶段被拒绝。放开分派后，空 Result 又会被正式 Actor RPC 判为非法结果，仍无法 ACK。

## 修改边界

两个独立示例使用 `task.command` 作为可靠任务命令约定：始终要求 Call 完成确认，但允许 Handler 只返回 error。成功且没有业务响应体时，由命令适配器补充空载荷 `task.completed` 结果；业务错误原样交给任务适配器。普通 RPC 的 Call/Tell 匹配、Agent 的上行确认和客户端下行行为保持不变。

业务不需要新增一个生成响应协议，也不要求将任务写成 Agent 上行。带响应体的 Handler 仍可执行，消费者不将结果自动推送客户端。这个标识属于示例命令协议，不是 tx 新增的统一消息类型或鉴权能力。未新增完成表、生产队列配置、业务奖励接口或默认消费者。

## 验证

- 两示例的新命令测试在修改前均失败，错误为 `command delivery mode does not match handler`，业务调用次数为零。修复后覆盖成功、业务错误传播、普通 Call 不放宽、任务 Tell 被拒绝及不自动发布客户端响应。
- txgame 新增 `TestQueuedGeneratedCommandRetriesUntilHandlerCompletes`：独立 JetStream 文件工作流及死信流，显式 ACK、无限重投、AckWait=300ms、MaxAckPending=1；正式 Bee/CModTaskQueue、生成 UploadManifest 命令、懒加载 room Actor。Handler 第一次返回临时错误，第二次返回 nil；两次业务完成前源任务与待确认计数仍为 1，第二次能观察到重投，成功后源流为空，死信流为空。
- 真实测试在只修复分派时再次失败，记录 `actortrial: invalid result`，证明空结果仍不符合传输契约；补充 task.completed 后带 race 通过（用例 0.46s，包 1.537s）。此前测试服务缺少显式 CModActor 声明也曾导致启动失败，已修正测试装配。
- 测试使用受控 Handler，不包含实际 GD 写入或最终存盘验收，不能据此宣称两个游戏示例已默认启用可靠消息。

txgame/server 与 txgameslg/server 的 `go test ./... -count=1` 均通过；未启用的外部依赖测试沿用明确 skip。SLG 命令契约 `-race` 单独通过（1.051s）。三工程 `git diff --check` 通过；交付前 Ponytail review 未发现需调整项，保留独立示例与关键可靠性测试。tx 本轮仅更新文档，未修改库实现，也未重跑性能基准。

复现：先准备隔离 JetStream 并确认元数据就绪，然后在 txgame/server 执行：

```sh
TX_TASK_NATS_URL='nats://127.0.0.1:<隔离端口>' \
TX_ETCD_ENDPOINT=127.0.0.1:2379 \
go test ./services/internal/actorcmd \
  -run 'TestQueuedGeneratedCommand|TestTaskCommand' -race -count=1 -v
```

未设置任一依赖地址时真实测试明确 skip；地址已设置但依赖不可用则失败。流名与 etcd 前缀随机，清理前核对 owner；失败保留现场。

本轮失败前缀 `/txgame/tests/tasks/PYZ7E32NRCVAPQC56GQ6JZYMVU`（装配缺声明）及 `/txgame/tests/tasks/ATESMTOXDGI6HDDYFYRKHY2S3R`（空结果非法）保留，对应源流/死信流分别为 `txgame_tasks_<后缀>`、`txgame_dead_<后缀>`。隔离网络 `tx-task-baseline-j3s2mbsc` 与三个容器保留并停止；配置及日志位于 `/var/folders/gq/xxck3g710bq4w3q85kq21jfm0000gn/T/tx-task-baseline-j3s2mbsc`。成功测试自己的资源已清理，未修改共享 NATS 或此前性能试验现场。

下一项评估项目可选队列配置与具体业务入口。基础契约已可接入无响应体任务，队列资源仍由部署侧预创建；不为了示例接线自动创建共享流或改变普通启动依赖。


## 接续：项目可选任务配置

在上述命令契约之后，两个独立示例已接入项目配置转换。txgame 使用 `runtime.taskQueue`，txgameslg 沿用分层布局使用 `runtime.cmods.taskQueue`；debug/test/release 均显式为 null。非空对象让选中的 game 或 slggame/slgworld 声明 CModTaskQueue，未选中的业务服务不引入依赖；同进程能力仍由 tx 统一装配一次。未新增空壳 worker 服务或第二套消费生命周期。

八个资源/超时字段直接映射到现有 tasks.Config：url、stream、subject、consumer、deadLetter、recoveryStream、recoveryConsumer、timeout。Consumer 留空为纯发布，配置对象为空不是关闭。配置仅在能力被选中时解析；流存在性、durable、有限 MaxDeliver 恢复绑定等仍由现有 tx 契约验证，不在项目层复制实现。资源由部署侧预创建，两个 README 完整记录 YAML 路径和独立项目的主题隔离。

验证：配置绑定测试在修改前失败，框架 TaskQueue 全为空；修改后两工程分别覆盖完整映射、未选中不解析、选中拒绝非法超时、纯发布省略消费超时，以及组合根为业务服务选择任务能力。txgame 的真实队列测试改为通过项目 Framework 配置提供器组装，并在隔离 JetStream 下带 race 通过（用例 0.48s、包 1.565s），仍验证业务失败保留、成功才确认。

两个 server 全量 `go test ./... -count=1` 均通过，配置和组合根定向 race 均通过；全量中的未启用外部依赖测试仍按约定跳过。本轮测试网络 tx-task-baseline-v3kwmwwe、三个容器与临时配置已清理，成功用例的流和 etcd 归属范围已清理。未触碰此前失败现场。

交付前 Ponytail review 检查本轮差异：复用能力选择与配置提供器，不新增业务 facade、自动资源创建器或能力开关布尔值；独立示例各自维护配置，不跨项目耦合。未发现需调整项，git diff --check 通过。tx 本轮仅更新文档，不重复库全量或性能基准。

下一项评估并接入具体 GD 业务任务入口；本次没有自动投递任务、默认消费或新增玩家奖励协议。可选配置已具备条件，GD 业务验收尚未完成。

## 接续：真实 Game 与 Fusion GD

新增 txgame `TestQueuedStorageProbeWritesFusion`，复用生产组合根、既有 `System_StorageProbe` Handler、生成 GD 和正式 CModTaskQueue。测试中的轻量发布服务只取得公开 Publisher，不手工构造内部任务信封，也不增加生产业务 API。

验收顺序：预建隔离文件工作流与死信流 → Game 尚未启动时持久发布并确认源流为 1 → 启动实际 Game → 懒加载 role 并执行 GD 写入 → 源流清空且待 ACK 为 0 → 使用生成的 LoadGDStorageProbeByID 从 Redis 回读 → 单独等待 Mongo storage_probes 的名称字段一致 → 死信流为空。Mongo 检查在 ACK 检查之后执行，但不据此推断实际落盘发生的先后顺序；MQ 不等待最终存盘。

首次运行因测试未声明 storageProbe 路由失败（30.03s）；第二次因测试 ID 使用纳秒时间戳，触发 Fusion Lua JSON 53 位整数保护而失败。两次均记录业务错误并保持重投。修正测试路由及毫秒 ID 后，真实验收带 race 通过：用例 2.24s，包 3.311s。txgame/server 全量 `go test ./... -count=1` 通过，其中本 opt-in 用例在全量时跳过、上述显式运行时执行。没有修改库、生成物或生产配置，没有重跑 benchmark 或作性能提升声明。

本轮使用 NATS 2.14.6 单节点 JetStream、Redis 8.8、Mongo 8.0、etcd 3.5.15 的临时 Docker 容器。此用例验证功能衔接，不验证断电持久性，也不替代此前强同步 R1/R3 基线。复现命令与环境要求见 txgame README 的“可靠任务与 Fusion GD 验收”。

失败现场保留于 `/var/folders/gq/xxck3g710bq4w3q85kq21jfm0000gn/T/tx-gd-task-e91p8eme`（缺路由）及 `tx-gd-task-gl_mvlqb`（超范围 ID，以及修正后的成功运行）；目录含容器 ID、日志，容器停止保留。测试本身不删除后端资源，不触碰共享服务。

下一项评估 txgameslg 的世界 Procedure 与直接 Mongo 写回任务，重点核对任务完成、事务提交和异步写回边界；不复制无业务差异的 Fusion 用例。长期稳态、生产同步策略、物理故障域及掉电仍待独立验收。

交付前 Ponytail review：未发现本轮新增的多余生产抽象或可明确删除的验收环节（Lean already. Ship.）。正确性检查与两工程 git diff --check 通过；其余四工程工作区干净，未提交 Git。

## 接续：世界 Procedure 与任务路由键

真实 SLG 世界验收复用 World_MoveUnit、项目 app.New、公开 Publisher、首次归属分配及正式 Procedure。将测试周期写回设为 1h：任务完成 ACK 后，Mongo 坐标仍为 (10,20)；正常 Stop 排空写回后变为 (47,83)，死信为空。这直接验证内存事务完成与异步 Mongo 存盘的职责边界，不为 MQ 增加 GD 完成表，不改生产写回配置。

首次真实运行失败：接收端报 `route key kind does not match descriptor: command=World_MoveUnit got=0 want=1`。根因是 protocol.RouteKey 字段未导出，任务信封直接 JSON 编码时变成 `{}`。此前 Fusion 探针没有显式 RouteKey，因此未暴露此问题。已在该值类型增加 JSON 编解码，保留 kind/numeric/text 与整数位模式，未知 kind 返回错误；不修改 gRPC/NATS RPC 二进制路径或向业务侧泄漏编码逻辑。空对象恢复无路由键；历史已经丢失的键无法凭空恢复，旧失败任务不会自动修补或重投。

新增往返测试在修复前失败，覆盖无键、int64 最小/最大值、uint64 最大值、中文字符串及空字符串；修复后协议包 race 通过。真实世界任务修复后 race 通过（用例 0.14s、包 1.199s）。复现命令见 txgameslg README。该测试验证正常处理与关闭边界，不构成崩溃后 GD 不丢失或全局恰好一次保证。

本次 tx、txgame/server、txgameslg/server 全量 `go test ./... -count=1` 均通过（opt-in 外部用例在全量中未启用）；另在本次依赖上重跑 Fusion 真实验收 race 通过（用例 2.55s、包 3.604s）。Ponytail review 未发现可删除的多余抽象，保留独立示例和关键边界检查；三工程差异检查通过。未跑 benchmark：编码修复限定 JSON 持久任务信封，未修改高频二进制 RPC 实现，不作性能结论。

本轮现场目录 `/var/folders/gq/xxck3g710bq4w3q85kq21jfm0000gn/T/tx-gd-task-q4f6jl0t` 保存 state.json、失败/修复后日志与全量日志；四个专用容器停止保留，未修改共享服务。失败任务后缀 `iibizz7sghx6jvrzqmnxsyeyeb` 保留，修复后的世界任务后缀 `hr5yeh2dh7nkayl2slgautro46`。下一项尚需评估任务调用上下文与失败分类的剩余覆盖，不重复正常写回用例。

## 接续：调用上下文与失败分类收敛

本次没有新增生产分支或重复测试套件。扩展既有 TestJetStreamFormalActorTaskLoop：通过正式 taskRuntime.Publish 代替手工 JSON，带入命令号、中文字符串 RouteKey、二进制 Payload、借用账号操作身份的全部字段，并保持 UpSeq=0。Actor 每次接收均检查上下文一致且为 Call；覆盖跨实例、进程退出后重投、临时错误恢复及同实例消费排水。发布请求 Context 在 PubAck 后立即结束，任务仍继续消费，验证发布请求生命期不会撤销已持久化工作。

操作身份是发布时的业务输入快照，不代表消费时原连接仍在线，也不是重新认证或自动授权。是否在任务执行时复核业务权限由业务决定；本次不增加登录、会话续期或客户端序号确认逻辑。

失败分类核对：业务返回成功即确认；普通错误不 ACK、由 durable 重投；ErrActivationDenied 先转存死信、取得持久确认再 ACK；转存失败保留源任务。解码失败和超时仍不确认，依部署的 MaxDeliver 决定是否耗尽恢复；不擅自将这些错误改为永久业务拒绝。现有真实测试已覆盖相关业务分类、恢复和排水，不再为同一语义增加套件。

本轮隔离 NATS/etcd 的正式闭环 race 通过（用例 13.27s、包 14.333s），覆盖续期、共享在途背压、进程退出续领、临时失败、永久拒绝、MaxDeliver 离线恢复与框架排水。下一项先核对 T5 总表中的激活/计时器分类与既有生命周期证据，避免总表历史未勾选项造成重复建设；生产同步策略、长期稳态和物理故障域仍独立待决策/验收。

总表核对完成：并发激活共享、战斗结束与卸载后拒绝、旧计时器不能唤醒替代实体分别已有 actorhost 的 TestActivationSharedAndIndependent、TestBattleDeadlineAndTimerIsolation、TestOldTimerCannotWakeReplacement。本轮重新执行定向 race，避免为已有职责重复新增队列侧计时器逻辑。结合本轮真实任务闭环及此前已记录的 R1/R3 故障、同步基线，T5 工程功能汇总项已勾选；生产长期稳态、同步策略和物理故障验收独立保持未完成。

本轮 tx 全量通过；Ponytail review 未发现需调整项，仅扩展既有闭环，无新测试框架、接口或生产分支。未修改两个示例本轮代码，因此不重复其全量；此前累计未提交变更保留。下一项评估 T6 整合收益与 API/SPI 收口，不启动 MsgQ 传输替换或 etcd 替换。
