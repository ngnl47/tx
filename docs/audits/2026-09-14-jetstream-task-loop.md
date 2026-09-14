# JetStream 最小任务闭环验证

日期：2026-09-14。基于 tx `1cae909`，本轮未提交 Git。

## 目的与位置

默认 Core NATS 迁移已收口，本轮确认 JetStream 能把持久任务接到正式 Actor 分派，而不另建一套实体加载、归属或业务入口。

测试位于 `internal/framework/jetstream_tasks_test.go`，复用正式 Game 与 Worker 双实例夹具。任务使用测试内部的 Identity/Delivery JSON，消费侧调用正式 `Actor.Call`，成功返回后调用 SDK `DoubleAck`；调用或确认失败直接返回错误。不新增公共任务 API、生产消费循环或完成表。

例如：奖励任务先进入 JetStream，Worker 领取后调用 `role/reward`，tx 负责在 Game 上定位并懒加载 Role；业务返回成功后才确认任务。任务产生的客户端通知仍应走业务服务 → MsgQ → Agent 的 gRPC 下行，本测试未模拟客户端通知。

## 本轮实测

现有 `tx-nats-trial` 容器使用 `-js -sd /data -m 8222`，`/data` 已挂载 `tx-nats-trial-data` 持久卷，无需重新安装。客户端 SDK 为 nats.go v1.53.1。

测试使用随机独立 Stream、Subject、NATS Namespace 与 etcd Prefix。成功时核验所有者后清理；失败保留对应范围供排查。流为 FileStorage / WorkQueuePolicy / Replicas=1；共享 durable `worker` 使用显式确认、AckWait=2s、MaxAckPending=1，这些仅为验证参数。

实测断言：

1. 消费者尚未创建时发布成功，流中仍有任务。
2. 正式 Actor 业务处理被阻塞期间，任务仍为 ACK pending；完成后 DoubleAck 成功，任务从工作队列移除。
3. 独立消费子进程首次领取任务后退出，不执行业务、不 ACK。新连接复用同一 durable，在 AckWait 到期后领取原任务，投递次数至少为 2。
4. 替代消费者经同一正式 Actor 入口完成业务并确认，累计执行两次，无残留待确认任务。

本轮执行：

```sh
TX_ACTORTRIAL_NATS_URL=nats://127.0.0.1:4222 TX_ETCD_ENDPOINT=127.0.0.1:2379 \
  go test ./internal/framework -run '^TestJetStreamFormalActorTaskLoop$' -count=1 -v

TX_ACTORTRIAL_NATS_URL=nats://127.0.0.1:4222 TX_ETCD_ENDPOINT=127.0.0.1:2379 \
  go test -race ./internal/framework -count=1
```

结果：目标集成测试通过（包耗时 2.245s）；framework 整包 race 回归通过（8.858s），实际设置了服务地址，主闭环未跳过。子进程专用测试在普通遍历中跳过，由主测试单独启动执行。

## 结论与未覆盖边界

本轮证明持久流、durable 重投与正式 Actor 执行后确认可以组合。测试数据只驻留内存，不将 GD 宕机丢失纳入 MQ 职责，也不证明执行成功但 ACK 丢失时不会重复。

消费进程退出是领取后正常退出，不是业务执行中强杀；尚未覆盖 Broker 重启、断电、磁盘同步窗口、多副本故障或高可用。配置 FileStorage 与卷不等于这些故障已经验证。当前没有生产消费运行时、业务拒绝/死信分类、排水停止领取、慢任务续期与背压，也没有任务吞吐 benchmark；本轮无生产热路径改动，不给性能提升或无退化结论。

下一项：先补业务明确结束、永久加载拒绝和临时失败的结果分类及死信转存确认顺序，再接入消费停止领取与排水。继续复用正式 Actor 入口，不引入成功完成表；自动 Pod 交接仍延期。


## 后续：任务结果分类与死信顺序

基于已提交的 `8d16328` 继续扩展同一正式框架夹具，仍不新增公共 API。业务明确结束以测试结果 `task.finished` 表达成功消费，不要求在 GD 写入幂等标记。普通业务错误返回消费侧，保留源任务，由 durable 在 AckWait 后重投；不在 RPC 内循环重试。永久加载拒绝通过现有 `ErrActivationDenied` 分类，卸载、容量不足或普通错误不据此判定永久结束。

测试先向不存在的死信流转存，断言失败后源消息仍处于 ACK pending；创建 FileStorage 死信流后，重新领取原任务，经发布确认成功后再 DoubleAck 源消息。最后检查源队列清空、死信仍保留，并逐字节核对原任务及拒绝原因。死信流用默认 LimitsPolicy，不因没有死信消费者就删除消息。

例如过期房间拒绝重新加载，任务转入死信供后续处理；若房间已加载且业务判断这条 Buff 已无效，可以明确返回任务结束。两者均不改变 Tell/TellAccepted/CallFuture/Call 的语义。

本轮目标测试实跑通过（6.186s）；相同服务环境下 `go test -race ./internal/framework -count=1` 整包通过（12.909s）。原有消费进程退出续领断言同时通过。没有测量性能，生产热路径未变。后置 ponytail-review 未发现需要删除的冗余。

当前仅验证上述顺序，不承诺死信写入与源 ACK 跨流原子性；转存成功但源确认丢失可能产生重复死信。不建成功完成表，不把 Term 当转存。MaxDeliver 通知持久捕获、残留源任务恢复、排水停止领取与慢任务续期仍未完成；下一项继续消费生命周期与排水验证。


## 后续：SDK 消费生命周期与排水

基于已提交的 `c14ced5`，复用同一正式 Actor 夹具与 durable，使用 nats.go v1.53.1 原生 `Consumer.Consume`、`ConsumeContext.Drain` 和 `Closed`。不增加第二套 worker 完成计数。SDK Drain 会处理已进入消费缓冲的消息；不能把调用 Drain 的瞬间宣称为跨进程停止所有在途投递的原子边界。

实测让 Actor 业务阻塞后启动消费 Drain：`Closed` 不提前关闭；调用方等待超时后保留消费连接与正在执行的业务，释放业务后回调完成 DoubleAck，消费排水才结束。随后关闭该消费连接，发布下一条任务，由新连接绑定的同一 durable 首次领取并完成，投递次数为 1。

目标真实集成测试通过（6.313s），原有持久投递、进程续领、结果分类和死信顺序断言仍通过。当前 MaxAckPending=1 / PullMaxMessages=1；不据此宣称批量预取、慢任务续期、多 Worker 并发排水已通过，也未接入框架 Stop。等待超时用独立等待 Context，Actor 执行 Context 没有被取消。

后续正式接线必须在关闭 Actor 运行时前完成消费排水，并保留超时后继续收尾所需的连接。SDK Closed 表示消费回调结束，不能替代应用对回调错误、ACK 结果和持久化收尾的处理。当前仍是测试内适配，不新增业务死信补偿 SPI，没有性能测量或生产热路径变更。

下一项：慢任务 InProgress 续期与消费背压边界，再收敛正式消费运行时；MaxDeliver 通知持久捕获及残留源消息恢复仍保留为 T5 未完成项。

本轮补充验证：framework 整包 `-race` 通过（13.052s）；后置 ponytail-review 删除一处重复 Closed 断言后，目标闭环 `-race` 再次通过。最终差异检查通过。


## 后续：慢任务续期与共享在途背压

基于已提交的 `d35bbf7`，延长既有正式 Actor 排水阻塞场景，复用当前消费者 AckWait=2s、MaxAckPending=1 配置。消费适配侧每 400ms 调用 SDK `InProgress`，让业务阻塞超过 3s；另一连接绑定同一 durable，每次续期后尝试领取，均超时，没有领取到原任务或后续任务。此时消费者状态为 NumAckPending=1、NumPending=1，已完成业务计数不变。

随后释放业务，等待其 DoubleAck 和消费排水完成；状态转为 NumAckPending=0、NumPending=1。替代消费者首次领取后续任务（NumDelivered=1），执行并确认后源队列清空。原有无续期的消费进程退出与临时错误用例仍验证 AckWait 后重投。

这说明 SDK 原生续期与共享 durable 的在途上限可以用于正式消费适配，不需要业务调用 NATS SDK。测试为验证服务端已收到续期而调用 Flush，不表示生产每次续期都需要额外 Flush。测试只在正常连接下确认该行为，续期不提供幂等或绝对防重保证；断连、暂停或续期失败仍可能触发重投。

本轮真实目标集成测试通过（9.631s），相同服务环境下 framework 整包 `-race` 通过（16.337s）；后置 ponytail-review 未发现需精简项，差异检查通过。没有 benchmark，没有生产热路径变更。当前续期由测试控制，尚未实现生产自动续期、续期失败日志与终止策略；也未验证多条预取、多 Actor 并发或整个集群限流。原排水测试调整为在排水期间积压后续任务，释放名额后再由替代消费者领取。

下一项：根据上述实测边界收敛正式消费适配与关闭顺序；继续保留 MaxDeliver 通知持久捕获、残留源消息恢复和存储资源上限为未完成验收项。死信自动补偿 SPI 仍不扩展。


## 后续：内部消费适配落地

基于已提交的 `7720fbb`，新增 `internal/taskqueue`，把测试内任务协议、Actor 调用和死信分支迁入内部适配。`Processor` 接受正式 Actor Caller、已有 JetStream、死信主题与任务等待期限；`Worker` 使用已有 durable，读取有效 AckWait（存在 BackOff 时取首项），按其三分之一自动 InProgress，复用 SDK 单条拉取与 Drain/Closed。

Actor 调用、转存及 ACK 错误返回消费层并记录 error；续期错误记录 error，不据此取消已开始业务或重新调用 Actor。普通错误及解码错误不确认，永久加载拒绝先确认死信转存再确认源任务。不创建完成表或第二份实体归属。超时仅结束本次等待，不保证远端业务已结束，消息随后可能重投；具体业务执行上限仍沿用 Actor/业务自己的契约。

Worker 的启动 Context 仅用于启动查询，消费使用独立、带期限的 Context；排水等待超时不关闭连接、不取消已领取业务，可以再次 Drain。Drain 成功表示回调已结束，不等于每条任务业务成功：失败任务仍由 JetStream 保存。适配器不持有连接关闭权限，宿主必须先完成消费排水，再关闭 Actor 及连接。

真实夹具已移除手写 InProgress，切换到正式 Worker 自动续期，并以短等待 Drain 超时后继续运行、再次 Drain 成功验证生命周期。原持久投递、错误分类、死信转存失败保留、消费进程退出续领与竞争领取断言保留。首次命令误在 workspace 根目录运行而未找到 go.mod，改到 tx 模块后目标集成测试通过（9.627s）。

这是内部可复用实现，尚未向业务公开配置/API，未接入默认框架 Stop 或两个示例。MaxDeliver 通知与残留恢复、存储资源上限仍未实现，不能据此宣布 T5 完成。没有性能测量；线上默认 RPC 路径没有改动，但新增消费适配的开销尚未评估。

下一项：收敛框架任务声明与资源配置，接入服务启动/排水顺序及一个业务示例；保持业务不直接操作 NATS SDK。死信自动补偿 SPI 继续延期。

本轮最终验证：`go test -race ./internal/framework ./internal/taskqueue -count=1` 通过，framework 包耗时 16.342s；taskqueue 的真实行为由 framework 集成夹具覆盖，包内没有另建重复测试。后置 ponytail-review 未发现需精简项；`git diff --check` 通过。


## 后续：框架能力、配置与生命周期接线

基于已提交的 `448dc87`，新增 `modules/tasks.Publisher` 和资源配置，由 Bee/Ant 的 `CModTaskQueue` 暴露。服务通过 Requires 声明能力；工厂只取得窄 Publisher，业务发布 Actor 身份与 Delivery，发布成功表示 JetStream 已确认，不等待业务执行。

源流、durable 和死信流由部署侧预先配置。启动绑定并检查源流 FileStorage/WorkQueuePolicy、死信独立 FileStorage/LimitsPolicy，正式 Worker 要求 durable 与显式 ACK。Consumer 为空时仅发布。不会自动创建或修改共享资源；过滤主题、资源额度、保留与满额策略仍需部署配置。配置资源错误会使启动失败，已打开连接沿 CMod 回滚关闭。

消费在服务初始化、RPC 启动和发现标记 Running 后启动。Server.Drain 先关闭服务外部准入，完成消费回调排水后再关闭 Actor 准入；Stop 与 PrepareHandoff 复用该路径。启动失败清理同样先等待已启动消费，再清理 Actor。任务连接由 CMod 逆序关闭，业务没有关闭权限。这个顺序意味着任务消费排水期间仍保留 Actor 准入，不宣称一开始就同时封闭所有 Actor 请求。

真实测试让 Game 与 TaskWorker 共址，通过框架发布入口投递并阻塞本地 Actor；短期限 Stop 返回保留资源的错误，连接未关闭；释放业务后再次 Stop 成功、任务确认且队列清空。SDK 自动续期、死信、竞争领取和进程续领原断言保留。目标测试通过（9.844s）。

编译检查先发现 CModContext 漏加新能力方法，补齐后通过。全量回归先指出内置 CMod 依赖表缺少 task-queue，补齐 task-queue → actor 契约断言后，`go test ./...` 通过。期间一次命令在 workspace 而非 tx 模块目录执行，未找到 go.mod，改到模块目录后执行。

README 提供声明、配置和发布示例。两个游戏示例没有默认开启任务消费，本轮仅修改 tx；生产部署的资源配置与游戏协议任务仍待具体接线。MaxDeliver 通知/残留源消息恢复、资源容量验收未完成，不宣布完整 T5 达标，没有 benchmark 结论。

下一项：补齐 MaxDeliver 通知持久捕获及源消息残留恢复，再完善游戏示例和资源部署验收。死信业务补偿仍留给后续业务需求。


本轮最终验证：tx `go test ./...` 通过；设置真实 NATS/etcd 地址运行 framework/taskqueue/bee/ant 的 `-race -count=1` 通过（framework 16.674s）。正确性复查给任务排水读取 runtime 增加 Server 锁保护后，目标闭环与内置依赖契约 `-race` 再次通过（10.878s）。txgame/server 和 txgameslg/server 的 `go test ./...` 均通过；未单独启动两套客户端真实集群 e2e，不把条件跳过的外部服务用例算作本轮实跑证据。后置 ponytail-review 无需精简项，差异检查通过。
