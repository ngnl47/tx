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
