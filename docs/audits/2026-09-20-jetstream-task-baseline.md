# JetStream 可靠任务框架链路基线

基线 revision：`65e0338`，本轮仅新增独立测试入口和文档。原始六窗口及环境记录见 [JSON](baselines/2026-09-20-jetstream-tasks.json)。数据不包含 `-race` 结果，不与 Core NATS 空回显或历史上行吞吐混用。

## 口径

- Go 1.27.0 darwin/amd64，GOMAXPROCS=4；NATS Server 2.14.6、nats.go 1.53.1。
- 本地独立三 Broker Docker 网络，各自 `/data`，`sync_interval: always`、max_file_store=256MB；无 CPU 配额，同一宿主共享存储资源。没有测量磁盘 fsync 延迟或 CPU 归因。
- FileStorage / WorkQueuePolicy；按 R1、R3 顺序交替三轮，新建独立流和 durable，consumer 同副本数，AckWait=30s、MaxAckPending=4，不限制 MaxDeliver。
- 每窗口 1000 条任务，业务 payload 256 字节（含测试序号），JSON/base64 信封后字节数更大。32 个 Role Actor 预先完成归属解析和激活；4 个同步发布者闭环发送，4 个共享 durable 的单条拉取消费回调。
- Game 与 Worker 是同一测试进程内的两个正式 Server，使用独立 RPC endpoint、真实 etcd 隔离目录和 NATS Actor 调用。四个消费回调共享 Worker Actor 运行时，不等价于四个独立业务进程。
- Actor 执行内存计数并返回结果；没有 GD 标脏、Procedure、Mongo/Redis 写入或客户端通知。因此是框架链路基线，不是完整奖励业务容量。
- 发布复用生产 `taskRuntime.Publish`；回调复用 `taskqueue.Processor.Process` 和生产相同的 PullMaxMessages=1。为了在 DoubleAck 返回处计时，由测试回调包裹 Processor，未改生产 Worker 或引入指标 SPI。
- 发布延迟：Publish 开始至 PubAck 返回；完成延迟：该任务 Publish 开始至 Processor 返回，包含排队、跨 Actor Call 与 DoubleAck。每条采样、任务标识检查也消耗测试进程资源，未扣除这部分开销。
- 吞吐分别按窗口开始至全部发布返回、全部发布和消费确认完成计算。完成吞吐是这一批含积压排空的速率，不是已验证的长期稳定承载上限；计入首次拉取，不计入 Actor 冷启动与资源创建。

## 非 race 结果

表中为三轮窗口指标的中位数；P99 是各窗口 P99 的中位数，不是合并全部样本的 P99。

| 副本数 | 发布确认条/s | 完成条/s | 发布 P99 | 完成 P99（含排队） |
| --- | ---: | ---: | ---: | ---: |
| R1 | 597.3 | 356.8 | 9.39ms | 1136.06ms |
| R3 | 150.6 | 146.2 | 50.12ms | 266.61ms |

R1 完成速率范围约 279.5～417.5 条/s，R3 约 104.5～147.4 条/s，存在明显窗口波动，不给出精确生产容量推断。R1 发布快于消费，积压拉高完成 P99；R3 发布较慢、队列较短，完成 P99 较低不代表复制使链路更快。未控制相同到达率，不能用这组 P99 对比副本数的独立延迟代价。

6000 条任务均核验单次投递、唯一完成序号、1000 次/窗口业务执行、源流最终清空；无错误、无重投。没有进行生产实现优化，不宣称性能提升或回退；本轮建立的是后续同口径比较基线。

## 验证与修正

首次 `-race` 验证中，1000 次执行和 DoubleAck 完成后，R3 Stream Info 短暂仍显示 3 条消息；随后只读核对已为 0。原测试把“确认成功”与“源流查询立刻清空”等同，已改成完成计时结束后最多 5s 等待流清理状态收敛，超时仍失败，不把该查询等待混入完成延迟。

修正后 `-race` 三轮共 6000 条通过（用例 29.43s）；非 race 正式采集三轮共 6000 条通过（33.08s）。完整 `go test ./... -count=1` 另行执行并通过，不与测量窗口并行；未显式启用的外部依赖测试沿用跳过约定。普通全量运行不显式启用此性能入口，必须使用以下开关：

```sh
TX_TASK_BASELINE=1 \
TX_ACTORTRIAL_NATS_URL='nats://127.0.0.1:<隔离集群端口>' \
TX_ETCD_ENDPOINT=127.0.0.1:2379 \
GOMAXPROCS=4 \
go test ./internal/framework -run '^TestJetStreamTaskBaseline$' -count=1 -v
```

地址值中的占位符需替换或整体加引号；先按 [部署说明](../deployment/jetstream-tasks.md) 准备三节点网络和显式同步配置。此入口不创建 Broker、不核验 Broker 同步配置，调用者须记录实际配置；不足三个节点则 R3 创建失败，不能默默降为 R1。使用随机流名、带 owner 的 etcd prefix，成功仅清理本轮归属资源，失败保留。输出 `TASK_BASELINE` JSON 行用于保存结果。

首次失败的证据保留在隔离集群 `tx-task-baseline-t6jggs6r`（任务结束后停止），配置与容器 ID 清单位于 `/var/folders/gq/xxck3g710bq4w3q85kq21jfm0000gn/T/tx-task-baseline-t6jggs6r/state.json`；失败流 `tx_baseline_SZBBI5CVNJFMROA4Y6562EMXVX_0_3` 及 etcd prefix `/tx/tests/task-baseline/SZBBI5CVNJFMROA4Y6562EMXVX` 保留。后续成功窗口的随机资源均清理，不操作共享 NATS。

## 后续边界

下一项先做固定到达率下的积压/尾延迟验收，再决定是否需要定位或优化；不能直接从这组小并发闭环数据外推游戏人数。GD/存储业务负载、不同消费进程拓扑、生产磁盘和同步策略仍需单独确定。R3 物理故障域、掉电及网络分区不在本轮性能证据内，T5 不整体收口。

交付前正确性检查与 Ponytail review：计时和失败检测限于测试文件，复用正式 Publisher/Processor/Actor，不新增生产 SPI、指标依赖或消息协议。保留持久确认及唯一执行核对，未发现可直接删除的过度设计。`git diff --check` 通过，未提交 Git。
