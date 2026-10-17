# JetStream 任务资源与容量边界

适用当前 `modules/tasks` 和 `internal/taskqueue`。资源由部署侧提前创建，框架只绑定已有资源，不自动修改共享流。下表是单机演示的配置起点，不是生产容量或 SLA；生产必须按任务大小、峰值发布率和最长处置窗口重新预算。

## 三条流与两个消费者

| StreamConfig 字段 | 源流 | 死信流 | 耗尽通知流 |
| --- | --- | --- | --- |
| Name | GAME_TASKS | GAME_TASK_DEAD | GAME_TASK_EVENTS |
| Subjects | `game.tasks` | `game.tasks.dead` | `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.GAME_TASKS.game-workers` |
| Storage | FileStorage | FileStorage | FileStorage |
| Retention | WorkQueuePolicy | LimitsPolicy | WorkQueuePolicy |
| Discard | DiscardNew | DiscardNew | DiscardNew |
| MaxAge | 0 | 0 | 0 |
| MaxMsgs | 100000 | 10000 | 10000 |
| MaxBytes | 256 MiB | 64 MiB | 16 MiB |
| Replicas | 1 | 1 | 1 |

名称与主题对应 README 的 `tasks.Config` 示例。主题保持精确匹配，不能把源流订阅扩大为 `game.tasks.>` 而覆盖死信主题。先创建三条流和恢复消费者，再创建任务消费者，最后启动业务发布和消费；不能等首次耗尽后才创建通知流。

`MaxAge=0` 不按年龄淘汰；条数或字节任一先达到上限即拒绝新写入。它不是永久保存保证：磁盘故障、人工删除及配置变更仍可使数据丢失。若设置非零 MaxAge，到期删除仍会生效，DiscardNew 不阻止过期。源任务和通知的保留窗口必须覆盖处理停机、重试及人工处置时间，不能仅按正常业务耗时设置。

| ConsumerConfig 字段 | GAME_TASKS / game-workers | GAME_TASK_EVENTS / task-recovery |
| --- | --- | --- |
| Durable | game-workers | task-recovery |
| AckPolicy | AckExplicitPolicy | AckExplicitPolicy |
| FilterSubject | game.tasks | 上表完整通知主题 |
| AckWait | 30s | 30s |
| MaxDeliver | 5 | -1（无限） |
| BackOff | 30s、1m、5m、15m、30m | 不设置 |
| MaxAckPending | 32 | 1 |

两者都是 pull consumer，不设置 DeliverSubject。MaxDeliver 包含首次投递；BackOff 是部署侧指定的 ACK 等待序列，覆盖 AckWait，不是库自动生成的指数退避。当前 tx 普通失败不 ACK，执行中用 InProgress 续期，所以实际重投时刻不能简单解释成“错误返回后等待该时长”。MaxAckPending 限制共享 durable 的在途量，不代表消费并发数或吞吐量。

死信流采用 LimitsPolicy：查看消息或对其消费者 ACK 不会清除流中的记录。业务/运维确认处置完成后，才按准确流名与序号删除；不提供自动退款、重投或定期清空策略。若任务与角色余额等业务状态相关，不能因存在死信就直接判断业务从未执行。

## 满额时谁负责

- 源流满：JetStream 发布返回错误，调用方不能当成投递成功；是否稍后重试由业务决定。
- 死信流满：tx 转存失败，不确认源任务；耗尽恢复路径同时保留源残留与通知。扩容或处置死信后可继续恢复。不得直接清空源流解除压力。
- 通知流满：耗尽通知通过 Core NATS 发布，不能依靠发布者等待 PubAck 并重试。通知可能未被保存；释放空间不等于补回历史通知。此时源任务仍在，但 tx 当前不会自动扫描无通知的耗尽残留。

因此，有限 MaxDeliver + 通知捕获不是故障下无缺口的完成保证。通知流不可写、Broker 异常或保留到期时，应停止扩大问题、恢复资源可写性，并由运维核对源流残留及消费者状态。不能通过重新创建 consumer 或直接重投全部残留来“修复”，这可能再次执行业务。下节提供原生只读核对步骤；逐条自动判定耗尽和自动处置不在当前能力内。

至少监控三条流的消息数、字节数相对上限、最老消息年龄，以及消费者 NumPending / NumAckPending / NumRedelivered、发布失败和恢复错误日志。NumRedelivered 不是可直接替代死信清单的累计失败指标。可从只读入口开始检查：

```sh
nats stream info GAME_TASKS --json
nats stream info GAME_TASK_DEAD --json
nats stream info GAME_TASK_EVENTS --json
nats consumer info GAME_TASKS game-workers --json
nats consumer info GAME_TASK_EVENTS task-recovery --json
```

以上命令需要另行安装 NATS CLI；URL、账号和权限使用部署环境配置。控制接口及监控端口只开放给受信任运维网络。告警应在满额前触发，单靠 tx 日志发现不了所有未捕获通知。

## 无通知源残留的只读核对

不新增 tx 管理 API，直接复用 NATS 的 Stream Info、Consumer Info 和 Stream Message Get。以下 API 不创建 consumer、不领取任务、不发送 ACK。使用只允许相关 INFO / MSG.GET 请求及接收响应的运维身份；不需要授予业务发布、消费或删除权限。

```sh
# 保存源流、任务消费者、通知流的当前状态；单引号避免 shell 展开 $JS。
nats request '$JS.API.STREAM.INFO.GAME_TASKS' '{}'
nats request '$JS.API.CONSUMER.INFO.GAME_TASKS.game-workers' '{}'
nats request '$JS.API.STREAM.INFO.GAME_TASK_EVENTS' '{}'

# 123 仅为示例，替换为日志中的源序号或源流 state.first_seq。
nats request '$JS.API.STREAM.MSG.GET.GAME_TASKS' '{"seq":123}'
```

先检查响应 JSON 的 `error`；NATS request 收到 API 错误响应不等于查询成功。`message.data` 是 Base64 编码的原始任务内容，解码后可核对 Identity 与 Delivery；原始记录不带该 consumer 对它的完整投递次数和业务执行结果。需要检查更大范围时，按明确序号和数量上限逐条读取，不对线上流做无界扫描。序号中可能存在已 ACK/已删除的空洞；查不到消息不能解释为执行成功。

分别核对以下证据，不能把其中任何单项当作“任务已完成”或“应重投”的判断：

- 源流 `state.messages > 0`：仍有记录，不区分待投递、执行中或耗尽残留。
- Consumer `num_pending`：尚未投递数；`num_ack_pending`：等待确认数；`num_redelivered`：当前重投相关计数，均不是逐条耗尽记录清单。
- `delivered` 与 `ack_floor`：消费进度位置，不能当作连续业务成功区间。
- 通知流无积压：可能已被恢复消费，也可能当时未捕获，不能单独证明没有耗尽任务。
- 原始任务、已保存通知及死信的 `Task-Stream / Task-Consumer / Task-Sequence`：用于人工交叉定位。永久加载拒绝路径的死信不承诺具备这组三字段，不能套用耗尽恢复路径的定位方式。

本轮真实测试中，源序号 3 仍在，而任务消费者的 pending、ack_pending、redelivered 均为 0，通知流也为空；原生 MSG.GET 能读到源记录，前后投递位置、确认位置及上述计数保持不变。这证明仅监控 consumer 积压不足以发现这一类残留。测试知道它确实耗尽，是因为隔离夹具同时观察了真实通知；线上缺失该证据时只能报告待核对残留，不能伪造投递次数。

运行中的多次查询不是一致性快照。发现异常后先保存返回值、流/consumer 创建时间和日志；若需稳定核对，安排受控停止发布并排空相关消费者，再查询。完成取证后由业务/运维决定转存、删除或重投；不通过 `consumer next`、新建消费者、重建 durable 或清空流来模拟只读检查。

## 存储与验证范围

FileStorage 必须挂载持久卷，并限制 JetStream 总文件容量、预留文件系统空间；三个 MaxBytes 之和不是磁盘总占用，尚有索引、消费者状态和其他流。Replicas=1 仅为单机演示，不提供节点容灾；生产副本、故障域及文件同步策略必须另行确定，PubAck 不能泛化为任意掉电都不丢失。

2026-09-20 在 NATS Server 2.14.6、nats.go 1.53.1 上，通过随机独立文件流验证了源流拒绝新写入、死信满额后的保留和恢复、通知满额导致的捕获缺口。测试使用 MaxMsgs=1 注入边界，不填满宿主磁盘，不操作已有业务流；成功按 ownership 清理，失败保留。另有下述隔离 Broker 重启验证；未验证真实磁盘耗尽、宿主掉电、多副本故障或生产吞吐。

参考：[NATS 保留策略](https://docs.nats.io/learn/jetstream/retention-policies)、[NATS 确认与重投](https://docs.nats.io/learn/jetstream/acknowledgment)。以实际部署版本返回的配置为准。

## 隔离 Broker 重启验收

`TestMaxDeliverRecoveryAfterBrokerRestart` 仅使用指定的本地镜像创建独立容器，禁止自动拉取；只绑定 loopback 空闲端口，并固定映射保证重启后地址不变。数据存放于该测试容器 `/data`，本用例重启同一容器，不测试删除容器后恢复；生产需使用持久卷。配置为：

```conf
port: 4222
jetstream {
  store_dir: /data
  max_file_store: 64MB
  sync_interval: always
}
```

所有测试流为 FileStorage、R1。`sync_interval: always` 是本次故障试验的明确参数，不修改共享 Broker，也未决定生产默认同步策略；其吞吐开销需要单独测量。[NATS 同步配置](https://github.com/nats-io/nats.docs/blob/master/running-a-nats-service/configuration/README.md)

```sh
# 在 tx 根目录执行；替换为本地已存在且已核验版本的镜像 ID。
TX_TASKQUEUE_RESTART_IMAGE='sha256:<本地镜像ID>' \
  go test -race ./internal/taskqueue -run '^TestMaxDeliverRecoveryAfterBrokerRestart$' -count=1 -v
```

先完成一次死信转存，再保存新源任务与待消费的重复恢复通知，执行容器 SIGKILL 并启动同一容器。验证新源任务、既有死信、通知与 durable 均可继续访问；旧通知不重复转存，随后真实耗尽、满额恢复和只读核对继续通过。用例成功清理本次容器及配置，失败保留并输出定位信息；未设置镜像变量时跳过，显式启用后 Docker/镜像不可用则失败。

2026-09-20 在 NATS 2.14.6 实测通过。这是 Broker 进程强制终止后的恢复证据，宿主和 Docker VM 仍在运行，不能替代掉电、磁盘损坏或 R3 多副本故障验证；也未在本用例验证业务执行中 Broker 崩溃后的重复执行窗口。

## R3 单节点与多数派故障验收

`TestRecoveryAfterClusterLeaderLoss` 在新建 Docker 网络内启动三个 NATS 节点，节点使用独立 `/data` 和 `sync_interval: always`；源流、死信流、通知流及两个 durable 均显式配置 Replicas=3。客户端使用三个 loopback 固定端口，忽略容器网络内的发现地址；后者只是本机夹具连接方式，不是生产客户端默认配置。

```sh
TX_TASKQUEUE_RESTART_IMAGE='sha256:<本地镜像ID>' \
  go test -race ./internal/taskqueue -run '^TestRecoveryAfterClusterLeaderLoss$' -count=1 -v
```

验收顺序：

1. 等待三个流和两个 durable 的全部 follower 报告 Current，保存耗尽源任务、未确认通知及已有死信。
2. 查询源流实际 leader，仅对该测试节点执行 SIGKILL；等待剩余流/consumer 选出可用 leader。
3. 用剩余两节点确认新任务发布，并执行现有 Recovery：转存死信、删除源残留、确认通知。
4. 再终止一个节点，确认发布不能返回成功。错误/超时属于结果不确定，不自动补发，不据此断言消息未写入。
5. 启动两故障节点，等待三个流和两个 durable 的 follower 全部追平；核对多数派可用期间已确认的源任务、死信内容和通知清理结果。

成功按本测试持有的容器 ID 清理容器、网络及配置，失败保留并输出定位信息。现有共享 NATS、etcd 和业务资源不参与故障注入。源流 leader 不必同时是其他流或 consumer 的 leader；用例分别等待各组恢复，不以一次客户端重连替代 JetStream 各组就绪。

本机三容器只验证复制/选举协议及恢复适配行为，不是三个物理故障域。没有覆盖宿主掉电、网络分区、磁盘损坏、流量峰值或多数派丢失期间每个任务的最终执行结果。生产仍需规划独立故障域、持久卷、资源容量及同步策略，不据此承诺无损或 exactly-once。

周期同步改变确认后的落盘保证，不能仅凭短窗口吞吐决定生产配置；当前保留 always 作为保守验收基线。部署与测试创建资源前须验证 JetStream 元数据 API 可用，TCP 可连接不等于元数据选主完成。

## 可复现本地 demo

四个隔离依赖、两套任务资源清单、配置副本生成与正式示例启动步骤见 [task-demo](../../deploy/task-demo/README.md)。`cmd/task-resources` 仅作部署预创建，不修改框架启动职责；普通测试夹具与 demo 配置分别维护重试/容量参数，不能把单条在途测试参数当生产建议。
