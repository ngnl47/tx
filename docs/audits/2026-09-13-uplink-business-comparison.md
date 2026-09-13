# I2：完整上行与生成 GD 业务负载对照

日期：2026-09-13。基线 tx=`8de6005`，保留 I1 未提交变更。六工程只有 tx 有差异，未提交 Git；正式 framework、两个示例的生产接线和依赖版本未改变。本节点属于 [归属接线计划 I2](../superpowers/plans/2026-09-13-actor-ownership-integration.md)。

**采集完成后的范围决策：** 用户因大量上行转发的流量成本，明确排除“上行也经 NATS”。下文该组仅为已经完成的历史对照；后续测试和实施固定 Agent 上行直连 gRPC、Actor 间 RPC 使用 NATS，不再测试全 NATS 上行方案。

## 已发现并修正的问题

真实业务解码暴露原型缺陷：`Delivery.ToProto()` 生成 Tell/RequestId=0，DirectTransport 和 NATS CallFuture 仅把 Mode 改为 Call，现有 `DeliveryFromProto` 因缺少非零 Call 标记而拒绝。此前部分原型夹具直接消费 actorpb，另一些回显夹具手工传 RequestId=1，均未覆盖通用业务入口。

两种 Call 适配在副本上设置非零协议标记；Tell/TellAccepted 共用的发布入口清零。原型关联响应依靠独立 Future、NATS inbox 或 gRPC 请求，标记不是业务幂等键，不新增完成表或第二个请求序号分配器。新增真实 NATS 远端/本地和直连回归，使用现有业务解码器，并检查调用方信封未被改写。正式 ProtoActor API/SPI 保持现状。

## 负载与完成边界

链路：独立 Go TCP 客户端 → Agent Frame/BON/Session → MsgQ Rec → game Actor → Procedure/生成 GD → 约 10% 请求调用 world Actor → MsgQ PubSession/FetchSome → TCP 下行和精确上行 ACK。下行确认由下一次上行捎带；每个 Session 最后一个下行的服务端确认清理不计入完成延迟。

五个应用进程分别为 client、agent、game、world、msgq。512 个 game 实体、每实体两个 Session，共 1024 连接；64 个调用 worker，每 Session 最多一个在途请求。world 有 32 个身份，处理跨 Actor 校验响应，不模拟重型战斗计算或子协程池。两种新传输共享相同本地实体运行时；拓扑/assignment 固定，不把这项吞吐测试当作 Discovery 租约或正式宿主验收。

- 每次事务更新现有 txgame 生成的 Redis 类型 `ResRankItem`：一个根标量及四个 `ResourceRank` 子项，初始共 16 个子项。通过实际 `txn.Runtime`、默认 writeback.Manager 和 Redis Lua 适配写回；没有手工重写 Store。
- 按 `(SessionID + UpSeq) % 10 == 0` 发起跨 Actor RPC，错开 Session 相位。全量预热覆盖 game 和 world，首次建档、握手、连接建立均排除在采样之外。跨 Actor 请求/响应正文各 128 B。
- 上行和下行包含 BON 信封，表中的 128/1024 B 是业务 padding 大小，不是实际线上包大小。业务端解码 BON、执行事务、编码下行；客户端核对本次关联号、载荷及独立 ACK 后才计成功。
- writeback 周期为 100ms。完成 ACK 对应内存提交与 MsgQ 入队，不等待每条事务刷盘。每个采样窗口后显式 Flush，使用生成 Load 回读全部根和四个子字段，与累计完成数核对；这部分另外计时。
- 专属 Redis 关闭 RDB 定时保存及 AOF；本轮证明 Redis 写入/回读，不代表磁盘耐久性或 Mongo/Fusion 验收。没有真实业务分布、热门实体、重连、多 Session 广播和跨主机负载。

| 对照 | Agent → game | game → world | MsgQ 链路 |
| --- | --- | --- | --- |
| ProtoActor | 现有 ActorDispatcher/固定 Kind | 现有 Actor Call | 现有 gRPC |
| hybrid | 新 Dispatcher + SlotResolver + 现有 rpc.Sender/直连 Receiver | NATS Call | 同上 |
| NATS | 新 Dispatcher + SlotResolver + NATS Call | NATS Call | 同上 |

## 测量环境与统计

Go 1.27.0、linux/amd64、CGO_ENABLED=0；每个应用及 NATS/etcd 进程 GOMAXPROCS=4，无独占绑核/CPU 配额。Docker Desktop 29.6.1，Linux 6.12.76-linuxkit，VM 16 vCPU、8320471040 B 内存；Intel i9-9880H 宿主。所有进程共享同一 Linux 网络命名空间，经 loopback 通信。此布局不是多物理机或生产容量测试。

NATS 2.14.6、etcd 3.5.15、Redis 8.8 均使用本机已存在镜像，固定镜像 ID 见 manifest。nats.go 1.53.1、gRPC 1.82.1、go-redis 9.17.3 及 ProtoActor 锁定版本保持不变。无 TLS、JetStream、Queue Group、逐消息 Flush 或用户容器改动。

正式批按 ProtoActor/hybrid/NATS 轮转顺序运行五轮，每例预热 1024 次，显式 GC 后采样 4 秒。闭环测 128 B 与 1 KiB；固定到达率在 1 KiB 下测 1000/3000 次/s。后者排队容量 64，满时记录未发送，不等同于 NATS 丢失已接受消息。计划到达 P99 包含生成器晚点、排队和业务链路，仅针对成功样本；未发送/错误另外披露。原始延迟使用单调时钟，汇总重新排序计算 P99。

CPU 是五应用及三依赖的容器快照差，包含负载客户端；Go 分配只含五应用。目标/依赖及 RSS 快照的时间边界略宽于 4 秒窗口，CPU/完成数是同口径近似值，RSS 是预热/GC 后驻留值，不是峰值或世界内存需求。低到达率 CPU/请求还会放大后台与调度成本，不按它线性推算生产机器数。

首个五轮探索批使用所有 Session 同相位的“每十次”，导致实际跨 Actor 比例约 6%～11%，且可能把首次 world 加载混入窗口。该批完整保留在 `phase=repeat` 与 `exploratory/`，不作为最终选型结果。最终统计只选择修正后的 `phase=final`；源码和两份二进制 hash 已独立保存。

## 正式五轮结果

共 60 个正式窗口，完成 896982 次业务请求，执行错误 0；15 个高到达率窗口合计 2095 次未发送。各窗口及预热之后的累计事务回读均通过，不把预热数混入吞吐。下表为五轮中位数，括号内为范围。

| 对照 | padding B | 完成/s（范围） | P99 ms | 总 CPU μs/完成 | Go 分配 KiB/完成 | RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ProtoActor | 128 | 6,888（5,862～11,133） | 23.70 | 1462.3 | 42.75 | 344.4 |
| hybrid | 128 | 5,160（4,930～7,867） | 31.79 | 1919.3 | 54.28 | 316.0 |
| NATS | 128 | 4,852（4,124～5,108） | 34.14 | 1981.1 | 45.08 | 307.8 |
| ProtoActor | 1024 | 5,661（4,654～8,558） | 29.42 | 1811.7 | 66.84 | 421.8 |
| hybrid | 1024 | 4,642（3,961～5,159） | 38.24 | 2189.7 | 77.93 | 316.2 |
| NATS | 1024 | 4,264（3,467～4,426） | 45.53 | 2274.2 | 69.41 | 317.2 |

1 KiB 下，hybrid 相对 ProtoActor 中位吞吐低 18.0%、P99 高 30.0%、CPU/完成高 20.9%；全 NATS 中位吞吐低 24.7%、P99 高 54.8%。两种原型的该点 RSS 约低 25%，但这是固定 512 个小实体的驻留快照，不是重型 world 的内存估算。

按轮配对，hybrid 的 1 KiB 吞吐差为 -39.7%～-0.2%，其中三轮超过 10% 退化；全 NATS 为 -59.5%～-4.9%，其中四轮超过 10%。波动明显，不能把中位差宣称为固定倍率，也没有据此证明 hybrid 满足原门槛。

| 对照 | 计划到达/s | 完成/s 中位数 | 调用 P99 ms | 计划到达 P99 ms（范围） | 每窗未发送中位数（范围） |
| --- | ---: | ---: | ---: | ---: | ---: |
| ProtoActor | 1000 | 998.8 | 26.02 | 34.24（28.83～36.74） | 0（0～0） |
| hybrid | 1000 | 998.7 | 27.68 | 31.41（19.80～44.77） | 0（0～0） |
| NATS | 1000 | 998.5 | 35.80 | 42.50（34.05～47.06） | 0（0～0） |
| ProtoActor | 3000 | 2984.9 | 31.01 | 43.21（34.49～75.76） | 35（4～169） |
| hybrid | 3000 | 2967.0 | 40.37 | 49.95（42.28～126.57） | 97（32～667） |
| NATS | 3000 | 2956.5 | 42.95 | 56.23（44.34～65.49） | 134（31～245） |

1000/s 的全部 15 个窗口均完成 4000/4000 次，无错误或未发送；完成/s 稍低于 1000 是窗口末尾在途请求完成耗时造成的。3000/s 的 15 个窗口均有未发送，三组分别为 291/60000、1110/60000、694/60000，即 0.49%/1.85%/1.16%。未发送包括压测机调度晚点及队列满，不能单凭这些数字判断远端服务瓶颈；也不能用成功请求 P99 掩盖该部分。

两条直连的实现不同：固定版本 ProtoActor 的 EndpointWriter 在长期 gRPC stream 中发送 MessageBatch；hybrid 的 [DirectTransport](../../internal/actortrial/direct.go) 复用 [rpc.Sender](../../modules/rpc/sender.go) 的 unary Call，并在普通 RPC 信封中再次编码 Actor protobuf。当前结果是整条完成链路比较，不能归因为 Core NATS 的服务器吞吐上限，也不能据此断言单次额外跳数解释了全部 CPU 差。

## 独立连接字节与成本情景

另外三次诊断使用计数构建，各固定 1000/s、10 秒、10000 次完成，均无错误或未发送。此处只使用字节，不引用其吞吐/延迟。gRPC 链路取两端 net.Write/Writev，broker/Redis 原生进程的反向流量取 NATS/go-redis 客户端 net.Read，包含各自协议字节。gRPC 1.82.1 的 RawConn 非阻塞读取绕过 net.Read，不能将缺失的读计数写成零流量；etcd 控制面因此不纳入字节合计。客户端 TCP 两端累计读写逐连接完全一致，见 wire-validation.json。统计未包含 TCP/IP 头、重传、TLS、容器桥接或跨 broker 路由；不能直接当作云账单。下面每行合并该段两个方向，单位 B/完成业务。

| 链路 | ProtoActor | hybrid | NATS |
| --- | ---: | ---: | ---: |
| client ↔ agent | 2263.0 | 2263.0 | 2263.0 |
| agent ↔ game | 1394.1 | 1325.5 | 0.0 |
| agent ↔ nats | 0.0 | 0.0 | 1367.3 |
| game ↔ world | 64.9 | 0.0 | 0.0 |
| game ↔ nats | 0.0 | 54.2 | 1421.5 |
| world ↔ nats | 0.0 | 54.2 | 54.2 |
| game ↔ msgq | 1216.4 | 1212.3 | 1198.9 |
| agent ↔ msgq | 1346.7 | 1328.6 | 1309.6 |
| game ↔ redis | 4154.6 | 4143.7 | 4152.1 |

ProtoActor 还包含约 4.6 B/完成的 agent/world 控制流量；MsgQ 的 Redis 归属续租在这次短窗口未触发，不能宣称生产中没有这项流量。GD 刷盘约 4.15 KB/完成是本负载及 100ms 合并周期的结果，不外推其他 GD 规模或吞吐。

高流量上行这一段单独拆分：hybrid 的 agent→game 1246.0 B、game→agent 79.5 B，合计 1325.5 B；全 NATS 为 agent→broker 1319.2 B、broker→game 1321.2 B、game→broker 46.1 B、broker→agent 48.1 B，合计 2734.7 B，约为 2.06 倍。这里已从 game/broker 总量扣除同样的 world 调用。

计入共同的 MsgQ 与 Redis 后，已测后端业务及 Redis 链路（不含 client/agent 和 etcd 控制面）的连接字节由 hybrid 的 8118.5 B/完成增到 NATS 的 9503.7 B/完成，约增加 17.1%。broker 入站加出站从 2843.1 降到 108.4 B/完成，hybrid 降低约 96.2%；这是 10% 小型 Actor 间调用占比下的结果。

采用每个计费方向的公式 `月成本 = Σ(该方向 B/完成 × 平均完成/s × 2592000 / 10^9 × 该方向元/GB)`，GB 按十进制演示，供应商规则另核。一个发送到接收的方向只算一次；broker 转发是另一条有真实字节的方向。

- 若所有这些边的计费单价都为 0，额外转发的按量流量费为 0，但仍消耗 NIC 和 broker 处理资源。此项是假设，不是云厂商现行报价。
- 若平均 1000 次/s 持续 30 天，只按上行路径净增的 1409.2 B/完成估算，增加约 3652.5 GB/月；仅作算例，若单价为 0.1 元/GB，则约 365 元/月。
- 若仅 broker→game 跨收费区域、原 agent→game 在免费区域，应对 broker→game 的完整 1321.2 B/完成计费，而不是取路径净增差。
- 未确定区域、可用区和连接方向，不填写腾讯云或其他厂商的虚构账单；也不把单 broker 数字当作 NATS 集群复制成本。

## 采用判断与下一项

**I2 评审完成，T4/I3 正式迁移继续暂停。** 当前尚未通过原“共同负载退化超过 10% 则停止默认推广”的筛选线；本轮不升级依赖、不继续封包/发送器微优化，也不把已确认的传输分工等同于性能验收通过。

上行直连、Actor 间 NATS 的传输分工已确认，流量优势有实测支撑；上行也经 NATS 退出后续候选与测试。若希望继续 I3，需要决定是否用明确业务预算替换原筛选线：可考虑把**单个本机试验拓扑按 1000 请求/s 投递且全部完成、计划到达 P99 ≤100ms、零错误、零未发送、窗口后 GD 核验一致**作为下一接线阶段的临时准入。hybrid 已在短窗口达到该点，最慢一轮计划 P99 为 44.77ms，距该延迟预算约 2.2 倍；最低闭环完成吞吐约为该到达率的 4 倍，但闭环余量不等同于固定到达余量。

该候选只决定是否值得迁入正式宿主，不是生产 SLA；没有 CCU、每玩家上行频率、长时间/跨机测量，不能宣称游戏容量已足够。若维持原 10% 门槛，则保留正式 ProtoActor 路径，NATS 原型和此轮证据继续保留。

## 原始材料与复现

材料位于 `/private/tmp/tx-uplink-i2-20260913`：`run.py` 复用已有 `compare.py` 的进程、Docker 资源和 ownership 管理；`source-manifest.json` 固定源文件、原始生成包和二进制 hash；`*-manifest.json`/`*-peers.json` 保存实际镜像与配置。`results.jsonl` 及 `.json.gz` 为原始窗口与成功延迟，`persistence.jsonl` 保存窗口后累计事务核验；`summarize.py`、`wire-summary.py` 独立重算结果。首轮缺少下游 deadline、信封解码失败及探索批均保留，不计为成功样本。以下是本轮已执行的历史命令，原 `run.py final/wire` 包含已退出的全 NATS 组，后续不得直接重跑该矩阵。

```bash
python3 internal/actortrial/testdata/uplink/build.py /private/tmp/tx-uplink-i2-20260913/build
python3 internal/actortrial/testdata/uplink/build.py /private/tmp/tx-uplink-i2-20260913/wire --wire
python3 /private/tmp/tx-uplink-i2-20260913/run.py final
python3 /private/tmp/tx-uplink-i2-20260913/run.py wire
python3 /private/tmp/tx-uplink-i2-20260913/summarize.py
python3 /private/tmp/tx-uplink-i2-20260913/wire-summary.py
```

新夹具及覆盖构建入口在 [testdata/uplink](../../internal/actortrial/testdata/uplink/README.md)。普通测试和生产构建不会加载 txgame 依赖或网络计数器。计数器通过临时标准库 overlay 记录 net.Read/Write/Writev，原工具链和模块缓存未改写；其数据只用于连接字节评估。

## 本轮验证、差异审查与资源清理

以下为 I2 修复后的本轮结果，覆盖累计 I1 代码，不以 I1 历史结果代替：

| 验证 | 结果 | 原始记录 |
| --- | --- | --- |
| 实际业务解码回归 | 修复前直连、本地/远端 NATS Call 均被拒绝，Tell 未完成；修复后通过 | `decode-red-v2.log`、`decode-green.log` |
| tx 全量 `go test -mod=readonly ./... -count=1 -json` | 1185 pass / 40 skip / 0 fail | `full.jsonl`、`test-summary.json` |
| 真实依赖定向 race，运行三次 | 24 pass / 0 skip / 0 fail，无 race 报告 | `race.jsonl` |
| 最终 Linux 覆盖构建的 `TestUplink*` | 2 pass，验证错误 ACK/响应不能计完成及生成 GD 根/子字段回滚 | `fixture-check.log` |
| 完整业务与最终 Redis 回读 | 60 个性能窗口及三次独立字节诊断均通过 | `results.jsonl`、`persistence.jsonl` |

全量及定向 race 显式设置 `TX_ACTORTRIAL_NATS_URL=nats://127.0.0.1:4222`、`TX_ACTORTRIAL_ETCD_ENDPOINT=127.0.0.1:2379`、`TX_ACTORTRIAL_REDIS_ADDR=127.0.0.1:6379`。race 选择 `TestSharedRouting|TestOwnershipIntegration|TestSlotRollingHandoff`，`-count=3`。pass 计数包含子测试；40 个 skip 是其他未启用的依赖测试或独立压测入口，完整名单见 `test-summary.json`。不宣称其全部通过，也不宣称已解决第三方 ProtoActor Provider 的其他关闭 race。

成功测试的 22 个专属 etcd 前缀、12 个 Redis 键清理后读回均无残留；失败探索样本按原约束保留。六批临时容器标签均无残留，原五个开发容器仍运行。见 `cleanup-scopes.json`、`cleanup.log`、`container-cleanup.txt`。没有清空共享存储或停止用户容器。

累计未提交 Go 差异为 **+1073/-2，净增 1071 行**：I1 净增 433 行测试；I2 新增 633 行回归/业务夹具、5 行原型信封修正。正式生产组件及两个示例的运行代码没有变化，不能把本轮对照差异当作正式库的性能变化或瘦身收益。覆盖构建脚本另有 81 行，采集完全隔离。

普通正确性审查覆盖信封解码与输入所有权、收尾失败和响应未知边界、生成 GD/ACK 核验以及测量口径；字节审查明确排除了未完整计量的 etcd 控制面。必要验证之后执行一次累计差异 `ponytail-review`，结论：**Lean already. Ship.** 这里仅表示未发现多余复杂度，不代表 T4 性能门槛通过；保留必要 Store SPI、故障注入和独立示例边界，不开展全库 audit。

收尾仅更新范围决策、验证记录并删除构建脚本一处行末空白；被测 Go 源码、原始生成包及两份二进制 hash 均保持一致，无需重复压测。构建时脚本副本与交付 hash 复核见 `sampled-build.py`、`delivery-verification.json`。
