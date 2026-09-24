# Agent→MsgQ Rec 与同进程装配对照

## 目的与边界

验证 `Rec` 本地调用和复用连接的 gRPC 调用成本，并观察 `agent,msgq` 同进程装配对正式 Ping/Pong 链路的影响。本轮不改变 Seq/Ack、业务分派、下行 `StreamFetch` 或确认时机。基准和进程采样只存在于 `_test.go` 与旁路脚本，不进入服务运行路径。

所有本机数据来自 Intel i9-9880H、8 物理核/16 逻辑线程、Go 1.27.0、Node 26.0.0。完整链路共用本机 Docker 依赖，使用 `cluster-perf` 原有正式服务与 TCP 客户端：两个 Game、两个 Login；分进程为两个 Agent 加两个 MsgQ，合部署为两个 `agent,msgq`。两种拓扑的 `sum(GOMAXPROCS)` 均为 16：分进程每实例 2，合部署的 `agent,msgq` 每实例 4，其余实例 2。合部署复用现有 `LocalMsgQ`，`Rec`、`FetchSome`、`Confirm` 本地调用；Game→MsgQ 发布和 Agent→Game 分派仍为原有 gRPC。`GOMAXPROCS` 不是 CPU 硬配额，两种拓扑的调度与归属也不完全相同。

## Rec 定向基准

`BenchmarkRecTransport` 对同一进程内的 MsgQ 邮箱逐序号调用 `Rec`；gRPC 组使用复用的连接和 `127.0.0.1` TCP 监听，初始化不计入操作时间。`go test -benchmem -count=5` 的中位数：

| 路径 | 每次耗时 | 每次分配字节 | 每次分配次数 |
| --- | ---: | ---: | ---: |
| 本地 `SessionClient` | 463.5 ns | 127 B | 2 |
| 复用连接的 loopback gRPC | 113,023 ns | 9,782 B | 156 |

此差值含客户端与服务端的序列化、gRPC/HTTP2、loopback TCP、调度与处理，不能只称为网络时间，也不能按单次差值直接外推万连 P99。另一次 5 秒 gRPC CPU/heap profile 的基准为 120,905 ns/op、9,491 B/op、156 allocs/op；分配画像的主要条目包括 gRPC `newClientStream`、HTTP2 header 解析和服务端请求处理。CPU flat 样本主要在系统调用与调度等待，不能把它们简单解释为 MsgQ 邮箱锁竞争。

## 完整链路 A/B

同机顺序运行，TCP 客户端进程数为 2、建连上限按档设置、预热 2 秒、日志 `warn`，不启用服务内压测计数或性能开关。两个档位均保留原始客户端结果和每秒旁路 `ps` 采样。表中 P99 是客户端完整 Ping/Pong 的延迟。

| 会话 × 每会话上行/s | 拓扑 | 完成/计划 | 未发送 | P50 | P99 |
| --- | --- | ---: | ---: | ---: | ---: |
| 1000 × 5，20 秒 | 合部署 | 100,000/100,000 | 0 | 0.622 ms | 1.653 ms |
| 1000 × 5，20 秒 | 分进程 | 100,000/100,000 | 0 | 1.070 ms | 4.440 ms |
| 1500 × 10，30 秒 | 分进程 | 446,564/450,000 | 3,436 | 19.225 ms | 134.205 ms |
| 1500 × 10，30 秒 | 合部署 | 446,362/450,000 | 3,638 | 3.203 ms | 109.201 ms |

1000 × 5 两组均完整发出并完成，合部署 P99 比分进程低约 63%。正式窗口内 `ps` 样本的 Agent+MsgQ 组合 CPU 百分比均值约为分进程 498%、合部署 281%；Game 约为 222%、196%。1500 × 10 两组均出现约 0.8% 计划未发送，吞吐都约 14.9k/s，不能算容量通过，也不能据此宣称合部署提高最大吞吐；该档 Agent+MsgQ 的旁路 CPU 均值约为分进程 610%、合部署 507%，Game 约为 284%、309%。这些均值来自单轮、约 15～25 个 `ps` 时点；`ps` 百分比有平滑且两组进程数不同，仅用于观察趋势，不是精确 CPU 时间或纯 `Rec` 归因。

前一档的差异表明跨进程会话通信是值得优化的成本；后一档没有显示吞吐上限随合部署提高，客户端计划发送和其他服务仍可能约束总量。合部署同时消除了 `Rec`、下行流和 `Confirm` 的跨进程传输；本轮不能把完整链路收益全部归给 `Rec`。这里的本机结果也不能代替此前四机 10,000 会话、4/10 档的容量结论。

## 恢复边界

定向测试模拟 MsgQ 已提交 `Rec(Seq=1)`、Agent 在收到结果前取消，确认业务 Dispatcher 未执行；恢复连接后 Agent 的 `FrameReady.Ack` 为 1。另一个 MsgQ 测试使用真实内存邮箱确认：`Rec` 推进的 `UpMsgSeq=1` 会由恢复 `Init` 返回。两项测试说明已接受上行与已执行业务之间存在可观察的确认窗口；这是受控取消与存根组合，不是生产进程 `kill` 或跨主机故障注入。客户端按 `FrameReady.Ack` 释放重放前缀，因此该窗口可能使未执行请求在恢复时不再重放。

当前 `Rec` 在业务调用之前推进序号。因此“业务成功但 Agent 完成 ACK 丢失，MsgQ 仍是旧序号而导致重放”不符合当前实现；若将 `Rec` 右移到业务之后，则必须重新评估这个重复执行窗口。普通上行的确认语义和故障后补偿尚需决策，不能凭改用 gRPC 双向流解决。

## 复现与验证

- 基准：`go test ./modules/msgq -run '^$' -bench '^BenchmarkRecTransport$' -benchmem -count=5`。
- 正式对照：`cluster-perf` 使用 `--colocate-agent-msgq` 切换拓扑；两档分别设置 `--concurrency 1000 --rate-per-session 5 --duration 20s --connect-rate 100` 和 `--concurrency 1500 --rate-per-session 10 --duration 30s --connect-rate 50`，均指定 `--client-processes 2 --warmup 2s --log-level warn`。
- 原始报告、`Rec` CPU/heap profile、旁路进程采样位于 `tx-artifacts/rec-colocate-20260924/`。第一次 16 会话冒烟完成 96/96 后因编排器严格解析不认识新客户端统计字段而失败；补齐 `ClientPerfResult` 的 `extraDown`、`downCompleted`、`downThroughputPerSecond` 字段后，上述正式对照均使用同版工具。
