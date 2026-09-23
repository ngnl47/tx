# Agent→Game 短窗口合批预评估

日期：2026-09-23。此记录包含传输层微基准、真实链路的到达采样和隔离的合批上界试验，不是生产合批实现或端到端收益验证。

## 当前调用次数

每条有效上行命令先调用 MsgQ `Rec`，再通过直连 Actor `Call` 等待执行结果；连接按目标 endpoint 复用，`Call` 仍是逐命令 unary gRPC。Agent 会话按收到的帧顺序等待每条命令完成。Mandarin v3.3.13 的对照路径也逐条 `Rec`、逐条调用 Game 或 Actor；它累计部分客户端 ACK，但没有 Agent→Game 的窗口合批。

## 本地单次调用成本

在 Intel i9-9880H Mac、Darwin/amd64、`GOMAXPROCS=4` 上运行：

```text
GOCACHE=/private/tmp/tx-gocache GOMAXPROCS=4 go test ./modules/rpc -run '^$' -bench '^BenchmarkCallTransport$' -benchmem -count=3 -benchtime=1s
```

基准使用复用的 loopback gRPC 连接、256B 请求载荷、256B 响应载荷和仅校验请求的接收器。三次结果中位数：进程内接收 34.74ns/op、0 alloc/op；loopback 串行 Call 120.41µs/op、约 10.83KB/op、159 alloc/op；loopback 并发 Call 38.89µs/op、约 10.63KB/op、147 alloc/op。并发 `ns/op` 是总吞吐摊算值，不是单请求延迟。基准没有 MsgQ、Actor 路由/邮箱、业务计算或跨主机网络，不能外推端到端 P99。

## 短窗口可聚集规模

既有四机稳定档为 10,000 会话、37,485 次/s、两台 Agent 各 5,000 会话，目标有两台 Game。仅按平均到达速率计算，每条 Agent→Game 路径约 9.4 条/ms；实际负载有突发，不能用该均值代表桶大小。

四机同版短负载再次建立 10,000 会话，实测建链 22.796 s、438.7 会话/s；30.24 s 内 1,130,000/1,130,000 次完成，零失败/未发送，吞吐 37,367.7 次/s，端到端 P99 66.734 ms。在 Game A 私网入口对单方向 gRPC 采样 5 秒，按 HTTP/2 新请求 HEADERS 识别两个 Agent 连接，分别得到 46,780 和 46,068 个请求。单次抓包无内核丢包，离线解析有一个 TCP 缺口；请求数约为全局吞吐的一半，与 Game A 承载一半请求的拓扑相符。原始报文解析后已删除，仅保留到达分布汇总，不留存请求载荷。

离线以“同一 Agent 连接→Game A”为桶，从首条到达起计时，到 1/2ms 或 100 条先满足即刷新；下表是两条连接的范围，等待是新增排队时间的估算，不含批量 RPC 自身耗时。

| 触发窗口 | 平均项/桶 | 桶大小 P50 | 桶大小 P95 | 100 条触发次数 | 单项新增等待 P50 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1ms | 15.63–16.31 | 12–13 | 39–44 | 每连接 18 次 | 0.773–0.776ms |
| 2ms | 25.77–26.90 | 22 | 62–68 | 每连接 32–43 次 | 1.254–1.257ms |

这些是一个 Game 实例、约 5 秒、同一版本短负载的结果，不能代表全部流量形态。100 条主要是突发时的安全上限，通常由时间窗触发。

隔离的 `BenchmarkCallBatchUpperBound` 复用现有 gRPC `Call`，只把 1、10、20 份 256B 载荷合成一次请求与一次回复。运行 `GOCACHE=/private/tmp/tx-gocache GOMAXPROCS=4 go test ./modules/rpc -run '^$' -bench '^BenchmarkCallBatchUpperBound$' -benchmem -count=3 -benchtime=1s`，Mac、P4、三轮中位数分别约为 119.4、123.3、138.6µs/次，即 119.4、12.3、6.9µs/项。这里没有逐项路由、结果、错误、取消、接收端分发和额外等待，**仅是传输成本的乐观上界**，不能把约 10–17 倍的每项差距当成真实集群收益。

短窗口合批必须按目标实例聚集，MsgQ `Rec` 与 Game Actor `Call` 分别分桶，并保留每条命令的身份、Seq、取消、执行结果及错误；不同命令不能用 `singleflight` 去重。现有单条 `Rec`/`Call` gRPC 协议无法通过客户端等待来减少 RPC 次数，真正合批需要接收端有批量入口。Actor 身份目前存于每次 gRPC 的 metadata，批量协议必须将每项身份纳入批内，不能把整桶共享 metadata 当作同一个 Actor。现有 Agent CPU 画像中，Actor 直连 `Call` 累计约 21%，因此即使移除这条路径的全部开销，也不能消除其余 MsgQ 和客户端链路成本。

**下一步**：先做 Actor `Call` 的逐项结果批量协议与按目标实例分桶的 A/B 候选，重点核对逐项身份、邮箱投递、取消、排水和失败归类；候选验证后再评估 MsgQ `Rec` 是否值得独立合批。云端用相同 10,000 会话、相同上下行速率对照 CPU、吞吐及端到端 P99。当前不改生产分派或 Seq/Ack 契约。
