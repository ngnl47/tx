# 2026-09-26 四机 RecStream 分段等待定位

## 目标与口径

本轮沿用四机万连 `system-ping`：10,000 个 TCP Session，每个 Session 每秒 10 次上行及 10 次下行，Ping/Pong 1:1，八个 Node worker，每 Session 最多四条在途。正式窗口 30 秒；服务配置为 Agent×1 P8、Login×2 P4、Game×2 P4、MsgQ×2 P4，MsgQ 与 Redis/Mongo/etcd/NATS 同宿主机。目的不是寻找单台 8 核机的极限，也不是扩大压力档，而是把 Agent `Rec` 的百毫秒级等待拆成 lane 排队与流往返。

测试二进制仅在 `txbench` 构建中记录分段计时，并按 `(roleID+seq)%128==0` 抽样。普通构建对应空实现，不包含采集、JSON 写盘或采样锁。先用 100 Session 冒烟验证两类计时，再完成两轮万连短测。两轮是独立窗口，不能拼接各自延迟分位数；第二轮的 Agent `Rec`、分道等待和流往返按相同 `(roleID, seq)` 配对。

## 第一轮：MsgQ 内部 `mailbox.Rec`

第一轮对 Agent 完整 `Rec` 与 MsgQ 内部 `mailbox.Rec` 低比例采样，配对 23,554 条。正式窗口完成 2,828,528/3,000,000，未发送 171,472，业务失败 0，完成吞吐 94,284/s；端到端延迟 P50/P95/P99 为 200.90/532.46/599.68ms，最大 1,066.98ms。Agent `Rec` P50/P95/P99 为 52.841/135.105/158.193ms；MsgQ `mailbox.Rec` 为 0.001/0.002/0.007ms，最大 0.695ms。

因此，MsgQ 邮箱内游标处理不是这轮 Agent `Rec` 长尾的耗时来源。Agent 计时还包括等待本地 RecStream 分道、gRPC 流发送与接收以及相关调度，不能把它等同于 MsgQ RPC 服务端执行时间。

## 第二轮：RecStream 分道等待与流往返

第二轮只在 Agent `RecStream` 两个区间记时：获取 `SessionID % 64` 对应 lane 的等待，以及获得 lane 后的 `Send+Recv` 往返。配对样本 23,808 条。正式窗口完成 2,849,822/3,000,000，未发送 150,178，业务失败 0，吞吐 94,994/s；P50/P95/P99 为 181.34/513.03/587.58ms，最大 1,069.20ms。Agent `Rec` P50/P95/P99 为 54.885/130.040/147.534ms。

| 阶段 | P50 | P95 | P99 | 平均 |
| --- | ---: | ---: | ---: | ---: |
| 等待 64 路 RecStream lane | 53.782ms | 128.861ms | 146.194ms | 56.652ms |
| lane 内 gRPC `Send+Recv` | 0.637ms | 3.090ms | 12.134ms | 1.168ms |

以相同键配对时，两阶段耗时之和的 P99 为 147.431ms，Agent `Rec` 的 P99 为 147.534ms，二者只差 0.103ms。分位数不是严格的逐级可加指标，但该配对结果显示这次采样的 `Rec` 延迟几乎都落在 lane 等待和 lane 内往返中；独立的 MsgQ 内部计时则显示邮箱处理本身只有微秒级。阶段计时会带来少量扰动，结果用于定位等待，不作为无采样吞吐基线。

Agent P8 本轮平均约 7.30 核、峰值约 7.7 核；MsgQ 两实例各约 1.96 核。八个客户端 worker 事件循环利用率均值约 92.32%，各 worker P99 停顿约 114–146ms。客户端也接近供给上限，因此端到端未发送和 P99 不能全归因于 Agent。此前无采样的 64→256 lane 对照未显示整链收益；当前数据虽定位到 lane 等待，却不足以说明单纯增加 lane 数能提高吞吐。

## 结论和后续

本轮把主要的 Agent `Rec` 长尾定位到 64 路 lane 的排队等待，而不是 MsgQ `mailbox.Rec` 内部执行或 lane 内 gRPC 往返。它解释了 Agent `Rec` 在高负载时为什么很慢，但没有单独解释端到端长尾，也没有给出修复 lane 竞争就会提高整链吞吐的证据：客户端事件循环同样接近饱和，且此前 64→256 的无采样对照没有吞吐提升。

复核 2026-09-25 的 15 秒 Agent CPU profile（16 lane 的 `txbench` 诊断构建，108.44 CPU 秒，画像会扰动负载）发现：`Syscall6` 占样本 20.85%；其中 Agent 到客户端的 vectored `writev` 栈累计 16.28 秒，占总样本约 15.0%，调用路径经过 `writeBuffersFull`、`tcpTransport.Write` 和 `sessionFrameWriter`。TCP 读取栈约 3.18 秒，占 2.9%；该画像中的 RecStream 和下行 syscall 数值不能直接代表 2026-09-26 的 64 lane 测量。实现已经用 `net.Buffers.WriteTo` 执行向量写，不能仅凭 writev 样本再加一层缓冲或重写写入路径。

保留 64 lane 和当前默认行为，不启用租约、不继续盲目调 lane 数。下一步在当前 64 lane 二进制和同类负载下采集短 CPU profile，并与 Agent 每秒 TCP 收发包/字节和客户端事件循环指标对齐，确认高 CPU 是否仍主要落在客户端 socket 写入，还是转移到其他路径。只有确认可由代码降低的局部热点后才做短 A/B；若只是减少 `Rec` 等待而不改善完成吞吐、漏发或 P99，则停止该方向，不堆云服务器。

原始数据：`tx-artifacts/cloud-20260925-four-host/msgq-internal-stage-20260926/`、`tx-artifacts/cloud-20260925-four-host/recstream-breakdown-20260926/`。各阶段原始归档 SHA-256 记录在同级 `tx-artifacts/cloud-20260925-four-host/SHA256SUMS`。
