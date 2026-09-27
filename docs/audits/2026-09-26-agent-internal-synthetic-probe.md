# Agent 内部合成会话隔离探针

## 目的

将外部登录、客户端事件循环、TCP accept/read/write 从负载入口中拿掉，观察 Agent 会话建立后的正式 MsgQ 与 Actor 路径。探针只存在于 `-tags=txbench` 构建，使用 Agent 已有 WebSocket HTTP 监听器上的同机 POST 路径：只接受 loopback 来源或来源 IP 等于该 HTTP 监听器的绑定 IP；普通构建不包含控制入口。探针创建独立 Account、Role、Session 身份，不把一条业务请求复制到同一角色。

保留的路径：Agent 会话登记、MsgQ `Acquire/Init`、上行逐条 `Rec`、正式 Dispatcher 与 Agent→Game Actor 调用、MsgQ 下行读取，以及 Agent 会话 goroutine。绕过的路径：登录票据认证、客户端与 Agent 之间的 socket 和 txclient 调度。

## 调用

先用同一工作区源码构建 `txbench` server，并按当前集群配置部署。对 Agent 的 WebSocket HTTP 监听端口，在 Agent 主机本地向绑定地址发送 POST；当前私网绑定示例为 `172.21.16.5:19001`。不要从其他集群节点调用该入口。

以下样例要求已经准备好 2,000 个独立压测角色：当前专用夹具中的角色 ID 从 480100 开始，对应账号 ID 从 1 开始，服 ID 为 1001；更换夹具后需重新核对映射。Session ID 使用未占用区间，不能与在线真实会话或其他压测轮次重叠。先以少量会话冒烟，再跑 2,000 Session × 10 次/s × 30 秒。`System_Ping` 的 BON 请求编码为 `CAIFAWMFC1N5c3N0ZW1fUGluZwUBYg==`，命令号为 10228。

```sh
curl --max-time 240 -sS http://172.21.16.5:19001/__txbench/agent-sessions \
  -H 'Content-Type: application/json' \
  -d '{"sessions":2000,"accountIDStart":1,"serverID":1001,"roleIDStart":480100,"sessionIDStart":700000000,"ratePerSession":10,"durationMs":30000,"commandID":10228,"payloadBase64":"CAIFAWMFC1N5c3RlbV9QaW5nBQFiCAA="}'
```

`plannedMessages` 是计划请求数；`sentMessages` 是成功放入每会话有界队列的数；`dropped` 是发生器无法及时放入队列的数；每会话最多保留一条正在执行和三条等待请求，与当前客户端最多四条在途设置对齐。`completed` 是 Agent 已向合成会话写出的上行完成 ACK 数；`downMessages` 是从 MsgQ 读取到的下行消息数；`failedSessions` 与 `firstError` 用于识别运行中断。服务端按现有 `TXGAME_AGENT_TIMINGS_DIR` 抽样时，仍可得到 Rec、Dispatcher/Actor Call、下行写入分段样本；对照轮必须采用相同采样配置。

## 对照与边界

用同一服务实例数、`GOMAXPROCS`、MsgQ 路径、游戏角色夹具和 30 秒负载，先跑合成会话，再跑正式 txclient TCP 会话。若合成路径接近目标、而真实客户端路径明显下降，差异落在被绕过的接入环节或负载发生器；若两者都慢，再用分段计时和 CPU/调度数据在 Agent、MsgQ、Game 内继续拆解。

合成结果不代表真实 TCP 万连、登录吞吐或网络带宽验收，也不等同于完整客户端行为。控制入口不用于生产二进制；压测结束后恢复无 `txbench` 标签的服务构建。

## 2026-09-26 云端验证结果

在四机集群现有部署上，Agent 使用单实例 P8，Game 与 MsgQ 各两实例 P4；登录服务没有参与本探针。以 2,000 个独立会话、每会话 10 次/s、30 秒运行两轮，结果均为：计划、入队、Agent 完成 ACK、MsgQ 下行读取各 600,000 条，0 丢弃、0 失败会话，耗时约 30 秒。该结果表明 Agent 建会话后的正式 TX 路径在此配置下能够处理 20,000 次/s；它不测外部 TCP 接入，也没有采集单请求 P99。

第一轮 `pidstat` 采样：Agent 平均约 77.6% 单核，RSS 约 132 MiB；两台 Game 各约 37%，RSS 约 133 MiB；两台 MsgQ 各约 28%，RSS 约 77 MiB。就这组样本而言，MsgQ CPU 没有先达到单核饱和。第二轮同步采样结果接近：Game 各约 42%，MsgQ 各约 29%。这只是固定实例数下的一个负载点，不能据此排除负载更高时的队列等待或其他共享资源瓶颈。

第二轮同时采集了 Agent 30 秒 CPU profile，文件见工作区外部测试产物 [`agent-internal-20k-20260926.cpu.pprof`](/Users/a47/workspace/tx-artifacts/agent-internal-synthetic-20260926/agent-internal-20k-20260926.cpu.pprof) 与 [`agent-internal-20k-20260926.cpu-top.txt`](/Users/a47/workspace/tx-artifacts/agent-internal-synthetic-20260926/agent-internal-20k-20260926.cpu-top.txt)。调用栈覆盖 Agent 上行 `Rec` 接收、Actor Dispatcher/Call、MsgQ 下行流读取和会话 goroutine；累计栈比例有重叠，不能相加为总耗时。平坦 CPU 样本中可见 runtime 调度、channel、锁以及 gRPC 接收成本；该 profile 只覆盖 Agent，且采样期间同时运行发生器，因此用于指路，不能视为纯 Agent 服务 CPU 分布。

初步判断：本轮目标负载下没有复现 TX 内部路径的吞吐失败，因此此前真实客户端压测中的未发送/长尾不应直接归因于 MsgQ 已饱和。当前仍需分辨 Agent 外部 TCP/客户端调度成本，以及 Agent 内部发生器本身与真实 socket 输入的差异；本轮探针也未证明 20,000 次/s 时的 P99 满足实时目标。

## 10,000 会话已知长尾点复测

为对齐此前的 100,000 次/s 长尾点，再用同一 Agent P8、Game×2 P4、MsgQ×2 P4 拓扑测试 10,000 会话×10/s、30 秒。内部探针完成 2,966,441/3,000,000，丢弃 33,559，零失败。该探针发生器由一个定时循环每 100ms 向 10,000 个会话逐个投递；它在这个档位自身可能限速，不能把这部分丢弃当成 TX 服务丢包，也不能用 98.9k/s 作为正式容量。

独立客户端机上的真实 TCP 对照复现了长尾：计划 3,000,000，完成 2,698,018，未发送 301,982，业务失败为 0，完成吞吐 89,934/s，P50/P95/P99 为 420/563/637ms。八个 Node worker 平均事件循环利用率约 79%，事件循环 P99 停顿均值约 73ms、最大约 504ms；其在途窗口等待累计约 49.4 小时，30.2 万个未发送中约 29.8 万是达到四条在途上限后跳过的计划槽位。服务进程平均 CPU：Agent 约 3.4 核、Game 各约 1.0 核、MsgQ 各约 0.82 核；没有一台 8 核宿主机被打满。TCP 客户端把 `System_Ping` 计划间隔固定为 100ms，响应慢时不补发错过的时刻，因此“未发送”是客户端端的供给结果，不代表服务端拒绝。

同拓扑低频 1/128 计时复测完成 2,796,698/3,000,000，未发送 203,302，零业务失败，吞吐 93,223/s，P99 617ms。该轮开启阶段计时写盘，只用于阶段定位，性能数字不与无采样轮做收益比较。Agent 侧分位数为：`Rec` P50/P95/P99 65/132/150ms，Actor `Call` 6.8/84.7/113.3ms，`resolve` P99 0.008ms，`downWrite` P99 0.037ms。相同 `(roleID, seq)` 配对后，`Rec + Dispatcher` P50/P95/P99 为 92/161/196ms；这里按请求逐条求和后再取分位数，没有相加两个独立 P99。

`Rec` 计时从进入 `SessionClient.Rec` 到获得确认，包含本地 RecStream lane 排队和流往返。此前的[RecStream 等待拆分](2026-09-26-recstream-lane-wait-isolation.md)已经把其中绝大多数延时定位到 64 路 lane 等待：P99 约 146ms，lane 内 `Send+Recv` P99 约 12ms；MsgQ `mailbox.Rec` 执行 P99 约 0.007ms。当前 `Rec` P99 150ms 与该证据相符。此前 64→256 路的无采样完整链路对照没有提高吞吐，因此本轮没有据此扩 lane。Actor `Call` 也达到百毫秒级 P99，单独缩短 `Rec` 不能解释或清除全部长尾。

客户端已有的 `serverTime` 抽样显示 Handler 前 P50/P95/P99 为 397/526/571ms，Handler 后为 13/98/192ms。Agent 计时仅覆盖消息已到达 Agent 后的 `Rec` 与 Dispatcher/Call；上述两组分位数属于独立样本，不能相减归因。它们共同说明处理前排队、Agent 上行确认和 Actor 调用都会进入热路径；下行写帧自身不慢。20k/s 的同口径真实 TCP 轮此前完成 600,000/600,000，P99 3.45ms；因此长尾是在提高到 100k/s、单 Agent 承载 10,000 会话时随排队出现，不是固定网络往返时间。

本轮诊断后停止了 txbench Agent，并恢复 `/home/ubuntu/tx-perf/fourhost/bin/server` 标准二进制和 P8；100 会话真实 TCP 冒烟 500/500 全部完成、零未发送/失败，P99 2.72ms。云端压测保留原有 Game/MsgQ 实例；本地证据位于 [`tcp-10k`](/Users/a47/workspace/tx-artifacts/agent-internal-synthetic-20260926/tcp-10k/) 与 [`Agent CPU profile`](/Users/a47/workspace/tx-artifacts/agent-internal-synthetic-20260926/agent-internal-20k-20260926.cpu.pprof)。

当前可下的结论是：TX 内部路径在 20k/s 档稳定通过；100k/s 档完整 TCP 路径确有长尾，且逐条 Rec 的 lane 等待与 Agent→Game Call 都是有量级的串行服务时间。CPU 未满不代表这条每 Session 顺序执行的链路没有队列瓶颈。会话租约曾在四机 A/B 中降低 Agent/MsgQ CPU，但只提高约 0.32% 完成吞吐，P99 反而增加约 12ms，因此仍保持默认关闭；当前数据支持继续诊断 Call 服务端与连接内串行积压，不足以为降低确认语义开绿灯。

## Game Actor 执行时长探针已准备

`txbench` 构建现在会在 Game actor 的正式 `Receive` 开始执行时，按与 Agent 相同的 `(roleID, upSeq)` 每 128 条采样记录 `actorReceive`。它可与 Game 侧既有的 `runtimeCall` 计时按键配对：`runtimeCall - actorReceive` 近似表示投递后到 actor 开始处理前的队列等待及框架调度，不包含业务 Receive 本身。该差值不是纯 mailbox 指标，但能直接判断 113ms 的 Agent→Game `Call` P99 是否主要落在 actor 邮箱开始执行之前。普通构建使用空实现，不写文件。

本地已通过新增采样身份测试、普通及 `txbench` 两种 Linux 服务构建，以及 `actorrpc`、`framework` 两包的 race 测试。随后已在 Game×2 的 `txbench` 实例采集到云端样本：此前 10k×10/s 正式 TCP 轮的 `actorReceive` P99 为 43.612ms，`runtimeCall` P99 为 48.995ms。样本可与 Agent `Call` 计时配对，用来判断时延是否在 actor 开始执行前形成；它不是纯邮箱等待指标。

## Agent 内部合成路径 CPU 画像

为和上一轮的空 profile 区分，本轮在 Agent P8 txbench 进程上让 10,000 个合成会话以 10 次/s 运行 30 秒，同时请求 30 秒 CPU pprof，二者时间重叠。该路径保留正式 Agent 会话、MsgQ RecStream、Actor Call、下行读取与会话写入，绕过 Login、客户端 TCP/txclient 和外部 socket 读取。

发生器计划 3,000,000 条，成功入队并完成 2,969,055 条，约 98.9k/s；30,945 条未能入队，0 失败会话。未入队来自探针每会话有界队列，不是 MsgQ 或业务拒绝。pprof 30 秒收集 151.94 CPU 秒，约 5.06 核平均使用。平坦样本较明显的开销包括 `runtime.fpTracebackPartialExpand` 8.24%、`runtime.selectgo` 4.59% 和系统调用 3.74%；累计栈中 `Agent.dispatchUpMsg` 42.38%、`MsgQ.recStreamPool.Rec` 22.21%、`Agent.writeDownMsgs` 18.46%、`directActorAccess.Call` 11.33%。这些累计比例互相包含，不能相加。完整文件见工作区外部产物 [`agent-internal-100k-20260926.cpu.pprof`](/Users/a47/workspace/tx-artifacts/agent-internal-synthetic-20260926/agent-internal-100k-20260926.cpu.pprof) 和 [`agent-internal-100k-20260926.cpu-top.txt`](/Users/a47/workspace/tx-artifacts/agent-internal-synthetic-20260926/agent-internal-100k-20260926.cpu-top.txt)。

这是一份定向画像，不是干净的吞吐基线：Agent 开启了 1/128 阶段计时并写出约 13 MiB JSONL，pprof 栈采集本身也出现在热点中；不能把该轮 98.9k/s 与不带这些诊断开销的 TCP 轮直接相减归因。它证明 Agent 内部完整路径在该探针负载下处理了接近 100k/s，尚未证明无采样时的容量，也没有证明 20/50 玩家目标的 P99。当前证据没有显示单个业务函数独占 CPU；主要工作横跨 RecStream/gRPC 收发、actor 调用、下行流和大量会话循环/运行时调度。

## 诊断服务恢复

本轮结束后恢复了标准 `server` 二进制：Agent 单实例 P8，Game×2 与 MsgQ×2 各 P4，Login×2 保持 P4；诊断 pprof、Game 计时和 MsgQ 计时单元均已停止。恢复前导出 1,024 个测试 `role` 槽位并保存到 [`role-slots-before-restore-20260926.etcd.json`](/Users/a47/workspace/tx-artifacts/agent-internal-synthetic-20260926/role-slots-before-restore-20260926.etcd.json)，停止旧 Game 后仅删除 `/tx/e2e/txgame/discovery/slots/actor-v1.cm9sZQ.p` 前缀，再由标准 Game 实例在真实 TCP 冒烟期间重新分配。100 会话×5/s×10 秒完成 5,000/5,000，0 未发送、0 失败，P99 2.53ms；新生成的 94 个 role 分区槽位分配给两个新 Game InstanceID（48/46），快照见 [`role-slots-after-restore-smoke-20260926.etcd.json`](/Users/a47/workspace/tx-artifacts/agent-internal-synthetic-20260926/role-slots-after-restore-smoke-20260926.etcd.json)。

本轮新增的 `txbench` gRPC 服务端 `PubRole` Begin→End 计时已通过普通与 `txbench` 编译测试，但尚未部署；它是下一项诊断，用来判断 Game 的 `PubRole` RPC 长尾是否发生在 MsgQ RPC 服务端处理范围内。当前完整服务集群已恢复标准二进制。
