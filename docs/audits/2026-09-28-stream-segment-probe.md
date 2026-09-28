# Agent 单流分段探针接续记录

目标是保持六机 10,000 会话、20/50、双 Agent 单流拓扑不变，区分 Agent 发送锁、gRPC `Send`、Game 入邮箱与客户端发包前的等待。服务端新增 `tx_rpc_stream_send_lock_seconds` 和 `tx_rpc_stream_send_seconds`，每条流按 1/64 抽样；现有 `tx_rpc_stream_submit_seconds` 继续记录 Game 接收后入邮箱耗时。客户端在现有 `TXCLIENT_PERF_TIMINGS=1` 的 1/64 抽样中新增计划时隙到实际发起 Ping，以及发起 Ping 到本机 Socket 写入回调的耗时。新增项都使用本进程单调时钟，不使用跨机墙钟相减。Socket 写入回调只表示本地写入完成。

本地 `tx go test ./...`、`tx go test -race ./metrics ./modules/rpc`、`txclient npm test`（107 项）及 `npm run build` 通过。Linux 诊断二进制 SHA-256：`0cafd7cf396e8334467d66c9d5b2ed15e264f93c39740075e2df2a3d008a8f6e`。两台 Agent 部署该构建，两台 Game、两台 MsgQ 保持前轮构建；客户端使用同源码新诊断入口。

云端按顺序完成专用夹具 `prepare`、八个业务进程启动、10,000 角色准备和 8 会话冒烟；冒烟完成上行 80、下行 200，未发送 0。两台客户端各 5,000 会话就绪，四台业务主机已保存窗口前 `/metrics`。`2026-09-28` 启动双客户端 60 秒正式窗口后，本地到**六台**云实例的公网 SSH 连接同时超时。随后刷新腾讯云 CVM 控制台，六台实例均显示「待回收，14 天后释放」，这解释了 SSH 中断。**尚未取回正式结果和窗口后指标，也无法远端确认业务进程停止及夹具清理**。这轮不能用于性能判断；不为取数擅自恢复实例。

用户随后恢复六台实例。客户端 A 留下失败结果，客户端 B 没有正式结果；原窗口仍不可用于六机性能判断。下列接续操作已完成：取回 A 的残留诊断、停止旧业务服务、清理专用夹具，并在相同拓扑使用最终版探针重新跑完整窗口。新窗口的结果、限制和后续判断见[六机 20/50 单流分段诊断](2026-09-28-stream-segment-result.md)。

原先准备的恢复步骤（现已执行）：

1. 检查两个客户端 `/home/ubuntu/tx-perf/results/dual-client-2050-segment-20260928-{a,b}/` 的 `client.json`、各 worker 的 `.timings.json` 和运行进程；不要重启同名正式轮。
2. 从接入机 `19304`、第二 Agent 机 `19307`、Game 机 `19302/19303`、存储机 `19300/19301` 抓取窗口后 `/metrics`。窗口前快照位于各机 `/home/ubuntu/tx-perf/fourhost/results/stream-segment-20260928/before-<port>.prom`。若采样窗口跨越异常中断或进程重启，需标为无效。
3. 停止八个 `tx-perf-four-*.service` 业务单元，确认不再运行；等 etcd 临时成员租约释放后，在存储机执行 `/home/ubuntu/tx-perf/fourhost/bin/cluster-fixture-prom-20260928 --action cleanup`。只清理此专用夹具。
4. 取回日志和快照，再决定是否需要重跑。两台客户端公网地址 `82.156.250.60`、`58.87.70.175`；接入机 `101.42.5.166`，第二 Agent `49.233.148.8`，Game `42.193.104.252`，存储 `82.156.155.195`。

初版云端 Agent 抽样指标记录发生在释放发送锁之前；重跑前已将记录移到释放锁之后，并在两台 Agent 部署最终版 Linux `txbench` 构建 `tx-artifacts/cloud-20260928-dual-client/server-stream-timing-final-20260928`，SHA-256 为 `fe834bd2d20837d8924fdbe7d5c4d622da70bf2e9daea2e934e475ecbe97d36d`。重跑结果见上方报告；传输结构仍为单流。

若改用新实例续测，应重新部署本地最终版探针和客户端构建、准备专用夹具，并重新跑双客户端 5,000+5,000、20/50、60 秒同版窗口；旧实例上的前后快照不能与新实例拼接比较。
