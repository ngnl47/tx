# 多进程排水回归验收

## 范围与结论

2026-09-09 依次执行两套独立示例的现有真实集群套件，均返回 `status=passed`、命令退出码 0。
本轮没有修改运行时代码，也没有修改第三方源码。验证目标是排水流程、关闭后的重新路由以及持久化失败恢复，
不把功能通过描述为第三方 Provider 关闭竞态已经修复。

执行前代码基线：tx `4e64997`，txgame `3828c0a`，txgameslg `965948b`，txclient `fe2c7e8`，txclientslg `1c9ee88`。

## 无状态示例

在 `/Users/a47/workspace/txgame/server` 执行：

```sh
go run ./cmd/cluster-e2e --artifact-root /private/tmp/txgame-drain-e2e --timeout 6m
```

本地证据：`/private/tmp/txgame-drain-e2e/20260909-110351.274879000/summary.json`。

- 独立启动双 Game、双 Agent、双 MsgQ、双 Login；包含 Game、MsgQ、Login 重启，共记录 12 个进程生命周期，全部退出码为 0。
- Game-A 排水关闭后，`client/game-a-drained.jsonl` 的 `game_probe` 明确返回 Game-B 实例 `txgame-1855-14160`。
- Game-A 重启获得新实例身份，后续恢复双实例处理；MsgQ 单实例承接及重启后分流通过。
- TCP/WebSocket、上行重放、下行确认、同角色跨 Agent 双 Session、Fusion 存储探针通过。
- Login 停服前后已建立连接的 SessionID、ConnectionID 保持不变，后续请求继续处理。
- 最终服务发现为空，套件仅清理 ownership 校验通过的 Redis DB 12、Mongo `txgame_cluster_e2e` 和专用 etcd 前缀；本地日志与摘要保留。

## 有状态示例

在 `/Users/a47/workspace/txgameslg/server` 执行：

```sh
go run ./cmd/cluster-e2e --artifact-root /private/tmp/txgameslg-drain-e2e --shutdown-failure --timeout 8m
```

本地证据：`/private/tmp/txgameslg-drain-e2e/20260909-110504.184197000/summary.json`。

- 独立启动双 Agent、双 MsgQ、双 Login、slggame、slgworld，后续启动恢复实例，共记录 13 个进程生命周期。
- RoleActor 与 WorldActor 调用、客户端直达 WorldActor、同角色双 Session 共享下行、Seq/Ack 和恢复通过。
- 使用专用 Mongo 集合 validator 拒绝 checkpoint，保留真实 WAL；向 `slgworld-a` 发送 SIGTERM。
- `slgworld-a.log` 中关闭失败 `attempt=1..4`，对应首次尝试加三次重试；随后记录重试耗尽并自行退出，`.exit` 为 `exit status 1`。
- 解除 validator 后由 `slgworld-b` 恢复 WAL；摘要为 `walRecovered=true`、`worldGeneration=2`、`worldCheckpointSeq=1`。
- 旧 generation 写入被拒绝，恢复后的 WAL 清理通过；slggame 正常排水交接后 `slgGameGeneration=2`。
- `msgq-a` 的 `signal: killed` 是显式故障注入；除它与预期关闭失败的 `slgworld-a` 外，其余 11 个进程均以 0 退出。
- 最终服务发现为空，套件仅清理 ownership 校验通过的 Redis DB 13、Mongo `txgameslg_cluster_e2e` 和专用 etcd 前缀；本地日志与摘要保留。

## Actor 双进程排水窗口补充验收

在 tx 工程执行：

```sh
TX_ETCD_ENDPOINT=127.0.0.1:2379 go test ./modules/actor -run '^TestActorDrainAcrossProcesses$' -count=3 -v -timeout=90s
```

三次均通过，本地日志：`/private/tmp/tx-drain-process-acceptance.log`。
该用例取代同进程双 Runtime 的重复拒绝测试，使用两个独立进程、真实 etcd 和 Actor Remote。
测试管道只传递屏障信号，业务请求不经过管道，不新增生产协议或测试配置。

- 请求 1 在目标处理器内等待释放，请求 2 已进入同一目标邮箱；目标观察到两条待完成工作后才关闸。
- 未释放请求 1 时，有限时间的 Drain 必须返回超时，不能提前声称排空。
- 此时跨进程发起的迟到 Call 和 TellAccepted 均返回 `CallErrorCodeRejected`，不进入业务处理。
- 释放屏障后，目标核对请求 1、2 顺序完成，Drain 成功；调用方收到两条成功响应后才允许目标关闭，子进程退出码为 0。

## Agent 实例拒绝缓存补充验收

在 tx 工程执行：

```sh
TX_ETCD_ENDPOINT=127.0.0.1:2379 go test ./modules/agent -run '^TestActorDispatcherDrainCacheAcrossProcesses$' -count=3 -v -timeout=90s
```

三次均通过，本地日志：`/private/tmp/tx-agent-drain-cache-acceptance.log`。
调用方使用真实 ActorDispatcher 与 Runtime，目标为独立进程中两个不同 PID 的固定 Kind 宿主，依赖真实 etcd 与 Actor Remote。
测试包装器仅在调用真实 Runtime.Call 前计数，不制造成功或拒绝响应，不添加生产监控接口。

| 阶段 | 累计 Runtime.Call 次数 | 结果 |
| --- | ---: | --- |
| 分别预热两个 Actor | 2 | 均收到正确的处理完成确认 |
| 目标关闸后首次请求 | 3 | 远端明确拒绝 |
| 请求同实例另一个 Actor | 3 | 本地缓存拒绝，没有再次调用远端 |
| 真实等待缓存到期后请求 | 4 | 重新调用远端，仍收到排水拒绝 |
| 同地址发现快照改为新实例身份 | 5 | 不沿用旧身份缓存，重新调用远端 |
| 快照切回旧身份 | 5 | 旧身份刚刷新的缓存仍生效 |

反向验证曾仅在测试装配中移除 Directory，跨 Actor 步骤以 `Runtime.Call count = 4, want 3` 失败；
恢复后通过。反向验证日志：`/private/tmp/tx-agent-drain-cache-negative.log`，测试源码不保留该临时改动。
原单元测试移除上述重复场景及等待，保留未知身份、普通超时和迟到响应身份变化的独立边界。

## txgameslg 组合根最终写回补充验收

在 txgameslg/server 执行：

```sh
TXGAMESLG_SHUTDOWN_TEST=1 go test ./tests/integration -run '^TestSLGGameShutdownFlushesPendingRole$' -count=3 -v -timeout=60s
```

使用真实项目组合根、生成 Role_Rename 协议、RoleActor、Procedure 和 Fusion，周期写回间隔设为 1 小时。
改名响应成功后直接读取 Redis，名称仍为空；Server.Stop 成功返回后再次读取，名称为 `shutdown-role`，
保存的 Messenger 再调用则返回 `ErrRuntimeStopped`。这证明关闭承担了最终写回，而非短周期后台任务提前完成。
测试替换原有周期刷盘用例，不新增生产接口。日志：`/private/tmp/txgameslg-shutdown-flush.log`。

代码核对：`internal/framework/shutdown.go` 先执行 Server.Drain，再执行 cmodHost.drain，
成功后才清理服务并执行 cmodHost.stop；这属于源码顺序证据，不是测试的逐阶段事件记录。

## 证据边界

- 本轮使用正常构建，不是 `-race` 验收；已知第三方 Provider 关闭竞态仍保留为依赖风险。
- 示例套件在 Game 排水关闭后继续客户端场景；上面的窗口补充验收直接验证 tx Actor Runtime，
  不经过客户端、Agent 或框架持久化收尾，不能据此声称已验证“同一排水窗口内的最终刷盘与进程退出顺序”。
- 窗口用例使用 Runtime 配置后的固定实体邮箱与直连 PID，刻意排除虚拟 Actor 放置变化；不证明网络分区下的唯一所有权。
- 缓存补充验收的计数位于 Runtime.Call 边界，不是网络包数；调用方直接进入 ActorDispatcher，不经过客户端登录、Agent 网络入口或 MsgQ。
- 实例身份变化由本地 ServiceDirectory 快照注入，不是实际 Watch 更新或在 1 秒内重启目标进程；不能将它描述为完整滚动重启验收。
- 到期后目标仍处于排水状态，因此这里只证明重新尝试远端调用，不声称目标恢复业务成功。完整关闭后的路由恢复由两套示例验收另行覆盖。
- 组合根最终写回用例在同一测试进程中运行 slggame，业务调用已完成后才启动关闭；不代表执行中的业务与关闭并发窗口已完整覆盖，也不声称同一时刻已完成 Mongo 延迟落地。

未覆盖项的优先级、实施范围和后续安排由 [实施计划](../IMPLEMENTATION_PLAN.md) 统一维护，本报告只保留本次证据及其边界。
