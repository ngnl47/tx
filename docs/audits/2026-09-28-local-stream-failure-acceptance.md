# 异步长流本地故障与排水验收（2026-09-28）

本轮只验证少量会话的状态转换，不以本机结果推断万连吞吐。云实例已归还；先使用本机回环 gRPC、内存 SessionStore 和 txclient 测试传输，随后使用本机 Docker 依赖运行真实服务进程。

| 边界 | 本地证据 | 结果 |
| --- | --- | --- |
| Agent→Game 单向流 | 同流消息按序进入 Actor 邮箱；新增关闸期间已接收消息阻塞、后续单向消息不执行的组合断言 | 排水等待已接收工作；发送端不获得逐项拒绝回执 |
| Agent→Game 有结果调用 | 服务端断流时在途调用失败，新调用重连；单项超时不关闭共享流 | 未自动重投结果未知的旧调用 |
| Game→MsgQ 发布流 | 本地队列背压、关闭期限及正常 Close 排空后检查邮箱顺序 | 关闭失败可见；正常关闭后已排队消息入邮箱 |
| MsgQ→Agent 邮箱流 | 下行发送受阻时仍可处理 Ack；慢会话被隔离后用新连接恢复 | 未确认下行保留并按序续取，其他会话不受影响 |
| txclient | 原会话重连、下行处理后 Ack、跳号序号 | 22 项会话与客户端测试通过 |
| 真实服务停机 | etcd 支持的 Actor 应用调用与排水测试，`-race -count=3` | 已接收处理完成前，服务停机持续等待 |
| 八进程集群 | 双 Login、双 Agent、双 MsgQ、双 Game，使用本机 NATS、etcd、Redis、Mongo | 少量会话验收通过；Agent 重启后原 Session、MsgQ owner 与未确认下行续接，Game/MsgQ/Login 排水和重启通过 |
| Agent 异常退出 | 相同八进程夹具增加 `--kill-agent`，旧进程退出记录为 `signal: killed` | 新 Agent InstanceID 不同；原 SessionID、MsgQ owner 不变，新 ConnectionID 收到待确认下行 |

Go 侧 `modules/rpc`、`modules/msgq`、`modules/agent`、`internal/actorrpc`、`internal/framework` 的相关测试以 `-race -count=3` 通过；新增单向流排水测试另以 `-race -count=20` 通过。`txgame/server` 的 `actorcmd`、`game` 与 `game/handler` 相关测试以 `-race -count=3` 通过。`txclient` 的 `session.test.ts` 与 `client.test.ts` 共 22 项通过，本轮完整客户端测试 107 项通过，类型检查通过。

八进程夹具首轮暴露了客户端验收脚本仍要求直发通知的 `Delivered` 计数；正式协议仅对 Topic 广播返回该计数。修正验收脚本和测试替身后，第二轮通过，摘要位于 `/private/tmp/txgame-cluster-e2e-20260928/20260928-215501.383226000/summary.json`。直发以实际收到 `GlobalNotify` 验证，不能用直发响应推断 MsgQ 已入队。

Agent 强制退出轮的最终通过摘要位于 `/private/tmp/txgame-cluster-e2e-20260928/20260928-221033.684425000/summary.json`；`client/agent-restart.jsonl` 记录 `pendingDelivered: true`。该测试在旧 Agent 已关闭客户端连接、下行已交给 MsgQ 后杀死进程；不覆盖杀进程瞬间仍在途的上行命令。

重复回归时发现两条独立边界。第一，`msgq-restarted` 的实例探针复用先前已终止邮箱的角色 470203，新 Session 生成新角色归属代次，Game 的旧 `RoleRouter` 缓存使第一条异步响应被拒；夹具改用未参与前序故障场景的 470215 起始探针角色，默认排水轮通过。这是 `SendRole` 明确拒绝后只失效缓存、不重投该条消息的既有契约，不能将探针规避解释为生产登录无丢失。第二，一次强杀轮在全部业务阶段通过后，最终关闭 `game-b` 报 MsgQ 流 `EOF`，整套摘要为 failed；相同最终代码的下一轮通过。失败现场位于 `/private/tmp/txgame-cluster-e2e-20260928/20260928-220800.852092000/`，尚未证明该清理失败的根因，不把它归咎于 Agent 强杀。

这不是跨机器故障恢复或长期稳态验收。Agent 异常退出已在上述固定边界验证；真实 Redis/MsgQ 故障和较长稳态仍待后续环境验证。
