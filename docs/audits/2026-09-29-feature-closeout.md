# NATS 与异步会话流 feature 收尾（2026-09-29）

## 当前状态

功能迁移和万连吞吐目标已完成，当前收尾仅在 Mac 本地执行。生产部署验收另列。主线代码与集群工具重构已由用户提交；本地合并前核验通过，远端刷新因 HTTPS 凭据不可用而未完成。本轮仅更新收尾文档，未执行合并。关闭契约已确认：开始关闭之前已经结束的发布流，其历史错误只记录 error，不再阻断 Game 停止；本次排水期间发生的断流、超时继续返回失败。

## 已完成

- 修正 README、design、异步流设计与计划：Agent 本地检查上行游标和下行 Ack；每次上行提交及客户端 Ack 都经 MailboxStream 异步同步，无 Agent Rec lease、无定时低频游标同步、无远端逐项入队成功回执。
- 明确序号归属：客户端分配 UpSeq，MsgQ 分配 DownSeq；Agent 检查和转发游标。新 Login 创建独立 Session，Restore 更新原 Session 的 ConnectionID。
- 新增 Agent1→Agent2→Agent1 三次新登录回归，验证旧会话上行 Seq=100 不妨碍新会话从 Seq=1 开始，三个邮箱下行均从自己的 Seq=1 开始。
- 扩充共享 MailboxStream 测试：两 Session 接管均唤醒旧订阅，按完整 Header 核对错误；旧连接不能 Rec、裁剪或终止新连接邮箱。
- 记录隔离范围：同 Session 的旧 Agent 感知订阅错误后关闭 socket；已发往 Game 的上行不撤销，不提供跨 Agent 上行 fencing。当前不为此增加定时租约或业务重放。
- 独立审查已复核历史错误与本次排水错误的并发边界，未发现合并阻塞；指出的旧邮箱拒收描述已修正；Ponytail 检查未引入公共抽象、配置或兼容层。

## EOF 定位与修复

真实 gRPC 定向测试先复现旧 publisher 保存 ErrRouteUnavailable 并让 Close 失败。按确认后的契约，`TestPublishStreamHistoricalDisconnectDoesNotBlockClose` 改为断流后两次 Close 均成功，历史错误仍保留并记录日志；后续发布替换旧流，不重投旧消息。新断流和排水超时测试保持错误断言。正常半关闭的 io.EOF 与 gRPC Unavailable 中的底层 EOF 不等价，不能统一忽略。

八进程夹具在中途主动重启 MsgQ。若 Game 后续访问同端点的新实例，连接缓存会替换旧连接；没有再次访问时，旧连接错误保留到最终 Close。因此正常业务阶段均完成后，最终清理仍可能失败。历史日志未记录具体流端点，定向测试证明机制，但不将其当作历史逐连接追踪证据。

修复前异常退出回归再次出现该失败，失败现场保留。修复以 Close 入口观察流是否已经结束为界，不按错误文本过滤 EOF。历史错误在关闭时记录 error；首次关闭一旦开始排水，之后的错误仍返回，重复 Close 不把本次排水失败重新归为历史错误。历史故障流关闭成功只表示资源清理成功，不表示故障时消息已可靠送达。

## 关闭契约收口时的验证

| 验证 | 结果 |
| --- | --- |
| tx `go test ./...` | 通过；依赖环境变量启用的专项集成测试不据此宣称已运行 |
| txgame/server、txgameslg/server `go test ./...` | 通过 |
| txclient `npm test`、`npm run check` | 107 项通过、类型检查通过 |
| MsgQ、Agent、RPC、Actor RPC、framework `-race` | 通过 |
| Agent 会话回归 `-race` | 通过，包含新登录回到原 Agent |
| 接管与旧发布流定向测试 `-race -count=20` | 通过 |
| 本地八进程正常排水轮 | passed |
| 修复前本地八进程 `--kill-agent` 轮 | 业务阶段通过，最终 game-b Close 返回 Unavailable/EOF，整轮 failed |
| 修复后本地八进程 `--kill-agent` 轮 | passed；两 Game 实际记录历史 Unavailable/EOF，随后正常退出 |
| 修复后关键六包 `-race`、发布流 `-race -count=20` | 通过；重复 Close 不掩盖本次排水失败 |

正常轮证据：`/private/tmp/tx-feature-closeout-20260929/20260929-005334.716954000/summary.json`。

异常轮证据：`/private/tmp/tx-feature-closeout-20260929-kill/20260929-005647.150836000/summary.json`。失败现场保留，不以重跑通过覆盖。

修复后证据：`/private/tmp/tx-feature-closeout-history-fixed/20260929-011010.333125000/summary.json`。`game-a-r2.log` 和 `game-b.log` 均含 `closing previously failed downlink publish stream` 的 error，整轮 passed；不是通过避开断流获得通过。全量 Go/客户端验证在契约收口时执行，关闭逻辑修改后追加上述 race 和真实集群验证。

## 不随本次合并承诺的范围

- [万连 20/50](2026-09-28-seven-host-2050-acceptance.md) 是既有三次 180 秒云端证据，本轮没有新云端压测。
- JetStream 生产副本、磁盘同步策略、物理故障域，以及真实 Redis/MsgQ 故障与长期稳态属于部署验收。
- 自动扩 Pod、世界自动交接、单实体热迁移、管理脚本平台不属于本轮迁移欠账。
- 未执行 Git 合并。以下祖先关系只针对已核验的本地引用；实际合并前仍需刷新远端。


## 集群工具收敛（已提交）

两个示例工程将部署与进程能力放入 `clusterkit`，验收场景保留在 `clustere2e`，压测编排放入 `clusterperf`；基础工具不反向依赖场景。移除旧同进程对照入口和开发者绝对路径，存储采样改为显式启用，失败时保留现场并结束本轮进程。

重构提交前，两工程全量 Go 与工具包 race、两套真实集群验收及小规模上下行/SLG 场景通过。证据位于 `/private/tmp/cluster-refactor/`。这些是前一轮证据，本次没有重复运行真实集群或云端负载。

## 合并前核验

2026-09-29 核验时六工程工作区均干净，以下版本组成当前本地验证组合：

| 工程 | 主干 | 已核验 HEAD | 本地主干至 HEAD 范围 | 待合入提交数 |
| --- | --- | --- | --- | --- |
| tx | main | 4762153 | e98c9c6..4762153 | 57 |
| txgame | master | 56c13fe | b611ebb..56c13fe | 20 |
| txgameslg | main | 511ca15 | 0b46521..511ca15 | 17 |
| txclient | main | 2cb0fe5 | 已在主干 | 0 |
| txclientslg | main | 8e703f5 | 已在主干 | 0 |
| txgen | main | de11137 | 已在主干 | 0 |

三个 feature 均为 `feature/nats-actor-runtime`，本地主干均为 HEAD 的祖先，可以快进；快进后的代码树就是本轮验证的 HEAD，不涉及文件合并冲突。存储的远端跟踪引用与对应本地分支一致，但六工程 `git fetch origin` 均因 `could not read Username for 'https://github.com'` 失败，不能据此确认远端最新状态。

合并顺序为 **tx → txgame / txgameslg**，两个示例之间无先后依赖。两个 server 通过 `replace github.com/ngnl47/tx => ../../tx` 使用同工作区 tx；生成工程同时引用本地 txgen。该组合验证不等于已发布独立模块版本，本轮不更改依赖发布方式。

本次重新执行的本地验证：

- tx、txgame/server、txgameslg/server 的 `go test ./...` 均退出 0，部分结果命中 Go 测试缓存；受环境开关控制的集成测试不据此宣称执行。
- txclient 107 项、txclientslg 109 项测试全部通过，两者 `npm run check` 均退出 0。
- Go 与客户端类型检查日志位于 `/private/tmp/tx-merge-readiness-20260929/`；客户端测试计数由本轮命令输出核对。

本轮只修改 tx 的收尾与计划状态文档。本文版本范围不包含这次尚未提交的文档；用户提交后 tx 范围终点与提交数相应更新。完成文档提交并成功刷新远端、复核祖先关系后，可按上述顺序合并。本次核验不是对整个 feature 差异重新逐行审查。
