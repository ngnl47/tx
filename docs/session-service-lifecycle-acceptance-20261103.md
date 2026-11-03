# 正式包会话服务生命周期验收

日期：2026-11-03。两组工程补齐 `cmd/cluster-e2e.sh --session-lifecycle`，使用普通程序包验证 Login、Agent、MsgQ 的有限生命周期转换。本轮不修改 tx 运行时、生成协议或公共客户端状态机，不跑云测试，不提交。

## 构建与入口

基线为 tx `5d4bd95`、txgame `3261fb8`、txgameslg `8753304`、txclient `c3ef6f7`、txclientslg `9ad2b75`；新增验收工具为本轮未提交变更。两组沿用本机 Redis、Mongo、etcd、NATS 与已有隔离规则，顺序运行，每个服务 `GOMAXPROCS=2`。

```text
正式 package 入口（diagnostics=false）
  → 解包到独立目录 → 包内 start.sh 从源码外启动八个服务进程
  → 建立同 Agent 的 TCP / WebSocket 会话，分别落到两个 MsgQ
  → 逐台替换 Login → 保持原会话，存活及新实例签发票据
  → 正常停止 Agent → 离线通知入原邮箱 → 替换后恢复两个原会话
  → 正常停止一个 MsgQ → 原会话恢复被拒 → 同 Role 重新登录
  → 重启 MsgQ → 关闭客户端 → 服务排水、实际退出 → 清理专用数据
```

txgame 使用双 Game，SLG 使用一个 SLGGame 和一个显式启动 `--world-id 1` 的 SLGWorld；其余均为双 Login、双 Agent、双 MsgQ。全部替换复用同一程序包，包元数据、程序 SHA256 和 `go version -m` 均确认没有 `txbench`。副本监听地址由包内资产变体提供，不读取源码目录中的运行资产。

## 最终实测

| 核验项 | txgame | txgameslg |
| --- | --- | --- |
| 客户端阶段 | 10/10 通过 | 10/10 通过 |
| 探针记录的新发 SessionID | 11 个，无重复 | 11 个，无重复 |
| Login 逐台替换 | 原 TCP/WS 会话、ConnectionID、MsgQ owner 保持；存活与替换 Login 均可签票并连接 | 同左 |
| Agent 在线停止后恢复 | 两种传输均被实际 Stop 断开；原 AgentClient/Session 恢复，ConnectionID 换新、原 MsgQ owner 保持 | 同左 |
| 下行与上行 | 停止期间发布的通知送达，DownSeq 接续；恢复不重投旧上行，新上行成功；另验证新登录进入替换 Agent | 同左 |
| MsgQ 正常停止 | 角色租约释放，旧 Session 恢复被明确拒绝；同 Role 新 Session 转到存活 owner，generation 换新；存活会话连接不变 | 同左 |
| 正常退出 | 12/12，退出码均为 0 | 12/12，退出码均为 0 |
| SLG 业务归属 | 不适用 | 10 个阶段实际读取 Game 分组和 `slgworld-1` 两个槽位，owner 与 generation 均保持，generation 为 1 |

Agent 恢复阶段客户端耗时分别为 **86ms / 79ms**；该阶段包含两种传输恢复、通知接收、新上行以及一次新登录，且开始于替换进程 Running 后，不是完整停服时长或单次握手延迟。有限步骤全部符合契约，不由这些小样本推算 99.99% 可用率、长期稳定性或吞吐。

MsgQ 的旧 Session Redis 记录允许按现有 TTL 保留。验收核对的是角色租约条件释放、旧绑定未被改写成新 owner，以及重新登录产生的新 Session；不要求旧记录立即消失，不承诺跨 MsgQ 迁移邮箱或恢复其未确认正文。

恢复拒绝使用组合证据：已发 Restore、未收到 Ready、没有传输错误、客户端进入关闭状态；再从阶段起点之后的 Agent 日志匹配同一 RoleID/SessionID 的 `msgq: session instance is unavailable`。仅捕获 `HandshakeRejectedError` 或普通网络断开不算通过。

## 回归与审查

- txgame：`go test ./cmd/cluster-e2e ./cmd/internal/clustere2e ./cmd/internal/clusterkit -count=1` 通过。
- txgameslg：同上并包含 `./cmd/internal/ops`，通过。
- txclient：`npm test` 118/118；txclientslg：129/129，均无跳过；两组 `npm run build` 通过。
- 差异 review 补齐两处工具观察时序：最终阶段先确认客户端正常退出再读取结果，不能用结果文件掩盖非零退出；SLG 等待最多 5 秒读取精确拒绝日志，读错或截断立即失败，不重试业务。对应正常/异常退出、延迟日志及读取失败回归均通过。
- 两组首轮真实验收已通过；上述工具修正后各重新跑一轮，本文及机器记录引用最终轮。两组 `git diff --check` 通过；本轮未重跑 tx 全量测试或 race。

日志并非零告警：最终 txgame 有一条预期旧会话拒绝，以及两条旧 MsgQ 下行流已断开的关闭日志；SLG 分别为一条和一条。它们没有阻断服务正常关闭，原始日志均保留。未新增自动恢复、业务重投或无损邮箱迁移。

## 收尾与复现

独立检查了最终两轮 **24 份 `.exit`**，全部为 0；没有仍匹配本轮目录的服务或客户端进程。Redis DB12/13 均为 0 键，两个专用 Mongo 库无集合，`/tx/e2e/txgame/` 与 `/tx/e2e/txgameslg/` 均无 key，包含对应精确 SessionID 发号键。只由既有 ownership 保护入口清理专用范围，未删除容器或其他数据。

从各应用工程根目录分别执行：

```sh
env GOCACHE=/private/tmp/tx-delayqueue-go-cache \
  sh cmd/cluster-e2e.sh --session-lifecycle --timeout 8m \
  --artifact-root <本轮独立证据目录>
```

总证据目录为 `/Users/a47/workspace/tx-artifacts/session-service-acceptance-20261103`，保留初轮、最终轮、普通包、日志和回归输出。仓库仅记录[机器可读汇总](validation/session-service-lifecycle-20261103.json)。

| 最终结果 | 相对证据目录 |
| --- | --- |
| txgame | `txgame-final/20261103-012945.632918000/session-lifecycle-summary.json` |
| txgameslg | `txgameslg-final/20261103-013153.421146000/session-lifecycle-summary.json` |
| Go / 客户端回归 | `unit/` |
| 环境与独立核验 | `environment.json`、`preflight.json`、`evidence.json` |

SLG Role/World 的正式发布仍沿用停服策略。本项仅补接入组件的生命周期证据，不替代业务数据最终 Mongo 回写、World 重启恢复、云端容量或异常接管验收。

## 验收客户端失败退出补修

2026-11-03 后续收尾核对发现工具失败分支缺口，使用真实本地子进程复现：两组 `Terminate()` 均在 Node 完成清理前返回；SLG 取消后 npm 父进程已结束，实际 Node worker 仍存活。此问题不推翻前述正常路径的退出与独立清理证据。

两组工具统一直接启动 Node，取消与提前失败共用私有取消范围。`exec.CommandContext` 先发送 SIGTERM，现有 2 秒等待后可强制回收直接子进程；`Terminate()` 最多等待 5 秒确认实际退出，不读取一次性的 `Done()` 结果。正常执行仍须核对原始退出错误，强制回收不能算正常业务完成。调用方汇总清理错误，删除重复等待；SLG 同步客户端入口也移除 npm 包装层。

txclient 的多进程 CLI 已自行管理 worker，新增真实父进程 SIGTERM 测试确认：父函数返回前，两名 worker 均写出收尾标记且 PID 已消失，原中断错误与父退出码仍保留。因此未增加通用进程组、生产 API 或恢复逻辑。

| 本轮验证 | 实际结果 |
| --- | --- |
| txgame `go test ./cmd/... -count=1`、`go vet ./cmd/...` | 通过；真实集群及部署场景仍按原环境开关跳过，不计为重跑 |
| txgameslg 两个命令入口与 clusterkit/clustere2e/clusterperf/ops 六包回归 | 通过；clusterkit/clustere2e/clusterperf vet 通过 |
| 两组新增子进程用例 `-race -count=3` | 通过；SLG 加强退出码断言后再跑一轮通过 |
| txclient `npm test`、`npm run build` | 119/119，无失败或跳过；构建通过 |
| 后置差异审查 | 未发现本批遗留必修问题 |

SLG 初次 Go 回归及 txclient 首次全量测试受沙箱本地监听权限限制，允许测试使用本机端口后按原命令通过，原失败日志保留。证据在上述总目录的 `client-cleanup/`，含红测、修复后回归与 race；机器记录另列 `toolCleanupFollowup`，不覆盖原集群记录。本轮不跑真实集群、云压测或框架全量回归，未提交。
