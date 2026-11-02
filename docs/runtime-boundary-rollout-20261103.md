# 运行时边界收敛后的本地正式包验收

日期：2026-11-03。本轮验证已提交的运行时权限、共享冷加载取消、存储后台工作生命周期、RPC 发送取消及 Procedure 回滚改造在两组示例中的实际接入。只更新验收记录，没有修改运行时代码；不跑云测试，不提交。

## 范围与构建

被测版本为 tx `84f095e`、txgame `3261fb8`、txgameslg `8753304`。七个工程的完整 HEAD、分支、验收前干净状态和工具链记录在[机器可读结果](validation/runtime-boundary-rollout-20261103.json)。本机为 Intel macOS 15.7.9，Go 1.27.0、Node 26.0.0；服务进程沿用 `GOMAXPROCS=2`。

```text
当前源码构建程序包 → 解包到独立目录 → 从源码目录外启动
  txgame：180 秒连续流量 → 在线发布 B → 在线回滚 A
  SLG Role / World：启动 A → 流量 180 秒 → 正常停服并保存
                   → 启动 B → 流量 180 秒 → 正常停服并保存
                   → 启动 A → 流量 180 秒 → 最终停服并保存
  → 独立核对退出记录、槽位、Mongo 回读和专用数据清理
  → 补验 World 存档重载
```

- txgame 为 50 个 TCP 连接、每连接 5 次/s；以整段 180 秒的 45,000 次计划请求核验 99.99%，包含在线交接窗口。四个发布子窗口单独报告，不要求各自达到 99.99%。
- SLG Role 与 World 均为 4 个连接闭环负载，A/B/A 各 180 秒分别核验 99.99%。停服区间单列，不计作在线滚动或无中断可用率。
- 每组 A→B→A 只构建一次程序、校验程序摘要不变，仅切换完整外部配置。txgame 使用普通构建；SLG 性能入口沿用显式 `txbench` 诊断构建，业务协议与处理路径不变。
- A/B 复用上一轮完整配置快照，逐字节一致；两组当前正式加载器与配置打包入口均验证通过。北京组为 `Asia/Shanghai`，结构版本相同，数据版本 A `1c26286a…92ef4d`、B `a88d4fe5…15b9d`，分别仅启用道具 1、2。未改 Excel 或手写 manifest。

## 持续流量结果

七个 180 秒测量窗口均达到 **99.99%**。

| 场景 / 配置 | 计划或尝试数 | 完成数 | 失败数 | 完成率 | 成功请求 P99 | 最大延迟 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| txgame 在线 A→B→A | 45,000 | 44,997 | 3 | 99.993333% | 1.320ms | 278.508ms |
| SLG Role 初始 A | 334,871 | 334,871 | 0 | 100.000000% | 3.762ms | 131.064ms |
| SLG Role 发布 B | 372,518 | 372,518 | 0 | 100.000000% | 3.019ms | 107.640ms |
| SLG Role 回滚 A | 378,541 | 378,541 | 0 | 100.000000% | 2.797ms | 109.220ms |
| SLG World 初始 A | 1,130,515 | 1,130,515 | 0 | 100.000000% | 1.201ms | 98.555ms |
| SLG World 发布 B | 1,063,389 | 1,063,389 | 0 | 100.000000% | 1.267ms | 119.472ms |
| SLG World 回滚 A | 1,101,277 | 1,101,277 | 0 | 100.000000% | 1.219ms | 108.300ms |

txgame 未发送为 0，没有重投超时请求。3 次失败分别落在更新 Game B、回滚 Game A、回滚 Game B 的子窗口，已计入总体失败数；对应子窗口分别完成 748/749、749/750、499/500。四次发布步骤的成功请求 P99 为 42.700 / 175.466 / 145.543 / 187.251ms，整体 P99 不代表交接期间没有长尾。延迟分位数只统计成功请求。

四次 txgame 更新/回滚耗时 1.796 / 1.979 / 2.499 / 1.973 秒，每次核对全部 50 个角色的目标归属。SLG Role 三轮累计完成 **1,085,930** 次，World 累计完成 **3,295,181** 次，均零失败。不同业务与开环/闭环负载不直接比较吞吐。

SLG 发布 B / 回滚 A 的停服发布步骤：Role 为 **5.625 / 5.628 秒**，World 为 **7.922 / 7.935 秒**。这是工具从停止旧集群到新集群就绪的步骤时间，含停止、切配置与启动，不声称是精确的外部不可用时长。

## 归属、业务与持久化

- SLG 角色分组槽位、`slgworld-490001`、`slgworld-490002` 在 A/B/A 均观察到 generation **1→3→5**，各轮 owner 不同，稳定运行期间归属不变。每 10 秒只读采集一次，证明稳定阶段的新归属，不声称捕获了全部释放瞬态。Stop 释放与后续认领各递增一次的语义沿用现行实现。
- Role 每轮源码均等待 4 个角色的 Mongo 名称与最后成功响应一致；整轮通过证明三次检查都成功，摘要只保留末轮计数。Fusion Redis 只检查角色键存在，不将其解释为逐值校验或最终 Mongo 证明。
- World 按指定 ID 启动并加载业务实例，最后全停后直接回读 **4 个单位、4 个存储桶**，坐标与最后成功响应一致，文档版本合计 **21**。本入口不单独归档前两轮的 Mongo 回读证明。
- txgame 跨发布、回滚的领奖与背包验证通过，最终为道具 1×6、道具 2×3；缺 JSON 和错误 schema 均拒绝新进程启动，原服务仍可正常处理。该入口未等待正式 Fusion 的 600 秒 Mongo 延迟，**不证明 txgame 最终 Mongo 回写**。
- 补跑现有 `TestOwnedBucketsMongoRoundTrip`，显式连接真实 Mongo，验证周期保存、最终保存，以及运行时重建后在新写入前恢复坐标 `(55,52)`、无脏改动和分区存在。用例通过且未跳过；使用自己的随机专用库并清理。它是**运行时重建**验证，不是正式包独立进程之间的首次只读查询断言：持续流量每轮预热会先写新坐标，不能补出这项未观测的保证。

日志不等于零错误：除交接期的 3 条 `actortrial: runtime draining` 上行单向流投递拒绝外，txgame 业务探针角色 `roleID=1` 还记录一次 `role binding changed` 拒绝，旧、新绑定代次不同。它不属于从 480100 开始的压测角色，后续领奖与背包断言通过；源码仅对明确尚未入邮箱的绑定拒绝刷新寻址，不将这一条日志直接算作压测请求丢失，也不据此推定未知投递可以重放。本轮未调整日志等级或重试策略。

## 收尾与验证限制

已逐一核对 **47 份进程记录**：45 次正常退出 0，2 次为预期坏配置启动失败；独立 `ps` 检查没有仍匹配本轮路径的服务进程。Redis DB12/13 均 0 键，两专用 Mongo 库无集合，`/tx/e2e/txgame/` 与 `/tx/e2e/txgameslg/` 下无残留 key，含精确 SessionID 发号键。清理由既有入口按 ownership 保护执行，随后只读核对；没有清理其他数据库或容器。

本轮证明当前构建的本地组合接入与正常关闭路径，不替代前批定向取消、回滚及失败重试测试；没有重跑全量 race。未测云端容量、万连 20/50、长期稳态、故障自动接管或大规模 Submit 队列；不把本机数值差异归因于某一项改造，也不为阈值内的极少量交接失败追加新机制。

## 复现与证据

完整证据目录：`/Users/a47/workspace/tx-artifacts/runtime-boundary-acceptance-20261103`。仓库保留[机器可读结果](validation/runtime-boundary-rollout-20261103.json)，大日志与发布包留在本地目录。

| 内容 | 相对证据目录 |
| --- | --- |
| txgame 在线发布回滚 | `txgame/txgame-config-release-1995579705/summary.json`、`txgame-run.log` |
| SLG Role | `slg-role/20261103-002549.920237000/release-summary.json`、`slg-role-run.log` |
| SLG World | `slg-world/20261103-003607.621737000/release-summary.json`、`slg-world-run.log` |
| World 存档重载 | `world-recovery.log` |
| 版本与配置校验 | `environment.json`、`configs/preparation.json`、`configs/validation/` |
| 归属、退出与清理 | `txgameslg-slots.jsonl`、`evidence.json`、`cleanup-dependencies.json` |

```bash
# txgame/server；A、B 指向证据目录 configs/a、configs/b。
TXGAME_CONFIG_RELEASE_A=<A绝对目录> TXGAME_CONFIG_RELEASE_B=<B绝对目录> \
TXGAME_CONFIG_ARTIFACT_ROOT=<已存在的本轮父目录> TXGAME_CONFIG_TRAFFIC=1 \
go test ./cmd/internal/deploytest -run '^TestConfigReleaseRollback$' -count=1 -v -timeout=10m

# txgameslg 根目录；两组顺序执行，不并行争用专用环境。
sh cmd/cluster-perf.sh --workload role-rename --concurrency 4 \
  --release-a <A绝对目录> --release-b <B绝对目录> \
  --warmup 2s --duration 3m --timeout 15m --artifact-root <Role证据目录>
sh cmd/cluster-perf.sh --workload world-move-unit --world-ids 490001,490002 --concurrency 4 \
  --release-a <A绝对目录> --release-b <B绝对目录> \
  --warmup 2s --duration 3m --timeout 15m --artifact-root <World证据目录>

# txgameslg/server；启用真实本机 Mongo，未设置 URI 时的跳过不算通过。
TX_MONGO_URI=mongodb://127.0.0.1:27017 \
go test ./services/slgworld/modules/internal/world -run '^TestOwnedBucketsMongoRoundTrip$' -count=1 -v -timeout=60s
```

本轮复用 `/private/tmp/tx-delayqueue-go-cache`。不要并行使用相同专用数据库或前缀；失败应保留产物并排查，不删除未标记环境来强行通过。
