# Actor 路由三批改造后的本地正式包验收

日期：2026-11-02。验证条件路由修正、正常关闭释放槽位和目录增量发布组合后的实际装配。复用现有打包、发布和客户端入口，不增加运行时代码，不跑云测试、不提交。

## 验收边界

```text
当前源码构建程序包 → 解包到独立目录 → 从源码目录外启动
  txgame：连续流量 180 秒 → 在线滚动配置 A→B→A → 核对角色归属
  SLG：启动 A → 流量 180 秒 → 正常停服 / 保存 / 释放
       → 启动 B → 流量 180 秒 → 正常停服 / 保存 / 释放
       → 启动 A → 流量 180 秒 → 最终保存 / 释放
  → 核对持久化、进程退出与专用数据清理
```

txgame 按 50 个 TCP 连接、每连接 5 次/s 的计划量计算完成率，在线交接窗口计入统计。SLG 分别运行 Role 与 World，均为 4 个连接闭环负载；三个运行窗口分别核验 99.99%，停服发布间隔单独记录。SLG 的结果不代表不停服发布，也不代表将停服时间计入后的在线可用率。

不重投超时请求；延迟分位数只包含成功请求。不同业务、开环/闭环负载之间不直接比较吞吐。本机结果不更新历史万连 20/50 的云端容量结论。

## 持续流量结果

七个 180 秒窗口均达到 **99.99%**；SLG 三个窗口之间的计划停服不计入请求完成率。

| 场景 / 配置 | 计划或尝试数 | 完成数 | 失败数 | 完成率 | 成功请求 P99 | 最大延迟 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| txgame 在线 A→B→A | 45,000 | 44,999 | 1 | 99.997778% | 1.335ms | 268.860ms |
| SLG Role 初始 A | 323,446 | 323,446 | 0 | 100% | 3.962ms | 112.190ms |
| SLG Role 发布 B | 348,650 | 348,650 | 0 | 100% | 3.224ms | 85.030ms |
| SLG Role 回滚 A | 356,528 | 356,528 | 0 | 100% | 3.095ms | 82.925ms |
| SLG World 初始 A | 1,084,092 | 1,084,092 | 0 | 100% | 1.280ms | 91.686ms |
| SLG World 发布 B | 1,045,643 | 1,045,643 | 0 | 100% | 1.308ms | 90.016ms |
| SLG World 回滚 A | 1,049,767 | 1,049,767 | 0 | 100% | 1.308ms | 89.263ms |

txgame 未发送为 0。四个滚动窗口的成功请求 P99 分别为 33.194 / 175.579 / 123.156 / 176.648ms，失败发生在第二次更新期间，保留在总量中；整体 P99 不能掩盖交接长尾。四次更新/回滚耗时为 1.776 / 1.927 / 2.451 / 1.939 秒，每次均核对全部 50 个角色的归属。

SLG Role 共完成 1,028,624 次请求，World 共完成 3,179,502 次请求。Role 发布 B / 回滚 A 的停服发布步骤分别为 5.634 / 5.635 秒，World 为 7.908 / 7.910 秒。这是从开始停止旧集群到新集群启动就绪的步骤时间，包含停止、切配置和启动，不声称是精确的外部不可用时长。

## 归属与持久化

- Role 分组槽位和两个 World 槽位，在 A / B / 回滚 A 时均观察到 generation **1→3→5**，每轮 owner 不同。每次正常停止释放、再次认领各递增一次；不是工具跳过释放直接晋升。原始 etcd 只读快照保存为 `slg-{role,world}-slots-{a1,b,a2}.json`。
- SLG Role 每轮确认 4 个 Fusion Redis 角色键存在，并回读 Mongo 名称，与最后成功响应逐一比较一致。该结果证明本轮最后修改已写回 Mongo；没有逐值比较 Redis 内容，不能把键存在检查或 Procedure 返回等同于完整持久化证明。
- World 最终排水后回读 4 个测试单位、4 个存储分区，Mongo 坐标与客户端最终结果一致，文档版本合计 19。两个 World 业务实例都在每轮服务启动时显式加载，普通请求不隐式加载。
- `TestOwnedBucketsMongoRoundTrip` 显式连接真实 Mongo，验证周期保存、最终保存和重新加载后的坐标；`TestFinalSaveFailureRetainsOwnedWorldAndRetry` 验证保存错误保留对象并允许重试。两项通过；后一项为可控保存错误，不冒充真实 Mongo 宕机。
- txgame 最终领奖背包为道具 1×6、道具 2×3；缺 JSON 与错误结构版本均拒绝新进程启动，原服务继续可用。本入口没有等待该角色 600 秒的 Fusion Mongo 延迟，不将背包响应解释为最终 Mongo 已同步。

- 指定 World 正式包补验通过：重复 owner 启动非零退出，原归属不变；正常停止后记录旧 g1→空闲 g2→新 g3。两个服经客户端直达 World 与 Role→World，停服前后共 8 条完成通知均来自对应 owner；无 World 映射的服前后均明确拒绝，GDWorld 保持同一身份文档。
- 收尾独立核对：58 个服务进程记录均无对应存活进程；55 次正常退出 0，3 次预期失败分别为缺 JSON、错误 schema、重复 World owner。Redis DB12/13 均为 0 键，两专用 Mongo 库无集合，两个专用 etcd 前缀无 key。其余容器与业务数据未改动。

## 构建与复现

原始证据目录：`/Users/a47/workspace/tx-artifacts/actor-routing-rollout-20261102/`。`source-state.json` 记录三批未提交工作区及相关工程的 HEAD、分支、差异与未跟踪文件指纹，不只以 HEAD 代表被测程序。

仓库内归档：[机器可读验收记录](validation/actor-routing-rollout-20261102.json)。完整摘要、逐阶段指标、归属快照、工具链、PID/退出记录和清理核对集中于此；原始大日志保留在本地证据目录。

| 场景 | 原始证据（相对证据目录） |
| --- | --- |
| txgame 在线滚动 | `txgame-config-release-3500472433/summary.json`、`txgame-rollout.log` |
| SLG Role | `slg-role/20261102-112635.752949000/release-summary.json` |
| SLG World | `slg-world/20261102-113643.441432000/release-summary.json` |
| World 指定启动 | `world-startup/20261102-114656.246068000/world-startup-summary.json` |
| World 存档重载 | `world-recovery.log` |
| 进程及专用数据清理 | `evidence.json`、`cleanup-dependencies.json` |

A/B 配置复用 2026-10-30 通过当前生成器正常导出的完整快照，拷入本轮证据目录；两组程序与快照的结构版本仍一致，由正式打包和启动加载器复核。没有手写 manifest、修改源 Excel 或更改验收门槛。每组 A→B→A 复用同一个二进制，仅切换完整配置目录。

本机为 Intel macOS 15.7.9、16 个逻辑 CPU，Go 1.27.0、Node 26.0.0；服务进程 `GOMAXPROCS=2`。txgame 使用普通构建；SLG 性能入口沿用 `txbench` 诊断构建，正式业务协议与处理路径不变。工具链和负载不同于旧日期报告，不把数字差异归因为这次目录优化。

```bash
# txgame/server；程序重新构建，缺文件和错误 schema 的启动拒绝也由此入口覆盖。
TXGAME_CONFIG_RELEASE_A=/Users/a47/workspace/tx-artifacts/actor-routing-rollout-20261102/configs/a \
TXGAME_CONFIG_RELEASE_B=/Users/a47/workspace/tx-artifacts/actor-routing-rollout-20261102/configs/b \
TXGAME_CONFIG_ARTIFACT_ROOT=/Users/a47/workspace/tx-artifacts/actor-routing-rollout-20261102 \
TXGAME_CONFIG_TRAFFIC=1 \
go test ./cmd/internal/deploytest -run '^TestConfigReleaseRollback$' -count=1 -v -timeout=10m

# txgameslg；两个负载顺序执行，避免本机同时施压。
sh cmd/cluster-perf.sh --workload role-rename --concurrency 4 \
  --release-a /Users/a47/workspace/tx-artifacts/actor-routing-rollout-20261102/configs/a \
  --release-b /Users/a47/workspace/tx-artifacts/actor-routing-rollout-20261102/configs/b \
  --warmup 2s --duration 3m --timeout 15m \
  --artifact-root /Users/a47/workspace/tx-artifacts/actor-routing-rollout-20261102/slg-role

sh cmd/cluster-perf.sh --workload world-move-unit --world-ids 490001,490002 --concurrency 4 \
  --release-a /Users/a47/workspace/tx-artifacts/actor-routing-rollout-20261102/configs/a \
  --release-b /Users/a47/workspace/tx-artifacts/actor-routing-rollout-20261102/configs/b \
  --warmup 2s --duration 3m --timeout 15m \
  --artifact-root /Users/a47/workspace/tx-artifacts/actor-routing-rollout-20261102/slg-world

sh cmd/cluster-e2e.sh --world-startup --timeout 5m \
  --artifact-root /Users/a47/workspace/tx-artifacts/actor-routing-rollout-20261102/world-startup

# txgameslg/server；显式启用真实 Mongo，测试创建自己的随机隔离库。
TX_MONGO_URI=mongodb://127.0.0.1:27017 go test ./services/slgworld/modules/internal/world \
  -run '^(TestOwnedBucketsMongoRoundTrip|TestFinalSaveFailureRetainsOwnedWorldAndRetry)$' \
  -count=1 -v -timeout=60s
```

依赖沿用本地 NATS 4222、etcd 2379、Redis 6379、Mongo 27017。只使用各工具已有的 ownership 专用范围：Redis DB12/13、Mongo `txgame_cluster_e2e` / `txgameslg_cluster_e2e`、etcd `/tx/e2e/txgame/` / `/tx/e2e/txgameslg/`。成功后精确清理，失败保留；没有重建或清理其他环境。

本轮是三批后的真实装配验收；已完成的包级全量、race 与微基准证据仍见[三批记录](actor-placement-acceptance.md)，不据此覆盖既有 MsgQ 测试夹具 race，也不重复声明全套 race 通过。

本轮没有新增运行时代码，不重复包级全量测试；不覆盖云端、长期稳态、意外杀进程、异常自动接管、持续 Submit 排队或大规模目录抖动下的业务流量。txgame 只在线滚动 Game，SLG 则完整停服再启动；没有声称 Login/Agent/MsgQ 的全链路不停服发布已通过。后续可以汇总三批改造及本轮证据提交，不继续无目标地重复本轮负载。
