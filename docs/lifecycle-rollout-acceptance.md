# 启动与关闭补修后的本地正式包验收

日期：2026-10-30。以当前工作区中的启动致命错误传播、传输入口异常上报、消费者静态校验及关闭期限修复为被测版本。复用现有入口，不修改运行时或业务代码，不提交，不跑云测试。

## 结果

三轮持续流量均达到 **99.99%**，每轮完整运行 180 秒；超时保留，不重投，不剔除交接窗口。

| 场景 | 连接数 | 尝试数 | 完成数 | 失败数 | 完成率 | 成功请求 P99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| txgame Game 更新/回滚 | 50 | 45,000 | 44,997 | 3 | 99.993333% | 1.320ms |
| SLG Role 发布/回滚 | 4 | 323,840 | 323,832 | 8 | 99.997530% | 3.245ms |
| SLG World 发布/回滚 | 4 | 1,083,198 | 1,083,190 | 8 | 99.999261% | 1.214ms |

txgame 按每连接 5 次/s 计划量计算，未发送为 0；SLG 沿用闭环负载，以实际尝试数计算。成功响应最大延迟分别为 142.390ms、115.083ms、107.661ms；分位数不包含超时请求，不能据此声称没有交接长尾。不同业务和发生方式的吞吐不可直接比较，也不代表生产容量。

## 启动、交接与存储证据

```text
构建部署包 → 解包到独立目录 → 从无源码 cwd 启动
  → 客户端持续流量 → 发布完整配置 B
  → 旧实例关闸 / 排水 / 必要保存 → 槽位晋升 → 新实例继续处理
  → 回滚完整配置 A → 再次受控交接
  → 完成测量 → 校验持久化 → 正常关闭 → 核对进程与专用数据清理
```

- **Role/Room 生命周期通过**：正式包启动五进程，Role Ready→Unloaded→Ready，重载后道具数量保持为 1；Room 在 60 帧结束后 Unloaded。全部排水退出。
- **txgame 四步滚动通过**：每次更新/回滚都核对全部 50 个角色归属；耗时分别 1.740s、1.825s、2.449s、1.868s。最终领奖角色背包为道具 1×6、道具 2×3。缺 JSON 与错误结构版本均拒绝启动，原服务仍可领奖。此项不将业务响应当作所有 Fusion 数据已完成延迟 Mongo 写回。
- **SLG Role 通过**：主服放置槽位代次 1→2→3，实例身份变化；交接后核对新增完成请求，4 个角色最终名称在 Fusion/Redis 与 Mongo 均一致。
- **SLG World 通过**：490001、490002 两槽位均到第 3 代；最终排水后回读 4 个测试单位、4 个分区，Mongo 坐标一致，文档版本合计 16。
- **World 指定启动通过**：重复 owner 进程退出 1；原 owner 未被抢占。受控交接后代次 1→2，两个服、客户端直达与 Role→World 两条路径，前后共 8 条完成通知均来自正确 owner。无映射服两阶段均明确拒绝，GDWorld 保持一条。
- **World 保存与重载补测通过**：显式启用真实 Mongo 的 TestOwnedBucketsMongoRoundTrip，核对 Monster 坐标重新加载；TestFinalSaveFailureRetainsOwnedWorldAndRetry 验证保存失败保留旧对象并可重试。后者为可控保存错误，不能冒充真实 Mongo 宕机。
- **收尾通过**：39 条记录 PID 均已退出；36 个正常进程退出 0，3 个预期失败分别为缺 JSON、错误结构版本和重复 World owner。Redis DB12/13 均为 0 键，两专用 Mongo 库无集合，etcd `/tx/e2e/` 无 key。隔离 Mongo 恢复为停止状态，保留容器数据；其他容器不变。

## 配置准备与验证边界

首次 txgame 发布验收使用旧 A/B 副本，在启动前因结构版本不匹配失败；原始日志保留。随后从当前 txconfig 复制隔离输入，仅在测试副本中切换道具 enabled，经当前 txgen/Luban 完整三时区导出 A/B。结构版本与程序一致，A 仅启用道具 1、B 仅启用道具 2；没有修改源 Excel、应用生成文件或手写 manifest，没有放宽加载器校验。

三轮流量包分别构建一次，轮内 A→B→A 只切配置，二进制摘要不变。流量入口沿用既有诊断构建；Role/Room 与 World 指定启动入口使用普通正式构建，均走 package.py、包内 start.sh、正式协议与业务处理。被测服务 `GOMAXPROCS=2`，驱动上限 4，Go 1.26.5，Intel Mac。本轮不据此更新万连 20/50 容量结论。

启动异常注入与并发边界由[定向回归](local-acceptance-matrix.md#2026-10-30-启动失败定向回归)承担；本轮验证修改后的真实装配、滚动及正常收尾，不向业务增加故障开关。SLG Role 使用同步 Procedure，World 使用独占网格；不声称覆盖集群滚动期间大量 Submit 排队。

## 复现入口与证据

原始目录：`/Users/a47/workspace/tx-artifacts/lifecycle-rollout-20261030/`。机器可读归档含当前提交及差异指纹、完整摘要、持久化字段和进程退出记录：[验收数据](validation/lifecycle-rollout-20261030.json)。

沿用[正式包入口](local-package-rollout-acceptance.md#复现入口)，增加 `GOCACHE=/private/tmp/tx-go-build`；Mongo 使用 `mongodb://127.0.0.1:27029`，分别指定原有专用库名。A/B 路径为本轮 `generated/{a,b}/server/json`，两个应用共用当前相同结构的完整导出副本；txgame 用环境变量 TXGAME_CONFIG_RELEASE_A/B，SLG 用 --release-a/--release-b。显式设置 TXGAME_CONFIG_TRAFFIC=1，不使用普通 Go 测试的跳过结果代替实测。

| 运行 | 入口 / 原始证据 |
| --- | --- |
| Role/Room | TestPackagedActorLifecycle；txgame-lifecycle.log、txgame-actor-lifecycle-1180752083/summary.json |
| txgame 流量 | TestConfigReleaseRollback；txgame-rollout-current.log、txgame-config-release-3829614359/summary.json |
| SLG Role 流量 | cluster-perf.sh --workload role-rename --concurrency 4 --duration 3m；slg-role/20261030-200921.252037000/release-summary.json |
| SLG World 流量 | cluster-perf.sh --workload world-move-unit --world-ids 490001,490002 --concurrency 4 --duration 3m；slg-world/20261030-201306.429965000/release-summary.json |
| World 启动 | cluster-e2e.sh --world-startup --timeout 5m；world-startup/20261030-201656.559037000/world-startup-summary.json |
| 保存重载 | TX_MONGO_URI 显式启用 world 包的两个定向用例；world-recovery.log |
| 清理 | process-verification.json；专用依赖只读核对记录见验收数据 |

本轮没有新增生产代码，因此不重复上一轮全量 Go/race 测试；本轮只运行上述正式包多进程验收与保存重载补测。未覆盖云端、长期稳态、全服务滚动、意外杀进程或跨物理机容灾。Login/Agent/MsgQ 本轮覆盖启动与最终关闭，持续流量只滚动 Game/SLGGame/SLGWorld。

下一步可归拢当前生命周期修复与验收文档的提交内容；后续架构审查按剩余明确范围推进，不反复重复本轮负载。
