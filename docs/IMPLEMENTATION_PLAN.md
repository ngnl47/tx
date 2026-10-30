# 当前状态与验收计划

本页只维护当前待办和证据索引。首次接入读 [README](../README.md)，模块职责读 [设计文档](design.md)，检查业务用法读 [工程约定](engineering-conventions.md)。已完成施工步骤由 Git 历史追溯，不重复列为待实现能力。

## 当前能力与证据

| 能力 | 当前方案 | 验收边界 |
| --- | --- | --- |
| Actor 放置与生命周期 | 默认分区、命名分组、独立槽位；Ready 晚于业务必备加载；正常交接受控晋升 | [放置](actor-placement-acceptance.md)、[Role/Room](../../txgame/docs/acceptance/actor-package.md)、[World](../../txgameslg/docs/acceptance/world-startup.md)；不承诺运行中单实体热迁移 |
| GD 访问 | 无缓存显式存取；共享实体 Find 仅限有效 Procedure，普通 Snapshot；World 独占排除 | [本地回归与微基准](entity-access-acceptance.md)，不是云端容量 |
| EntityCache 管理 | Stats、List、SnapshotCached、TryRelease | [管理验收](entity-cache-management-acceptance.md)，无管理页面/全量导出 |
| Procedure | Lockey 与 Store 解耦；Call、子 Call、同步 CallAfterCommit；有界异步 Submit | [锁](procedure-locks.md)、[Submit 真实业务](../../txgameslg/docs/procedure-submit-acceptance.md)；默认 64/8192，合成突发对照未覆盖 8192 档 |
| 正式包滚动发布 | txgame、SLG Role、独占 World 三分钟窗口均达到 99.99% | [2026-10-30 报告](procedure-rollout-acceptance.md)；Role 是同步 Call，不证明滚动期间大量 Submit 队列 |
| 应用合服 | GDServerMerge 定时关系；txgame 在线刷新，SLG 停服后冻结新快照 | [txgame](../../txgame/docs/acceptance/timed-merge-20261030.md)、[SLG](../../txgameslg/docs/acceptance/timed-merge-20261030.md)有限多进程通过；保留原服入口，无业务暂停/自动迁移 |

合服属于应用示例，tx 本轮不增加合服能力。验收已核对 Fusion Redis 当前等级及 Mongo 账号投影、合服目录，不将其解释为最新角色数据已经完成 Mongo 延迟回写。

## 大验收前的下一项

1. **文档整理已完成（2026-10-30）**：README 导览、按功能设计、API/配置参考、稳定契约与有日期报告分开；已收敛完成的施工步骤并校验跨仓链接、章节锚点和 Markdown 围栏。此项只做文档验证，没有重新生成或运行集群。
2. **SLG 合服后的 World 寻址联合验收尚未执行。** 复用现有打包/集群入口，验证 OriginServerID → MainServerID → Excel World 绑定 → WorldID，并分别走客户端直达与 Role→World。现有两项独立验收不替代这一联合证据。先保持 World 独占、停服快照与受控 owner，不增加世界数据迁移或赛季玩法。
3. 完成后以两组应用计划汇总大验收矩阵，分开报告功能、持久化、排水、完成率和性能。遵守专用数据 ownership，不因文档整理自动启动新测试。

两组应用的具体待办：[txgame](../../txgame/docs/IMPLEMENTATION_PLAN.md)、[txgameslg](../../txgameslg/docs/IMPLEMENTATION_PLAN.md)。生成器没有待完成的已批准新功能，扩展以 [txgen 计划](../../txgen/docs/IMPLEMENTATION_PLAN.md) 为准。

## 需要专用环境或独立授权的验收

- **当前构建云端回测与长期稳态**：复用正式服务和客户端，记录完成率、P99、CPU/内存、队列及客户端计划发起延迟。[七机 20/50 报告](audits/2026-09-28-seven-host-2050-acceptance.md)只证明历史构建三次 180 秒窗口。不自动租赁云实例。
- **JetStream 生产部署**：按 [部署说明](deployment/jetstream-tasks.md)确认磁盘同步、副本与物理故障域，同机容器结果不替代跨宿主容灾。
- **会话积压容量**：当前示例 1024 条/8 MiB。取得实际慢客户端、离线时长、消息大小后再评估，不放开为无界缓存。
- **持续 Submit 滚动验收**：真实业务大量使用 Submit 时单独验证；当前同步 Role 与独占 World 的发布结果不覆盖此场景。

## 需要新需求才能实施的方向

- 配置中心/进程内切换：当前外部 JSON 整套校验、滚动重启；主动拉取、在途快照及双端协调另行设计。
- 控制面认证、TLS/mTLS 与证书轮换。
- 异常接管、自动扩容与单实体热迁移；不把成员过期当唯一执行者证明。
- 新存储后端、自动 getter 加载与更深生成器扩展。
- 历史榜单合并、奖励结算、赛季和 World 数据迁移；均是应用业务，不为合服目录闭环提前实现。

## 文档与验证规则

设计描述现行实现，计划描述未完成工作，报告描述指定日期与构建条件。链接调整不改写历史数据，不把跳过的外部测试算通过；文档审查不能替代运行时测试。轻量差异审查使用 [tx-contract-review](../.agents/skills/tx-contract-review/SKILL.md)，规则只维护于工程约定与审计标准。
