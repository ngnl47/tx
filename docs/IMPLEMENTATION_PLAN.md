# tx 后续工作

本文件只维护当前状态、待办及验收索引。公共用法见 [README](../README.md)，机制见 [设计文档](design.md)，业务接入检查见 [工程约定](engineering-conventions.md)，架构质量和问题分级见 [审计标准](architecture_audit_standard.md)。中间方案与施工过程通过 Git 历史追溯。

## 当前状态（2026-10-30）

Procedure 锁与 Store 解耦、同步后续调用、独立异步 Submit，以及两组应用接入已完成并提交。本地正式包持续流量滚动验收已完成；本轮文档整理不增加运行时能力，不改变已有验收结论。

| 已完成能力 | 当前说明 | 验收证据与边界 |
| --- | --- | --- |
| 锁与对象登记分离 | [Lockey / StoreTarget](lockey-first-batch.md)；LockType 枚举与 lock 注解，已知键预加锁，未知键动态补锁 | 四工程回归、定向 race、真实 Redis/Mongo/Fusion；不等于分布式数据库事务 |
| 同步与异步 Procedure | [Call / CallAfterCommit](procedure-calls.md)、[Submit](procedure-submit.md)；默认 64 个执行、8192 个总接纳、10 秒期限 | [突发基准](procedure-submit-burst.md)不包含 8192 档；[真实业务 Submit](../../txgameslg/docs/procedure-submit-acceptance.md)覆盖 64/8192 冷暖缓存、排水和 Mongo 写回 |
| 共享 GD 与独占 World | [实体访问](plans-entity-access.md)、[访问与存储边界](runtime-access.md) | [本地回归和微基准](entity-access-acceptance.md)；World 独占 GD，不进入共享缓存 |
| EntityCache 管理 | [统计、元数据、安全快照与尝试回收](entity-cache-management.md) | [管理验收](entity-cache-management-acceptance.md)；不新增管理页面或逐消息探针 |
| Actor 放置与生命周期 | [通用放置](actor-placement-plan.md)、[生命周期](service-actor-lifecycle.md)、[World 启动](../../txgameslg/docs/actor_lifecycle.md) | [Role/Room 正式包](../../txgame/docs/plans/actor-package-acceptance.md)、[World 启动交接](../../txgameslg/docs/plans/world-startup-acceptance.md)；不承诺运行中单实体热迁移 |
| 两组正式包滚动发布 | [2026-10-30 验收](procedure-rollout-acceptance.md)及[原始摘要](validation/procedure-rollout-20261030.json) | txgame、SLG Role、World 各三分钟达到 99.99%；Role 使用同步 Call，不能作为滚动期间异步 Submit 队列排水的证据 |

已确认的通用放置能力由 tx 管理策略查询、缓存、稳定槽位归属及受控交接；业务定义分组。SLG 的 Role、Legion、ServerActor 同主服共槽，GlobalGameActor 与 World 保持独立。Agent 上下行直接走 gRPC，Actor 内部同实例投递本地邮箱、跨实例使用 Core NATS，可靠任务使用 JetStream；详细契约由设计文档与工程约定维护。

## 下一步

1. 工程约定与轻量审查入口已整理：使用 [tx-contract-review](../.agents/skills/tx-contract-review/SKILL.md) 检查指定差异或路径，只引用当前规则，不默认修改代码或启动集群。本轮文档和 Skill 变更等待用户提交。
2. 提交后评审合服业务示例，不直接恢复此前撤下的实现。txgame 保留静态 GDServer 目录和两阶段建角，在线合服协议、运维执行和合榜尚待业务方案确认；txgameslg 采用停服合服，World 归属另行明确。合服不是 tx 的固定业务能力，不在运行中直接修改目录来替代完整流程。
3. 如真实业务开始依赖大量 Submit，可单独设计持续异步业务流量下的滚动验收；现有同步 Role 和独占 World 结果不替代该证据。不为此自动新增压测服务或继续搜索合成负载阈值。

## 待开展的部署验收

以下任务需要实际部署参数与专用环境，不自动租赁云实例，也不向生产环境注入故障。

- **新构建的云端回测与长期稳态**：复用两组应用及客户端集群入口，观察完成率、端到端延迟、CPU、内存、队列和客户端计划发起延迟。[七机 20/50 验收](audits/2026-09-28-seven-host-2050-acceptance.md)是对应历史构建三次 180 秒窗口的证据，不证明当前构建或长期生产容量。
- **JetStream 生产配置**：按 [部署说明](deployment/jetstream-tasks.md)明确磁盘、同步策略、副本和物理故障域。同机容器故障测试不能替代跨物理机容灾，补偿和重投由业务决定。
- **会话积压容量**：示例继续使用 1024 条/8 MiB 限额；取得慢客户端、离线时长及消息大小分布后再评估，不增加无界缓存或静默覆盖。

## 需要独立需求的方向

- 配置中心与进程内配置切换：当前采用外部 JSON、整套校验和滚动重启；主动拉取、在途快照及客户端版本协调另行设计。
- 控制面认证、TLS/mTLS、证书加载与轮换。
- 异常接管、弹性部署和单实体迁移：不从成员过期推导唯一执行者，不自动重投业务。
- 新存储后端与生成器扩展：以具体应用需求为前提，另见 [txgen 计划](../../txgen/docs/IMPLEMENTATION_PLAN.md)。

## 验证与记录

- 框架可靠性、生成契约、应用验收的职责划分见 [工程约定 EC-10](engineering-conventions.md#ec-10)。
- 有日期的报告保留原始结果和测试条件，不把历史通过改写为当前构建通过。未启用的外部测试属于跳过。
- 普通文档整理不重生成未变更产物，也不要求重跑集群。本轮仅验证文档引用、规则一致性及 Skill 使用行为。
