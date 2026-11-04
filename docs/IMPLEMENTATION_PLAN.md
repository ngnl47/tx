# 当前基线与后续验收计划

更新：2026-11-04。本页仅维护当前基线、真实待办及最新验收索引；稳定契约见 [设计文档](design.md)与[工程约定](engineering-conventions.md)。历史实施、失败及复验记录保留在对应日期报告和[验收矩阵](local-acceptance-matrix.md)，不在本页重复施工过程。

## 当前基线

已批准的实现与本地验收已完成，当前没有已确认但尚未实施的修复项。不因历史报告中的“下一步”“未提交”或旧清单重新启动工作。

- **工程边界**：tx 提供通用运行时与 SPI；txgen 维护生成契约；应用负责业务身份、策略、状态和部署编排。Bee/Ant 各自保留独立 L4 门面政策，底层资源由所属运行时管理；层级及公开能力见[职责映射](design.md#包结构与依赖方向)。
- **寻址与生命周期**：InstanceID 是每次启动的独立身份；SessionID 使用 etcd revision 发号，不再分配 work slot。Actor 仍使用逻辑槽位与命名分组；正常排水、必要写回及收尾完成后条件释放，显式交接独立处理。通信 Actor 与 Room/World 业务对象生命周期解耦，不因失联自动接管。见[Actor 放置](actor-placement.md)与[生命周期](service-actor-lifecycle.md)。
- **数据与事务**：生成查询显式传 ctx，通过 Provider 取得资源；共享可变 GD 在有效 Procedure 内 Find，普通读取用独立 Snapshot，World 独占路径排除 EntityCache。Procedure 保留 Call、子 Call、同步 CallAfterCommit 和有界 Submit；存储完成语义按具体模式区分。见[数据访问](runtime-access.md)、[同步事务](procedure-calls.md)、[异步事务](procedure-submit.md)。
- **会话与投递**：Login 认证、授权与签票；Agent 接入时定位角色 owner MsgQ，通过 MailboxStream 协调接入、顶号与关闭。上行不重放，结果未知不盲重投；普通断线保留逻辑邮箱，主动退出等待终止确认。应用采用独占借号，决策留在应用侧。见[会话设计](design.md)。
- **发布契约**：Agent 原地址先关闭再启动，原 Session Restore；MsgQ 正常关闭后旧会话恢复明确拒绝，再由客户端新登录。保留完整计划分母，分别报告正常运行 99.99%、发布中断、未发送与在途未知，恢复契约独立核验。见[现行统计契约](session-rollout.md#3-正常运行发布中断与完整分母)。

最新本地正式包六窗的正常阶段及对照组均为 100%，恢复、持久化及清理核验通过。两组 Agent 全窗口分别为 35,881/36,000、35,880/36,000，仍未达到全窗口 99.99%；发布中断及未发送保留，不能称为全过程达标。报告只证明其记录的构建、拓扑和负载，不替代当前版本云端容量或长期稳态结论。

## 真实待办与进入条件

以下为尚未覆盖的验收或部署事项，不是已经批准的自动执行清单。开启前确定实际需求、范围与验收标准。

| 事项 | 进入条件与责任边界 | 需要补齐的证据 |
| --- | --- | --- |
| 生产接入与部署 | 应用确定认证、密钥、限流、Agent 稳定地址、备份和升级策略；复用[部署要求](design.md)，不把示例能力等同生产就绪 | 部署契约及恢复步骤在目标环境可执行；控制面认证或 TLS/mTLS 等实现需求另行确认 |
| 当前版本云端容量与长期稳态 | 用户明确提供环境和目标负载；不自动租赁云实例 | 20/50 目标下完成率、P99、CPU/内存、队列及客户端计划发起延迟；[历史七机结果](audits/2026-09-28-seven-host-2050-acceptance.md)不替代新构建 |
| JetStream 生产可靠性 | 使用可靠任务并确定磁盘、副本及物理故障域，按[部署说明](deployment/jetstream-tasks.md)执行 | 跨宿主故障及持久化恢复；同机容器不证明生产容灾 |
| 会话积压容量 | 取得实际慢客户端、离线时长和消息大小；当前继续使用既定有界邮箱 | 容量、内存与裁剪表现，不放开无界缓存 |
| 大量 Submit 下的滚动排水 | 业务实际使用大量异步 Procedure；先确定并发、接纳额度和期限 | 排队、拒绝、结果、写回与实际退出；同步 Role/独占 World 的结果不覆盖该场景 |

配置中心与进程内热更、异常接管、自动扩容、单实体热迁移及新存储/生成器扩展，需新需求另行评估。历史榜单合并、奖励结算、赛季和 World 数据迁移属于应用业务，不列为 tx 框架待实现功能。

## 最新验收索引

各能力引用覆盖自身边界的最近证据，不将最新会话报告当作全部功能重测。历史失败不删除，也不按新统计口径改判。

| 日期 | 范围与结果边界 | 证据 |
| --- | --- | --- |
| 2026-11-04 | 两组普通正式包 Login/Agent/MsgQ 各 180 秒；正常阶段、恢复、22 次正常退出及清理通过，Agent 全窗口未达标单列；SLG 120 角色与 World 最终 Mongo 核验通过 | [完整报告](session-rollout-v2-acceptance-20261104.md)、[机器记录](validation/session-rollout-v2-20261104.json) |
| 2026-11-04 | 立即重连与旧接入关闭重叠修复；有限会话、顶号、旧票据、退出及正式包补验 | [重连报告](reconnect-window-acceptance-20261104.md)；此前接入拒绝和持久化失败见[补验报告](session-rejection-acceptance-20261104.md) |
| 2026-11-03 | 运行时边界收敛后的 txgame 在线发布、SLG Role/World 停服 A/B/A 与存档重载；各窗口的构建模式及停服计数分别记录 | [正式包报告](runtime-boundary-rollout-20261103.md) |
| 2026-11-02～04 | L0～L4 权限、共享加载取消、关闭期限、Reader/元数据、RPC 命名及 Agent/MsgQ 封装的定向回归与测量 | [日期化验收矩阵](local-acceptance-matrix.md) |
| 2026-11-02 | Actor 正常释放、重新认领、目录增量更新与缓存路径；World 启动边界 | [槽位验收](actor-placement-acceptance.md)、[边界收敛](actor-boundary-fixes-20261102.md)、[联合发布](actor-routing-rollout-acceptance.md) |
| 2026-11-02 | SessionID 独立发号、运行身份和真实 etcd 压缩恢复；不证明长期存储容量 | [发号与压缩恢复](session-id-allocation-acceptance.md) |
| 2026-10-30 | 两应用合服后的最终 Mongo 回写与重启读取；SLG 合服后两条 World 寻址路径 | [txgame](https://github.com/ngnl47/txgame/blob/master/docs/acceptance/timed-merge-20261030.md)、[txgameslg](https://github.com/ngnl47/txgameslg/blob/main/docs/acceptance/timed-merge-20261030.md)、[World 联合验收](https://github.com/ngnl47/txgameslg/blob/main/docs/acceptance/merge-world-20261030.md) |

EntityCache、GD 与 Procedure 的专项证据继续见[实体访问](entity-access-acceptance.md)、[缓存管理](entity-cache-management-acceptance.md)及[SLG Submit 业务验收](https://github.com/ngnl47/txgameslg/blob/main/docs/procedure-submit-acceptance.md)。应用各自维护接入边界：[txgame 计划](https://github.com/ngnl47/txgame/blob/master/docs/IMPLEMENTATION_PLAN.md)、[txgameslg 计划](https://github.com/ngnl47/txgameslg/blob/main/docs/IMPLEMENTATION_PLAN.md)；生成器待办以 [txgen 计划](https://github.com/ngnl47/txgen/blob/main/docs/IMPLEMENTATION_PLAN.md)为准。
