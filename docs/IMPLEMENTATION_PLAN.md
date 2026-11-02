# 当前状态与验收计划

本页只维护当前待办和证据索引。首次接入读 [README](../README.md)，模块职责读 [设计文档](design.md)，检查业务用法读 [工程约定](engineering-conventions.md)。已完成施工步骤由 Git 历史追溯，不重复列为待实现能力。

## 已完成补验：etcd 历史压缩后的目录恢复（2026-11-02）

etcd 历史维护说明已补入 [使用文档](api-reference.md#etcd-使用与维护)。本次只补真实压缩下的框架接入证据，不把运维维护当作业务功能，不修改共享 etcd 配置，不跑云测试、不提交。

1. 使用本机已有镜像创建独立 etcd；成员和槽位 Watch 分别从已压缩 revision 启动，核对真实 `ErrCompacted`。
2. 验证目录失效、两份新快照恢复及后续增量继续生效，不增加生产测试钩子或业务重投。
3. 验证同一发号键在压缩后仍取得更大的合法 SessionID；复用现有包级和 race 入口，记录实际结果并清理本轮环境。

本地 etcd 3.5.15 独立容器验收通过：两类真实压缩场景及既有 Watch 轮次屏障完成三轮 race；etcd、SessionID、Discovery 三包启用真实依赖后回归通过。没有修改生产代码，测试键和独立容器已清理。本项不验证自动压缩的长期稳态容量，也不重复两组应用整轮集群验收，见 [SessionID 验收报告](session-id-allocation-acceptance.md#etcd-历史压缩恢复补验2026-11-02)。当前已批准的实现和本地补验已完成，下一步可汇总未提交变更；不自动扩大到云端容量或异常接管。

## 当前已完成：SessionID 独立发号与运行身份收敛（2026-11-02）

用户已确认采用 etcd 单次写入 revision 发号、删除数值 work slot 的方向，具体见 [专项实施计划](session-id-allocation-plan.md)。实现及本地验收已完成，证据与未验证范围见 [验收报告](session-id-allocation-acceptance.md)：

1. 补充通用 revision 写入原语与 SessionID 分配器，保持正数及 JavaScript 安全整数契约。
2. 复用进程级 etcd 客户端，切换 CModSessionID；InstanceID 改为每次启动独立唯一身份，移除 work slot 分配、拼接及延迟复用。
3. 同步两组应用验收工具、隔离发号键清理和文档，保持 Login API、票据和客户端 wire 不变。
4. 本地正式包验证双 Login、重启、原 Session 恢复及 Actor 归属不回退，再测完整发号入口开销。不跑云测试、不提交。

Actor 逻辑槽位、Placement、正常排水后条件释放与显式交接继续保留。首次新旧发号算法切换采用统一版本和旧会话失效边界，不引入混部兼容或自动恢复编号机制。现行 API/设计文档已同步；完成情况和实测证据在专项计划中归档。

## 已完成主线：Actor 寻址、归属与生命周期收敛（2026-10-31）

目标与分批约束统一见 [专项实施计划](actor-routing-lifecycle-plan.md)。已完成固定版本 Orleans/Akka 源码对比、目标确认及三批实现：

1. **第一批：路由修正闭环已完成。** 保留缓存命中直达，增加旧归属条件失效、按 SlotID 共享刷新和单向投递异常控制反馈；收敛传输与归属职责，不重投结果未知的请求。
2. **第二批：正常关闭释放已完成（2026-11-02）。** 排水、必要持久化与 Service.Stop 实际完成后，条件释放仍属于本实例的槽位，允许普通服务池后续重新分配。保留跨代版本、显式交接及 World 指定部署；不把失联自动视为可接管。
3. **第三批：目录增量更新已完成（2026-11-02）。** 对照 1,024 / 8,192 / 65,536 槽位及每批 1 / 32 / 256 变化，确认全量重建成本后改为私有增量发布；保留初始/重连完整快照、版本保护与旧轮次拒绝。tx 全量、核心七包 race 和真实 etcd 槽位定向 race 通过；补齐此前未开启的 Actor etcd 夹具成员登记。见 [第三批证据](actor-placement-acceptance.md#2026-11-02-目录更新测量与增量发布)。

第一批 tx 全量、两组示例接入回归及七包 race 通过，见 [第一批证据](actor-placement-acceptance.md#2026-10-31-条件路由修正与共享冷刷新)。第二批 tx 与两组 server 全量测试、核心包 race、真实 etcd 回归和两组正式包有限请求验收通过；全量 `tests -race` 暴露既有 MsgQ 拨号测试夹具竞争，保留失败记录，不标为全套 race 通过。正常释放及 World 重新认领见 [第二批证据](actor-placement-acceptance.md#2026-11-02-正常关闭释放与重新分配)。

**三批后的本地正式包复验已完成（2026-11-02）**：txgame 在线滚动 180 秒，44,999/45,000 完成，99.997778%；SLG Role / World 按当前停服重启策略各运行 A/B/A 三个 180 秒窗口，分别合计 1,028,624 / 3,179,502 次请求，均零失败。计划停服单列，不冒充在线发布成功率。槽位 1→3→5、新 owner、最终存储及 58 条进程退出记录已核对，见[联合验收](actor-routing-rollout-acceptance.md)。本轮只补验收记录，不增加运行时代码，不跑云测试、不提交。三批已批准步骤和本地验收完成，可汇总提交；剩余云端容量、长期稳态和异常接管仍按下述独立边界安排，不把控制面收益当作业务吞吐提升。

**整体审查后的收敛已完成（2026-11-02）**：权威槽位写入统一校验成员状态，缓存仅读；正常释放的 Probe 返回 Unloaded；新增本实例 Claim 并移除 World 自连控制面、代次拼装和 Watch 轮询；删除 tests-only 旧 Placement，保留现行分组放置。tx、两组 server 全量本地回归、相关六包 race 及正式包 World 启动/停服重启场景通过，见 [本轮证据](actor-boundary-fixes-20261102.md)。不重复云端或持续流量压测，本轮未提交。

## 当前能力与证据

| 能力 | 当前方案 | 验收边界 |
| --- | --- | --- |
| Actor 放置与生命周期 | 默认分区、命名分组、独立槽位；Ready 表示通信入口就绪；业务对象由应用管理；正常 Stop 条件释放，显式 PrepareHandoff 保留归属供受控晋升 | [放置](actor-placement-acceptance.md)、[Role/Room](https://github.com/ngnl47/txgame/blob/master/docs/acceptance/actor-package.md)、[World](https://github.com/ngnl47/txgameslg/blob/main/docs/acceptance/world-startup.md)；历史报告不替代本轮验证，不承诺运行中单实体热迁移 |
| GD 访问 | 无缓存显式存取；共享实体 Find 仅限有效 Procedure，普通 Snapshot；World 独占排除 | [本地回归与微基准](entity-access-acceptance.md)，不是云端容量 |
| EntityCache 管理 | Stats、List、SnapshotCached、TryRelease | [管理验收](entity-cache-management-acceptance.md)，无管理页面/全量导出 |
| Procedure | Lockey 与 Store 解耦；Call、子 Call、同步 CallAfterCommit；有界异步 Submit | [锁](procedure-locks.md)、[Submit 真实业务](https://github.com/ngnl47/txgameslg/blob/main/docs/procedure-submit-acceptance.md)；默认 64/8192，合成突发对照未覆盖 8192 档 |
| 正式包滚动发布 | txgame、SLG Role、独占 World 三分钟窗口均达到 99.99% | [2026-10-30 报告](procedure-rollout-acceptance.md)；Role 是同步 Call，不证明滚动期间大量 Submit 队列 |
| 应用合服 | GDServerMerge 定时关系；txgame 在线刷新，SLG 停服后冻结新快照 | [txgame](https://github.com/ngnl47/txgame/blob/master/docs/acceptance/timed-merge-20261030.md)、[SLG](https://github.com/ngnl47/txgameslg/blob/main/docs/acceptance/timed-merge-20261030.md)有限多进程通过；保留原服入口，无业务暂停/自动迁移 |

合服属于应用示例，tx 本轮不增加合服能力。初次合服验收仅核对 Fusion Redis 当前等级及 Mongo 账号投影、合服目录；2026-10-30 补验已等待正常回写并核对最终 Mongo 等级、出生服与重启读取，见下节。

## 大验收前的下一项

1. **文档整理已完成（2026-10-30）**：README 导览、按功能设计、API/配置参考、稳定契约与有日期报告分开；已收敛完成的施工步骤并校验跨仓链接、章节锚点和 Markdown 围栏。此项只做文档验证，没有重新生成或运行集群。
2. **SLG 合服后的 World 联合寻址已完成（2026-10-30）。** [八进程正式包验收](https://github.com/ngnl47/txgameslg/blob/main/docs/acceptance/merge-world-20261030.md)覆盖 OriginServerID → MainServerID → Excel World 绑定 → WorldID。客户端直达与 Role→World 在停服重启后均改向主服的 World，新 owner、CAS generation 和 World 最终保存已核对；12/12 条最终通知属于有限功能证据，不是持续流量或云端容量结论。
3. **架构审查首批修复已完成（2026-10-30）**：服务后台任务关闭等待遵守期限并保留资源重试；共享放置查询与请求取消隔离；Procedure 仅暴露执行视图。冷查询采用 1024 软告警阈值、限频 Warn 并继续接纳，不以阈值拒绝批量业务。[本地回归与缓存命中对照](actor-placement-acceptance.md#2026-10-30-生命周期与冷查询边界修复)已通过，未跑集群或云测试。
4. **本地整体验收已完成（2026-10-30）：** [统一矩阵](local-acceptance-matrix.md)中的关闭重试、共享查询取消、冷寻址软告警通过三轮定向 race（36 次顶层用例）；真实依赖交接、txgame 与 SLG 多进程场景通过。修正 SLG 旧角色夹具、World 拒写注入范围及旧日志断言，保留失败证据并完整重跑；进程和专用数据清理已独立核对。有限请求不作为当前构建的持续流量完成率证据，未跑云测试。

5. **生命周期补修（2026-10-30）**：CMod、根任务和初始化回滚增加限时等待与重试保留；Placement/CMod 任务视图不暴露取消/等待所有权。正常 Stop 保留服务发送器关闭早于根传输入口取消的顺序，失败 CMod 不进入正常 Drain；验证记录见 [定向回归](local-acceptance-matrix.md#2026-10-30-生命周期补修定向回归)。本批不重复整轮集群。

6. **启动失败补修（2026-10-30）已完成**：启动期致命错误取消启动操作并拒绝成功收尾；RPC/控制面终止性异常传回 Server；可靠任务消费者静态校验移到 CMod 准备阶段，消费仍晚于业务就绪。三项失败回归、tx 全量与两组应用包级回归通过，定向 race 三轮通过，见[复审记录](audits/2026-10-30-start-stop-review.md)与[验收证据](local-acceptance-matrix.md#2026-10-30-启动失败定向回归)。

7. **正式包滚动复验（2026-10-30）已完成**：两组工程三轮 180 秒流量分别达到 99.993333%、99.997530%、99.999261%；Role/Room、World 指定启动与重载、持久化回读和 39 条进程退出记录均核对通过。见[本轮报告](lifecycle-rollout-acceptance.md)。旧 A/B 快照过期已在隔离目录按当前生成契约重新导出，未修改运行时代码或验收门槛。

生命周期修复与验收记录已提交，相关工程已快进合入本地主干；合并前回归通过。

## 合服后最终 Mongo 回写与重启读取：已完成（2026-10-30）

1. 复用两组工程 `cmd/cluster-e2e.sh --server-merge`，保留各工程 GDRole 的正式 Fusion 延迟（txgame 600 秒、txgameslg 1 秒），不增加强制写回或缩短配置。
2. 保持回写工作进程运行，等待 Mongo 等级与最后一次客户端结果精确相等，并核对 RoleID、OriginServerID 和账号投影。
3. 正常排水后重启，SLG 复用受控槽位晋升；客户端仅读取、不再次升级，核对等级和原服入口。保留 Redis，重启读取按正常 Fusion 路径执行，不冒充 Mongo-only 冷加载验证。
4. 最终回读 Redis/Mongo，确认进程退出后仅清理 ownership 匹配的专用数据。失败保留证据。

两组 `--server-merge --timeout 20m` 已通过：

| 工程 | 最终 Redis/Mongo 等级 | OriginServerID | 实际等待 Mongo 追平 | 正常退出 |
| --- | --- | --- | --- | --- |
| txgame | 两角色均为 5 | 1、2，不变 | 600.094 秒 | 12/12 |
| txgameslg | 两角色均为 4 | 1、2，不变 | 2.003 秒 | 24/24 |

正式客户端重启读取一致，宿主精确匹配本轮目标新实例。两组 server 全量本地回归和集群工具定向 race 通过；差异审查发现的新宿主断言缺口已修正并对原始结果独立复核。进程退出、Redis DB12/13、专用 Mongo 库及 etcd 清理分别核对。证据见[txgame 补验](https://github.com/ngnl47/txgame/blob/master/docs/acceptance/timed-merge-20261030.md)、[SLG 补验](https://github.com/ngnl47/txgameslg/blob/main/docs/acceptance/timed-merge-20261030.md)。

该轮未跑云测试、未提交；tx 当时仅更新计划，不增加框架能力或业务合服协调。上述 Mongo 补验步骤已完成，当前新主线见页首；容量或生产部署验收仍按下述独立边界安排。

此前“CMod.Start 提交 Submit 后失败”为既有 P0 的判断已撤回：当前内置组件及两组应用没有该实际调用路径，不作为阻塞验收项，不为此增加启动回滚机制。启动错误必须终止本次启动；失败清理只释放已取得资源，不恢复或维持部分服务运行。

两组应用的具体待办：[txgame](https://github.com/ngnl47/txgame/blob/master/docs/IMPLEMENTATION_PLAN.md)、[txgameslg](https://github.com/ngnl47/txgameslg/blob/main/docs/IMPLEMENTATION_PLAN.md)。生成器没有待完成的已批准新功能，扩展以 [txgen 计划](https://github.com/ngnl47/txgen/blob/main/docs/IMPLEMENTATION_PLAN.md) 为准。

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

## 已完成：通信 Actor 与业务对象生命周期分离（2026-10-31）

- 框架已补齐 `WithIdlePassivation(0)` 显式禁用；未指定时保留原配置回退，指定类型策略时优先。ActorHost/公开模块及全库本地测试、Actor 定向 race 已通过。
- 应用按类型声明 Role 10 秒、Room 70 秒、World 禁用空闲卸载，移除重复 YAML。Room/World 由模块管理业务对象、Tick 和收尾，通信工厂仅绑定协议。
- 本地验证包含通信 Actor 卸载重激活不丢业务状态、World Tick 与最终保存、Room 通知与排水。Room 多房间慢通知隔离已通过先失败后修复的回归及 race 检查。
- SLG 显式启动已实施：通用 `WithExplicitAssignment` 约束普通寻址及 Ensure/Retry，默认 Role/Room 行为不变。按 --world-id 登记后直接加载 World，通信 Actor 不前置激活。初版匹配旧 InstanceID 晋升的本地回归及正式包证据见 [指定启动记录](https://github.com/ngnl47/txgameslg/blob/main/docs/acceptance/world-explicit-startup.md)。2026-11-02 正常 Stop 已改为释放，后续启动直接认领空闲代次；旧身份仅用于仍保留归属的显式处置，见页首第二批记录。
