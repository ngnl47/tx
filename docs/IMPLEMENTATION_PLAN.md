# tx 后续工作

公共 API 与接入方式见 [README](../README.md)，内部职责与可靠性契约见 [设计文档](design.md)，维护检查见 [审计标准](architecture_audit_standard.md)。完成的施工步骤和废弃方案由 Git 历史追溯。

## 当前实现与已完成验收

会话链路以 [会话序号与确认契约](design.md#会话序号与确认契约) 为准。以下是 2026-10-17 的实现及验收状态，完成不代表已提交或已部署：

| 独立约束 | 当前状态与核对入口 |
| --- | --- |
| UpSeq 止于 Agent 当前连接 | 已实现；[连接处理](../modules/agent/session.go) 与 [Agent 回归](../tests/agent_session_test.go) 覆盖旧号过滤、跳号及 `TestAgentRestoreOnlyRestoresDownlinkMailbox` |
| 上行不确认、不重放 | 已实现；[写帧回归](../modules/agent/session_writer_test.go) 验证服务端不发上行 Ack；[客户端会话测试](../../txclient/tests/client.test.ts) 验证恢复不重放 |
| 业务与 MsgQ 不接收 UpSeq | 已移除 Delivery、HandlerContext 及 MsgQ 上行游标；生成器与两套应用产物已同步，Go/客户端回归已完成 |
| 压测关联与传输序号分离 | 已实现；[ProbeID 回归](../../txclient/tests/cluster-perf.test.ts) 覆盖乱序响应、单条超时后继续且不重投 |
| 本地持续流量滚动发布 | 已完成；[验收记录](../../txgame/docs/acceptance/local-rolling-release.md)：50 连接、180 秒、45,000/45,000 完成，四次 Game 更新/回滚及最终排水通过 |

本地滚动验收覆盖 txgame/txclient；txgameslg/txclientslg 的源码、生成产物和本地单元/构建回归已同步，本次未重跑其真实多进程集群。Agent Rec lease 不是当前待实现事项；MsgQ 会话 TTL 和角色归属租约仍按各自职责存在。

## 投递契约收敛验收

2026-10-17 已完成以下本地验收，本轮变更未提交：

- 来源与完成：`DeliveryOrigin` 统一采用 `OriginInternal`、`OriginClient`、`OriginTask`，区分入口语义而非节点或传输；`Delivery.Origin` 由 Agent/任务入口设置，`Result.Completed` 表达无响应体成功；消息名称不再决定来源和任务确认。两套应用及 txgen 生成接口已同步。
- 身份编码：标准 Actor Key 编解码由 tx 提供，既有 i:/u:/s: 编码保持不变；自定义 Key 不受限制。
- 探针：txgame 额外下行使用 `System_ProbeNotify.ProbeID`；SLG 行军完成使用 `World_MarchCompleted` 的结构化字段；两端产物及关联测试已同步。
- 常规回归：tx、txgen、txgame/server、txgameslg/server 的 `go test ./...` 通过；txclient 115 项、txclientslg 109 项测试及两个客户端构建通过；txbench Handler 测试通过。
- 并发与真实任务：tx 的 taskqueue、actorrpc、agent 包 race 回归通过；txgame 的 `TestQueuedGeneratedCommandRetriesUntilHandlerCompletes` 在本机 NATS/etcd 上以 race 模式通过，验证业务失败保留/重试、完成后才 ACK，并仅清理本轮随机测试资源。
- 独立变更复核未发现实质问题。本轮没有重跑真实多进程集群、完整滚动发布或云端容量测试；其他需显式环境变量的数据库集成测试未启用，不算通过。

## 待开展的部署验收

以下任务需要实际部署参数与专用环境，不自动租赁云实例，也不向生产环境注入故障。

1. **新构建的云端回测与长期稳态**：本地滚动发布已经完成；后续通过 txgame/txclient 及 txgameslg/txclientslg 的集群入口验证持续负载、滚动更新和依赖故障恢复。观察业务完成率、端到端延迟、CPU、内存、队列和客户端计划发起延迟。[七机 20/50 验收](audits/2026-09-28-seven-host-2050-acceptance.md) 仅是三次 180 秒窗口的证据，不代表长期生产容量。
2. **JetStream 生产配置**：按 [部署说明](deployment/jetstream-tasks.md) 确定磁盘、同步策略、副本及物理故障域，验收发布确认、恢复和通知流容量。同机容器故障测试不能替代跨物理机容灾；死信补偿和重投由业务决定。
3. **会话积压容量**：示例继续使用 1024 条/8 MiB 限额；取得慢客户端、离线时长与消息大小分布后再评估容量，不增加无界缓存或触限后静默丢弃机制。

## 需要独立需求才能启动的方向

- 配置中心与进程内配置切换：目前采用外部 JSON、整套校验和滚动重启。后续若启用主动拉取，先明确快照发布、失败保留旧版、在途房间持有原快照及客户端版本协调。结构变化仍需重新生成和构建。
- 控制面认证与 TLS/mTLS：生产身份、权限、证书加载及轮换需独立设计。
- 异常接管、弹性部署与单实体迁移：不从成员过期推导唯一执行者，不在框架内自动重投业务；正常排水和稳定槽位交接沿用现有契约。
- 新存储后端与生成器扩展：以具体应用需求为前提；生成器方向由 [txgen 计划](../../txgen/docs/IMPLEMENTATION_PLAN.md) 维护。

## 维护与验收边界

- tx 保留邮箱顺序、准入、排水、归属 CAS、任务恢复和存储一致性等框架回归；真实 Redis/Mongo/etcd/NATS 测试由明确环境变量启用，跳过不等于通过。
- txgen 保留输入校验、完整生成包编译、生成 API 和配置快照契约，不承接数据库或事务写回实现测试。
- 完整集群编排和端到端容量验收由应用工程维护；无状态与有状态示例分别验收，不互相替代。
- 各库只保留直接测量现行热点的小型 benchmark。新增优化须有可复现瓶颈，不恢复阶段性档位搜索、旧实现 A/B 或固定吞吐采样工具。
- 公共契约变化分别核对字段的产生者、持有者、跨进程传递与释放时机，并逐项记录实现和验收证据；不能用“取消确认”代替“字段不再下传”的核验。同步 README、design、两套应用接入和必要生成产物；普通文档整理不要求重跑集群或重生成未变更产物。
- 有日期的历史设计和压测记录须标明适用版本或已被替代的契约，不作为当前实现依据；原始结果保留，不能改写为新构建已验收。
- 每轮说明实际执行、依赖跳过与未验证的范围，运行差异检查并进行变更复核；未经要求不提交。
