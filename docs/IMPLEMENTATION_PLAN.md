# tx 实施计划

本文只记录尚未完成的任务和需要再次决策的方向，不保存稳定 API、内部算法或已完成阶段。

- 公共 API、配置和业务示例以根目录 [README](../README.md) 为准。
- 内部职责、状态机、固定术语和技术选型以 [设计文档](design.md) 为准。
- 架构质量要求以 [审计标准](architecture_audit_standard.md) 为准；特定 revision 的证据保存在 `docs/audits/`。
- 已完成的实施过程由 Git 历史和 `docs/archive/`、`docs/superpowers/` 保存，不回填到本文。

## 当前主线

### P0：正确性与跨工程回归

1. 保持 `tx`、`txgen`、`txgame`、`txgameslg`、`txclient`、`txclientslg` 的行为同步。涉及生成 Store、集合、StoreView、字段名或存储边界的变更，必须同时检查生成器测试、两个模板的生成产物和业务侧真实 Redis/Mongo 回归；涉及 BON、Frame、路由或 Seq/Ack 时必须分别回归两个客户端。
2. 持续审计 Redis、Mongo、Fusion、WAL 和延迟任务的失败契约。未知返回值、矛盾文档状态、非法路径、路由错误和依赖缺失必须返回明确错误，不能静默成功、自动修复持久化数据或错误清理 Store 状态。
3. 发现旧接口、过渡结构、淘汰测试或旧方案文档时，在确认没有生产调用后直接删除。生效文档只描述当前方案；必要的技术取舍只保留在设计文档。
4. 新增或修改无状态持久化能力时优先使用 `txgame` 生成的真实 `GD*`/`GD*Store`；涉及 Procedure、事务写回、WAL 或 checkpoint 时使用 `txgameslg` 的有状态生成模型，避免 runtime 测试与生成契约割裂。
5. 按下列“可信会话后续 P0 顺序”推进会话投递、登录和两套项目客户端的真实多进程验收。

### 可信会话后续 P0 顺序

MsgQ 的 SessionID 精确邮箱、RoleID 唯一主会话投影、Agent 物理断连/逻辑终止和全 Agent 控制广播已经完成并进入稳定文档，
不再列为待办。控制广播只使用服务发现内存快照，不查询 Redis owner；失活终止只遍历 MsgQ 本地 Session。真实集群 P0
仍未整体完成；以下能力必须按顺序推进：

tx 侧的 `tx/etcd`、Discovery 自动 `workSlot/InstanceID` 和 `CModSessionID` 已完成；txgame、txgameslg 尚未接入这些契约，
旧项目配置与 MongoDB SessionID 流程也尚未删除，不能把 tx runtime 完成视为项目链路验收完成。

1. 分别完成 txgame、txgameslg 的 Login 接入：补齐 AccountID/RoleID 分段 SPI、LoginAccessGuard、`CModSessionID`、短期 ticket 和 AgentTarget 分配，并删除旧 MongoDB SessionID 流程；Login 不依赖 MsgQ。
2. `Create_Role` 使用预分配 `RoleID` 的标准 Actor 路由，成功后 Game 写 `(AccountID, ServerID) -> []RoleID` 投影；PendingRole 仅允许建角和心跳。
3. Borrow 沿用同一 ticket、握手、Session 流程：主 `AccountID`、`BorrowAccountID`、目标 `RoleID`、只读 `AccessMode`、独立 `SessionID`，只响应发起 Session，不成为角色主会话。
4. 同步 `txclient`、`txclientslg`，并依次完成无状态、有状态两套真实多进程验收。

### P1：性能基线

1. 继续以 `VisitChanges`、StoreView 捕获/合并、Redis/Mongo 变更文档构建、事务精确 Key 锁注册表、缓存服务发现路由和
   Actor 解析/激活/销毁及滚动成员拓扑 benchmark 为热点基线。
2. 热点优化必须记录对象规模、嵌套深度、变更密度、Go 版本、耗时和分配；没有基准证据时不引入 pool、`unsafe` 或新的中间批次。
3. `MaxRootChanges` 继续只按变更位置数收敛 Store 状态树；进程 Heap 只用于告警，不能成为 WriteBackManager 拒绝业务的唯一依据。只有真实服务证明路径数和待刷根对象数不足以解释常驻内存时，才重新评审字节预算。
4. Redis 大型整字段写入的安全整数检查只有在 profile 证明为热点后才优化；任何优化都不能降低整数精度保护。
5. 下一阶段补齐 MsgQ 进程内与真实 gRPC 的发布、拉取和 Ack 对照基线，以及服务发现缓存命中和多会话断线恢复的耗时、吞吐与分配。真实网络与恢复数据复用现有多进程集群闭环采集，不增加生产监控 API、配置或常驻采集任务，也不能用本地快捷路径数据代替真实网络结果。

## 待决策能力

下列事项在进入实现前必须重新评审需求、边界和成熟依赖，不能以占位配置或兼容接口提前进入主线。

### getter 触发的懒加载

当前已加载字段状态只描述读取结果，getter 不执行 I/O。若要支持 getter 懒加载，需要先确定：

- loader 由谁持有，是否允许数据对象依赖运行时；
- getter 如何返回 I/O error；
- Actor、Procedure 和普通请求三种所有权边界下是否允许隐式读取；
- 并发加载、失败重试和部分字段定位如何保持一致。

在这些问题决策前，不生成隐式 I/O getter。

### 可靠业务消息

付费发奖、跨服务必达命令和 SLG 跨服务关键操作需要发布、消费确认、重试、幂等与死信语义。`modules/msgq` 是客户端会话邮箱，WAL 是有状态内存变更的预写日志，二者都不能代替可靠业务消息。

后续单独评审 RocketMQ 5.x 等成熟实现。进入实现前必须提供真实服务用于链路测试，并确定消息身份、生产确认、消费组、重试、死信、排水和业务幂等边界。

### WAL 其他 backend

当前 WAL 只实现 Mongo backend。只有 SLG 等项目明确引入消息中间件后，才增加 RocketMQ backend；新实现必须复用 `wal.Store` 的追加、连续读取、checkpoint 和 generation 契约，不能把 RocketMQ 配置提前塞入当前 Mongo 装配。

### 控制面认证与 TLS/mTLS

当前控制面和内部 gRPC 只要求调用方显式选择 TLS credentials 或本地 insecure。生产认证、证书加载、轮换和权限模型尚未实施。后续设计必须保持 `Sender`、`Delivery`、`Drain` 和槽位交接的业务 API 稳定，并优先适配本地研发与云私网部署。

### 异常接管与 fencing

逻辑槽位支持受控排水后的 generation CAS 交接；`slgworld` 示例也已用 Mongo generation fence 阻止旧 checkpoint 覆盖新
owner。尚未完成的是异常宕机、网络分区和自动接管的通用 Lease/placement 策略，以及由业务枚举恢复 stream 的组织方式。
不能假定 `WaitGroupContext`、etcd 成员过期或单次 Mongo CAS 已经独立保证单 owner。

### 可选存储 backend

RedisJSON/ReJSON、SQL 和其他存储后端不进入当前主线。评审时必须先确认目标云服务支持、本地测试方式、分片路由、部分读写收益和运维成本；通用 Store、`change.Source` 和变更文档契约不能依赖具体后端。

### 可选业务工具替换

`util.EventBus` 和 `util.DelayQueue` 是面向业务的可选工具。只有候选成熟库完整覆盖现有并发、顺序、取消和 transactional async 语义，并通过回归与 benchmark 后才替换；不能因为 tx runtime 当前未直接引用而删除。

## 明确不做

- 不在 tx 内实现分布式事务、跨实例锁或 Redis/Mongo 原子双写。
- 不为 Fusion 热写与延迟任务发布增加同槽持久交接索引、启动全槽扫描或容量闸门；进程存活期由本地去重重试和结构化错误日志保障任务发布。
- 不允许 Fusion 根记录物理删除；业务使用状态字段逻辑删除。
- 不把 Store、StoreView、变更文档或 WAL 记录声明为业务协议字段。
- 不把 Actor 与 GD、Store、Procedure 或持久化 Runtime 强绑定。
- 不在普通持久化链路重新引入字符串路径变更批次或操作重放日志。
- 不为尚未确定的 backend、协议或运维能力保留无效配置。

## 每轮完成条件

1. 公共行为更新 README，内部实现或技术选型更新 design；本文只增删未完成事项。
2. 文档、GoDoc、代码注释和模板注释使用 design 中的固定术语。
3. 运行 `git diff --check`，并对受影响工程执行完整 `go test ./... -count=1`。
4. 涉及热点结构时补 benchmark；涉及 Redis、Mongo、etcd 或 gRPC 时优先补真实链路验证。
5. 阶段性报告必须给出具体使用示例和符合 Angular 规范的中文 Commit Message。
