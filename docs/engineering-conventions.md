# 工程约定与接入检查

适用于 tx、txgen、txgame、txgameslg 及配套客户端。本文集中维护可执行的接入约定和适用范围；机制细节通过链接引用现有契约，不复制实现手册。各工程 AGENTS.md 只补充目录、业务和环境约束；Skill 只提供检查流程，不另存规则正文。

审查时先确认对象归属和调用路径，再选择适用条目。审计严重级别、证据要求与通用架构原则复用 [架构审计标准](architecture_audit_standard.md)；文档与实现冲突应单独报告，不能自动把现有实现或历史方案当作正确答案。下面的编号是稳定引用，不代表严重级别。

<a id="ec-01"></a>
## EC-01 工程职责与生成源

- tx 提供通用契约和运行时；业务身份、玩法、合服规则及项目组合留在应用。txgen 负责输入校验与生成，不承接数据库和事务运行时。
- 两组应用独立示例，不互相 import。生成错误修改输入或模板，并验证完整生成包，不能手改 server/gen 或客户端生成产物。
- 不以统一外观为由删除确有语义差异的路径或必要 SPI；不为少传一个 ctx 引入隐式协程变量、可选 ctx 或第二套 GD 查询包装。

依据：[层级与 API](architecture_audit_standard.md#依赖层级基线)、两组 [txgame AGENTS](https://github.com/ngnl47/txgame/blob/master/AGENTS.md) / [txgameslg AGENTS](https://github.com/ngnl47/txgameslg/blob/main/AGENTS.md)。

<a id="ec-02"></a>
## EC-02 模块入口与资源归属

- 业务模块获取统一为 modules.Role()/Bag()/Rank() 等进程固定入口；模块可以有配置缓存等状态，单例不保证分布式唯一性，也不等于向 Actor 投递消息。
- Handler 按 Actor 匿名内嵌业务片段、整组绑定一次，不保存当前玩家或请求状态。模块之间经公开薄入口调用，内部实现仅由对应薄入口使用。
- CMod 显式声明，启动绑定一次；资源型模块必须能随启动重建和关闭，不用永久 sync.Once 固定旧 Provider。跨子服务模块复用不转移 Sender、订阅或存储连接所有权。
- World、Room 的创建、加载和销毁由应用模块管理，不强制绑定 Actor 生命周期；模块可以按业务身份持有对象，不能把多个实体合并为单个全局对象。直接调用模块仍须遵守实体访问边界，不能替代 Actor 邮箱或 Procedure。

依据：[启动绑定](runtime-access.md#启动绑定)、[txgame 装配](https://github.com/ngnl47/txgame/blob/master/docs/runtime.md#模块装配与统一访问)、[SLG 装配](https://github.com/ngnl47/txgameslg/blob/main/docs/design.md#业务模块与共享-handler)。

<a id="ec-03"></a>
## EC-03 操作上下文

- Handler 外建立操作范围；模块沿用传入 ctx。Procedure 内查询与修改传 tx.Context()，不以原 Handler ctx 或 context.Background() 绕过事务关联、期限或使用期。
- 不保存请求 ctx 供后续后台复用。生命周期、独立后台任务使用其自身范围；Submit 的上下文隔离由运行时完成。
- ctx 取消是协作信号，不能证明 Handler、远端调用、复制或保存已退出；实际结束前不得提前释放锁、使用权或依赖资源。

依据：[执行上下文](runtime-access.md#必须显式保留的执行边界)、[Submit 上下文](procedure-submit.md#上下文与结果)。纯计算无需添加无用途的 ctx；特定清理使用独立有界 ctx 不属于绕过业务取消。

<a id="ec-04"></a>
## EC-04 GD 访问先分所有权

| 路径 | 读取与修改入口 | 不应误判为违规的情况 |
| --- | --- | --- |
| 启用 EntityCache 的共享 GD | 普通读取 SnapshotGD*；有效 Procedure 内 FindGD*，经 Store 受控修改 | 事务内 Snapshot 可复用锁读取自己的修改 |
| 无缓存或显式排除的存储权威记录 | 显式 ctx 的生成查询及应用 db 存取；条件更新等遵守存储契约 | txgame 无缓存；GDUser/GDServer 不是为了满足缓存规则而强制纳入 Procedure |
| 应用模块独占 World | 原生 Mongo 单份加载，由网格邮箱串行修改与保存 | GDWorld/GDWorldBucket 的 @noEntityCache、原生游标和直接 db.Save 是有意设计 |

- 共享对象及其 map、slice、子指针不能带出事务给异步任务、Actor 字段或响应使用；锁内提取值或深复制。框架公开字段不是并发安全沙箱，recover 不能兜住并发 map 致命错误。
- Snapshot 是独立数据，不能作为原共享实体 Save/Remove 的写入口；需要多记录一致性时不能把多次独立快照当作同一事务。
- 原生 Mongo/Redis 及显式 Reader 的存储读取不等于缓存最新值，也不得旁路修改受管的同一记录。核对生成注解和启动配置，不能仅凭 Find/Save 名称判断。

依据：[统一 GD 实体访问](runtime-access.md#统一-gd-实体访问)、[Mongo 原生查询](runtime-access.md#mongo-原生查询)。

<a id="ec-05"></a>
## EC-05 一致性域锁与事务边界

- 已知业务键用生成 LockType.Key 在 Locks 预声明，无需先加载 GD。类型枚举值越大先获取，同类整数键按数值排序；同一一致性域必须使用同一锁身份。
- 未知键允许动态补锁，不能一概要求预知所有键；死锁检测与根重试不使不可回滚副作用变安全。
- Store 记录身份与锁身份分离，多 GD 可以共锁，但 undo、脏状态和写回各自登记。不得以快照、副本或另一锁身份绕过唯一对象约束。
- 根事务用 txruntime.Call；当前事务内复用流程用 tx.Call。不要嵌套启动根事务；只对 Store 受控变更承诺回滚，不提供跨进程数据库 ACID。

依据：[锁契约](procedure-locks.md)、[事务调用](procedure-calls.md)。

<a id="ec-06"></a>
## EC-06 后续工作与完成语义

- tx.CallAfterCommit 登记提交、解锁后的独立事务，按登记顺序同步执行；外层调用等待它完成。它不能替代不同根事务之间的全局顺序。
- txruntime.Submit 是独立异步根事务，不是 Actor 邮箱投递；队列 FIFO 不保证并发执行或完成顺序。需要请求顺序时在邮箱内同步执行。
- Submit 只携带可独立使用的输入，不并发复用 Procedure 或携带共享 GD/短生命周期资源；读取 Procedure 输出应在同步返回或 Future 完成后。
- 接纳失败、事务失败、提交后失败分别处理。Future 成功不等于持久化完成，Await 超时不取消任务，AfterCommitError 不允许重投已提交的原请求。

依据：[同步调用](procedure-calls.md)、[异步执行与额度](procedure-submit.md)。参数默认值只在配置/调用文档维护，不在本表或 Skill 再维护一份。

<a id="ec-07"></a>
## EC-07 写回、创建、删除与回收

- 共享创建/删除在有效 Procedure 内通过 db.Create/Remove 登记；首次创建初值、可见性、回滚和失败 pending 沿用既有契约。活跃 Procedure 内不调用直接 Save/SaveChange。
- 内存提交、WriteBack 登记、Redis/Fusion 写入和最终 Mongo 落盘分别判断。失败不得清除待写状态或伪装成功；结果未知不得盲目重投创建。
- 缓存回收不等于数据库删除或 Actor 卸载。管理 SnapshotCached/TryRelease 在事务外调用，只用有效 Runtime 的 EntryRef，不自行遍历可变 GD 或构造引用。
- World 由应用模块所有者通过邮箱执行周期保存，服务 Flush 保留必要最终写回；移除 Actor 卸载回调不能省略保存。失败保留对象和依赖，由既有 Flush 重试，不同时纳入共享缓存和 WriteBack。

依据：[GD 写入契约](runtime-access.md#统一-gd-实体访问)、[EntityCache 管理](entity-cache-management.md)、[服务持久化收尾](runtime-access.md#服务持久化收尾)。

<a id="ec-08"></a>
## EC-08 准入、排水与实际退出

```text
关闭外部新业务准入
  → 排空已接纳邮箱/业务任务 → 服务拥有的数据 Flush
  → 关闭 Procedure 新根准入并等待 Call/Submit（含排队与同步后续）
  → 最终 WriteBack → 关闭模块及连接
```

- 排水期间保留已接纳工作的依赖；不能只等 goroutine 启动或把超时当作退出。启动失败清理也必须等待已接纳工作。
- 持久化/排水失败沿用资源保留与有限重试，不能返回正常交接成功。致命身份丢失的强制退出是独立故障路径，不扩大为宕机必达保证。
- 已接纳业务的下游 Actor 仍可能拒绝新投递；不为跨节点业务成功新增全局屏障或自动补发。

依据：[Procedure 排水](procedure-calls.md#契约)、[服务收尾](runtime-access.md#服务持久化收尾)、[tx 关闭边界](../AGENTS.md#rpcmq-与优雅关闭边界)。此图表达依赖顺序，具体任务消费者、服务和 CMod 阶段以实现及详细契约为准。

<a id="ec-09"></a>
## EC-09 Actor、传输与路由身份

- tx 管理通用放置查询、缓存和槽位 owner；项目解析业务分组。客户端 RouteKey、Actor Type/ID、SlotID、物理 InstanceID 各有职责，不用业务 ID 替代实例身份。
- 同槽只保证归属实例，不共享邮箱或状态。Ready 只表示 Actor 通信入口初始化完成，仍须等待工厂及全部初始化/激活回调成功；World、Room 的业务可用性由应用判断。等待取消不撤销已经开始的加载，不自动重派或重投。
- Actor 的 Call、Tell、TellAccepted、CallFuture 及可靠任务按各自确认语义检查；同实例本地邮箱、跨实例 Core NATS、JetStream 持久任务不能混为一种保证。发送成功不普遍代表远端业务成功。
- UpSeq 止于 Agent 当前连接，上行不确认/重放；下行 Ack 用于邮箱裁剪。业务请求关联标识不承担传输排序、去重或全局完成顺序。

依据：[会话与投递设计](design.md)、[Actor 生命周期](service-actor-lifecycle.md)、[通用放置](actor-placement.md)。

<a id="ec-10"></a>
## EC-10 验证证据与文档维护

- tx 测框架可靠性；txgen 测输入校验、完整生成包编译和输出契约；应用工程测真实接入，容量与集群流量复用客户端及既有集群入口。
- 跳过不算通过；普通测试、race、真实依赖、本地集群和云端容量分别记录。同步 Call 或独占 World 的验收不证明异步 Submit 集群队列排水。
- 性能缺陷需要同条件基准或画像；新增排队不等于提升处理能力。本地有限批次与三分钟窗口不能外推长期稳态。
- 契约修改同步其说明文档、调用方、生成输入/模板及必要测试；编号条目只更新检查边界，参数、实现细节与实测值留在原说明/报告。

依据：[审计证据标准](architecture_audit_standard.md)、[当前计划与验收索引](IMPLEMENTATION_PLAN.md)。

## 使用审查 Skill

仓库内入口为 [tx-contract-review](../.agents/skills/tx-contract-review/SKILL.md)。安装到 Codex 技能目录后可使用：

```text
$tx-contract-review 检查 tx、txgameslg 当前未提交变更，只报告问题。
$tx-contract-review 检查 txgameslg/server/services/slggame 的 GD 使用与 Procedure 边界。
```

默认检查用户指定范围；未指定则检查当前工程暂存、未暂存及未跟踪文件，不自动扩展到所有兄弟仓库。审查报告引用 EC 编号、文件行号、触发路径和影响；不以关键词命中直接判错。无问题时说明已查范围与未验证项，不宣称穷尽正确性。Skill 只读，修复、启动依赖或压力测试另行遵循用户授权。
