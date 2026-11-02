# 通用 Actor 放置

```text
tx/
├── modules/
│   ├── actor/                 # [L2] Type/Placement 公共契约
│   └── discovery/             # [L2] 稳定 assignment 与受控交接
└── internal/
    ├── actorplacement/        # [L2] 分组查询与缓存
    └── framework/
        └── placement.go       # [L3] 自动装配本地/远端查询能力
```

目标服务提供分组策略，调用方不手动绑定目标业务依赖。

本页描述现行能力。路由缓存修正和正常 Stop 后条件释放已接入；实现与验收进度见 [专项实施计划](actor-routing-lifecycle-plan.md)。实例失联、启动失败及收尾失败不等于已释放。

归属中的 InstanceID 是本次启动生成的 `AppID-UUID` 不透明身份；Actor 逻辑槽位、owner 和 Generation 保持独立。SessionID 使用单次 etcd 写入 revision，不占用或释放逻辑槽位。发号键从规范化 Discovery 根 `R` 派生为 `R + "-session-ids/issuer"`，位于成员/槽位 Watch 和容量 CAS 域之外，不触发放置目录刷新；成员租约注销也不自动删除持久 assignment。

## 声明与调用

Type 使用 `WithPlacement(name)` 引用服务的 `ServiceSpec.Placements`。策略工厂在目标服务被选中且 CMod 就绪后执行；只启用消费者时不会构造目标数据库能力。应用解析业务身份并返回稳定 GroupID；tx 将服务池、策略名和分组 ID 编为槽位。同服务池跨 Type 同组共用实例归属，邮箱与状态仍独立。

```text
Identity → 本地 Identity→SlotID 缓存
  → 冷查询：同实例策略直调 / 共享 RPC 查询目标服务
  → 成功后缓存 SlotID
  → Discovery SlotResolver → 当前 owner 与 Generation
  → 传输投递 → 目标宿主权威核验 → Actor 激活/邮箱
```

已知组的运维入口使用 `actor.PlacementSlot(service, policy, groupID)`，不手写框架编码。普通分区、纯计算 WithSlotMapping 和 World 独立槽位保留，不强迫所有 Type 都查询业务存储。

当前只有这套基于稳定 assignment 的放置路径。`WithPlacement`、`GroupResolver` 和分组缓存仍是正式能力；本地 Actor 宿主不再另存成员哈希 Placement 快照，也不按成员变化重新计算已有 Actor 的 owner。

## 缓存、失败与生命周期

- Identity→SlotID 默认使用 65,536 项有界缓存，同身份冷查询合并；查询上限 3 秒，成功才缓存。
- 共享查询使用框架任务生命周期与独立的 3 秒期限，不继承首个请求的取消或期限；所有等待者（包括首个）均可独立退出。失败不回退默认分区、不自动重投业务。
- 每进程不同 Identity 的实际在途冷查询超过 1024 时继续接纳，仅输出 Warn（含在途数量与阈值），同一进程最多每分钟一次；不因这项阈值拒绝批量业务。相同 Identity 合并、缓存命中不增加在途数量。
- 1024 是观察软阈值，不是硬上限、在线 Actor 数量或生产容量结论。等待超时或取消不移除仍在执行的查询，只有解析器实际退出才删除登记；没有额外等待队列或框架自动重投。若解析器不响应取消，在途工作仍可能增长，告警不能替代实际资源容量验证。
- SlotID→实例使用既有 Watch/assignment/CAS；owner 受控交接不改变分组，因此可以继续使用 Identity→SlotID 缓存。
- 正式路由保留发送时的 SlotID、InstanceID、Generation。框架拒绝或传输不可用只条件失效这份归属；迟到反馈不能撤销已更新的地址，业务同名错误不作为框架拒绝。Handler 或初始化回调返回下游 Actor 的拒绝时保留错误链，但不将其当作当前 Actor 的准入拒绝。
- 冷路由、旧归属失效及有期限的归属不可用等待，按服务池/SlotID 共享查询，独立期限 3 秒。等待者取消不取消查询，实际退出前不移除登记；不可用期间每槽至多一条等待任务，目录变化通知只重查本地缓存，权威回源保持 100ms 间隔，不因其他槽位的更新加速轮询；不再逐请求 10ms 轮询。无期限且宿主不可用的调用仍立即失败。
- 主动读与 Watch 写入同一位置目录。默认 etcd 采用线性读的响应 revision，而非键的 ModRevision；旧 Watch/旧查询不能覆盖更新版本。失效目录仍 fail-closed，单键查询不恢复整个目录。自定义存储可实现 VersionedSlotReader 并与 ReplaceSnapshot 使用同一版本序列；无版本读不合入 Watch 缓存。
- 默认 etcd 的槽位 Watch 在首次/重连时完整重建，正常事件批次只更新变化项并通知一次；复用原目录、版本保护和单轮发布屏障。空 owner 表示已释放，不能当作删除；相同归属事件不解除已有的路由拒绝。更新成本与热路径检查见 [本地测量](actor-placement-acceptance.md#2026-11-02-目录更新测量与增量发布)。
- 排水关闭新的外部查询，实际执行归子生命周期；关闭等待超时返回错误，保留实际工作与依赖供重试，不能强制终止不响应取消的业务 I/O。
- 策略工厂不承载独立后台资源。服务池不同的组不提供跨服务部署亲和。

```text
请求 → 缓存命中：直接返回
     → 同 Identity 正在查询：独立等待已有结果
     → 新 Identity：登记冷查询 → 本地解析 / RPC → 实际退出后删除登记
                   └─ 超过观察阈值：限频 Warn，继续查询
请求取消只结束自身等待；共享查询继续受框架期限和关闭信号管理。
```

```text
发送使用旧归属 → 框架拒绝 / 传输不可用 → 条件失效本地位置缓存
下一次请求 → 共享权威查询 → 更新位置 → 直达目标
                         └─ 仍不可用：有期限地等待或返回错误
```

gRPC 单向投递与 Core NATS 单向投递都复用已有 NATS 实例控制入口发送异常通知；不新建连接、订阅或成功 ACK。Agent 的业务正文仍只经 gRPC 直达。归属标识和反馈来源只在内部传输信封中，不进入业务 Delivery 或客户端消息头。目标已失联时不能依赖它回报错误，后续连接错误和 Watch 仍参与修正。Call、TellAccepted 及可靠任务的成功边界不变，反馈和刷新都不重投原请求。

## 应用边界

分组运行期稳定。tx 不管理服务器合服、赛季表或业务数据迁移；应用决定稳定分组的来源和变更流程。SLG 采用带生效时间的 GDServerMerge 启动快照，停服重启后重算归属；无状态 txgame 不因合服改变 Actor 放置。

首次及释放后的分配沿用 SlotOwnership 和容量预检；正常 Stop 在排水和必要持久化成功后释放仍属于本实例的槽位。空闲记录保留代次，后续 CAS 再分配；未释放的旧 owner 不能仅凭离线被替换。显式 PrepareHandoff 保留旧归属供 Promote 比较，不进入正常释放分支。历史本地调用、缓存零分配及受控交接证据见 [验收记录](actor-placement-acceptance.md)。

运维指定宿主时，Ant 应用使用 `CModLocalAssignments().Claim(ctx, slotID, previousOwner)`；框架绑定本实例并管理代次，成功后用带 revision 的权威读回合并本地目录。应用只保留身份映射及业务加载，不自连控制面、手算 generation 或等待 Watch。其他旧 owner 需要显式匹配及人工停止写入确认，未知写入结果不自动重试；详见 [显式槽位登记](design.md#显式槽位登记与-etcd-更新)。
