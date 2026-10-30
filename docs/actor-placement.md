# 通用 Actor 放置

包结构：`modules/actor` 提供 Type/Placement 契约，`internal/actorplacement` 管查询缓存，`internal/framework/placement.go` 负责自动装配，`modules/discovery` 保存稳定 assignment 并执行受控交接。目标服务提供分组策略，调用方不手动绑定目标业务依赖。

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

## 缓存、失败与生命周期

- Identity→SlotID 默认使用 65,536 项有界缓存，同身份冷查询合并；查询上限 3 秒，成功才缓存。
- 首个调用取消会终止共享查询，后加入等待者可独立退出；失败不回退默认分区、不自动重投业务。
- SlotID→实例使用既有 Watch/assignment/CAS；owner 受控交接不改变分组，因此可以继续使用 Identity→SlotID 缓存。
- 排水关闭新的外部查询，实际执行归子生命周期；关闭等待超时返回错误，保留实际工作与依赖供重试，不能强制终止不响应取消的业务 I/O。
- 策略工厂不承载独立后台资源。服务池不同的组不提供跨服务部署亲和。

## 应用边界

分组运行期稳定。tx 不管理服务器合服、赛季表或业务数据迁移；应用决定稳定分组的来源和变更流程。SLG 采用带生效时间的 GDServerMerge 启动快照，停服重启后重算归属；无状态 txgame 不因合服改变 Actor 放置。

首次创建稳定归属及容量预检沿用 SlotOwnership；受控交接要求旧 owner 排水和必要持久化成功。不能仅凭成员离线重新分配。历史本地调用、缓存零分配及受控交接证据见 [验收记录](actor-placement-acceptance.md)。
