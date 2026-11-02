# Actor 寻址、归属与生命周期收敛实施计划

> 执行方式：按 `superpowers:executing-plans` 分批推进；本文件以复选框记录完成情况。未经用户要求不执行 Git commit，不自动启动云端验收。

**目标：** 保留稳定槽位、缓存直达和本地邮箱，补齐旧路由修正与正常关闭释放，减少发布编排，不扩大消息可靠性承诺。

**架构：** 业务提供 Identity 到分组的规则，tx 管理 SlotID 的权威归属、路由缓存、接收准入及通信 Actor 生命周期。World、Room 和 GD 的生命周期仍归应用或既有存储组件。复用进程级传输，不引入旁路进程、全局协调器或第二份归属目录。

**技术栈：** Go、现有 etcd SlotStore/CAS、Core NATS、gRPC 长流；可靠任务继续使用 JetStream。

**依据：** 2026-10-31 已确认的源码对比与目标模型；现行契约见 [Actor 放置](actor-placement.md)、[生命周期](service-actor-lifecycle.md)、[工程约定](engineering-conventions.md)及 [AGENTS.md](../AGENTS.md)。

**状态：三批实现、对应本地回归及正式包联合验收均已完成。** 第一批见 [路由修正记录](actor-placement-acceptance.md#2026-10-31-条件路由修正与共享冷刷新)；第二批见 [正常释放记录](actor-placement-acceptance.md#2026-11-02-正常关闭释放与重新分配)；第三批依据 1,024～65,536 槽位的对照测量采用增量发布，见 [目录更新记录](actor-placement-acceptance.md#2026-11-02-目录更新测量与增量发布)。随后完成[联合验收](actor-routing-rollout-acceptance.md)：txgame 在线滚动及 SLG 停服重启后的七个运行窗口均达到 99.99%；不据此证明业务吞吐提升或在线 SLG 发布。全量 race 暴露的既有 MsgQ 测试夹具竞争另行记录，不能标为全套 race 通过。

## 1. 固定参考与采用边界

| 参考 | 固定版本 | 借鉴内容 | 不引入内容 |
| --- | --- | --- | --- |
| Orleans | v10.4.2，`876ffe0338e41bc6339e85ee5696dbee6cf9a4c4` | 旧地址条件失效、错误反馈、共享冷查询、放置与目录分离 | Grain 与业务对象强绑定、转发与业务重放、完整成员协议 |
| Akka | v2.10.24，`8619c97e0680beb54db179fca4774bb1c0d36d4d` | 接收准入与实际停止之间的边界、旧激活结束后才能重建 | 全集群交接确认、中心协调器、交接缓冲与重放 |

源码位于同级 `orleans`、`akka` 仓库。核对入口：Orleans 的 `GrainDirectory/LruGrainDirectoryCache.cs`、`CachedGrainLocator.cs`、`Messaging/MessageCenter.cs`；Akka 的 `ShardCoordinator.scala`、`ShardRegion.scala`。Akka 交接包含为避免乱序丢弃缓冲的分支，不引用“绝对无损交接”作为设计依据。

采用机制后按 tx 契约独立实现，不默认允许复制第三方源码。若确有复制需要，另行核对对应许可证。本轮没有重新运行第三方测试或性能实验。

## 2. 全局约束：不得在实施中漂移

1. Identity、SlotID、InstanceID、本地激活、业务对象是不同概念。Actor 空闲卸载不释放共享槽位，不销毁 Room/World，不删除 GD。
2. 保留 `Identity{Type, ID}` 和现有分区/Placement 规则。同主服的 Role、Legion、ServerActor 共享归属，邮箱与业务状态独立；不新增逐角色归属记录。
3. 区分 `Identity→SlotID` 与 `SlotID→InstanceID+Generation` 两类缓存。宿主变化只修正位置缓存，不连带清空稳定的业务分组缓存。
4. 缓存命中不增加权威查询、逐消息成功 ACK 或中转节点。Agent 上行和 MsgQ 下行继续走直达 gRPC；内部调用保持本地邮箱/Core NATS，可靠任务保持 JetStream。
5. 路由刷新不等于业务重投。未知执行结果不自动重放；业务错误不得被当作框架准入拒绝而重试。没有发送过的请求可在自身期限内等路由，不能越过期限继续投递。
6. 保留数量容量检查、接收准入、已接纳工作排水、Procedure、EntityCache 和必要写回边界。取消或等待超时不是实际工作结束的证明。
7. 新成员加入不重算已有归属。仅正常关闭完成后允许条件释放；失联、租约过期、连接错误都不自动释放或接管。
8. `WithExplicitAssignment` 仍禁止普通寻址和 Ensure/Retry 自动首次分配。指定 World 由应用登记归属并显式加载；通信 Actor 不负责加载业务 World，也不把它随机分配到其他宿主。
9. txgen 继续只生成身份、引用和协议绑定；没有具体契约缺口不改生成器。业务服务器/赛季/合服解析留在应用，不引入固定游戏类型判断。
10. 不重写邮箱、不新增业务模块容器、全局状态机或独立连接管理；不把正常关闭优化扩大为异常接管、单实体热迁移或无损业务链保证。

## 3. 目标模型与包职责

```text
Identity{Type, ID}
  → 默认映射 / 应用 Placement → SlotID
  → 权威归属及位置缓存 → InstanceID + Generation
  → 进程内对应子服务 → 通信 Actor 邮箱 → Handler
  → 应用模块 → GD / Room / World
```

| 位置 | 保留或收敛的职责 |
| --- | --- |
| `modules/actor` | Type、Identity、Ref 及控制契约；不认识业务对象 |
| `internal/actorplacement`、`internal/framework/placement.go` | 业务分组查询、Identity 缓存及本地/远程装配 |
| `modules/discovery` | 唯一槽位权威、CAS、位置缓存与版本合并；不执行业务加载 |
| `internal/actorrpc` | 现有调用入口和传输适配；路由策略解除对具体 NATSTransport 的依赖，不复制到各传输 |
| `internal/actorhost` | 接收准入、激活、邮箱串行化、控制状态、空闲卸载 |
| `internal/framework` | 进程启动/关闭编排；实际排水和持久化完成后才能释放归属 |
| 两组应用 | 分组数据、World 指定部署与显式加载、Room 创建/结束、业务最终保存 |

优先在现有包内收敛；仅实际依赖方向要求时提取中立内部路由包，不预先为每个阶段增加抽象层。owner 始终指本轮启动生成的 `AppID-UUID` 不透明 Runtime InstanceID，不是子服务名、进程 PID 或 WorldID。SessionID 已独立使用每次 etcd Put 的 revision，运行身份不再分配 work slot；Actor 的逻辑 Slot、Generation、成员租约及受控交接边界保持不变。发号键位于 Discovery 根的 Watch/CAS 域之外，具体键与连接所有权见 [运行身份与发号](design.md#运行身份与-sessionid)。

## 4. 生命周期与请求流程

### 4.1 通信 Actor：保留现有状态机

```text
Unloaded → Initializing → Ready → Closing → Unloaded
               │           │         └─ 关准入、完成已接纳工作和清理后才释放容量
               │           └─ 空闲 / ctx.Deactivate() / 宿主排水
               └─ 初始化失败 → 清理结束 → Failed
                                             └─ RetryActivation → Initializing
```

Ready 晚于工厂和初始化/激活回调，不保证业务对象存在。普通 Handler 错误不等于激活失败。Ensure 共享初始化，WaitReady 只结束自己的等待；清理结束前不得建立同身份的新激活。失败记录沿用现有有界保留策略，不将其解释为业务永久不存在。`WithIdlePassivation(0)` 禁用空闲卸载，不禁用排水。

```text
World：启动参数 → 应用登记指定归属 → 加载 World → 开放业务准入
       → Handler 查询已有 World → 服务 Flush 保存、关闭 World
Room：路由到 Room Actor → Start Handler 创建业务 Room
      → 业务决定战斗结束/超时/销毁
```

进程发现 Running、子服务业务准入、Actor Ready 是不同事实。当前通信基础设施先于 Service.Start 可用；不能通过改名把发现 Running 当成业务加载完成，也不为此改写全部启动顺序。

### 4.2 路由缓存修正：第一批目标

```text
命中两级缓存 → 直达宿主 → 接收准入 → Actor 邮箱
缓存缺失 / 旧归属反馈 / 连接不可用
  → 按 SlotID 共享查询 → 权威目录
      ├─ 当前归属可用：条件更新同一份位置缓存
      ├─ 无归属且允许自动分配：现有容量预检 + CAS
      └─ 不可用：有期限地返回失败，不猜测新 owner
```

失效操作携带失败时观察到的归属，迟到失败不能删掉新归属。Watch 与主动查询按权威版本合并，旧快照不能覆盖新查询；不另建相互覆盖的缓存。Generation 区分归属，etcd revision 用于事件/快照新旧判断，两者不混为一谈。

单向投递仅在框架拒绝等异常时通过控制路径提示路由失效，不逐条回复成功。提示不是新归属权威，仍需核对目录；目标已死亡而无法通知时，由后续连接失败/查询/Watch 修正。业务请求不转发、不缓冲重放，不承诺失败的那条消息自动恢复。

### 4.3 正常关闭释放：第二批实现

```text
关闭新准入 → 完成已接纳邮箱、后台任务与 Procedure
  → 完成业务收尾及 WriteBack → Service.Stop 等待模块自有队列退出
  → 条件释放本实例仍拥有的槽位
  → 关闭剩余控制/发现连接并注销身份

下一次寻址 → 确认可分配 → 容量预检 + CAS → 新宿主按需激活
```

这替代普通服务池必须指定 A→B 的部分发布编排，不要求所有调用方同步看到新归属。仍保留显式交接高级能力；`PrepareHandoff` 成功后不能提前释放，否则会破坏既有 Promote 的比较条件。

释放必须保留可比较的单调版本：不能直接删除后重新从 Generation=1 建立。当前使用空 RuntimeInstanceID 表示已释放，Generation 非零且每次转换递增；空闲记录不计容量，Watch 保留，同一 SlotStore CAS 重新认领；不引入第二张归属表。多槽位释放允许部分成功，重试只继续仍属于本实例的记录；迟到释放不能删除已经晋升到其他实例的归属。

正常 Stop、启动失败回滚、致命身份丢失清理不得共用一个无条件释放分支。未知释放结果先读权威状态；失败保留必要连接供有限重试。实例 Lease 过期不作为释放确认。Generation 也不是 Mongo/Redis 写入隔离令牌，不能靠它保证失联旧进程停止写入。

## 5. 分批实施与验收

各批先补可失败的关键回归，再实施最小改动；只在验证结束后勾选。新增公共签名、协议字段和持久格式随所属批次确定并记录，不将本文概念名直接扩张成公共 API。

### 第一批：路由修正闭环与传输职责收敛

**范围：** `internal/actorhost/target.go`、`internal/actorrpc/{ownership,routing,direct,nats}.go`、`modules/discovery/{slot,slot_directory,slot_etcd}.go`、Actor 内部传输信封及必要的框架装配。使用当前 SlotAssignment/SlotTarget，确保发送时的归属信息不在内部路由中丢失。

- [x] 在 `modules/discovery/slot_refresh_test.go`、`internal/actorrpc/{route_feedback,handoff_route}_test.go` 和 `internal/actorhost/route_rejection_test.go` 补齐旧失败晚到、共享刷新、调用取消、业务错误不触发重投的回归。
- [x] 补齐主动查询与 Watch 乱序、旧全量快照覆盖新结果、目录不可用场景；未建立安全的新鲜度策略前保留失效后的拒绝行为，不能直接取消 fail-closed。
- [x] 实现基于观察归属的条件失效及按 SlotID 合并查询；共享工作有独立期限，等待者独立取消，实际退出前不移除登记。沿用已有任务所有权模式，不新增接纳硬阈值。
- [x] 替换逐请求 10ms 等待轮询，保持未发送请求的期限边界；权威仍返回不可用时不能形成无间隔查询循环。
- [x] 解除 EntityRouter 对具体 NATS 传输的策略依赖；复用现有进程级 RPC/控制连接，容量检查与归属决策仍只有一处。
- [x] 补齐 gRPC 单向和 Core NATS 单向投递的异常路由反馈及测试：只反馈框架准入错误，不增加成功 ACK，不向业务暴露路由版本，不重投原请求。
- [x] 验证正常投递、Call 结果、同 Actor 顺序、Tell/TellAccepted、JetStream 原契约不变；本地和跨进程路径均覆盖。
- [x] 完成下述包级、race 与缓存命中 benchmark 对照，更新现行放置文档和本批证据；本批不释放槽位。

**完成条件：** 地址更新不会被旧错误撤销；查询共享且可取消等待；单向拒绝能修正后续寻址；缓存命中没有新增网络交互，未知结果不重投。

### 第二批：正常关闭后的归属释放与再分配

**范围：** `modules/discovery/{slot,ownership,slot_etcd,slot_directory}.go`、`internal/framework/{shutdown,handoff}.go`、相应控制适配。应用通过现有 `cmd/` 与 `server/cmd/` 入口验收，不创建第二套部署工具。

- [x] 在 `modules/discovery/slot_release_test.go` 补齐释放后重新争抢、版本不复用、容量回收、多槽部分完成及已释放后的权威回读；重复请求模拟释放已成功但调用方未确认的状态，不冒充真实网络丢包注入。
- [x] 扩展现有槽位契约表达正常释放，保持首次创建与再分配使用 CAS；未释放的旧 owner 即使不在线也不能被自动替换。
- [x] 在 `internal/framework/slot_release_test.go` 补正常 Stop、写回失败、未退出工作、重复 Stop、启动回滚、PrepareHandoff 及服务私有队列完成顺序；迟到释放冲突在 discovery 回归中覆盖。
- [x] 接入正常关闭顺序；启动失败、致命退出和异常接管不扩大自动清理权限。正式控制面也使用权威写入口，CAS 核对目标成员版本，拒绝预检后已排水或消失的目标。
- [x] 保持指定 World 的显式分配与加载，由应用适配已释放归属的登记；不要求普通服务池指定下一宿主，也不允许普通消息为 World 随机分配。
- [x] 同步 `AGENTS.md`、现行设计、两组应用部署说明，区分显式交接、正常关闭释放、异常失联处置；移除普通重启中冗余的应用晋升分支。
- [x] 完成真实 etcd 的并发/失败回归与两组本地正式包有限请求验收，核对进程、工具 ownership 清理和最后持久化结果。

**完成条件：** 正常停服后的普通槽位可重新分配；失败关闭不提前释放；无第二份权威，无异常自动接管；组合进程不在其他子服务工作未完成时释放其共享归属。

### 第三批：按证据决定目录更新优化

**范围：** `modules/discovery/slot_etcd.go`、`slot_directory.go`、`runtime.go` 单轮发布接线及对应测试/benchmark。

- [x] 测量 1,024 / 8,192 / 65,536 槽位、每批 1 / 32 / 256 变化的耗时、分配和独立进程峰值，前后交替三轮。
- [x] 8,192 条单变化的旧目录发布中位数约 1.49ms、1.31MiB，采用私有增量更新；保留初始快照、断 Watch 后重建与旧事件拒绝。
- [x] 完成版本、失效、真实 Watch 及缓存命中回归；扩大实际启用的 etcd 测试后补齐旧 Actor 夹具的权威成员登记，未放宽运行时校验。

**完成条件：** 有可复核证据决定保留或优化；不能将控制面收益宣称为上下行吞吐提升。

## 6. 验证入口与证据要求

在 tx module 中执行，先按当批 Test 名选择定向用例，再扩大到以下包；具体命令及退出状态写入验收报告：

```bash
go test ./internal/actorrpc ./internal/actorhost ./internal/actorplacement ./internal/framework ./modules/discovery ./modules/rpc ./tests
go test -race ./internal/actorrpc ./internal/actorhost ./internal/actorplacement ./internal/framework ./modules/discovery ./modules/rpc ./tests
go test ./internal/actorrpc ./internal/framework ./tests -run '^$' -bench 'Benchmark(SharedCachedRoute|ActorPlacementHit|SlotRouterCachedResolve)' -benchmem -count=5
```

改动前后使用同机、同 Go 版本及参数；记录 ns/op、B/op、allocs/op、波动。保持现有缓存零分配断言，显著且可重复的性能回退必须解释并解决，不能用历史云压测代替。第三批另测目录更新，不混入路由命中结果。

真实依赖测试先核对现有用例的启用条件；测试跳过不算通过。复用已有隔离资源与 ownership 清理规则，不删除未授权数据。必要时再运行 `go test ./...` 及应用回归；未改变生成契约不为了形式重生成全部工程。

最终本地验收复用两组工程正式包和集群入口，覆盖普通服务池滚动、同主服三类 Actor 归属一致、指定 World 停服启动及数据恢复。持续流量仍以 99.99% 完成率核验，失败项不能由自动重投掩盖；功能测试与持续流量证据分开记录。不租赁云实例。

2026-11-02 三批后联合验收完成：txgame 在线 A→B→A 的 180 秒完成率为 99.997778%；SLG Role / World 各三个 180 秒运行窗口均为 100%，计划停服单独记录。复用结构兼容的完整 A/B 导出并重新构建程序；World 指定启动、重复 owner 拒绝、两条行军路径、存档重载和保存失败保留也通过。持久化证明、58 条退出记录及专用数据清理已独立核对。详见[联合验收](actor-routing-rollout-acceptance.md)；不恢复旧 World 在线交接，不改门槛，不跑云测试、不提交。

## 7. 重点复审与交付记录

| 风险 | 所属批次与必须证明的结果 |
| --- | --- |
| 迟到错误/Watch 覆盖新归属 | 第一批：同一位置缓存的条件失效和版本合并 |
| 单向请求已经可能执行却再次发送 | 第一批：异常反馈只更新路由；传输调用次数断言 |
| 大量等待者取消造成共享查询泄漏或反复创建 | 第一批：登记到实际退出，独立期限与任务关闭 |
| 关闭部分成功/释放结果未知造成跨代误删 | 第二批：逐槽 CAS、权威回读、可重复关闭 |
| 业务持久化失败或 World 未加载却开放新宿主 | 第二批：失败不释放，显式部署与业务准入不绕过 |

每批交付写明：实际变更、测试命令及结果、未验证边界、性能证据和下一批入口。现行设计只描述已实现部分，总计划维护进度，本文件维护目标与分批约束，验收报告维护构建证据；避免把同一套规则复制到多个实施文档。

- [x] 2026-10-31：固定版本源码对比、目标确认和本计划落地；仅静态分析及文档检查。
- [x] 第一批完成，证据已登记；缓存命中新增成本及完整链路未验证项已如实记录。
- [x] 第二批完成，证据已登记。
- [x] 第三批依据测量采用增量更新，正常变化不再重建全目录；回归边界及成本证据已登记。
- [x] 两组本地正式包有限请求与持续流量验收完成，现行文档与本轮行为一致；区分 txgame 在线滚动和 SLG 停服重启后的运行窗口。

## 8. 整体审查后收敛（2026-11-02）

保留 `WithPlacement`、业务分组查询与 Identity→SlotID 缓存；仅删除旧的成员哈希 Placement。按以下顺序实施，本轮不提交、不跑云测试。

- [x] 槽位写入在权威存储中统一复核成员状态与容量，不依赖具体类型断言；Probe 将正常释放视为 Unloaded。
- [x] 提供仅认领本实例的槽位能力，复用权威 CAS 与版本缓存合并；World 删除自连控制面、手算代际与 Watch 轮询。
- [x] 删除旧 Placement 实现，保留有效生命周期测试，统一 API 与示例文档。
- [x] 完成本地失败回归、相关依赖集成测试和最终差异审查，记录实测与未测边界。

实测命令、失败夹具修正与边界见 [本轮回归记录](actor-boundary-fixes-20261102.md)。
