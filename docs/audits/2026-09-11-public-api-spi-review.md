# tx 对外 API/SPI 职责与易用性首轮审阅（2026-09-11）

> 后续接口迁移：Context 的 Storage/CMod 能力现已改为单返回值，增加 CModSlotResolver，由同步装配边界处理能力错误。下文保留首轮审阅和槽位拆分阶段记录；当前签名、失败语义和迁移方式见 [README](../../README.md)。

## 范围与结论

基于 tx f4f3d38 的当前代码，以四项 P0 的外部接入面为范围：ant 组合根/能力上下文、storage/change、txn/writeback、Actor/RPC、Agent/MsgQ、Discovery/Control/Lifecycle，并追踪 txgame、txgameslg 的实际接入。不是全库正确性或安全认证；日志、BON 编解码及可选 util 工具未做逐项深审。

使用官方 Ponytail 4.9.0 的审计视角检查冗余，同时单独审查职责与易用性。首轮只提出意见；随后用户授权的实现记录见下节。配置生成降低优先级、认证暂缓、异常排水交给运维，不将这些决策转化为新增库能力。

## 本次实施

已按用户要求保留 game 侧高级能力：增加 SlotReader 和 SlotResolver，ant.Context.CModSlots 及 SLG 业务只依赖读取；SlotStore 嵌入读取契约，原 SlotRouter 的 ResolveSlot/CreateSlot/PromoteSlot 及 control.SlotPromoter 保持可用。高级 SlotRouter 组合并复用 SlotResolver，不复制路由算法、不按运行时类型断言决定写能力。原审阅对 NewSlotRouter 的只读建议在核对完整调用后修正，不直接删除已有管理方法。

SlotReader 是编译期能力声明，不是进程内安全边界。game 高级组件由组合根显式注入完整 SlotStore，不能依赖对业务读取上下文做向下断言。测试中创建 assignment 也改由独立高级入口完成，保留真实 Agent 路由和世界写回证据。Stop 和 CollapsibleChangeStore 的公开说明已同步修正。下节保留发现及理由，当前实现和验证以本节及文末为准。

## 首轮发现与处理理由

1. **P2：业务槽位读取入口暴露了控制面写权限。** `ant/context.go:93,277` 的 CModSlots 返回完整 discovery.SlotStore；该接口在 `modules/discovery/slot.go:50` 同时含 ResolveSlot、CreateSlot、PromoteSlot。返回值来自 advanced.Slots，CachedSlotStore 的写方法直接转发持久层。SLG 的 Agent、Game 路由及 World loader 实际只需要解析归属。建议业务能力只暴露 ResolveSlot，完整 SlotStore 留给控制面与底层装配；只读解析应仅依赖读取能力；进一步核对发现原 SlotRouter 同时承担高级创建/晋升，实施时必须保留该完整 API。保留现有创建/晋升 SPI，不删除运维能力，不增加自动接管。这是编译期职责收窄，不是进程内安全隔离；涉及公开接口和示例调用，已按上节的实施说明完成拆分与回归，保留完整高级接口。
2. **P2：Stop 的公开说明容易被理解为错误返回后也已释放资源。** `ant/server.go:39` 仅写“执行排水并释放全部物理资源”，实际 `internal/framework/shutdown.go:132–143` 在常规排水/持久化失败时保留资源并返回错误。建议 GoDoc 明确单次 Stop 的失败保留语义，以及 Run 负责首次尝试加三次重试、最终返回失败；进程退出和运维处置由调用方负责。先修正公开说明，不为此新增重试包装或异常接管接口。
3. **P3：变更预算职责的公开 GoDoc 已过时。** `storage/changes.go:45` 把 CollapsibleChangeStore 的使用条件写成 txn.Runtime 配置单根容量；当前 MaxRootChanges 在 `modules/writeback/manager.go:61`，校验与折叠也由 Manager 执行。建议改为 writeback.Manager 的职责，保持 Store 折叠能力与事务执行器分离。

## 应保留的边界

| 对外能力 | 当前职责及保留理由 |
|---|---|
| ant.Context 与能力视图 | 在构造期按 CMod 声明取得能力，不把 client Close、ActorSystem 或依赖图所有权交给业务。视图的转发有权限和生命周期边界，不能按包装层数量删除 |
| storage.Create/Save 与 ChangeApplier | 业务入口负责全字段创建/增量保存和成功清脏；SPI 负责后端应用。Save 失败保留变更，Create/Save 明确收敛 Upsert。多参数低阶 SPI 不应直接成为日常业务入口，也无需新增一套同义请求对象 |
| change.Source / Visitor / ValueResolver | 元数据遍历、路径值访问与存储/编码解耦；必要同步、所有权及重复遍历契约已明确，保留接口和性能证据 |
| txn.Executor / Procedure / CommitParticipant | 业务只执行 Procedure，提交预约由框架装配。Runtime.Call 成功为内存提交，不等同于持久化完成；不能把回调、写回与事务包装为一个模糊完成信号 |
| writeback.Runtime / Manager / CModProcedureWriteBack | Manager 管理唯一根身份、pending、刷盘与回收；CMod 适配负责装配生命周期。保留项目扩展加载/淘汰/索引的 SPI，不因当前主要使用默认 Manager 就删除 |
| actor.Messenger / Future | 业务投递不取得集群关闭权；Tell、TellAccepted、Call、CallFuture 分别表达不同确认边界。Await 超时只停止等待，不能据此重发业务。PID 暴露符合当前明确保留 ProtoActor 的选型，不为假想可替换性加一层句柄 |
| Agent Dispatcher / IdentityResolver | 连接、Seq 和可信身份由 Agent 管理，业务认证与命令分派由项目实现；测试桩和独立示例是有效使用边界 |
| MsgQ SessionClient / PubSession / PubRole | 本地和 gRPC 共用会话契约；会话级 Seq/Ack、精确投递、角色扇出与部分成功不同，不能合成无区别的 Send。只读 payload 所有权转移避免多会话复制，保留文档和测试 |
| Discovery 与 Control | Watch 读取、持久归属 CAS、控制面操作和运维策略有不同职责；需要修正的是业务读取入口的能力范围，不是把异常接管塞进 Discovery 或 Mongo 写入 |

## 易用性判断与下一项

高阶入口基本明确：无状态 GD 使用 storage.Create/Save，有状态服务使用 CModProcedure 的 Executor，网络请求使用 Messenger 或 SessionClient。主要问题是槽位读取入口过宽，以及两处公开说明没有准确表达已经存在的边界；未发现需要重建统一 facade 或合并存储/事务/消息 API 的依据。

本次已收窄业务槽位读取契约并修正 GoDoc，保留控制面写入及关键验收。可靠业务消息的 Go 生态选型随后独立讨论，尚未选择 RocketMQ 或 Kafka。

## 本次实现验证

本次重新执行验证，日志与预编译 benchmark 二进制位于 `/private/tmp/tx-slot-reader-20260911`。以下计数包含子测试，不将重叠的定向测试累加到全量数字。

| 本轮命令范围 | 通过 | 显式跳过 | 失败 |
|---|---:|---:|---:|
| tx `go test ./... -count=1 -timeout=180s` | 1103 | 39 | 0 |
| txgen 同上 | 506 | 0 | 0 |
| txgame/server 同上 | 235 | 3 | 0 |
| txgameslg/server 同上 | 329 | 5 | 0 |
| tx `go test -race ./modules/discovery ./modules/control ./ant -count=1 -timeout=180s` | 36 | 4 | 0 |
| SLG 真实 Agent 路由、世界命令 Mongo 写入定向测试 | 2 | 0 | 0 |

默认未启用的真实依赖测试仍明确记为跳过；不借用上一批 47 项补跑作为本轮通过计数，也不以本轮局部 race 结果宣称第三方 ProtoActor etcd Provider 关闭竞态已修复。两客户端无代码改动，本轮未重跑构建。

新只读测试先以缺少 NewSlotResolver 编译失败，再验证只含 ResolveSlot 的实现可以接入，且 starting/draining/stopping 目标均被拒绝。初次测试分别发现新测试实例缺少 Services、测试侧 etcd 配置缺少 LeaseTTL；已修正测试夹具并复用项目 DiscoveryConfig，最终定向和全量通过。沙箱禁止本地监听的测试在授权环境重新执行。

### 路由缓存性能

本机 Go 1.27、darwin/amd64、Intel i9-9880H；修改生产代码前编译 before.test，修改后编译 after.test。串行交替先后顺序，6 组配对，使用相同现有测试：

```sh
./before.test -test.run='^TestSlotRouterCachedResolveDoesNotAllocate$' -test.bench='^BenchmarkSlotRouterCachedResolve(Parallel)?$' -test.benchmem -test.benchtime=400ms -test.cpu=1,16 -test.count=1
# after.test 使用完全相同参数；每轮交换前后顺序。
```

| 缓存读取场景 | 修改前中位 ns/op | 修改后中位 ns/op | 每轮配对变化的中位数 |
|---|---:|---:|---:|
| 串行，GOMAXPROCS=1 | 111.45 | 112.40 | -0.5% |
| 串行，GOMAXPROCS=16 | 113.10 | 112.45 | -0.1% |
| RunParallel，GOMAXPROCS=1 | 114.80 | 115.15 | +2.3% |
| RunParallel，GOMAXPROCS=16 | 55.265 | 55.560 | +1.2% |

所有样本均为 0 B/op、0 allocs/op；零分配测试和持久层读取次数为零的断言全部通过。配对变化中位数不是两个独立中位数的比值；短路径波动大于上述变化，16 路并行单轮变化范围为 -4.9% 至 +21.3%，因此只报告本次未观察到稳定退化，不宣称加速。这里验证保留的高级 SlotRouter 经共用 SlotResolver 的缓存读取路径，不代表网络、etcd 或端到端业务吞吐。采集和结果文件均在临时目录，不进入生产路径。

本次 Go 生产代码 tx 净增 31 行，txgameslg 净增 0 行；测试分别净增 28、2 行。此前未提交的 txgen benchmark 另增 64 行。本次为明确能力边界而保留必要代码，不作为瘦身收益。

公开签名迁移：自定义 ant.Context 实现的 CModSlots 返回类型需改成 SlotReader；只读调用方从 NewSlotRouter 改为 NewSlotResolver。原高级 SlotStore、NewSlotRouter 和控制面管理 API 不变。

## Ponytail 复杂度结论

本次最终验证后按官方 ponytail-review 审阅累计未提交差异；未发现应删除的额外抽象。SlotReader 是业务所需的最小契约，SlotResolver 与高级 SlotRouter 共用读取算法，保留该划分。上述职责收窄与文档意见不冒充可删行收益。没有确认可安全直接删除的 API、必要 SPI 或依赖；不依据单实现接口、函数转发或测试桩数量判断冗余。

Lean already. Ship.
