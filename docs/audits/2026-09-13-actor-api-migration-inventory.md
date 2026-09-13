# I3c：Actor API 迁入核对与空闲卸载承接

起点 tx `1cf224c`，六工程干净。本轮串行实施、不提交 Git；核对当前代码和示例配置，不依据历史跑分确认新行为。

## 实际调用与承接边界

| 当前使用点 | 当前用途 | 迁入方式与状态 |
| --- | --- | --- |
| `modules/agent/actor_dispatcher.go`、两示例 `services/agent/{actor_dispatcher,*resolver}.go` | 生成命令路由到 PID，Call 后检查 ProcessedUpSeq | 改为 Identity 和直连目标解析；保持可信会话信息、Seq/ACK 校验。尚未接入正式路径 |
| 两示例 `services/internal/actorcmd/command_actor.go` | producer、Context.Message、LifecycleContext、Respond | factory 返回 Behavior，Receive 接收信封并返回结果；BON 校验、业务执行、下行发布及完成 ACK 的顺序必须保留。无需在业务里模拟 ProtoActor Context |
| txgameslg `services/slggame/world_caller.go` | FixedKindPID 后调用 world | 统一 Identity、槽位缓存解析及 NATS Call；不在 game 侧拼 subject |
| `modules/actor/kind.go` | NewKind、NewFixedKind、AdaptKind 声明 Kind | 保留 Kind factory、可选策略与必要高级入口；原生 ProtoActor 类型无法源兼容，不能直接删除整个扩展能力 |
| `Runtime.ActorSystem()`、FixedPID、内部 Props | 底层本地投递、固定宿主、响应、邮箱和生命周期装配 | 当前两示例非测试代码没有直接访问 ActorSystem 或 Props。普通需求由本地 Post、After、Deactivate、返回结果承接；高级按实例调用继续携带启动身份和加载准入。尚未删除这些公开接口 |
| 两示例 assets 的 idlePassivation | txgame role 5m；txgameslg role 5m、world 30m | 新原型缺少这项既有能力，本轮新增可选 Behavior.IdlePassivation。后续 Kind/config 装配须保留原有覆盖规则和校验 |
| CModProcedureWriteBack、Advanced.Slots、SlotStore/SlotRouter | 默认持久化和高级 CAS | 原样保留职责，不因 Actor 迁移再造写回或 owner 表 |

本轮查询未在 txgameslg 的 `services/slgworld` 非测试 Go 文件中发现直接启动子 goroutine 的代码；现有命令适配器同步等待 handler 返回，再发布下行及完成确认。这只说明当前示例，不证明未来并行 world 的完成边界。若业务将工作交给协程池，必须等待实际完成才能确认请求，且实际任务必须纳入排水；本轮不提前新增协程池或将 Receive 返回等同于异步工作的完成。

## 空闲卸载实现

Behavior 的零值不启用自动卸载，显式 Deactivate 和业务定时消息继续可用。启用时，每个激活复用一个标准库 Timer；从初始化完成及每次处理完成后计时，不为每条消息创建计时器或 goroutine。

到期仅关闭准入，已入队消息继续处理，沿既有退出回调完成清理，清理结束前不删除激活占位。结束房间是否允许重新加载仍由业务 factory 判断，不把空闲卸载解释为永久删除。

例如角色设置 `IdlePassivation: 5 * time.Minute`；一个请求执行较久不会中途卸载，执行完成后重新计算五分钟。世界也可以保持零值，由业务明确触发结束。

## 验证与性能

- 新增测试先因缺少 IdlePassivation 字段编译失败，再验证忙碌期间、空闲重置、清理占位、重新激活和旧句柄拒绝；零值不自动卸载。
- tx 全量：1225 通过、11 跳过、0 失败。随后增强清理阻塞断言，相关激活、卸载、真实依赖交接和共享入口 `-race -count=3`：57 通过、0 跳过、0 失败。
- 未修改两个独立示例或客户端，本轮没有运行它们的回归。
- 非测试 Go 新增 30 行、删除 1 行，净增 29 行；新增测试 115 行。没有新增依赖、后台采集或生产路径统计。

基准使用修改前后二进制，GOMAXPROCS=4、每轮 1s，前后交替各五轮，运行既有 BenchmarkRuntimeCall。未启用空闲卸载时，中位数由 1898 变为 1933 ns/op（+1.84%），均为 432 B/op、6 allocs/op。范围分别为 1877～1916 和 1878～1959 ns/op，样本重叠；暂未见明显局部偏移，不推断整体性能提升。它不代替 I2 完整上下行基线，不运行全 NATS 上行。启用计时器策略的吞吐本轮未单独测量。

原始日志、前后二进制、SHA256 与机器信息：`/private/tmp/tx-i3c-20260913/`。二进制编译时曾遇 Go 缓存沙箱拒绝，获准重跑后成功。

后置 ponytail-review：Lean already. Ship. 格式及 diff 检查通过。

## 下一步

继续迁入正式实体宿主及 Kind 装配，再处理共享 RPC 注册、Discovery/Actor 启动顺序与示例适配。本轮没有替换正式 ProtoActor，没有完成 I3 整体验收。
