# Actor 归属接线实施计划

> 使用 superpowers:executing-plans 串行执行，不使用子代理，不提交 Git。用户当前授权的是设计细化；下列编码任务均未执行。

**目标：** 先补真实持久化与实例生命周期证据，再按既定评审门槛迁入正式组件，保持两种传输共用实体与归属。

**架构：** 复用 Discovery、SlotStore CAS、SlotResolver、Procedure/writeback 和框架生命周期。试验区承担缺口验证；正式接线由 framework 组装，game 保留业务 factory 和必要高级 SPI。

**技术栈：** 当前 go.mod 固定的 Go、NATS 客户端、etcd、gRPC 及现有存储适配；不升级依赖，不引入 Dapr/GoAkt 集群组件。

**设计：** [Actor 归属组件正式接线设计](../specs/2026-09-13-actor-ownership-design.md)。原始试验门槛与 T4–T6 范围仍见 [总试验计划](2026-09-12-nats-actor-runtime-trial.md)。

## 执行约束

- 每个节点先检查六工程状态、相关 AGENTS 与实际配置；保留未提交变更，交付累计提交建议。
- 不逐消息查 etcd，不在高频入口自动分配、回退传输或重投业务；失败不宣称未执行。
- 先全实例收尾，再逐槽晋升；失败保留资源并有限重试，异常实例处置交给运维。
- 沿用各工程测试资源 ownership 和清理约束；两示例真实集群串行。采集及故障注入只在测试夹具或临时覆盖构建。
- 每节点完成相应验证后执行一次 ponytail-review；不为测试计数、删行数或小幅波动追加工作。

## I1：真实持久化与租约组合验证（建议下一实施节点）

范围仅 tx 试验与测试。已有 `slots_test.go` 的 Redis 自定义快照、`routing_test.go` 的手工成员目录继续作为定向测试，不把它们改名成真实 GD 验收。

文件：新增 `internal/actortrial/ownership_integration_test.go`；按实际缺口修改 `internal/actortrial/slots.go`、`routing_test.go`。参考 `modules/discovery/runtime.go`、`modules/writeback/manager.go`、`tests/writeback_manager_test.go`。本节点不迁移 `modules/actor` 或两个示例。

接口：继续通过 `SlotHostConfig{Store, SlotID, Flush}` 接入收尾，`Flush(context.Context) error` 绑定实际事务写回流程；成员与槽位缓存来自 `discovery.NewRuntime`、`Watch`、`WaitReady`、`Directory`、`Slots`。现有 Runtime.Slots() 只提供缓存读取，不能注入 SlotHost 或用作分配冲突后的权威复核；试验夹具可用相同 etcd 命名空间的 `EtcdRegistry.Slots()` 取得权威存储，不另建归属。正式 Runtime 的权威访问边界列入 I3。

- [ ] 写组合测试：A 的双入口访问同一实体，事务登记脏根；已接受事务结束前不能 ACK/交接，首次写回失败不晋升，重试成功后 B 加载最后状态。使用实际 txn/writeback/存储适配，不以 `Redis.Set(JSON)` 代替管道。
- [ ] 先在 tx 内复用现有 Store/Procedure 测试夹具验证管道；涉及生成 GD 的完整契约留在 I4 的独立示例验收，报告明确区分，禁止 tx import 示例的 internal 包。
- [ ] 用真实 Discovery.Runtime 取代组合夹具的手工实例 ID/目录：快照就绪前不发布 running，B 加入不改已存在 assignment；旧端点复用拒绝旧启动身份。租约失效后源方停止接纳新业务，不据此自动晋升。
- [ ] 注入第一次写回失败及准备响应丢失；检查待刷根、writer、有效 context 和控制连接仍可用；成功阶段重试不重复执行退出回调。租约失败与正常排水失败分开断言。
- [ ] 运行测试，定位确实缺少的接线；只补试验区组合，复用现有同步边界。若必须改正式 API/关闭语义才可继续，记录明确缺口交给 I3，不越过 T4 门槛。
- [ ] 运行相关无外部依赖测试及显式启用的真实依赖测试；真实依赖定向 race 单列。输出 pass/skip/fail、依赖版本和清理记录，不复用历史结果。

命令入口（在 tx 下执行；真实端点从当前环境核对后设置，未设置时产生的 skip 不算通过）：

```bash
go test ./internal/actortrial ./internal/framework ./modules/discovery ./component ./tests -count=1
go test ./internal/actortrial -run 'TestOwnershipIntegration|TestSlotRollingHandoff|TestSharedRouting' -count=1 -v
go test -race ./internal/actortrial -run 'TestOwnershipIntegration|TestSlotRollingHandoff|TestSharedRouting' -count=1
```

`TestOwnershipIntegration` 是拟新增测试前缀。现有真实依赖开关为 `TX_ACTORTRIAL_NATS_URL`、`TX_ACTORTRIAL_ETCD_ENDPOINT`、`TX_ACTORTRIAL_REDIS_ADDR`；新增存储场景先复用工程既有测试配置，需新开关时说明理由。

完成标准：真实租约与实际事务持久化组合成立，明确哪些仍是 Store 夹具而非示例生成 GD；失败不能通过关闭依赖或重新选 owner 掩盖。此节点不宣称正式 framework 已接线。

## I2：完整上行与业务负载采用评审

文件：复用 `internal/actortrial/benchmark_test.go`、`routing_test.go` 与现有隔离测量驱动，新增结果报告到 `docs/audits/`；只按测量需要增加测试夹具，不扩展生产统计。

- [ ] 先固定载荷、身份数、多 Session 分布、业务处理与持久化完成口径；对照当前 ProtoActor、Agent 直连 gRPC + Actor NATS、上行也经 NATS。同环境比较成功完成吞吐、固定到达率 P99、错误、CPU、内存和网络字节。
- [ ] 代表负载含 Procedure/GD，不能只用空回显或单向 Pub/Sub；计数/profile 单独构建，禁止混入吞吐样本。
- [ ] 列出调用端、业务实例与 broker 每段字节量，按实际部署位置另算计费流量；没有云区域和同区/跨区拓扑时仅给公式及情景，不填写虚构账单。
- [ ] 用结果评审 T4。原 10% 筛选线继续有效；若建议改为明确业务容量/P99/成本门槛，给出数字与余量后由用户确认，不把已确认传输方向解释为自动通过性能验收。

完成标准：给出可采用或应保留现状的证据与门槛决策；不继续收益很小的封包或发送协程微优化。

## I3：正式组件与框架接线（T4 门槛通过后）

主要文件：`modules/actor/{runtime,kind,delivery}.go` 及迁入的运行时/适配文件，`internal/framework/{assembly,cmod_actor,cmod_discovery,cmod_rpc,context_access,shutdown,drain}.go`，`modules/control/lifecycle.go`、`modules/control/proto/control.proto`；生成物经原生成入口更新。

- [ ] 先核对设计中的 PID、Props、Context、ActorSystem 实际调用清单，明确新 Identity、Kind factory 和生命周期能力；必要能力无替代时停在该项，不通过删 SPI 消除编译错误。
- [ ] 为宿主装配提供权威槽位访问，加载前只注入 SlotReader；普通 CModSlots/SlotResolver 继续缓存读取，不为此对业务暴露 etcd 客户端或再创建一套连接。
- [ ] 加载前权威校验在唯一实体宿主绑定；复用共享 RPC Host 注册 Actor Call，检查同进程所选服务的注册组合，确保 DeliveryService 仅注册一次；两种入口仍共享同一 Runtime。
- [ ] 调整启动顺序：绑定直连端点 → Discovery 启动身份与 Watch 就绪 → 装配 Actor 入口 → 服务与控制面就绪 → 标记 running。移除对旧 ActorEndpoint 的启动依赖，不增加绕环的可空中间对象供业务处理。
- [ ] 先写框架生命周期测试，再实现 PrepareHandoff 与 Stop 共享收尾进度；RPC/NATS/租约的最终关闭留在 Stop。保留 CMod 逆序收尾、失败阶段重试、fatal 身份丢失分支；当前 StateDrained 不冒充持久化成功。
- [ ] 验证 PrepareHandoff/Stop 并发、持久化失败重试、旧启动身份、成功回复丢失，以及多个槽位部分晋升；业务退出 RPC 失败不能否决卸载。
- [ ] 核对 world 子协程完成边界：若处理异步返回，实际写入任务必须可被生命周期等待；在此之前不能把 Receive 返回等同于整个业务完成。
- [ ] 运行相关单测、真实依赖回归与必要 race，再更新 README 的正式 API 和 design.md；不在本文设计完成时提前改写公开 API 文档。

完成标准：正式宿主取得框架所有权，原型连接内不再暗藏共享准入；维护成本可解释，无第二份归属、写回管理器或永久后端开关。

## I4：两个独立示例与依赖收敛

修改 txgame/server 的 `services/{game,agent,internal/actorcmd}`、txgameslg/server 的 `services/{slggame,slgworld,agent,internal/actorcmd}` 及两个工程已有 `internal/writeback` 接线；配置、go.mod、README 与集群测试按实际依赖调整。

- [ ] 普通实体稳定分区先确定编码与数量；世界维持独立槽位，演示多个世界分布在独立 slgworld 实例池，新增实例不自动搬走已存在归属。
- [ ] 先 txgame 角色多 Session，再 txgameslg 世界与跨世界侦查；factory 保持业务加载与生命周期声明，通用装配由 tx 提供。
- [ ] 沿用真实生成 GD 与默认写回组件，验证 Redis/Mongo/Fusion 各自既定持久化边界、失败收尾和有限关闭重试；不修改客户端协议，不将 Fusion 改为同步 Mongo 落盘。
- [ ] 完成 tx、两个 server 的 `go test ./... -count=1`，两客户端的 `npm test`、`npm run build` 及各自 README 指定的真实集群验收；报告三仓版本矩阵。
- [ ] 仅在必要 SPI 和行为有承接、原型对照证据可复现后删除被替代 ProtoActor 生产路径与依赖；若测试仍依赖它，先明确测试基线的保存方式，不宣称已完全移除。

此后才按总试验计划进入 JetStream。etcd 替代、MsgQ 传输整合、运行中单实体热迁移均不随本计划实施。
