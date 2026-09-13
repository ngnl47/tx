# I3m：Core NATS 私有传输迁入

在 tx `021bb2f` 及此前未提交的 1024 分区、稳定业务映射变更上继续实施。本轮仅修改 tx，不提交 Git。

## 迁入与职责

`internal/actorrpc` 新增 NATSTransport 与 RoutedCaller，复用 actorhost、已有信封和验证函数。NATS 与直连入口共享实体宿主；映射、缓存解析和业务消息不要求业务拼订阅主题。

Core NATS 四种语义保留：Tell 本地投递；TellAccepted 确认到达当前 broker，不要求目标在线；CallFuture 与 Call 等待同一执行结果。请求等待有界，取消、发送失败和关闭连接复用 pending 删除边界；断线不缓存 RPC 后续重发。Probe/EnsureActivation/RetryActivation 仍走宿主元数据控制入口。

正式适配器不再带“排水 + 业务写回”的 Drain 编排，只提供 Flush 和 Close。框架应先关闸、排空并完成必要写回，再 Flush，成功后关闭连接；Flush 失败保留连接。Close 同步释放 pending，也适用于启动失败回滚。此分离没有改变 framework 的现行 ProtoActor 后端。

## 比较基线隔离

原 actortrial 的五个普通 Go 文件改为 `*_baseline_test.go`，保留旧 NATS、试验槽位控制和 Agent 适配，仅供回归与比较。新生产需求不再追加到这些夹具；共享本地宿主仍只有一个实现。Go 包检查确认 actortrial 的 GoFiles 为空，actorrpc 的普通源码为 direct、nats、protocol、routing。

保留试验传输源码是为了维持合批 overlay 和完整上下行比较，不是第二个生产后端。request_batch/build.py 已改为读取测试专用源文件；完整上行 build.py 入口不变。既有 I2 原始数据、二进制与归档没有重置，后续仍只运行 proto/hybrid。

试验 SlotHost/SlotAllocator 控制接线没有直接复制到正式传输，避免与 framework 已有 PrepareHandoff、持久化收尾和 L2 SlotOwnership 重复编排。正式 Kind 工厂、容量回调、启动顺序和多世界装配仍需下一节点完成。

## 验证

- 新适配器测试先因缺少 ConnectNATS 而失败，再通过。迁移时修正了类型限定替换造成的编译错误；后续双向互通验证保持原有线上头字段与载荷。
- tx 最终全量：1252 通过、11 跳过、0 失败，启用现有 NATS/etcd/Redis 集成开关。
- 覆盖四种调用语义、未加载探测、直连入口与 NATS 共用实体状态、准入关闭拒绝、无接收者，以及 pending 上限、取消和连接关闭后的回收。
- 新旧试验传输双向互通；默认分区与自定义战区用例也消费迁入的 RoutedCaller，核验归属和状态未变化。
- 最终 NATS、pending、互通与映射 race：8 通过、0 跳过、0 失败。
- 隔离合批 overlay 回归：13 通过、0 失败。
- 完整上行对照 darwin/amd64 二进制构建成功；仅构建，没有运行全 NATS 上行或新吞吐采样。
- 原始日志和构建产物：`/private/tmp/tx-nats-extract-20260913/`；红绿日志：`/private/tmp/tx-nats-extract-{red,green}.log`。

本轮没有修改示例或客户端，未重跑它们及独立 cluster-e2e。NATS 私有实现 351 行、缓存路由 31 行；旧源码移入测试而非删除，不能把普通 Go 文件减少当作瘦身收益。本轮没有 benchmark，不宣称性能改善或完成正式替换后的 I2 验收。

下一项消费私有传输和宿主，接入正式 Kind、生命周期及按世界激活的装配；必要 SPI 与原有性能基线继续保留。

后置 ponytail-review：Lean already. Ship. 正式传输不承担排水/写回，试验副本限定测试构建并保留退出条件；没有新增生产后端开关。累计差异格式检查通过。
