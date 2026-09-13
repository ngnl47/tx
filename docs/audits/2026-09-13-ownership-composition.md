# I3e：归属装配与交接客户端收敛

本轮继续在 tx `409445f` 上实施，保留 I3d 未提交变更；其余五工程工作区干净。不提交 Git。

## 封装边界

上轮已把规则集中到 L2，但调用方仍要选择权威 SlotStore、构造组件，以及手写 PrepareHandoff 请求和响应身份校验。本轮将这些细节收进所属模块：

- Discovery Runtime 在初始化时装配并持有 SlotOwnership，通过 Ownership() 返回已有能力。使用的是同一运行时的权威存储与成员目录，没有创建新连接。原型 SlotAllocator 接收已装配的 Ownership，不再接收存储与目录自行组装。
- control.NewHandoffClient 复用已有 gRPC 连接并返回 HandoffController 窄契约，统一构造请求、验证成功响应的启动身份。传输失败或缺少有效身份确认都返回错误，调用方不需要访问 protobuf；连接所有权不转移。
- txctl 和正式框架的真实交接测试复用该客户端，交接编排可以直接绑定其 PrepareHandoff 方法，无需手写协议回调。
- NewSlotOwnership、AuthoritativeSlots 和低层 CAS SPI 继续供独立工具及高级组件使用；普通 game Context 不增加存储、连接或交接回调的装配入口。

例如组合根使用 `ownership := discoveryRuntime.Ownership()` 取得已装配能力，并将 `control.NewHandoffClient(conn).PrepareHandoff` 绑定到交接流程。Actor 侧仍只映射身份及执行加载，L2 仍负责决定交接顺序。

本轮不宣称正式实体宿主已迁入：Kind 装配、共享 RPC 注册与启动顺序仍待继续，不通过新增普通业务 API 提前暴露尚未完成的底层接线。

## 本轮验证

- 新测试先因缺少 Ownership 与 NewHandoffClient 编译失败，再验证对应实现。
- 缓存滞后时，Runtime.Ownership 仍读取权威归属；普通 Slots 缓存语义不变。
- 客户端覆盖有效确认、错误启动身份、缺失确认、空身份及未知结果；CLI 和真实 gRPC 两槽交接保留端到端校验。
- tx 全量：1236 通过、11 跳过、0 失败。
- 相关范围三轮 race：120 通过、0 跳过、0 失败，含本机 etcd/NATS/Redis 交接路径。
- 未修改两个服务示例或客户端，本轮未运行其回归。
- 后置 ponytail-review：`Lean already. Ship.`；diff 检查通过。
- 原始日志：`/private/tmp/tx-i3e-20260913/`。

相对当前 HEAD 的累计未提交 Go 变更：非测试新增 192、删除 80，净增 112 行；测试新增 250、删除 13，净增 237 行。包含上轮 L2 基座及本轮封装，不包含已提交的空闲卸载节点。

本轮没有 benchmark，不宣称性能提升；没有新增生产采集或每消息权威查询。完整上下行替换后的性能仍按冻结 I2 基线验收，不测试全 NATS 上行。

## 下一步

继续正式实体宿主和 Kind 装配，直接消费本轮已封装的归属能力；接线时继续检查必要高级能力和普通业务能力的边界，不让服务工厂承担传输协议与运行时生命周期细节。
