# I3d：Discovery L2 集群归属基座

起点 tx `409445f`，六工程工作区干净。本轮仅修改 tx，串行实施，不提交 Git。

## 决策与落地

用户确认将集群归属集中为 L2 基座，后续如有不适合的实际证据，先讨论决策再迭代。边界已写入 AGENTS、设计和 README。

- `modules/discovery/ownership.go` 的 SlotOwnership 接管试验 SlotAllocator 中的首次分配及正常交接规则，复用 SlotStore、SlotRouter、成员目录和既有哈希算法。
- Ensure 只在缺少 assignment 时无副作用预检候选，再 CAS 创建；明确冲突才权威复核，响应未知不改派。已有归属仍检查服务池与 running 状态，不因新成员加入而重算。
- Handoff 统一执行当前代次权威复核、目标容量预检、源实例准备和最终 CAS。prepare 回调必须确认源启动身份已完成全实例排水与持久化，不能把超时或响应身份不匹配视为成功。
- CheckSlotOwner 集中执行加载前权威判断；普通消息仍走同模块 SlotResolver 缓存。没有增加每条消息的 etcd 查询。
- Actor 侧 SlotAllocator 只映射实体到槽位，绑定 NATS 容量/收尾请求并执行后续激活；保持原有 Actor 错误分类。framework 仍执行实际排水与持久化。
- SlotReader、SlotStore、SlotRouter 和控制面显式 CAS SPI 保留。控制面的低级 PromoteSlot 不冒充已执行正常交接编排。

例如世界槽位首次分配到 B 后，新实例 C 就绪不会使它迁移。明确交接到 C 时，L2 先确认 B 全实例收尾，再 CAS 晋升，随后由 Actor 宿主加载。L2 不持有 NATS 连接、不解析 GD，也不新增权威服务进程或归属表。

早期 Placement 哈希实验仍只用于原型对照；正式稳定归属不使用成员变动重算策略。本轮没有把正式 framework Actor 默认实现切换为新运行时，ProtoActor 尚未移除。

## 验证

- 新单测先因缺少 L2 组件及权威校验入口编译失败，再验证预检候选、已有归属、服务池隔离、创建响应未知、CAS 冲突复核、准备失败及交接顺序。
- 定向回归：27 通过、0 跳过、0 失败。
- tx 全量：1230 通过、11 跳过、0 失败。
- 相关范围 `-race -count=3`：81 通过、0 跳过、0 失败。
- 真实 etcd/NATS/Redis 验证复用了已有分配、写回交接、租约丢失和共享入口测试；正式 framework 测试增加第二个真实注册实例，通过 PrepareHandoff gRPC 连续晋升两个槽位并回读 CAS 结果，验证源租约保留至 Stop。
- 未改动两个独立示例和客户端，本轮未运行其回归。
- 原始日志：`/private/tmp/tx-i3d-20260913/`；后置 ponytail-review：`Lean already. Ship.`；`git diff --check` 通过。

非测试 Go 新增 152 行、删除 69 行，净增 83 行；测试新增 184 行。新增行主要用于独立公共组件、执行回调和错误边界，原型中的算法已移走，没有新增依赖或生产采集代码。

本轮没有重跑 benchmark，不宣称性能提升；普通消息缓存解析与传输执行路径保持原实现。后续正式上下行替换仍需对冻结 I2 基线做同口径比较，不测试全 NATS 上行。

## 下一项

正式实体宿主与 Kind 装配消费该基座，再处理共享 RPC 注册和 Discovery/Actor 启动顺序。需要新的职责调整时先给出证据并确认，不通过继续堆叠适配层掩盖边界问题。
