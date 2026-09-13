# I3k：普通实体固定分区布局

用户确认普通 Kind 固定 1024 分区，世界保留独立槽位，并确认多世界共享实例属于后续正式迁移目标。本轮基于 tx `021bb2f` 和未提交的切换方案继续实施，不提交 Git。

## 实现

`internal/actorhost.PartitionSlot` 唯一实现 v1 布局：原始 Key 字节通过已有 xxhash64 取模 1024，槽位名为 `actor-v1.<RawURLEncoding(kind)>.p<分区号>`。没有新增依赖、可热改的分区数量配置或逐实体归属表。

宿主 BindSlots、首次分配 SlotAllocator 和普通 RoutedCaller 在没有提供自定义映射时共用这个函数。显式映射仍保留给世界等独立槽位。映射只确定持久化逻辑槽位；L2 SlotOwnership 继续决定物理归属，普通请求仍读取 Watch 缓存，首次加载执行权威校验。

例如 role 的 Key `8` 和 `26` 都落入 `actor-v1.cm9sZQ.p100`，但它们仍有独立状态、邮箱和生命周期。实例增加不会改变该 assignment；不能因为一个角色未加载，就绕过槽位归属把它分配到另一实例。

普通正式 Kind 工厂和 framework 仍使用 ProtoActor，本轮没有切换生产宿主。世界现有 Handler/加载器仍绑定单个 logicalSlotID，不能只删除启动参数就声称支持多世界；逐世界激活、归属校验和容量承接留在正式接线节点一起完成。

## 验证

- 固定向量测试先因缺少布局函数而失败，再验证原始 Key 字节、Kind 命名空间及已知同分区键。向量同时保护前导零、空格和中文 Key 的持久化编码。
- 真实 etcd、NATS、gRPC 测试验证两实体共用 assignment 而状态独立；直连和 NATS 进入同一实体；新增成员后归属不漂移；错误实例加载被拒绝。使用现有 Watch 缓存夹具及有 ownership marker 的隔离测试范围。
- tx 全量：1245 通过、11 跳过、0 失败，启用既有本机 NATS/etcd/Redis 开关。
- 分区及原有跨传输交接 race：3 通过、0 跳过、0 失败。
- 原始日志：`/private/tmp/tx-partition-20260913/`；固定向量红绿日志：`/private/tmp/tx-partition-{red,green}.log`。

本轮只改 tx，其他五工程未修改；未重跑示例、客户端及独立 cluster-e2e。非测试 Go 净增 30 行，新增测试 105 行。没有进行 benchmark，不声明性能收益或完成 I2 整体性能验收；采集逻辑未加入生产路径。

后置 ponytail-review：Lean already. Ship. 保留固定持久化向量和真实归属测试，没有再抽象一层默认映射工厂。差异格式检查通过。

下一步按切换方案迁出 NATS 传输所需逻辑，再将正式 Kind、共享宿主及多世界装配连续接线；本轮完成的是稳定布局及其现有接线路径验证。
