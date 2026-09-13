# I3b：权威归属读取与入口装配分离

实施起点：`f082cea`。六个工程工作区均干净，本轮仅修改 tx，不提交 Git。

## 原因与实现

原型原先在 `ConnectSlotHost` 中包装业务 factory，直连入口的归属保护依赖 NATS 连接初始化的副作用。现在由组合根在开放入口前调用 `Runtime.BindSlots`，共享运行时在首次加载前读取权威归属；NATS 连接只负责通信和现有最终收尾。

`discovery.Runtime.AuthoritativeSlots()` 复用已有持久存储和连接，提供低频首次分配、加载校验和交接复核。`Slots()` 继续读取 Watch 缓存。真实归属集成夹具不再向每个 peer 传入额外 registry 的 SlotStore，改用 peer 自身 Discovery Runtime 的权威存储。控制测试的外部 registry 仍用于创建和检查 assignment。

例如 `world-1` 已从 old 晋升到 new，old 即使收到直连请求，也会在工厂加载前返回 `ErrNotOwner`；new 的连续请求共用激活，只做一次权威读取。已激活实体的交接仍依赖源实例先排水和持久化，不把首次加载检查解释为每条消息的 fencing。

本轮没有迁入正式 framework 实体宿主，也没有移除 ProtoActor。`BindSlots` 仍位于试验包，须在开放任何入口前装配；后续由 framework 统一承担，业务不会逐入口手工绑定。

## 本轮验证

- 红灯：新增测试分别因缺少 `AuthoritativeSlots` 和 `BindSlots` 编译失败。
- 首次本地直连验证被沙箱禁止监听端口；随后在获准的测试环境重跑。
- tx 全量：1223 通过、11 跳过、0 失败。
- 相关归属、共享入口和真实交接回归：18 通过、0 跳过、0 失败。
- 上述范围 `-race -count=3`：54 通过、0 跳过、0 失败。
- 使用本机 etcd、NATS、Redis；覆盖真实写回交接及租约丢失。未运行两个独立示例和客户端回归，本轮没有改动这些工程或其调用的现有接口。
- 后置 `ponytail-review`：`Lean already. Ship.`；`git diff --check` 通过。
- 原始结果与命令日志：`/private/tmp/tx-i3b-20260913/`。

未重跑 benchmark；本轮移动加载前既有检查，测试确认已激活消息不重复读取权威存储，不据此宣称性能提升。完整上下行仍以冻结 I2 基线为准，待实际替换正式消息路径后同口径比较；不测试全 NATS 上行。

非测试 Go：新增 42 行、删除 28 行，净增 14 行；测试 Go：新增 80 行、删除 9 行，净增 71 行。没有新增依赖或生成文件。

## 下一步

继续将实体宿主与必要高级 API/SPI 接入 framework，处理共享 DeliveryService 注册及启动顺序；本节点不代表 I3 整体完成。
