# I3j：公开实体行为与本地控制契约

本轮在 tx `564e6e1` 及上一轮未提交的命令执行解耦上继续，未提交 Git。txgen 和两客户端保持干净。

## 实施取舍

`modules/actor` 公开 Identity、Behavior、BehaviorFactory、Context，直接别名到已有私有宿主类型。业务声明不包含 PID、Props、NATS subject 或物理实例地址；宿主继续独立负责准入、邮箱和生命周期，没有复制一套事件实现。

当前公开 Kind 工厂仍返回 ProtoActor Actor。它必须与正式 framework 宿主接线一起切换，因此本轮没有加入只能声明却无法执行的新 Kind 构造入口，也没有临时把两个邮箱串起来。新行为契约目前由私有宿主及直连回归使用，尚不是现行 NewKind 的可用参数。

Context 分开表达两个寿命：嵌入的标准 Context 在当前回调结束时取消；Post、After、Deactivate 则绑定本次激活，卸载后旧句柄不能投递到重新加载的实体。Post 返回 LocalFuture 值句柄，只开放 Await/Done；内部 Future 的 Complete 不再沿该业务入口外露。调用方不能在同一实体回调内等待自己的 Post，否则邮箱无法继续处理该消息。

例如房间回调可以用 `ctx.After(65*time.Second, roomEnded{})` 把结束事件放入本地邮箱，再在处理该事件时调用 `ctx.Deactivate()`。卸载前事件仍有机会调用 RPC，失败只记录错误，不否决卸载。既有宿主生命周期测试继续验证这些顺序，不重复新增一套策略。

## 验证

新增公开契约测试先因类型未定义而编译失败，再通过；初次执行因 Go 缓存权限阻断，获准后完成红绿验证。

- tx 全量：1243 通过、11 跳过、0 失败；启用本机 NATS、etcd、Redis 的既有集成开关。
- 相关公开契约、激活/卸载、共享直连与 NATS 准入 race：8 通过、0 跳过、0 失败。
- 两个示例重新全量验证：txgame/server 237 通过、3 跳过；txgameslg/server 332 通过、5 跳过，均无失败。未运行客户端和独立 cluster-e2e。
- 直连原有多 Kind 同 key 状态隔离与统一排水测试改用公开行为类型；新增本地控制测试覆盖回调结束后的 Post、结果等待及旧句柄拒绝。

原始日志在 `/private/tmp/tx-behavior-20260913/`，红绿验证日志在 `/private/tmp/tx-behavior-{red,green}.log`。本轮没有性能优化目标，未运行 benchmark，不声明吞吐或分配改善。尚未执行正式宿主替换后的 I2 完整链路验收。

本轮 tx 非测试 Go 新增 50 行、删除 3 行，净增 47 行；测试净增 53 行。累计此前两个示例的业务代码净减 36 行，三个工程未提交的非测试 Go 合计净增 11 行。必要的只读能力边界不按删行收益评价。

下一步将 Kind 工厂和正式实体宿主一起接线，保留现有共享 gRPC、发现归属、排水与持久化顺序；不再添加独立的过渡工厂或双后端配置。

后置 ponytail-review：Lean already. Ship. 别名复用唯一实现，LocalFuture 的值包装用于隐藏完成权限，未增加通用适配层或第二套生命周期。累计差异格式检查通过。
