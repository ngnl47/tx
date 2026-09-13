# I3g：私有 Actor 直连适配迁入

起点 tx `0b39eb7`，六工程干净。本轮仅修改 tx，不提交 Git，不累积已提交的 I3d～I3f。

## 实现与边界

当前正式 Kind 工厂仍返回 ProtoActor 原生 Actor，新宿主不能直接执行它。先将已经独立的直连入口从试验包迁到 `internal/actorrpc`，为后续组合根接线消除试验包依赖。

- DirectTransport 继续使用共享 RPC Sender 的连接、凭据及实例解析；NewDirectReceiver 继续注册到已有 DeliveryService，交给同一个 actorhost.Runtime 执行。
- 目标/期限和结果信封校验一起迁入。NATS 试验通过别名调用同一份规则，没有复制校验、协议或宿主实现。
- 不新增 listener、协议、后台任务、业务配置或自动重投。错误文本和分类保持原样；实例身份验证仍在入队前，权威归属校验仍只在激活时执行。
- 新增真实 gRPC 测试：role 与 world 共用一个 Host 和一次 DeliveryService 注册，同名 key 的计数独立递增，宿主排水同时拒绝两种实体请求。该测试不依赖 NATS 或试验包。

例如同一实例收到 role/51 和 world/51 的 Call，直连适配只解析身份并提交共享宿主，不为每种 Kind 再注册一个 gRPC 服务。后续由框架组合根接入此入口，game 仍只声明类型和业务工厂。

正式框架仍使用 ProtoActor。本轮没有实现新的 Kind 公共 API，也没有完成默认运行时切换。

## 本轮验证

- 新测试先在实现迁入前因缺少入口编译失败；修正测试载荷数值类型和缺失服务名夹具后运行回归。
- tx 全量：1237 通过、11 跳过、0 失败；显式配置本地 etcd/NATS/Redis 端点。
- 定向 race：16 通过、0 跳过、0 失败，覆盖直连、共享路由和真实归属集成。
- 隔离 Actor RPC 合批 overlay：13 通过、0 失败。未测试已排除的全 NATS 上行。
- 其他五工程未修改、未运行测试。
- 验证后 ponytail-review：`Lean already. Ship.`；`git diff --check` 通过。
- 原始日志、从 HEAD 导出的对照源码及前后测试二进制：`/private/tmp/tx-i3g-20260913/`。

BenchmarkDirectActorCall 使用独立二进制、1 KiB、GOMAXPROCS=4、前后交替各五轮、每轮 1 秒。耗时中位数 149303 → 148294 ns/op（-0.68%）；17501 → 17534 B/op（+0.19%）；198 allocs/op 不变。样本范围分别为 144162～174010 和 144052～167655 ns/op，未见明显退化，不宣称吞吐提升。这是回环直连 Call 的局部回测，不代替冻结 I2 完整上下行验收。

本轮 Go 非测试净增 21 行，测试新增 78 行；文件迁移不计作删除收益。没有生产路径采集逻辑。

## 下一步

迁移正式 Kind 工厂的业务行为和生命周期契约，再由 framework 使用私有宿主及本轮直连入口；同时检查必要高级能力、共享 RPC 唯一注册与启动顺序。该接线不得通过模拟整套 ProtoActor Context 或永久双后端开关实现。
