# I3h：Kind 规格与后端装配分离

起点 tx `1d63264`，上轮拓扑已提交。本轮只修改 tx，不提交 Git。

## 原因与实现

普通 NewKind/NewFixedKind 在声明时就创建 Props 和 Cluster Kind，规格层提前绑定后端对象。现只保留名称、原始业务工厂、放置类型和空闲卸载选项，Runtime 装配时创建后端对象及生命周期包装。工厂不在声明或装配阶段提前执行。

业务声明无需修改，例如 `MustKind("role", producer, WithIdlePassivation(5*time.Minute))` 仍可使用；显式选项优先于配置默认值。AdaptKind 保留原生 Props 与 Strategy，不为迁移删除高级入口。固定宿主不附加虚拟集群初始化逻辑。

核对当前 go.mod 的 ProtoActor 2024 版本后确认，cluster.NewKind 会追加初始化中间件。回归在本轮中间实现上复现原生适配收到两次 ClusterInit；现只对普通虚拟 Kind 追加一次，原生适配复用已有中间件，之后统一克隆并加入邮箱及准入适配。运行时默认空闲卸载的普通虚拟 Kind 也保留集群初始化能力。

本轮仍使用原生 Actor 工厂，没有模拟 ProtoActor Context、新增后端开关或提前切换默认宿主。下一步承接业务 Behavior/Context 契约，再接入私有实体宿主与共享 RPC。

## 本轮验证

- 新测试先复现普通 Kind 提前创建集群对象；迁移后验证可重复装配但不调用业务工厂。
- 原生策略保留、Props 克隆、原生适配不支持追加默认空闲策略的既有约束继续验证。
- 完善本地集群/身份扩展夹具后，实际激活测试复现重复 ClusterInit；修正后普通与原生适配均为一次。
- 最终 tx 全量：1242 通过、11 跳过、0 失败，显式配置本机 etcd/NATS/Redis 测试端点。
- 最终定向 race：12 通过、0 跳过、0 失败。
- txgame/server：236 通过、3 跳过；txgameslg/server：331 通过、5 跳过，均无失败。未启用的集成测试不算通过。
- txgen 与两个客户端无修改，本轮未运行其测试。
- 验证后 ponytail-review：`Lean already. Ship.`；`git diff --check` 通过，保留必要原生适配与生命周期测试。
- 原始日志：`/private/tmp/tx-i3h-20260913/`；final 日志对应最后实现，早期日志仅用于定位。

非测试 Go 新增 42、删除 33，净增 9 行；测试净增 117 行。本轮目标是装配边界，不宣称瘦身或性能提升。没有新增性能采集；未运行 benchmark，完整上下行性能仍须在正式替换时对照冻结 I2 基线验收。
