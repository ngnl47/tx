# GoAkt 运行时收敛评估

本目录只属于 `feature/goakt-runtime-evaluation` 的实验，不是 tx 公共 API，不由 Bee/Ant 装配。独立 go.mod 固定 GoAkt v4.5.4，主模块依赖和生产实现不变。项目间不增加引用关系。

## 目的与范围

验证把邮箱调度、空闲钝化、请求等待交给 GoAkt 后，tx 能否减少需要自行维护的并发机制。不得把“实验代码较少”等同于“完整替换已完成”。

当前最小适配由 `role.go` 和 `drain.go` 组成：

- 以数值 RoleID 作为 Grain 名称；Session 身份只随消息透传，不参与实体寻址。
- 使用官方泛型 `GrainOf`，不使用已标记弃用的工厂激活 API。
- 通过 ActorSystem Extension 注入本进程加载函数；每次激活创建一个 Handler，不把 GD/Store 放进 Actor 运行时。
- 使用内建有界 Grain 邮箱和空闲钝化；每次解析都携带容量和钝化选项。
- 使用 AskGrain 关联请求/响应；复用现有 protobuf Delivery/Result 数据结构，并复制跨调用边界的消息。
- 不维护本地实体 PID 表、自定义邮箱、空闲 timer、响应 Actor 或 pending 请求表。
- 复用 tx 的 `lifecycle.Ingress` 关闭本地调用准入，在进入持久化收尾前执行邮箱完成确认。

## 已验证

四个关键测试各运行三轮通过，另一次 `-race` 完整运行通过：

1. Session 1、2 并发向 RoleID=1001 各次发放 100，总计 20 次，余额为 2000，加载一次。Handler 保留每条请求的 SessionID。
2. 邮箱容量 1，首个处理阻塞时再投递 20 条：一条排队，19 条明确返回 ErrMailboxFull，最终只执行两条。
3. 请求开始执行后等待超时，Handler 仍保持执行，直到受控释放；证明调用等待结束不能代表业务结束。
4. 内建空闲钝化后再次访问相同角色，重新执行加载边界。

另增加三个排水契约测试，全部七个用例以 `-race -count=3` 运行通过：

1. 两个 Call 已超时，但一个 Handler 仍执行、另一个请求仍排队时，关闭返回超时且不得调用持久化收尾；两个业务都完成后排水成功。排水期间新 Call 返回 `lifecycle.ErrDraining`。
2. Grain 的加载函数仍在运行时，不能因当前活跃目录尚未包含它而宣布排水完成。
3. 持久化回调返回错误时，GoAkt 系统仍运行且准入关闭；再次收尾成功后才停止系统，成功关闭后不重复执行收尾。

这里的余额是加载函数创建的实验内存状态；持久化失败采用回调注入，不是生成 GD、Mongo、WAL 或 checkpoint 验收。本轮未启用集群；此前独立进程探针的通过不能替代本适配的跨进程验收。

```sh
cd evaluation/goakt
go test -tags=hashicorpmetrics ./... -v -count=3 -timeout 30s
go test -race -tags=hashicorpmetrics ./... -count=3 -timeout 60s
go test -tags=hashicorpmetrics ./... -run '^$' -bench '^BenchmarkLocalRoleCall$' -benchmem -count=3 -benchtime=300ms
```

在 tx 根目录执行 `go test ./...` 不会遍历本独立模块，必须另行执行上述命令。初始基线 `go test ./modules/actor ./modules/rpc` 和主模块 `go test ./... -count=1` 已通过；外部依赖专项用例是否运行仍由它们各自的环境开关决定。

当前本地 Call 基准（Intel i9-9880H、darwin/amd64、256 B 请求和响应载荷、共享 deadline、三轮各 300ms）为 10,533–10,613 ns/op，4,560–4,562 B/op，67 allocs/op。包含 Grain 解析、消息复制、准入闸门和 Ask 等待，不含网络、业务解码或数据库；尚未与主库四种投递语义对齐，不能据此宣布更快或无回退。

## 单进程排水方案

`Shutdown(ctx, flush)` 按下面的顺序执行：

1. 关闭 Ingress，拒绝新的 Call。
2. 等待已进入 Call 的激活、投递和响应等待阶段返回。此时业务仍可能执行，不能开始 flush。
3. 读取 GoAkt 的进程内 Grain 目录，对每个 Grain 发送普通 FIFO 邮箱确认消息。确认消息不进入业务 Handler，不走优先系统消息。
4. 所有确认返回后才执行项目传入的 flush。排水失败或 flush 失败均保留运行时，允许上层按既定有限次数重试；成功后才调用系统 Stop。

例如 A 在执行奖励操作、B 的奖励消息已排队，二者的 Call 均超时：第 2 步可以结束，但第 3 步必须排在两条奖励消息之后。因此业务完成时间与客户端等待时间不再混同。

该方案没有增加请求结果表，只借助已有入口闸门和 GoAkt 自己的队列顺序。目录遍历和邮箱确认发生在关闭路径，不是每条消息都遍历；当前没有查询或扫描 Redis/etcd。

约束：所有业务必须通过本进程 Call 进入；没有 Grain 定时器、异步继续投递的后台任务、跨节点新入站或 Actor 间再投递。当前未经这些路径的验收，不得接入生产关闭流程。启用集群后 `Grains` 可能查询集群目录，不能照搬此实现为“本地快照”。

确认消息也受普通邮箱容量和 deadline 限制；满队列或超时会使本次关闭失败，不能跳过。活跃实体很多时，逐个确认的关闭耗时需要评估。它不是通过重写热路径邮箱实现的通用排水算法。

## 尚不能得出的结论

- 不支持直接替代现有 Messenger：Tell、TellAccepted、CallFuture、稳定错误码及完整 Result 校验未移植。实验只复用 protobuf 数据字段，尚未声明旧 DeliveryMode/RequestID 的兼容语义。
- 没有自己的 MaxPendingCalls 限制；邮箱容量限制的是目标排队，不是所有调用方等待数量，二者不能混同。
- Runtime.Stop 仅用于实验清理；评估关闭使用 Shutdown。系统停止仍不能作为持久化成功的证明。
- Factory 只是存储衔接位置，不是落库实现。不能在 OnDeactivate 中直接保存后就当作已有失败保留、重试和最终关闭能力。
- 不做 Session 身份鉴权；输入假定来自可信分派层。不会以 SessionID 拆分角色，但角色授权仍由现有项目 SPI 负责。
- 尚未证明 GoAkt 性能不回退。本阶段不做不同 API 范围的行数或吞吐比例承诺。

## 后续步骤

- [x] 建立主库基线与独立评估模块。
- [x] 完成单进程 Role Grain、内建容量/钝化和超时边界验证。
- [x] 验证单进程 Call 范围的排水：不漏算排队消息、未完成激活或已超时业务；持久化回调失败时保留运行时。
- [ ] 将排水验证扩展到跨节点入站、内部再投递及实际持久化生命周期；不得把当前结果当作归属交接验收。
- [ ] 映射四种投递语义及等待容量，计算必须保留的 tx 机制，再判断是否真正收敛。
- [ ] 使用真实 etcd、多进程与项目生成 GD 进行适配验收；世界固定槽位独立评审。
- [ ] 在等价契约下比较基准与净代码量，再决定是否替换主模块。

下一阶段映射四种投递语义和等待容量，同时检查扩展投递入口后如何保持排水边界；不直接迁移存储、MsgQ 或改用自定义 BON。
