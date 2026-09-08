# GoAkt 运行时收敛评估

本目录只属于 `feature/goakt-runtime-evaluation` 的实验，不是 tx 公共 API，不由 Bee/Ant 装配。独立 go.mod 固定 GoAkt v4.5.4，主模块依赖和生产实现不变。项目间不增加引用关系。

## 目的与范围

验证把邮箱调度、空闲钝化、请求等待交给 GoAkt 后，tx 能否减少需要自行维护的并发机制。不得把“实验代码较少”等同于“完整替换已完成”。

首阶段 `role.go` 共 95 行：

- 以数值 RoleID 作为 Grain 名称；Session 身份只随消息透传，不参与实体寻址。
- 使用官方泛型 `GrainOf`，不使用已标记弃用的工厂激活 API。
- 通过 ActorSystem Extension 注入本进程加载函数；每次激活创建一个 Handler，不把 GD/Store 放进 Actor 运行时。
- 使用内建有界 Grain 邮箱和空闲钝化；每次解析都携带容量和钝化选项。
- 使用 AskGrain 关联请求/响应；复用现有 protobuf Delivery/Result 数据结构，并复制跨调用边界的消息。
- 不维护本地实体 PID 表、自定义邮箱、空闲 timer、响应 Actor 或 pending 请求表。

## 已验证

四个关键测试各运行三轮通过，另一次 `-race` 完整运行通过：

1. Session 1、2 并发向 RoleID=1001 各次发放 100，总计 20 次，余额为 2000，加载一次。Handler 保留每条请求的 SessionID。
2. 邮箱容量 1，首个处理阻塞时再投递 20 条：一条排队，19 条明确返回 ErrMailboxFull，最终只执行两条。
3. 请求开始执行后等待超时，Handler 仍保持执行，直到受控释放；证明调用等待结束不能代表业务结束。
4. 内建空闲钝化后再次访问相同角色，重新执行加载边界。

这里的余额是加载函数创建的实验内存状态，不是生成 GD、Mongo、WAL 或 checkpoint 验收。本轮未启用集群；此前独立进程探针的通过不能替代本适配的跨进程验收。

```sh
cd evaluation/goakt
go test -tags=hashicorpmetrics ./... -v -count=3 -timeout 30s
go test -race -tags=hashicorpmetrics ./... -count=1 -timeout 60s
```

在 tx 根目录执行 `go test ./...` 不会遍历本独立模块，必须另行执行上述命令。初始基线 `go test ./modules/actor ./modules/rpc` 和主模块 `go test ./... -count=1` 已通过；外部依赖专项用例是否运行仍由它们各自的环境开关决定。

## 尚不能得出的结论

- 不支持直接替代现有 Messenger：Tell、TellAccepted、CallFuture、稳定错误码及完整 Result 校验未移植。实验只复用 protobuf 数据字段，尚未声明旧 DeliveryMode/RequestID 的兼容语义。
- 没有自己的 MaxPendingCalls 限制；邮箱容量限制的是目标排队，不是所有调用方等待数量，二者不能混同。
- Runtime.Stop 仅用于实验清理，不是严格排水入口。系统停止不能作为持久化成功的证明。
- Factory 只是存储衔接位置，不是落库实现。不能在 OnDeactivate 中直接保存后就当作已有失败保留、重试和最终关闭能力。
- 不做 Session 身份鉴权；输入假定来自可信分派层。不会以 SessionID 拆分角色，但角色授权仍由现有项目 SPI 负责。
- 尚未证明 GoAkt 性能不回退。本阶段不做不同 API 范围的行数或吞吐比例承诺。

## 后续步骤

- [x] 建立主库基线与独立评估模块。
- [x] 完成单进程 Role Grain、内建容量/钝化和超时边界验证。
- [ ] 验证严格排水：入口关闭后，不漏算排队消息或已超时但仍执行的业务；持久化失败时不得过早释放归属与资源。
- [ ] 映射四种投递语义及等待容量，计算必须保留的 tx 机制，再判断是否真正收敛。
- [ ] 使用真实 etcd、多进程与项目生成 GD 进行适配验收；世界固定槽位独立评审。
- [ ] 在等价契约下比较基准与净代码量，再决定是否替换主模块。

下一阶段先验证排水边界，不直接迁移存储、MsgQ 或改用自定义 BON。
