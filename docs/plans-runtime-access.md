# 统一基建访问与示例对齐实施计划

依据本轮已确认方案实施，不提交，保留已有合服与 World 验收变更。

1. tx 增加服务作用域的统一访问入口，覆盖 Actor、三种 Reader、Writer、Procedure、业务 Redis。CMod 仍由开发者显式声明。Actor 激活、回调、服务启动与后台任务使用接收端本地上下文，不跨 RPC 传递运行时。关闭后入口失效，排水期间保留必要能力。
2. txgen 生成统一 `<Actor>At(ctx,id)`；保留显式 transport 构造作为独立协议测试入口。项目默认不再逐个构造 Client。存储生成入口优先使用绑定的服务上下文，独立生成契约测试仍可显式配置读取器。
3. 两组项目提供统一 db / Procedure 使用方式，收敛 Handler 的基础设施参数。保留真正的业务依赖。Fightroom 内部协议迁入统一 cmd，单次绑定。
4. txgame 补 Legion 查询；txgameslg 补 GlobalGameActor 和等级榜（上传、查询、资料事件），保持有状态事务与写回边界。业务接口对等，不扩展军团管理或结算发奖。
5. 同步生成产物、文档；本地单测、生成编译、真实依赖应用契约回归与作用域访问 benchmark；独立审查。本轮不进行云压测。

验收关注：同进程不同服务/应用的能力不串用；未声明能力明确失败；排水工作仍能访问基建；停止后旧上下文失效；查询/事务/Ref 不自行创建连接；生成标识与消息语义保持；排行榜不在事务失败时发布资料。

## 完成与本地验证

五项改造已完成，未提交。构造函数仍保留业务策略、业务模块与状态缓存；共享资源由服务一次装配，业务不按每次调用创建连接。txgen 的显式 Client 构造与独立 Reader 注册仅保留为工具/隔离测试入口，示例应用不安装全局默认 Reader。

- 全量：tx、txgen、txgame/server、txgameslg/server 的 `go test ./...`；两组 gen module 编译测试通过。
- 客户端：txclient 116 项、txclientslg 127 项测试通过；两端类型检查与构建通过。txclient 本轮未产生源码变化；txclientslg 同步等级榜的生成定义。
- 真实本地依赖：正式 Server 的发送/接收服务能力隔离、激活/排水上下文、停止后拒绝旧作用域；Fightroom 同 Runtime 与不同 Runtime 调用；SLG Role/Legion/Server 放置及 World 查询；Redis 等级榜排序、资料更新及事务失败无副作用；SLG 关闭刷盘与实际 10s 周期写回通过。
- 并发：作用域访问、后台任务、服务关闭和 Actor 接收端上下文的定向 race 回归通过。
- 微基准：本机 Intel Mac 上 `BenchmarkBoundActorAccess` 约 60.41 ns/op、0 B/op、0 allocs/op，仅表示已绑定入口查询成本，不代表端到端吞吐。
- 独立复核后，将 SLG 等级事务移回服务 Procedure 层，消除 rank 对 role 内部包的依赖，并补正式 Server 生命周期测试。

未运行云端压测、完整滚动发布或本轮专门的 JetStream 可靠任务场景；需要环境变量但未启用的外部测试仍为跳过，不算通过。历史容量结果不能代替本次构建回测。
