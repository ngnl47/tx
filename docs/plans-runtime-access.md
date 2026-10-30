# 基建访问与示例对齐验收

当前装配与执行上下文契约见 [runtime-access](runtime-access.md)，工程约定见 [EC-02 / EC-03](engineering-conventions.md#ec-02)。本文仅保留 2026-10-26 接入批次的验收证据，不作为后续施工计划；后续模块单例与显式 GD 上下文收敛以当前契约为准。

## 本地验证（2026-10-26）

- 全量：tx、txgen、txgame/server、txgameslg/server 的 `go test ./...`；两组 gen module 编译测试通过。
- 客户端：txclient 116 项、txclientslg 127 项测试通过；两端类型检查与构建通过。txclient 本轮未产生源码变化；txclientslg 同步等级榜的生成定义。
- 真实本地依赖：正式 Server 的发送/接收服务能力隔离、激活/排水上下文、停止后拒绝旧作用域；Fightroom 同 Runtime 与不同 Runtime 调用；SLG Role/Legion/Server 放置及 World 查询；Redis 等级榜排序、资料更新及事务失败无副作用；SLG 关闭刷盘与实际 10s 周期写回通过。
- 并发：作用域访问、后台任务、服务关闭和 Actor 接收端上下文的定向 race 回归通过。
- 微基准：本机 Intel Mac 上 `BenchmarkBoundActorAccess` 约 60.41 ns/op、0 B/op、0 allocs/op，仅表示已绑定入口查询成本，不代表端到端吞吐。
- 独立复核后，将 SLG 等级事务移回服务 Procedure 层，消除 rank 对 role 内部包的依赖，并补正式 Server 生命周期测试。

未运行云端压测、完整滚动发布或本轮专门的 JetStream 可靠任务场景；需要环境变量但未启用的外部测试仍为跳过，不算通过。历史容量结果不能代替本次构建回测。
