<p align="center">
  <img src="icon/tx-teddy-avatar.png" alt="tx 项目图标" width="160" height="160">
</p>

# tx

`tx` 是 Go 游戏后端运行时基础库，提供 Actor 与集群归属、客户端上下行、GD 存取、事务、缓存和优雅关闭。项目选择所需能力、实现业务协议与策略，再组合成自己的服务。无状态示例使用 Bee，有状态示例使用 Ant；它们共享装配内核，不是两套通信框架。

## 第一次阅读

| 你要做什么 | 从哪里开始 |
| --- | --- |
| 理解进程、子服务、Actor、GD 之间的关系 | [设计总览](docs/design.md) |
| 写无状态游戏业务 | [txgame](../txgame/README.md) 与其 [设计](../txgame/docs/design.md) |
| 写有状态 SLG/MMO 或独占大世界 | [txgameslg](../txgameslg/README.md) 与其 [设计](../txgameslg/docs/design.md) |
| 定义 GD、命令、配置并生成代码 | [txgen](../txgen/README.md)、[txconfig](../txconfig/README.md) |
| 查公共 API、配置字段与低层示例 | [API 参考](docs/api-reference.md) |
| 检查实现是否符合团队约定 | [工程约定](docs/engineering-conventions.md)、[审计标准](docs/architecture_audit_standard.md) |
| 确认已完成能力与下一阶段验收 | [当前计划](docs/IMPLEMENTATION_PLAN.md) |

先选择一组应用工程学习即可。txgame 和 txgameslg 分别包含完整的业务接入示例，不需要同时学习两组代码才能使用 tx。

## 最小系统模型

```text
客户端 → 应用 Login：认证、选服、签发票据
客户端 → Agent → gRPC 上行 → 目标业务 Actor
业务 Actor → gRPC 发布 → MsgQ 下行邮箱 → Agent → 客户端

内部 Actor 调用：同实例本地邮箱 / 跨实例 Core NATS
可靠任务：JetStream 持久发布 → 消费后调用 Actor
存储：生成 GD/Store → Redis / Mongo / Fusion
```

- Login 业务流程、业务身份、排行榜、定时结算、合服和 World 归属规则在应用侧。
- tx 负责通用寻址、连接、准入、邮箱、事务与资源收尾，不从消息名称推断业务权限。
- 同一进程可以组合多个 Service，共享启用的 CMod 和连接；每个 Actor 的邮箱与状态仍独立。
- Actor 不等于 GD 缓存条目，卸载 Actor 不等于删除数据库或回收全部实体缓存。

## 包结构

```text
bee/、ant/          应用启动门面与能力声明
runtime/            操作范围中的 Actor、存储、Procedure 访问
modules/            Actor、Agent、MsgQ、Discovery、事务、缓存等组件
storage/            Redis / Mongo / Fusion 及文档能力
meta/、change/      GD 元数据与字段级变更内核
protocol/           命令哈希、路由键、身份与 BON
component/、lifecycle/  依赖拓扑、准入与生命周期
log/、metrics/      日志和 Prometheus 封装
internal/           门面共用的装配、宿主和传输实现
cmd/                运维工具与固定版本协议生成入口
```

各模块的源码位置、输入输出、状态和流程见 [设计文档](docs/design.md#包结构与依赖方向)。应用不直接 import tx 的 internal 包。

## 接入步骤

1. 在项目 `gen/` 定义 GD 与统一 cmd，使用 txgen 输出 `server/gen/data`、`server/gen/cmd` 和客户端代码。
2. 项目解析配置，组装 `bee.Config` 或 `ant.Config`；服务显式声明 CMod，不由 GD 注解推导。
3. 用完整注册表声明各服务和 Actor 类型，在 Setup 绑定项目基础设施；只启动当前进程选中的服务。
4. 各服务 Init 组合 Module 与共享 Handler，必要时在 Start 中加载指定 Actor 并等待 Ready。
5. 使用显式 ctx 的生成 GD 入口和生成 Actor 调用器；不要在业务方法中创建数据库连接。

以下组合根片段中的 `data`、`config`、`services` 来自项目；完整可运行入口在两组示例的 `server/internal/app` 与 `server/cmd/server`：

```go
app, err := bee.New(config).
    GeneratedData(data.DefaultTypeRegistry(), nil).
    Services(services...).
    Build()
if err != nil { return err }

return app.Run(ctx, bee.RunOptions{
    Services:         []string{"msgq", "game", "agent", "login"},
    InstanceVersion:  buildVersion,
    InstanceEndpoint: internalEndpoint,
    RPCListen:        rpcListen,
})
```

`InstanceEndpoint` 发布内部 RPC 地址；`InstanceEndpoints` 可另外发布具名的客户端 TCP/WebSocket 等地址。InstanceID 由运行身份产生，不用 RoleID、ServerID 或 WorldID 替代。

`Requires` 自动展开依赖，但不会扩大当前服务的直接访问权；`After` 不自动启用能力。配置错误在启动前失败，启动/关闭失败保留明确错误。详见 [能力组合](docs/design.md#启动能力组合与执行上下文)。

## 选择正确的数据路径

| 场景 | 读取 | 修改与保存 |
| --- | --- | --- |
| 无共享缓存的 GD | `data.FindGD*(ctx, ...)` | Store 修改后显式 `db.Save(ctx, value)` |
| 共享缓存只读 | `data.SnapshotGD*(ctx, ...)` | 返回独立快照，不作为保存入口 |
| 共享缓存修改 | Procedure 内 `data.FindGD*(tx.Context(), ...)` | Store 修改，提交登记 WriteBack |
| 独占 World | Mongo 原生游标加载 | 网格邮箱内修改与保存，排水最终刷盘 |

`db` 是应用薄封装，不是 tx 强制要求的包名。共享缓存开关由生成配置选择，少数独占根使用 `@noEntityCache` 排除。GD、子对象和 map/slice 不能从事务逃逸后继续读写；普通查询使用安全快照。完整说明见 [执行上下文与 GD](docs/runtime-access.md)。

Procedure 使用 `txruntime.Call(ctx, p)` 执行根事务；子过程使用 `tx.Call(child)`。`CallAfterCommit` 是提交解锁后的同步后续事务，`Submit` 是独立异步队列，不是 Actor 邮箱。默认 64 执行、8192 总接纳只是资源保护配置，不能当生产吞吐指标。用法见 [同步调用](docs/procedure-calls.md)、[异步 Submit](docs/procedure-submit.md)。

## 必须区分的成功边界

- Agent 上行提交成功，不代表远端业务已经执行；取消上行确认和重放不取消下行 Ack。
- UpSeq 只在 Agent 当前连接过滤旧号；MsgQ 用客户端 Ack 裁剪下行，没有跨服务的上行序号排序。
- Call 等待业务结果；Tell 不等待执行；TellAccepted 的远端确认是 NATS 服务端到达，不是持久化。
- Procedure 成功表示内存提交及 WriteBack 登记；Fusion 成功不表示 Mongo 已完成延迟回写。
- 超时不证明操作未发生或 goroutine 已退出；不据此自动重投业务。
- 正常排水等待已接纳工作和必要持久化，失败不能伪装成成功交接。

## 开发与验证

Go 与工具链要求以 [go.mod](go.mod) 和协议生成入口为准。在 tx 根目录运行：

```sh
go test ./...
go test -race ./modules/txn ./modules/entitycache ./modules/writeback
```

外部依赖专项按相应测试的环境变量显式启用；默认跳过不算通过。tx 保留框架可靠性、并发和存储契约测试，txgen 保留生成校验，真实部署、客户端流量及滚动发布由应用入口验收。文档修改不要求重跑集群。

[实施计划](docs/IMPLEMENTATION_PLAN.md)区分当前本地证据、待做联合验收和生产部署待办。历史云端 20/50 结果只证明当时构建与负载条件，不证明当前版本或长期稳态容量。

设计与 README 维护现行方案；专题文档维护详细契约；有日期的报告保留原始条件。已完成施工步骤由 Git 历史追溯，不保留互相矛盾的多轮使用说明。
