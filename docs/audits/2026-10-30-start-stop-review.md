# 真实启动与关闭路径复审

范围：tx 当前工作区，结合 txgame、txgameslg 的正式进程入口、Service 装配、World 启动和可靠任务消费。按 EC-08 与工程审计标准检查。三项修复已完成；执行本地包级、race 与专用 JetStream 回归，未运行整轮集群。

## 结论更正与边界

此前将自定义 CMod.Start 提交异步 Procedure 后失败列为既有 P0，缺少当前实际调用和复现证据，现撤回。一个 Server 运行时仅一份 Procedure CMod；当前内置 CMod 和两组应用没有这种启动期 Submit 用法。不为可构造但未使用的场景增加关闭编排、接口限制或专门测试。

启动阶段的框架错误应终止本次启动，而不是恢复或维持部分服务。已取得连接、监听端口和启动任务仍需有界清理，清理不等于继续启动。正常运行后关闭须排空已接纳业务并完成必要持久化，这一边界保持不变。

本轮三个 P1 已修复，没有确认新的 P0。下面记录真实触发路径、当前处理和回归证据；不把可构造的扩展用法当作现有故障。

## 已修复：启动期致命错误未终止 Start

- 真实路径：Discovery 身份观察在服务启动前运行，SLG World 的 Service.Start 仍可能正在加载必备 World；此时身份失效进入 reportFatal。
- 原问题：Stop 等待 startDone，而启动操作未感知致命错误，随后可能开放准入并返回成功。
- 当前：[Server.Start](../../internal/framework/server.go) 使用独立的可取消启动范围；reportFatal 在启动期记录原因并取消该范围。CMod、Setup 和服务阶段检查取消，开放准入与最终成功收尾检查已登记错误。致命错误登记与成功提交共用 Server 锁。
- 边界：启动范围取消不提前取消供清理使用的根生命周期；不响应取消的回调不能被强制中断。失败清理仍使用既有限时清理与重试，不恢复启动。
- 回归：[startup_failure_test.go](../../internal/framework/startup_failure_test.go) 覆盖致命错误取消启动钩子、钩子返回 nil 仍失败、后续 CMod 不再启动、预先取消不得进入 Running。

## 已修复：RPC 和控制面后台异常未传回 Server

- 原问题：监听绑定失败会同步返回，但 Serve 的终止性错误只记录日志或延迟到 Stop 返回，Server 可能继续认为进程可用。
- 当前：[RPC Host](../../modules/rpc/host.go) 和[控制面](../../modules/control/server.go) 将非预期 Serve 退出回调给 Server.reportFatal。启动期使 Start 失败；运行期进入既有终止流程。正常取消、主动 Stop 与 grpc.ErrServerStopped 不报告故障。
- 不新增错误总线、自动重建或重连服务。Serve 最终退出与 SDK 自行处理的临时错误不是同一边界。
- 回归：两个传输包均用关闭底层 listener 确定性触发错误，并验证正常 Stop 不触发故障回调。

## 已修复：消费者静态校验晚于业务准入开放

- 真实触发：恢复 durable 的 MaxDeliver 配成正数，原先直到 openServices 后 StartRecovery 才拒绝。
- 当前：[tasksCMod](../../internal/framework/cmod_tasks.go) 读取 ConsumerInfo 后执行 Prepare / PrepareRecovery，完成 durable、显式 ACK、超时与恢复投递策略校验。准备阶段不消费；PreparedConsumer.Start 只在业务就绪后启动消费，不重复查询配置。
- 边界：已知静态错误必须在业务初始化和开放前失败；准备后 Broker 状态变化仍可能导致消费启动失败，不承诺分布式启动原子性，不新增缓冲队列。
- 回归：[task_startup_test.go](../../internal/framework/task_startup_test.go) 使用本机 JetStream 独立资源验证错误恢复策略在 CMod 准备阶段被拒绝；[prepared_test.go](../../internal/taskqueue/prepared_test.go) 验证准备不消费、开始消费不重新读取配置。既有 MaxDeliver 恢复集成测试通过。

## 当前路径中保留的合理设计

```text
启动
  -> 同步装配失败：立即返回错误，不执行后续阶段
  -> 已取得资源：有界清理，保留原始错误
  -> 全部必备阶段成功：开放业务准入

正常关闭
  -> 关闭准入 -> 排空已接纳工作 -> 最终持久化
  -> Service.Stop（下行发送器仍可完成关闭）
  -> 根任务退出 -> CMod 连接与存储释放
```

- 端口占用、同步 CMod/Service 初始化失败已沿调用链返回；正式 main 遇 Run 错误退出码为 1，未发现降级为部分服务继续运行的分支。
- 子服务初始化完成不等于业务准入开放；ServiceStarting 允许指定 World 的控制激活，不允许普通业务消息。这是实际 World 启动需求，不应把控制面可用误判为业务邮箱已开放。
- Discovery 的 Running 早于 World 的启动加载，当前用于控制面选址和启动引导；不能只看该枚举就推断普通业务已准入。是否改变其命名应另有需求，本轮不重构。
- 取消信号不证明任务退出；初始化失败的监听资源清理、任务等待和有限重试仍必要，不因撤回启动期 Submit 判断而删除。
- 既有关闭测试保留。Run 的持久化重试夹具改为先完成启动再取消；预先取消现在应直接启动失败，不能再用作正常关闭的触发方式。

## 验证与下一步

三项均有旧实现失败证据及修复后回归。tx 全量测试、txgame/txgameslg 的装配与服务包测试通过；针对性 race 连续三轮通过，并显式启用本机 JetStream 的消费者准备及 MaxDeliver 恢复测试。命令与日志见[本地验收矩阵](../local-acceptance-matrix.md#2026-10-30-启动失败定向回归)。

后续按既有验收矩阵复验正式包关闭与滚动路径；本轮不重复整轮集群，不增加启动自动恢复机制。
