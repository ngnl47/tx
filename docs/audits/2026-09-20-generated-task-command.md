# 示例生成命令的可靠任务完成确认

接续 T5 示例接线评估。txgame/txgameslg 的命令适配器原本要求内部 Call 必须对应有响应体的 Handler；而任务消费者固定使用 Call 等待业务执行。因此，只有 error 返回值的任务 Handler 会在分派阶段被拒绝。放开分派后，空 Result 又会被正式 Actor RPC 判为非法结果，仍无法 ACK。

## 修改边界

两个独立示例使用 `task.command` 作为可靠任务命令约定：始终要求 Call 完成确认，但允许 Handler 只返回 error。成功且没有业务响应体时，由命令适配器补充空载荷 `task.completed` 结果；业务错误原样交给任务适配器。普通 RPC 的 Call/Tell 匹配、Agent 的上行确认和客户端下行行为保持不变。

业务不需要新增一个生成响应协议，也不要求将任务写成 Agent 上行。带响应体的 Handler 仍可执行，消费者不将结果自动推送客户端。这个标识属于示例命令协议，不是 tx 新增的统一消息类型或鉴权能力。未新增完成表、生产队列配置、业务奖励接口或默认消费者。

## 验证

- 两示例的新命令测试在修改前均失败，错误为 `command delivery mode does not match handler`，业务调用次数为零。修复后覆盖成功、业务错误传播、普通 Call 不放宽、任务 Tell 被拒绝及不自动发布客户端响应。
- txgame 新增 `TestQueuedGeneratedCommandRetriesUntilHandlerCompletes`：独立 JetStream 文件工作流及死信流，显式 ACK、无限重投、AckWait=300ms、MaxAckPending=1；正式 Bee/CModTaskQueue、生成 UploadManifest 命令、懒加载 room Actor。Handler 第一次返回临时错误，第二次返回 nil；两次业务完成前源任务与待确认计数仍为 1，第二次能观察到重投，成功后源流为空，死信流为空。
- 真实测试在只修复分派时再次失败，记录 `actortrial: invalid result`，证明空结果仍不符合传输契约；补充 task.completed 后带 race 通过（用例 0.46s，包 1.537s）。此前测试服务缺少显式 CModActor 声明也曾导致启动失败，已修正测试装配。
- 测试使用受控 Handler，不包含实际 GD 写入或最终存盘验收，不能据此宣称两个游戏示例已默认启用可靠消息。

txgame/server 与 txgameslg/server 的 `go test ./... -count=1` 均通过；未启用的外部依赖测试沿用明确 skip。SLG 命令契约 `-race` 单独通过（1.051s）。三工程 `git diff --check` 通过；交付前 Ponytail review 未发现需调整项，保留独立示例与关键可靠性测试。tx 本轮仅更新文档，未修改库实现，也未重跑性能基准。

复现：先准备隔离 JetStream 并确认元数据就绪，然后在 txgame/server 执行：

```sh
TX_TASK_NATS_URL='nats://127.0.0.1:<隔离端口>' \
TX_ETCD_ENDPOINT=127.0.0.1:2379 \
go test ./services/internal/actorcmd \
  -run 'TestQueuedGeneratedCommand|TestTaskCommand' -race -count=1 -v
```

未设置任一依赖地址时真实测试明确 skip；地址已设置但依赖不可用则失败。流名与 etcd 前缀随机，清理前核对 owner；失败保留现场。

本轮失败前缀 `/txgame/tests/tasks/PYZ7E32NRCVAPQC56GQ6JZYMVU`（装配缺声明）及 `/txgame/tests/tasks/ATESMTOXDGI6HDDYFYRKHY2S3R`（空结果非法）保留，对应源流/死信流分别为 `txgame_tasks_<后缀>`、`txgame_dead_<后缀>`。隔离网络 `tx-task-baseline-j3s2mbsc` 与三个容器保留并停止；配置及日志位于 `/var/folders/gq/xxck3g710bq4w3q85kq21jfm0000gn/T/tx-task-baseline-j3s2mbsc`。成功测试自己的资源已清理，未修改共享 NATS 或此前性能试验现场。

下一项评估项目可选队列配置与具体业务入口。基础契约已可接入无响应体任务，队列资源仍由部署侧预创建；不为了示例接线自动创建共享流或改变普通启动依赖。
