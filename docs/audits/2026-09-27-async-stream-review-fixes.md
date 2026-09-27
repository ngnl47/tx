# 异步上下行流评审修复

## 范围与边界

本轮按 2026-09-27 评审实施 F1～F7；仅在 Mac 本地回归，未连接云实例、未部署、未提交。沿用异步单向投递、上行序号递增可跳号、不重放未知业务、MsgQ 下行由客户端 Ack 裁剪的共识，不恢复上行 FrameAck。

## 修复

- SLG 的 Agent Tell 不再被旧 Call 条件拒绝；业务响应和业务错误仍走 MsgQ。内部 Call/task 的结果契约保持独立。
- 共享 MailboxStream 的 Session Recv 响应自己的 Context；Open 与流失效在同一锁边界登记/结束，失效流不能接纳遗留 Session。
- 每 Session 64 条接收缓冲溢出时结束该 Session，其他 Session 继续消费。不推进丢弃内容的 Ack；原邮箱的未确认消息可在恢复时重读。服务端下行 Send 独立于 Ack/CLOSE 控制循环。
- Ack/Renew 显式携带调用期限；游标同步串行锁可以取消。关闭先清理本地 Session，再尽力发送 CLOSE；控制队列满显式返回错误。PublishStream 关闭增加 5 秒上界，期限到达后取消底层流，等待中的发送者也退出。
- 活动游标有变化时 Ack 已续期，不再额外发送 Renew。已经结束或被替换 Session 的迟到控制帧不再作为业务 error 记录；其他确认错误仍记录。
- SLG 通知验收以真实收到通知为单播成功依据；Topic 保留真实投递统计。协议字段从 gen 输入重新生成。
- 压测容量等待后再次核对截止时间，正式结果区分总完成与尾部完成，吞吐只统计窗口内完成。延迟样本保留尾部，不删长尾。Go 编排结果同步保留尾部计数。

## 收敛决定

删除旧 Actor Call 合批实现、协议和配置，删除 64-lane RecStream、专用探针及配置，删除无调用方的 AcceptedOnly 客户端入队确认分支。保留 OneWay 服务端按流顺序 Submit 的入口、Call/Future 结果语义和关键取消/恢复测试。

Ruling: 本轮保留仍被 SessionClient SPI 调用方使用的 FetchSome、StreamFetch 和同步游标适配，不为缩减行数删除既有 SPI 或把测试夹具整体改写成第二套流框架。正式默认 gRPC 与本地路由都使用 MailboxStream；无效的实验开关已移除。若以后收窄公共 SPI，应单独确定调用方迁移范围。

Ruling: 本轮没有引入新的自动重投、强一致租约或通用流状态机。旧审计中的性能数据保留为历史证据；删除代码不意味着已经证明新的吞吐收益。

## 本地验证

本地全量回归：tx、txgame/server 通过；txgameslg/server 首轮发现两处旧测试契约（入口夹具缺少 Enqueuer、集成测试等待已取消的 FrameAck），修正后全量通过。实际 SLG 业务响应与写回仍校验，不跳过集成测试。现有本机 Redis 中存在历史任务缺失 revision 的 error 日志，本轮未删除或修复这些无关数据。

客户端 txclient 101/101、txclientslg 109/109 通过，两个工程类型检查及生产构建通过。关键 Go 包 race 通过；共享流/关闭/游标同步 20 轮 race 通过。

最终新上下文审查补出三项：慢 Socket 写入未收到邮箱终止信号、服务端订阅登记未随读取者退出清理、Go 顶层 summary 丢弃尾部计数。均补回归后修复：MailboxStreamSession.Context 暴露单会话生命周期，Agent 使用 context.AfterFunc 取消连接；worker 完成通知按订阅对象核对后删除登记；summary 保留上下行尾部计数。Ack/Renew 拒绝已经失败的远程 Session，正常取消后的最终游标同步仍使用独立期限。

诊断构建还发现合成入口的旧 FrameAck 完成计数及旧断言。移除该伪计数，只保留发送量和实际下行量，不用下行条数伪装业务完成数。诊断构建最终复跑通过。

最后检查补齐 Init 成功但 Open 失败时的活动会话解绑；失败用例先复现，修复后 Agent/MsgQ 完整 race 回归、相关生命周期用例 20 轮 race 及 Agent 诊断构建均通过。

最终全量 Go 回归：tx 39 个、txgame/server 17 个、txgameslg/server 22 个有测试的包通过。关键五包 race、共享流/关闭/游标同步 20 轮 race 通过；客户端截止时间最后调整后的 21 个压测相关用例通过。五工程 git diff --check 通过。全量回归后仅补上述开流失败解绑，并针对受影响包完成回归。

日志归档：`tx-artifacts/reviews/2026-09-27-async-streams/local-fixes/`（workspace 下的相邻产物目录）。本轮结果证明本地正确性与生命周期回归稳定，不构成吞吐提升或 MMO 20/50 达标证据；四实例云端回测单独开展。

后置 ponytail-review 未发现需额外阻塞交付的复杂度问题。未引入新的工具依赖或通用流抽象。
