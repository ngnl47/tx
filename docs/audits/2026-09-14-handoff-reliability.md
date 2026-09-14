# 交接可靠性证据收敛

起点：tx `5f8032c`、txgame `6ab2f20`、txgameslg `93e734f`，六工程干净。本轮只修改 tx 测试与计划文档，未提交，生产实现不变。

## 已有覆盖与补充

| 边界 | 当前证据 |
| --- | --- |
| PrepareHandoff 与 Stop 并发、等待取消 | `TestPrepareHandoffCancellationKeepsCompletedFlushForStop`：阻塞写回期间 Stop 等待，调用方取消不返回交接成功；写回完成后 Stop 复用阶段，只冲刷一次、关闭一次。 |
| 写回失败重试、旧启动身份、最终释放 | `TestPrepareHandoffRetainsRPCAndRetriesPersistence`：旧身份不收尾；失败保留资源，显式重试成功，准备后 RPC 仍可用，最终 Stop 不重复冲刷。 |
| 准备期间身份失效 | `TestPrepareHandoffIdentityLossNeverConfirms`：不确认成功，走既有致命退出。 |
| 退出通知失败不否决卸载 | `TestDeactivateDrainsAndRejectsOldHandle`：退出前回调报错后仍完成退出后回调，旧句柄拒绝投递。 |
| 成功确认丢失、多槽部分晋升 | 本轮增强 `TestPrepareHandoffWithEtcdRetainsLeaseUntilStop`，复用真实 NATS/etcd 双实例与控制面 RPC；具体见下文。 |
| CAS 已提交但结果未知 | 本轮新增 `TestSlotOwnershipHandoffUnknownPromotion`，复用现有存储桩，在写入后返回超时。 |

没有为已有覆盖再建重复测试宿主，没有修改 Stop、排水、RPC 或归属生产实现。

## 真实双实例与确认丢失

两实体先经 NATS 和 gRPC 调用，确认共用同一邮箱状态。客户端拦截器在真实 PrepareHandoff RPC 成功后返回一次 Unavailable，模拟调用方未拿到确认：这是调用方可见结果的故障注入，不宣称真实网络丢包实验。

- 第一槽确认丢失：源两实体已卸载，但两槽仍保持原归属；不得自动晋升。
- 显式重试第一槽成功后，第二槽确认再丢失：第一槽保持新 owner/第二代，第二槽保持旧 owner/第一代；不得回滚第一槽或隐式推进第二槽。
- 两次失败期间源租约保持不变，源仍拒绝业务；显式续作完成后新 owner 懒加载，直到最终 Stop 才注销源。
- 确认调用恰好四次、源实体只卸载两次，检查没有隐式控制重试或重复卸载。写回不重复由已有框架测试单独证明。

## CAS 结果未知

存储桩执行晋升后返回 DeadlineExceeded；L2 返回错误与空结果，不返回伪成功。实际归属已是新 owner/第二代，写入和准备各一次。再次使用旧代次发起交接返回 ErrSlotConflict 与当前权威归属，不重复准备或写入。此项验证 L2 契约，不替代 etcd 网络故障实验，也不承诺跨槽原子交接。

## 本轮验证

真实 NATS `127.0.0.1:4222`、etcd `127.0.0.1:2379`，显式设置 `TX_ACTORTRIAL_NATS_URL` 与 `TX_ETCD_ENDPOINT`，新增集成测试未跳过。

- PrepareHandoff 专项 `-count=1 -v` 通过。
- framework、actorhost、control、discovery 四包完整 `go test ... -count=1` 通过。
- 新增 CAS 未知结果断言后，同四包完整 `go test -race ... -count=1` 通过。
- 后置 ponytail-review：Lean already. Ship. 复用现有真实夹具和存储桩，不新增生产故障注入接口。`git diff --check` 通过。

首次未提权执行因本机 etcd 不可达而超时，授权环境重跑通过。不将这次失败解释为交接行为失败。原始日志和 SHA256：`/Users/a47/workspace/tx-artifacts/handoff-evidence-5f8032c`。

本轮没有改消息热路径、性能夹具或冻结基线；未跑 benchmark、两个示例集群或客户端构建，不沿用历史结果宣称本轮验证。

## 下一项

I3 的这组交接可靠性项可以关闭，I3/I4 整体仍不直接勾完。下一项推进 I4 多世界实例池的正式示例验收：多个世界分配、加入空闲实例不迁移已有归属，以及跨世界访问的直连/Actor RPC 一致性。优先复用既有世界夹具，不引入自动扩 Pod、实时资源评分或 JetStream。
