# 实体缓存释放取消链复核

## 已确认问题

entitycache.Start 将 runReleaseWorker 注册到服务生命周期。淘汰任务调用 Options.TryRelease，但回调、项目写回门面和 tx writeback.Runtime.TryRelease 均不接收 context；Manager.TryRelease 使用 context.Background 调用 txn.WithLocks。

因此后台取消不能中断实体锁等待。既有 TestWriteBackManagerTryReleaseWaitsForActiveProcedure 证明“正常运行应等待活跃事务”，但没有覆盖生命周期取消，不应据此删除等待或改变事务互斥语义。

## 复现证据

在 txgameslg 的实体缓存包内运行临时 TestProbeReleaseWorkerCancellationWhileEntityLocked，使用真实生成 GDRoleStore、txn.WithLocks、writeback.Manager 和 runReleaseWorker；只主动注入一条 Otter 淘汰事件。

先持有目标实体锁，再让缓存任务进入 TryRelease，随后取消后台 context。100ms 内任务未退出，回归失败：

```text
缓存后台任务取消后仍被实体锁阻塞
```

释放实体锁后任务退出，探针完成清理。该实验不是实际网络故障、完整进程停止或永久死锁证明；它确认了注册后台任务的取消链缺口。只要持锁方迟迟不结束，等待该后台任务的生命周期收尾就可能被延迟。

探针已移出常规测试目录，保存在 `/private/tmp/tx-release-cancel.qX9EQM/cancel_probe_test.go`。正式修复时复用该场景，不将暴露缺陷的临时探针作为常规绿色用例。

## 下一步最小修复

1. 为 tx writeback.Runtime/Manager.TryRelease 增加 context，并传给现有 txn.WithLocks，不新建锁、轮询或超时配置。
2. txgameslg 的写回门面、世界写回适配及实体缓存释放回调传递同一任务 context；取消后停止领取新候选。
3. context 取消时释放结果为失败，不能移除缓存身份或写回登记；不抢占活跃 Procedure，不丢弃待刷数据。
4. 复用正常等待回归，增加取消后退出且不误释放对象的关键断言；分别回归库与有状态示例。

## 修复与验证

已按上述范围贯通 context。缓存任务在每轮领取候选前检查取消，等待中的释放回调传递同一 context；取消错误退出维护，不删除对象，也不作为异常反复记录日志。未修改 Otter，不添加停服清缓存行为，不改变 Procedure 执行与 WAL、写回、checkpoint 的原有收尾契约。

tx 复用原活跃事务等待测试：正常时保持等待，取消后返回 context.Canceled 且未释放，原 Procedure 仍可完成并保留待刷状态。txgameslg 将组合探针收敛为一个正式回归，验证缓存后台取消后退出并保留 GDRoleStore 对象。

验证：tx 和 txgameslg/server 的 `go test ./... -count=1` 通过；实体缓存与项目写回门面的 `go test -race ... -count=3` 通过。本轮没有运行真实多进程停服，不将受控锁等待实验描述为已证明实际死锁。
