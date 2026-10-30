# Procedure 调用与后续工作

本文描述同步调用契约，不改变 Actor 邮箱准入；独立异步调用另见 [Submit](procedure-submit.md)。

## 契约

- `txruntime.Call(ctx, p)`：通过当前服务 Procedure Runtime 执行根事务，纳入运行时排水计数。
- `tx.Call(p)`：当前事务内的子过程，共享锁与提交边界，保留 savepoint。
- `tx.CallAfterCommit(p)`：在当前过程内登记，根提交成功并释放锁后按登记顺序执行独立根事务；子过程回滚或根事务失败时撤销登记。后续事务仍使用原 Runtime 的 WriteBack，继承原操作取消与期限，不回投 Actor 邮箱。
- 后续调用同步完成后，外层 Call 才返回。后续失败返回可辨识的提交后错误，不能据此重投已提交的原事务。沿用回调遇错停止后续执行的契约。
- 运行时排水关闭新的根事务；已接纳调用内的后续 Procedure 继续执行。排水等待实际调用与回调结束，然后才开始最终 WriteBack；排水超时只结束等待，不取消或释放正在执行的事务。

```text
Handler → txruntime.Call → 登记一项根工作
    → 预加锁 → Process（tx.Call / tx.CallAfterCommit）
    → 提交并登记 WriteBack → 解锁
    → 后续 Procedure 独立提交/回滚 → 完成根工作

关闭外部准入 → 排空 Actor/业务任务
    → 关闭 Procedure 根准入并等待 → 最终 WriteBack → 关闭依赖
```

## 验收证据

2026-10-29 同步调用批次完成 tx、txgen、txgame/server、txgameslg/server 全量 Go 回归及后续调用、提交后异常、Runtime 与排水定向 race。覆盖独立锁与事务身份、同 Runtime 写回、后续失败保留原提交、savepoint 撤销、死锁重试不重复续接、排水超时保留工作及最终写回顺序；没有将默认跳过的外部依赖测试计为通过。

当前整合版本的 [2026-10-30 正式包滚动验收](procedure-rollout-acceptance.md)覆盖两组工程，不能将同步角色流量结果解释为异步 Submit 队列验收。独立异步契约与证据见 [Submit](procedure-submit.md)，业务检查入口见 [工程约定](engineering-conventions.md#ec-06)。
