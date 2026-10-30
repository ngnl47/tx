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

## 实施与验收

1. 测试定义提交后续调用、savepoint 撤销、失败和 panic 边界及排水等待。
2. 实现根调用门面、提交后调用与 Procedure Runtime 生命周期，复用现有 Ingress。
3. 迁移两组工程已有根调用并同步文档；全量本地回归与定向 race，完成差异审查。

同步调用不承诺提高吞吐；独立 Submit 沿用相同锁、写回和排水边界，使用独立有界队列，不回投 Actor 邮箱。

## 执行记录

- 前置确认：上一批锁解耦仍未提交，保留全部现有修改。参考 xdb 的提交后任务语义，不照搬线程池和隐式线程事务上下文。
- 同步与独立异步入口分别定义，CallAfterCommit 不因 Submit 的引入改变同步执行契约。
- 运行时和应用接入已实现。新测试先确认接口缺失及 CMod 提前写回，再完成实现；已有提交后 panic 错触发回滚回调的问题已复现并修复。
- tx、txgen、txgame/server、txgameslg/server 全量 Go 回归通过；txn、framework 和相关测试中的后续调用、提交后异常、Runtime 与排水定向 race 通过。
- 覆盖独立锁与事务身份、同 Runtime 写回、后续失败保留原提交、savepoint 撤销、死锁重试不重复续接、排水超时保留工作及最终写回顺序。原回调遇错停止语义不变。
- 本批未显式启动其他外部依赖用例，默认跳过不计为通过；未做客户端、云端或完整多进程滚动回测。客户端与本批生成契约无变化。
- 独立差异审查完成，无剩余阻断项；复核正常关闭与启动失败的实际 Stop 接线。四个工程差异格式检查通过，全部变更保留在工作区。
