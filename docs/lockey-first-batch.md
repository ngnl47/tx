# Procedure 锁与对象登记分离

本页维护锁与对象登记的当前契约及该批验收证据。根调用、后续工作与异步执行另见 [Procedure 调用](procedure-calls.md)和 [Submit](procedure-submit.md)。

- `Lockey` 表示锁类型、类型顺序和业务键，不持有 Store；`Procedure.Locks` 返回 `Lockeys`。
- 业务输入以 Go `LockType` 枚举集中定义顺序；值越大先获取，同类整数键按数值排序。GD 使用 `@transaction` 的 `lock` 参数引用常量，不再读取锁顺序 YAML。
- Store 登记与锁分离：取得对象后登记 undo 和 WriteBack；多个不同 GD 可以共享同一锁，脏状态仍逐对象处理。
- 已知键在 Process 前预加锁；未知键动态补锁，保留等待图检测及根事务重试。

```text
Procedure.Locks → 排序取得 Lockeys（无需对象）
    → Process → Find/Create → 复用或补锁 → 登记 Store
    → 提交/回滚每个 Store → 释放全部锁 → 完成回调
```

## 本地验收（2026-10-29）

- tx、txgen、txgame/server、txgameslg/server 的 `go test ./...` 全部通过；两组 gen module 回归通过，生成入口已重跑。
- txn、EntityCache 及相关事务/写回测试的定向 `-race` 通过。覆盖预加锁无需 Store、同锁多 Store、回滚、创建、动态死锁与重试。
- 实际启用本机 Redis/Mongo：`TestProcedureFirstInsertStorageIntegration` 的 Redis、Mongo、Fusion 三条路径通过；`TestFusionProcedureRollbackPreservesPendingAcrossFlushFailure` 通过。使用测试自有记录，没有清库或重置依赖。
- 差异审查修复两项：数字键与文本键混排的全序，以及动态补锁死锁报告缺失事务调用路径；新增回归均通过。最终无剩余阻断项，四个工程 `git diff --check` 通过。
- 锁表微基准冒烟显示单键串行取得/释放零分配；本轮没有同环境旧版对照，不能据此声称吞吐无回退。不跑云压测，也未重跑完整多进程滚动验收；默认跳过的其他外部集成用例不计入通过范围。

## 接入检查

业务已知身份预声明锁，多个对象共享一致性域及动态锁边界见 [工程约定 EC-05](engineering-conventions.md#ec-05)。后续调用与异步 Submit 已独立完成，不再作为锁解耦的待办。
