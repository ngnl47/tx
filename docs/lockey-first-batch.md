# Procedure 锁与对象登记分离

本批只调整本机事务锁、生成契约及应用接入，不扩展异步事务与排水。

- `Lockey` 表示锁类型、类型顺序和业务键，不持有 Store；`Procedure.Locks` 返回 `Lockeys`。
- 业务输入以 Go `LockType` 枚举集中定义顺序；值越大先获取，同类整数键按数值排序。GD 使用 `@transaction` 的 `lock` 参数引用常量，不再读取锁顺序 YAML。
- Store 登记与锁分离：取得对象后登记 undo 和 WriteBack；多个不同 GD 可以共享同一锁，脏状态仍逐对象处理。
- 已知键在 Process 前预加锁；未知键动态补锁，保留等待图检测及根事务重试。

```text
Procedure.Locks → 排序取得 Lockeys（无需对象）
    → Process → Find/Create → 复用或补锁 → 登记 Store
    → 提交/回滚每个 Store → 释放全部锁 → 完成回调
```

## 实施与验证

1. 运行时：预声明锁、共享锁多 Store、动态锁、回滚与写回独立性测试。
2. 生成器：解析 LockType 常量、校验 lock 引用、删除 YAML 入口；编译整个生成包。
3. 应用：同步两组输入、生成产物与 Procedure；已知身份使用预声明锁。
4. 文档及本地回归，必要并发测试与差异审查；不提交、不跑云压测。

## 进度

- 第一批已完成，本轮未提交。事务锁按 `Lockey.Type + Key` 区分；Store 按记录身份独立登记。同一一致性域内多个 GD 的撤销与写回不再互相覆盖。
- `txgen` 从输入 `LockType` 枚举生成 `Key` 构造入口，校验类型顺序与注解引用；已移除锁顺序 YAML 和配置入口。SLG 的已知 RoleID 在 Process 前声明锁，动态补锁保留等待图检测与根事务重试。
- txgame 不额外开启 EntityCache 或事务模式，仅同步相关测试契约；两端客户端无变更。

## 本地验收（2026-10-29）

- tx、txgen、txgame/server、txgameslg/server 的 `go test ./...` 全部通过；两组 gen module 回归通过，生成入口已重跑。
- txn、EntityCache 及相关事务/写回测试的定向 `-race` 通过。覆盖预加锁无需 Store、同锁多 Store、回滚、创建、动态死锁与重试。
- 实际启用本机 Redis/Mongo：`TestProcedureFirstInsertStorageIntegration` 的 Redis、Mongo、Fusion 三条路径通过；`TestFusionProcedureRollbackPreservesPendingAcrossFlushFailure` 通过。使用测试自有记录，没有清库或重置依赖。
- 差异审查修复两项：数字键与文本键混排的全序，以及动态补锁死锁报告缺失事务调用路径；新增回归均通过。最终无剩余阻断项，四个工程 `git diff --check` 通过。
- 锁表微基准冒烟显示单键串行取得/释放零分配；本轮没有同环境旧版对照，不能据此声称吞吐无回退。不跑云压测，也未重跑完整多进程滚动验收；默认跳过的其他外部集成用例不计入通过范围。

## 后续边界

后续另批整理根事务调用、提交后续 Procedure 与排水接纳语义。本批不新增异步事务执行器，不修改已接受工作排水规则，不扩展 EntityCache 管理接口。
