# Procedure 异步 Submit

## 契约与配置

`txruntime.Submit(ctx, p)` 接纳一个独立异步根事务并返回 Future。没有空闲执行额度时进入本 Runtime 的 FIFO 等待队列；达到总接纳上限或进入排水时拒绝，不阻塞提交者。

| 配置 | 默认 | 语义 |
| --- | --- | --- |
| MaxConcurrent | 64 | 已取得执行额度的根任务数，含等锁和同步提交后续过程 |
| MaxPending | 8192 | 执行与排队的接纳总数 |
| Timeout | 10s | 从接纳开始计算，覆盖排队与执行的独立期限 |

零值采用默认；MaxPending 为零时取 8192 和有效 MaxConcurrent 的较大值。负值及显式接纳总数小于执行上限均在启动时拒绝。两者相等表示不排队。额度仅限制 Submit，不限制同步 Call；默认值是资源保护初值，不是压测最优值。

CModProcedure 通过 `Transaction.Module.Async` 配置，独立 Runtime 可在首次 Call/Submit 前调用 ConfigureAsync。SLG YAML 对应 `runtime.cmods.procedure.transaction.submit.maxConcurrent/maxPending/timeout`；无状态示例不为说明额外启用 CModProcedure。

## 执行与期限

```text
Submit → 非阻塞接纳与登记排水 → 创建独立期限 → 返回 Future
    ├─ 有执行额度 → Procedure
    └─ FIFO 排队 → 取得执行额度 → Procedure
           └─ 排队过期 → 不执行业务，完成为 DeadlineExceeded

Procedure → 加锁、提交/回滚与 WriteBack 登记
          → 同步 CallAfterCommit → 清理与 Future 完成 → 交给下一排队项

关闭外部准入 → 排空 Actor/业务任务
    → 关闭 Procedure 新根准入 → 等待排队与执行全部结束
    → 最终 WriteBack → 关闭依赖
```

- 排队项不单独启动工作 goroutine；用队列节点及取消回调在过期时直接移除，不必等运行任务腾出位置。取得执行额度与超时并发时，执行入口再次检查期限。
- 期限不会强停已经运行的业务；不配合取消的任务未退出时仍占执行额度并阻止排水成功。不因期限已过强制回滚已经成功返回的 Process。
- CallAfterCommit 按登记顺序同步执行，沿用原独立期限及执行额度，遇错停止后续过程；不是再次 Submit。排水不撤销已接纳任务或它的同步后续过程。
- FIFO 只定义队列出队顺序，多并发不保证业务执行或完成顺序；需要 Actor 请求顺序的业务继续在邮箱内同步 Call。

## 上下文与结果

```go
future, err := txruntime.Submit(ctx, &PRefreshRoleIndex{RoleID: roleID})
if err != nil { return err } // 未接纳；ErrAsyncFull 表示总额度已满。
// Handler 可以先返回；持有 Future 的代码按需获取结果。
err = future.Await(waitCtx)
```

业务入口只重新绑定服务 Provider 和显式 EntityCache Runtime，不继承请求操作范围、取消、期限或任意请求 context value。低层 txn.Runtime.Submit 保留调用者提供的 values，直接使用低层 Executor 时不得携带短生命周期资源。业务统一使用 txruntime.Submit，把所需身份和值放在 Procedure 字段中。

Future.Done/Await 表示事务、同步后续过程和事务资源清理结束；可重复或并发等待，已完成时优先读取结果。Await 取消只结束等待，不撤销接纳。成功表示内存提交和 WriteBack 登记，不代表已经落盘。运行失败和排队超时记录诊断；异步 panic 通过 PanicError 返回，提交后失败保留 AfterCommitError，不据此重投原事务。

禁止使用有效或已结束事务 ctx 调用 Submit；事务内嵌套用 tx.Call，提交后独立同步过程用 tx.CallAfterCommit。Procedure 不能并发复用或携带共享 GD 裸指针，完成后才读取其输出。

## 生命周期保护

正常关闭和启动失败都等待已接纳工作。Init 回滚先等待 Procedure，超时保留已初始化及当前失败服务，后续 Stop 重试；原 Tasks 的 Cancel → Stop 资源 → Wait 顺序保留。未启动 Actor 的失败清理也不能忽略 CMod 排水错误而关闭依赖。清理回调 panic 仍尝试释放其他引用，提交后异常保留已提交标识。

## 本地验收范围

核心测试覆盖执行/接纳容量分离、并发提交、FIFO 出队、排队过期释放且不执行业务、父取消、Await、同步后续过程及排水。写回测试通过实际排队任务验证提交登记、回滚、落盘失败保留 pending 与恢复后写回；组件测试验证最终写回晚于排队与运行任务结束。保留启动失败资源保护测试。

tx、txgen、txgame/server、txgameslg/server 的 `go test ./...` 均通过。txn、tests、framework 中 Submit、ProcedureDrain、AfterCommit 和初始化回滚的定向 `-race -count=3` 通过。队列与配置新增契约均先通过失败测试确认缺口，再完成实现。独立 ponytail-review 未发现需修复项，四个工程 diff 检查通过。

未显式启用外部依赖专项测试，默认跳过不计为通过；不跑云或完整滚动集群，本轮不提交。[本地短时突发验证](procedure-submit-burst.md) 已完成 27 组各三轮：当前默认按配置决策调整为 64/8192；原负载验证覆盖接纳额度 256/1024/4096，尚未验证 8192 的负载表现。结果明确 I/O 突发收益、热点锁限制及扩大队列的延时代价；不将模拟负载结果作为生产最优值或集群吞吐证明。

64/8192 的[真实业务本地验收](../../txgameslg/docs/procedure-submit-acceptance.md)使用 SLG 背包领奖、生成 GD、EntityCache 和真实 Mongo，验证有限批次成功、排水与最终写回；不代表持续过载或集群吞吐验收。

当前整合版本的[本地正式包滚动验收](procedure-rollout-acceptance.md)已通过 txgame、SLG Role 和 World 三轮 99.99% 核验；角色流量仍是同步 Call，不能作为集群滚动期间异步队列排水的证明。
