# EntityCache 观察与管理

管理入口直接复用 `txruntime.EntityCache(ctx)` 返回的应用 Runtime。组合子服务共用该应用的实体身份，不建立另一份缓存，也不新增 CMod、管理页面或逐 GD 登记。

## 调用示例

```go
cache, err := txruntime.EntityCache(ctx)
if err != nil { return err }
stats, err := cache.Stats(ctx)
if err != nil { return err }
// Entries 包含加载/创建等过渡条目；MaximumSize 是淘汰策略预算，不是驻留硬上限。
log.Printf("缓存条目=%d，待回收=%d", stats.Entries, stats.DeferredReleases)

page, err := cache.List(ctx, entitycache.ListOptions{TypeName: "GDRole", Limit: 100})
if err != nil { return err }
for _, entry := range page.Items {
    // 元数据来自框架，不读取 GD 字段；Ref 不持有 GD 或缓存使用权。
    log.Printf("类型=%s，记录=%v，阶段=%s，使用权=%d", entry.TypeName, entry.Record, entry.State, entry.UseCount)
}
if len(page.Items) == 0 { return nil }
ref := page.Items[0].Ref
copy, err := cache.SnapshotCached(ctx, ref)
if err != nil { return err }
// copy 是独立 GD 深复制，序列化在记录锁之外进行。
encoded, err := json.Marshal(copy)
if err != nil { return err }
_ = encoded

result, err := cache.TryRelease(ctx, ref)
if err != nil { return err }
log.Printf("本次尝试回收结果=%s", result)
```

所有操作带显式 ctx。管理 SnapshotCached/TryRelease 在 Procedure 外调用，拒绝携带事务的上下文。事务业务仍使用已有 `data.FindGD*(tx.Context(), ...)` 和 `data.SnapshotGD*(tx.Context(), ...)`；后者允许复用当前事务锁、读取自己的修改。

## 统计与查询边界

- Stats 返回 Entries、DeferredReleases、MaximumSize、IdleTimeout；分别观察，不保证多个指标同时刻一致。Entries 包含过渡条目，不等于可读实体数量；DeferredReleases 是自动回收候选数，不等于 WriteBack pending。
- 不增加每次访问的命中计数、时钟采样或大小估算，不通过 GD 字段判断脏状态。UseCount 是使用权计数，不是线程或 Actor 数量。
- List 默认 100 条，上限 1000；只按 TypeName 过滤，不承诺顺序或稳定分页，Truncated 表示还有未返回的匹配项。筛选最坏 O(N)，并短时持有元数据读锁，只适合低频诊断。
- 不返回内部可变 map、Store 或业务字段；Record 沿用生成记录的完整标量身份。TypeName 由生成器填入已有私有 Descriptor，不参与缓存身份，不要求手工登记。
- EntryRef 只在当前 Runtime 有效；驻留代次与访问 generation 分离，旧引用不能作用于同 ID 重新加载的对象。不能持久化、跨进程使用或自行构造，也不能将列表视为后续操作的预留。

## 安全快照

```text
SnapshotCached
  → 管理准入 → 验证 Runtime / 完整身份 / 驻留代次
  → 只在缓存中取得使用权，不加载存储、不刷新淘汰热度
  → 等待同一记录锁 → 再检查删除可见性 → 生成 Clone 深复制
  → 释放锁与使用权 → 返回独立对象
```

不存在返回 ErrNotCached，旧代次返回 ErrStaleReference，加载/创建/删除/回收中返回 ErrEntryBusy（等待记录锁期间进入删除也可能返回既有 ErrDeletionPending）。不等待后台加载，更不会拉起 Actor 或回源 Redis/Mongo。可以看到当前缓存中已提交未落盘的内容。

每条快照独立一致，不承诺整批同一时刻一致。等待记录锁可取消；已经开始的同步复制不可强制中止，实际退出前不释放使用权。Go 导出字段无法阻止人为逃逸共享裸指针；业务仍须遵守 Procedure。大对象快照成本见 [实体访问基准](entity-access-acceptance.md)，不提供全量实体导出。

## 尝试回收

| 结果 | 含义 |
| --- | --- |
| Released | 指定驻留对象已经安全移除 |
| NotCached | 该身份当前未缓存 |
| Stale | 当前对象已经是另一驻留代次 |
| Busy | 有使用权，或处于加载、创建、删除、回收阶段 |
| Retained | 持久化检查不允许释放；不猜测更细的脏状态 |

错误通过 error 返回，发生错误仍保留对象。TryRelease 不刷盘、不删除数据库记录、不卸载 Actor，不启动后台重试。它与自动回收共用释放屏障和安全检查；回收成功后，正常业务查询仍可以重新加载。

```text
验证当前驻留对象、无使用者且阶段允许
  → 标记回收中，阻止新使用者取得旧对象
  → 释放缓存锁 → WriteBack.TryRelease / 同记录锁安全判定
  → 不允许或错误：保留对象，恢复访问
  → 允许：退休 policy / 待回收项，移除同代对象
```

不能持有缓存锁等待记录锁、存储或业务回调。pending、事务预留、脏状态均可阻止释放。释放中的新普通查询沿用原等待规则；事务内不得等待回收，以免持锁循环等待。

## 生命周期

```text
运行：管理与业务访问各自准入
  → Drain：关闭新管理快照/回收，等待已接纳管理操作实际退出
      → 已准入业务仍可访问 GD，Stats/List 仍可观察
      → 业务排空、服务保存与 WriteBack 最终排水
  → CloseManagement：拒绝包括元数据在内的全部管理请求
  → 生命周期任务与存储资源关闭
```

管理排水超时保留资源，可再次等待；ctx 取消不代表复制/检查已经退出。排水期间惰性创建的缓存继承管理关闭准入，不能重新开放。最终关闭后保留旧句柄也返回 ErrManagementClosed。独立使用 Runtime.Start 时，任务生命周期退出同样关闭管理入口并等待实际操作退出。

本接口不接管业务缓存使用期或 Actor 生命周期；World 独占 GD 不出现在 EntityCache 中。无缓存生成的 txgame 不因查询管理信息而自动纳管 GD。

本地回归和热路径对照见 [验收报告](entity-cache-management-acceptance.md)。
