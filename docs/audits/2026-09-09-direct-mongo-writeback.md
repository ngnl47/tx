# 有状态 Mongo 直接写回

Procedure 提交内存变更，WriteBackManager 在根对象锁内定期调用通用存储 Writer。Mongo 普通更新先读取持久化版本，再生成最新字段级变更，以 `_tx_version` 为条件更新并原子递增，使用 majority 与 journal 确认。成功清理变更，失败保留并由后续 Flush 重试。

数据库不记录世界槽位或实例身份；世界加载前只检查路由归属并加载一次 GD。显式创建与普通更新分开。路由及排水继续约束业务执行者，存储版本只防止已发出的迟到旧写入覆盖更新版本。

关键验证包括增量写入保留未修改字段、失败保留变更、迟到请求与确认丢失、真实 Actor 命令经 Procedure 写入 Mongo。具体故障注入与性能对照见 [Mongo 迟到写入验证](2026-09-09-mongo-late-write.md)。
