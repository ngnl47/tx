# Fusion 交接恢复基准

> 状态说明：本文只保留已淘汰同槽交接方案的历史性能数据。当前 Fusion 不再使用 handoff marker、
> token、固定 slot 扫描或恢复器；热写成功后由进程内延迟任务分发器去重发布，并在优雅关闭时限时排空。
> 当前行为以 `README.md` 与 `docs/design.md` 为准。

本文记录曾实施的 Fusion 同槽交接能力性能基线，供后续技术选型对照。它不是当前实现或公共 API 说明。

## 环境

- 日期：2026-08-23
- Go：1.26.5
- 系统：darwin/amd64
- CPU：Intel Core i9-9880H 2.30GHz
- Redis：本机 Docker 单节点，`127.0.0.1:6379`，DB 15

## 业务编排基准

命令：

```bash
go test ./tests -run '^$' \
  -bench '^BenchmarkFusionStrategyApplyChanges$' \
  -benchmem -count=5
```

旧 HEAD 只执行 Redis 变更投影和 delayed task 入队；当前实现额外编码稳定交接定位、调用原子 handoff writer，
并执行 revision + token 条件确认。两者都使用内存 fake，不包含网络 I/O。

| 路径 | 耗时范围 | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| 旧 SaveOnly | 1.10-1.17 us | 1624 | 8 |
| 旧 SaveAndDelayedDelete | 1.10-1.24 us | 1624 | 8 |
| 当前 SaveOnly | 1.83-2.88 us | 1785 | 12 |
| 当前 SaveAndDelayedDelete | 1.94-2.82 us | 1785 | 12 |

增加的 CPU 与分配主要来自每次保存都必须稳定编码 `Type + ID + ShardKey`。真实 Redis I/O 仍是主成本，
第一版不为此引入 `unsafe`、自研联合值编码或跨对象 marker cache。

## 固定 slot key 查找

命令：

```bash
go test ./storage/redis -run '^$' \
  -bench '^BenchmarkRedisHandoffKey$' \
  -benchmem -count=5
```

优化前每次按 tag 拼接完整交接 key，结果约为 `150-154 ns/op, 24 B/op, 1 alloc/op`。改为一次性保存
16384 个完整固定 key 后，稳态结果为 `96-98 ns/op, 0 B/op, 0 allocs/op`。完整 slot 表只初始化一次；
覆盖测试验证每个 key 都映射回对应物理 slot。

## 真实 Redis 基准

命令：

```bash
TXGAME_REDIS_URI='redis://127.0.0.1:6379?db=15' \
go test ./tests/integration -run '^$' \
  -bench '^BenchmarkGeneratedRoleRedisHandoff$' \
  -benchmem -benchtime=1s -count=5
```

该基准使用 txgen 生成的 `GDRole/GDRoleStore`。`PatchOnly` 执行一条普通 patch Lua；
`PatchAndHandoffConfirm` 执行原子 patch + marker/token Lua，再执行 revision + token 条件确认 Lua。

| 路径 | 耗时范围 | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| PatchOnly | 0.55-0.69 ms | 8640-8645 | 93 |
| PatchAndHandoffConfirm | 1.01-1.30 ms | 10643-10645 | 118 |

可靠交接闭环在本机单节点 Redis 上增加约一次往返、2.0 KiB 和 25 次分配。每次热写额外生成 128 位随机 token，
与 revision 一起防止旧生命周期确认误删新记录。它封住的是“热文档已经成功，进程在 delayed task 入队或确认前退出”
的数据丢失窗口，不能为了恢复旧延迟而退回无交接写入。

真实恢复测试以 pipeline 读取全部 16384 个固定 slot Hash，连同生成 GDRole 的任务重建和双栅栏确认约
0.17 秒。生产默认每轮只读 256 个 slot，并复用 Scheduler 轮询与全局退避参数，不在单次循环全量读取。
