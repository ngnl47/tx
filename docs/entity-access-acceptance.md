# GD 访问安全改造：本地验收与性能记录

日期：2026-10-29。对应 [实体访问契约](entity-access.md)。以下为当时第 1～7 批的验收记录，记录时未提交，未进行云端、万连或生产容量压测。

## 验收结论

- 共享可变根必须在有效 Procedure 中 Find；只读 Snapshot 在同一记录锁内深复制。创建/删除、事务回滚、延迟写回失败保留及排水边界通过本地回归。
- World 的 GDWorld/GDWorldBucket 排除共享缓存；四格 Monster 的初始化、tick、邮箱保存和重载已用真实 Mongo 验证。没有引入第二份完整世界数据。
- 两组 gen/run.sh 重跑后，服务端/客户端生成产物没有变化；生成包随完整 module 测试编译通过。
- 当前单实体热路径的 Find/单字段修改是微秒级，但分配并非零成本；Snapshot 随实体大小增长明显。没有旧分支 A/B 基线，不能声称本次改造相对旧版无性能回退，也不能由此推算集群吞吐或 P99。

## 测量方法

Mac Intel i9-9880H 2.30GHz，32GiB，16 逻辑线程，Darwin 24.6，Go 1.26.5 darwin/amd64。固定 `GOMAXPROCS=4 GOGC=100 GOMEMLIMIT=off`；串行调用，无并发锁竞争。其他本轮测试结束后再测，未固定 CPU 频率/亲和性，桌面后台活动仍可能影响结果。

基准位于 [应用契约基准](../../txgameslg/server/tests/entity_access_benchmark_test.go)，使用真实生成的 GDRole，背包含 0、32、512、4096 个独立 GDItem。先通过 `TestEntityBenchmarkExercisesManagedPaths` 验证缓存只加载一次、事务外 Find 拒绝、Snapshot 子对象隔离、提交后的新值可见且 WriteBack 有 pending。

| 项目 | 每次计时包含 | 不包含 |
| --- | --- | --- |
| FindProcedure | 完整 Procedure.Call、FindGDRole、使用权、动态记录锁、受管登记、退出 | 冷加载、存储 I/O、业务修改 |
| Snapshot | SnapshotGDRole、记录锁、完整业务字段深复制、释放 | 数据库查询、网络 |
| ModifyProcedure | FindProcedure 路径、Name 在两个常量间切换、Undo、提交及 WriteBack 登记 | 后台 Flush、持久化完成 |

预热和夹具构造在计时外；每次查询仍走正式生成入口。只缓存 1 个实体，MaximumSize=16，未触发 Otter 读策略阈值；这是单实体预热结果，不代表满缓存、容量淘汰、多实体工作集或并发竞争。WriteBack 未启动后台调度，重复修改合并为同一个 pending 根，不累计无界历史。Handler 十秒操作范围、Actor 邮箱和 RPC 不在计时内。

每项测 5 次，每次 1 秒。下表使用中位数，耗时范围为五次运行的最小/最大均摊值，**不是单请求分位数**；B/op 是每次操作累计分配字节，不是常驻实体大小。

| 背包项数 | 路径 | 耗时中位数 µs/op（五次范围） | B/op 中位数 | allocs/op |
| ---: | --- | ---: | ---: | ---: |
| 0 | FindProcedure | 4.825（4.756～4.904） | 3956 | 42 |
| 0 | Snapshot | 1.269（1.228～1.314） | 392 | 9 |
| 0 | ModifyProcedure | 8.470（8.403～9.093） | 8447 | 76 |
| 32 | FindProcedure | 5.294（4.888～5.490） | 3956 | 42 |
| 32 | Snapshot | 3.694（3.639～3.964） | 2354 | 44 |
| 32 | ModifyProcedure | 9.274（8.608～9.946） | 8447 | 76 |
| 512 | FindProcedure | 5.248（5.144～5.370） | 3956 | 42 |
| 512 | Snapshot | 37.717（37.083～38.415） | 31184 | 524 |
| 512 | ModifyProcedure | 9.029（8.977～9.528） | 8447 | 76 |
| 4096 | FindProcedure | 4.899（4.816～4.915） | 3955 | 42 |
| 4096 | Snapshot | 284.208（276.519～298.939） | 246561 | 4122 |
| 4096 | ModifyProcedure | 7.980（7.918～8.362） | 8443 | 76 |

[保留的原始 benchmark 输出](../../txgameslg/docs/performance/entity-access-20261029.txt)。

受管查询约 4.8～5.3µs、单字段提交约 8.0～9.3µs，中位分配约 4KB/8.4KB，不随这组未修改背包的大小明显增加。不能把改单字段结果推广到遍历修改全背包或多根事务。

Snapshot 从 32 项的 3.694µs/2354B 增至 4096 项的 284.208µs/246561B，复制成本随集合大小增长，且复制期间占用该记录锁。大对象高频整份读取会产生分配和 GC 压力；业务已在 Procedure 内时，应只提取本次响应所需字段并复制可变结果，不重复整份 Snapshot，更不能以返回裸指针避开保护。本轮只量化，不引入字段快照新 API 或其他性能重构。

## 回归矩阵

测试数为 Go JSON 中的 pass 事件（含子测试），不是业务请求完成率。完整结果和逐项跳过清单见 [机器可读记录](validation/entity-access-20261029.json)。

| 工程/范围 | 本轮结果 | 边界 |
| --- | --- | --- |
| tx `go test -count=1 ./...` | 42 个有测试包、1280 个 pass 事件 | 默认跳过 92 个外部设施专项；本批相关存储专项另行启用 |
| txgen 全包 | 3 个有测试包、567 个 pass 事件 | Luban 真正导出因未提供 .NET 8 跳过；本轮不改 Excel/配置导出 |
| txgame/gen、txgameslg/gen | 分别 1、2 个 pass 事件；两组重新生成一致 | 未手改生成文件 |
| txgame/server 全包 | 33 个有测试包、278 个 pass 事件 | 显式启用 Mongo 并发建角/预留及 Redis 排行榜 |
| txgameslg/server 全包 | 38 个有测试包、393 个 pass 事件 | 显式启用 Mongo 建角/World、Redis 排行榜、SLG 停服写回；包含正式 Agent→Role 与 World 启动 tick |
| tx 核心 race | EntityCache、txn 全包；writeback 包内无测试，其实现由 tests 的针对性 race 验证 | 不以普通功能通过代替并发证据 |
| 两组应用 race | Game 业务/DB，SLG Game/World/DB/测试包通过 | 外部设施专项未统一打开全部环境开关 |
| 真实存储专项 | Redis/Mongo/Fusion 条件创建、失败写回后回滚、删除重试不复活对象通过 | 删除链路使用真实 Redis/Mongo，故障点通过适配器注入；不停止共享数据库制造故障 |
| txgame 正式 Actor | 实际 etcd 下调用及已准入 Handler 排水通过 | 可靠任务专项未开启 |
| txclient / txclientslg | 116 / 127 项测试通过，两组构建通过 | 本轮不进行多机负载或长时间 soak |

真实存储用例只清理自己的随机记录或测试专属数据库；没有清空开发环境数据库。默认跳过的独立进程配置发布、sched、打包生命周期、JetStream 隔离任务、etcd/NATS 网络故障及 Redis 整数专项不在本轮验收范围。tx 的部分默认 skip 已由存储补跑覆盖，JSON 按每次运行保留原始状态，不把其他 skip 改成 pass。

## 复现命令

统一使用 `/Users/a47/sdk/go1.26.5/bin/go`，`GOCACHE=/private/tmp/tx-go-build`。在 tx、txgen、txgame/gen、txgame/server、txgameslg/gen、txgameslg/server 各 module 下执行：

```sh
go test -count=1 ./...
```

生成与两端：

```sh
# 分别在 txgame、txgameslg 根目录
GO_BIN=/Users/a47/sdk/go1.26.5/bin/go bash gen/run.sh
# 分别在 txclient、txclientslg
npm test
npm run build
```

本轮两组 server 全包额外设置下列对应工程变量（无关变量可省略）：

```sh
TXGAME_LOGIN_MONGO_TEST_URI=mongodb://127.0.0.1:27017
TXGAMESLG_LOGIN_MONGO_TEST_URI=mongodb://127.0.0.1:27017
TXGAME_RANK_REDIS_URI='redis://127.0.0.1:6379?db=15'
TXGAMESLG_RANK_REDIS_URI='redis://127.0.0.1:6379?db=15'
TX_MONGO_URI=mongodb://127.0.0.1:27017
TXGAMESLG_SHUTDOWN_TEST=1
```

以上变量需 export 或作为命令前缀传入；实际回归由进程环境注入。正式 Actor 集成依赖本机 etcd:2379、Redis:6379、Mongo:27017、NATS:4222。

```sh
# tx
 go test -race -count=1 ./modules/entitycache ./modules/txn ./modules/writeback
 TX_REDIS_ENDPOINT=127.0.0.1:6379 TX_MONGO_URI=mongodb://127.0.0.1:27017 \
 go test -count=1 ./tests -run '^Test(Fusion|Redis).*(Delete|Backfill|DelayedSave|PublicationFailure|Lease)'
 TX_REDIS_ENDPOINT=127.0.0.1:6379 TX_MONGO_URI=mongodb://127.0.0.1:27017 \
 go test -count=1 ./tests -run '^Test(ProcedureFirstInsertStorageIntegration|FusionProcedureRollbackPreservesPendingAcrossFlushFailure)$'
# txgameslg/server
 TX_MONGO_URI=mongodb://127.0.0.1:27017 go test -race -count=1 ./internal/db ./services/slggame/... ./services/slgworld/... ./tests
 go test ./tests -run TestEntityBenchmarkExercisesManagedPaths
 GOMAXPROCS=4 GOGC=100 GOMEMLIMIT=off go test ./tests -run '^$' -bench '^BenchmarkEntityAccess$' -benchmem -benchtime=1s -count=5
# txgame/server
 go test -race -count=1 ./internal/db ./services/game/...
 TX_ETCD_ENDPOINT=127.0.0.1:2379 go test -count=1 ./services/internal/actorcmd
```

## 审查与后续

独立变更级 ponytail-review 核对了共享字段逃逸、Procedure 使用期、WriteBack 失败保留、World 网格收尾及基准计时边界，未发现阻断缺陷。修正两组 DB 错误提示中仍建议受管对象直接 Save 的旧口径；文档明确 Snapshot 成本、存储快照与内存快照差异，以及 Go 公开字段无法阻止非法裸指针逃逸的限制。

第 1～7 批代码与本地验收已形成闭环。本记录不包含随后完成的 EntityCache 管理 API；后续实施见 [管理 API](entity-cache-management.md)，其热路径对照另行记录。不增加管理页面、全量导出或调试服务。云端吞吐和跨物理机故障域仍需另行授权验收。
