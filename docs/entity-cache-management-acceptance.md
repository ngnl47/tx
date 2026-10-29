# EntityCache 管理 API 本地验收

日期：2026-10-29。实现契约见 [管理 API](entity-cache-management.md)，本轮不提交、不跑云压测、不增加管理页面。

## 功能与并发

- Stats/List 不读业务字段、不触发加载；SnapshotCached 使用同一记录锁与生成 Clone，管理引用仅关联 Runtime 与驻留代次。
- TryRelease 复用自动回收屏障和 WriteBack 安全判定；使用中、pending、失败写回对象保留；旧引用不能操作同 ID 的新对象。
- 管理排水与业务准入分开；取消等待不会提前释放实际复制中的对象。现存缓存与排水中惰性创建的缓存均继承关闭准入，最终关闭拒绝旧句柄。
- 独立复核发现冷加载取消 loading 后才 policy.Set 的窗口，手动回收后可能重新持有旧 GD。新增并发测试在旧实现实际失败（记录473），修复为同一缓存锁内发布 policy 和 ready；最终核心 race、全包回归通过。此修复只改变冷加载发布。

## 验收范围

| 范围 | 结果 |
| --- | --- |
| tx 全包 | 42 个有测试包、1290 个 pass 事件；92 个外部专项跳过 |
| txgen 全包 | 3 个有测试包、567 个 pass 事件；实际 Luban 导出缺少 .NET 8 跳过 |
| txgame/server 全包 | 33 个有测试包、271 个 pass 事件；36 个外部/独立部署专项跳过 |
| txgameslg/server 全包 | 38 个有测试包、383 个 pass 事件；11 个外部专项跳过 |
| 两组 gen | 重新生成并测试通过；缓存根只新增 Descriptor.TypeName，未手改产物 |
| tx 核心 race | EntityCache、txn、framework 全包通过；writeback 包无包内测试 |
| txgame 定向 race | tests、DB、Game 业务通过，57 个 pass 事件，6 个外部专项跳过 |
| SLG 定向 race | tests、DB、Game、World 通过，84 个 pass 事件，3 个外部专项跳过；显式 TX_MONGO_URI、TXGAMESLG_SHUTDOWN_TEST 启用本机 Mongo/停服写回 |

pass 事件包括子测试，不是请求完成率。逐运行的跳过项见 [JSON记录](validation/entity-management-20261029.json)。本轮没有修改客户端协议或 TS 产物，没有重跑客户端测试；前批结果不计为本轮验证。未运行独立进程滚动发布、云端容量或长期稳态。没有清理共享数据库。

首次框架全包 race 因沙箱禁止回环监听失败，允许本地监听后原命令通过；该失败没有通过修改业务逻辑绕开。

## 热路径对照

同一 Mac Intel i9-9880H、Go1.26.5 darwin/amd64，GOMAXPROCS=4、GOGC=100、GOMEMLIMIT=off。修改前和最终回归完成后分别执行同一基准，每项1秒、5次，表中为中位数。各轮基准期间不运行本任务其他回归；未固定 CPU 频率或控制桌面后台活动，差异不是统计显著性证明。

原始数据：[改动前](../../txgameslg/docs/performance/entity-management-before-20261029.txt)、[改动后](../../txgameslg/docs/performance/entity-management-after-20261029.txt)。

| 路径 | 前 µs/op | 后 µs/op | 时间差 | B/op 前→后 | allocs/op 前→后 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Items0/FindProcedure | 4.389 | 5.520 | +25.8% | 3956 → 3956 | 42 → 42 |
| Items0/Snapshot | 1.148 | 1.356 | +18.1% | 392 → 392 | 9 → 9 |
| Items0/ModifyProcedure | 7.764 | 8.400 | +8.2% | 8447 → 8447 | 76 → 76 |
| Items32/FindProcedure | 4.436 | 4.977 | +12.2% | 3956 → 3956 | 42 → 42 |
| Items32/Snapshot | 3.451 | 3.694 | +7.0% | 2354 → 2354 | 44 → 44 |
| Items32/ModifyProcedure | 8.164 | 9.197 | +12.7% | 8447 → 8448 | 76 → 76 |
| Items512/FindProcedure | 4.854 | 4.995 | +2.9% | 3956 → 3956 | 42 → 42 |
| Items512/Snapshot | 35.783 | 34.566 | -3.4% | 31184 → 31168 | 524 → 524 |
| Items512/ModifyProcedure | 8.445 | 7.727 | -8.5% | 8447 → 8444 | 76 → 76 |
| Items4096/FindProcedure | 4.454 | 4.246 | -4.7% | 3955 → 3954 | 42 → 42 |
| Items4096/Snapshot | 266.132 | 240.814 | -9.5% | 246562 → 246495 | 4122 → 4122 |
| Items4096/ModifyProcedure | 7.390 | 6.791 | -8.1% | 8443 → 8442 | 76 → 76 |

第一轮前几项耗时中位数增加约7%～26%，后几项下降约3%～10%；allocs/op 不变，B/op 的个位数差异来自摊销测量。该波动不能直接解释为代码回退，因此保持代码不变补测一轮。没有在普通 Find/Snapshot 增加管理锁、计数器或时间采样；不能据此宣称管理 API 提升了查询性能。新驻留号仅在冷加载/创建时递增，管理计数只覆盖管理快照/回收。

同代码第二轮仍为每项1秒、5次，[原始数据](../../txgameslg/docs/performance/entity-management-after-repeat-20261029.txt)：

| 路径 | 改动前 µs/op | 改动后复测 µs/op | 时间差 |
| --- | ---: | ---: | ---: |
| Items0/FindProcedure | 4.389 | 4.399 | +0.2% |
| Items0/Snapshot | 1.148 | 1.220 | +6.3% |
| Items0/ModifyProcedure | 7.764 | 7.988 | +2.9% |
| Items32/FindProcedure | 4.436 | 4.541 | +2.4% |
| Items32/Snapshot | 3.451 | 3.593 | +4.1% |
| Items32/ModifyProcedure | 8.164 | 8.670 | +6.2% |
| Items512/FindProcedure | 4.854 | 4.680 | -3.6% |
| Items512/Snapshot | 35.783 | 33.692 | -5.8% |
| Items512/ModifyProcedure | 8.445 | 7.514 | -11.0% |
| Items4096/FindProcedure | 4.454 | 4.105 | -7.8% |
| Items4096/Snapshot | 266.132 | 242.181 | -9.0% |
| Items4096/ModifyProcedure | 7.390 | 6.825 | -7.6% |

复测差异为约 -11.0%～+6.3%，allocs/op 仍与改动前逐项相同。首轮最高25.8%的增加未稳定复现，不能把首轮所有差异认定为新增管理开销，也不将下降认定为性能优化。本机证据没有显示稳定的大幅热路径回退，但不是严格性能等价证明；需要更小差异的判定时，应使用受控硬件和交错基线复测，不能为消除这轮计时噪声改业务代码。

这不是管理接口本身的吞吐测试：List 过滤最坏 O(N)，会短时占用元数据读锁；SnapshotCached 仍承担记录锁和整份复制成本。基准仅一个热实体、无锁竞争，不覆盖容量淘汰、并发采集、冷加载、存储 I/O 或后台 Flush；不能由此推出集群吞吐没有变化。

## 复现

```sh
# tx
GOCACHE=/private/tmp/tx-go-build /Users/a47/sdk/go1.26.5/bin/go test -count=1 ./...
GOCACHE=/private/tmp/tx-go-build /Users/a47/sdk/go1.26.5/bin/go test -race -count=1 ./modules/entitycache ./modules/txn ./modules/writeback ./internal/framework
# txgen、两组 server/gen 各 module
GOCACHE=/private/tmp/tx-go-build /Users/a47/sdk/go1.26.5/bin/go test -count=1 ./...
# txgameslg/server，本机依赖与并发
GOCACHE=/private/tmp/tx-go-build TX_MONGO_URI=mongodb://127.0.0.1:27017 TXGAMESLG_SHUTDOWN_TEST=1 /Users/a47/sdk/go1.26.5/bin/go test -race -count=1 ./tests ./services/slgworld/... ./services/slggame/... ./internal/db
# txgame/server
GOCACHE=/private/tmp/tx-go-build /Users/a47/sdk/go1.26.5/bin/go test -race -count=1 ./tests ./internal/db ./services/game/...
# txgameslg/server，待其他测试退出后单独执行
GOCACHE=/private/tmp/tx-go-build GOMAXPROCS=4 GOGC=100 GOMEMLIMIT=off /Users/a47/sdk/go1.26.5/bin/go test ./tests -run '^$' -bench '^BenchmarkEntityAccess$' -benchmem -benchtime=1s -count=5
```
